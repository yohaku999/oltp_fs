#include "execution/executor.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <unistd.h>

#include "catalog/table.h"
#include "execution/parsers/create_index_parser.h"
#include "execution/parsers/create_table_parser.h"
#include "execution/parsers/delete_parser.h"
#include "execution/parsers/drop_table_parser.h"
#include "execution/parsers/insert_parser.h"
#include "execution/parsers/select_parser.h"
#include "execution/parsers/update_parser.h"
#include "storage/index/btreecursor.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"

class ExecutorTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "executor_test_table";
  static constexpr const char* kJoinTableName = "executor_join_table";
  static constexpr const char* kWalPath = "executor_test_table.wal";
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<Table> table_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    Table::removeBackingFilesFor(kJoinTableName);
    std::remove(kWalPath);
    wal_ = WAL::initializeNew(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
    table_ = std::make_unique<Table>(Table::initialize(
        kTableName,
        Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                                   Column("value", Column::Type::Varchar)})));
    table_->createIndex({"id"});

    for (const auto& [key, value] : std::vector<std::pair<int, std::string>>{
             {101, "row_101"},
             {103, "row_103"},
             {104, "row_104"},
             {107, "row_107"}}) {
      executor::insert(*pool_, *table_,
                       InsertParser("INSERT INTO executor_test_table VALUES (" +
                                    std::to_string(key) + ", '" + value + "')"),
                       *wal_);
    }
  }

  void TearDown() override {
    table_.reset();
    pool_.reset();
    wal_.reset();
    Table::removeBackingFilesFor(kTableName);
    Table::removeBackingFilesFor(kJoinTableName);
    std::remove(kWalPath);
  }

  static std::string singleVarcharValue(const TypedRow& row) {
    if (row.values.size() != 2 ||
        !std::holds_alternative<Column::VarcharType>(row.values[1])) {
      throw std::runtime_error("Expected row with integer key and varchar value.");
    }
    return std::get<Column::VarcharType>(row.values[1]);
  }

  Table initializeJoinTable() {
    Table join_table = Table::initialize(
        kJoinTableName,
        Schema(std::vector<Column>{Column("code", Column::Type::Integer),
                                   Column("label", Column::Type::Varchar)}));
    join_table.createIndex({"code"});
    return join_table;
  }

  void insertPrimaryRow(int key, const std::string& value) {
    executor::insert(*pool_, *table_,
                     InsertParser("INSERT INTO executor_test_table VALUES (" +
                                  std::to_string(key) + ", '" + value + "')"),
                     *wal_);
  }

  void insertJoinRow(Table& join_table, int key, const std::string& label) {
    executor::insert(*pool_, join_table,
                     InsertParser("INSERT INTO executor_join_table VALUES (" +
                                  std::to_string(key) + ", '" + label + "')"),
                     *wal_);
  }

  static std::string uniqueTableName(const std::string& prefix) {
    static int next_id = 0;
    return prefix + "_" + std::to_string(getpid()) + "_" +
           std::to_string(next_id++);
  }
};

TEST_F(ExecutorTest, ReadSelectReturnsCartesianProductForMultipleTables) {
  Table join_table = initializeJoinTable();

  insertJoinRow(join_table, 1, "alpha");
  insertJoinRow(join_table, 2, "beta");

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT * FROM executor_test_table, executor_join_table"));

  ASSERT_EQ(rows.size(), 8u);

  ASSERT_EQ(rows[0].values.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 101);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[1]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[2]), 1);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[3]), "alpha");

  ASSERT_EQ(rows[1].values.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 101);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[1].values[1]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[2]), 2);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[1].values[3]), "beta");

  ASSERT_EQ(rows[7].values.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[7].values[0]), 107);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[7].values[1]), "row_107");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[7].values[2]), 2);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[7].values[3]), "beta");
}

TEST_F(ExecutorTest, ReadSelectAppliesWhereFiltersAcrossJoinedRows) {
  Table join_table = initializeJoinTable();

  insertJoinRow(join_table, 1, "alpha");
  insertJoinRow(join_table, 2, "beta");

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
                  "SELECT * FROM executor_test_table, executor_join_table "
                  "WHERE executor_test_table.id = 101 "
                  "AND executor_join_table.code = 2"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 101);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[1]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[2]), 2);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[3]), "beta");
}

TEST_F(ExecutorTest, ReadSelectAppliesColumnComparisonWhereOnJoinedRows) {
  Table join_table = initializeJoinTable();

  insertJoinRow(join_table, 101, "alpha");
  insertJoinRow(join_table, 999, "beta");

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
                  "SELECT * FROM executor_test_table, executor_join_table "
                  "WHERE executor_test_table.id = executor_join_table.code"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 101);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[1]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[2]), 101);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[3]), "alpha");
}

TEST_F(ExecutorTest, ReadSelectAppliesOrderByBeforeProjectionOnJoinedRows) {
  Table join_table = initializeJoinTable();

  insertJoinRow(join_table, 1, "alpha");
  insertJoinRow(join_table, 2, "beta");

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
                  "SELECT value, code FROM executor_test_table, executor_join_table "
                  "WHERE id = 101 ORDER BY code DESC"));

  ASSERT_EQ(rows.size(), 2u);
  ASSERT_EQ(rows[0].values.size(), 2u);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[1]), 2);

  ASSERT_EQ(rows[1].values.size(), 2u);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[1].values[0]), "row_101");
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[1]), 1);
}

TEST_F(ExecutorTest, ReadSelectHandlesThousandByThousandJoinInput) {
  Table join_table = initializeJoinTable();

  std::vector<int> matching_keys = {101, 103, 104, 107};
  for (int key = 1000; matching_keys.size() < 1000u; ++key) {
    matching_keys.push_back(key);
  }

  for (std::size_t index = 4; index < matching_keys.size(); ++index) {
    const int key = matching_keys[index];
    insertPrimaryRow(key, "bulk_" + std::to_string(key));
  }

  for (const int key : matching_keys) {
    insertJoinRow(join_table, key, "join_" + std::to_string(key));
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
                  "SELECT id FROM executor_test_table, executor_join_table "
                  "WHERE executor_test_table.id = executor_join_table.code "
                  "ORDER BY id DESC LIMIT 5"));

  ASSERT_EQ(rows.size(), 5u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 1995);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 1994);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[2].values[0]), 1993);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[3].values[0]), 1992);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[4].values[0]), 1991);
}

TEST_F(ExecutorTest, ReadSelectReturnsSumForIntegerColumn) {
  std::vector<TypedRow> rows =
      executor::read(*pool_, SelectParser("SELECT SUM(id) FROM executor_test_table"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 415);
}

TEST_F(ExecutorTest, ReadSelectReturnsFilteredSumForIntegerColumn) {
  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
                  "SELECT SUM(id) FROM executor_test_table WHERE id >= 104"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 211);
}

TEST_F(ExecutorTest, ReadSelectReturnsCountStar) {
  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT COUNT(*) FROM executor_test_table"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 4);
}

TEST_F(ExecutorTest, ReadSelectReturnsFilteredCountForColumn) {
  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser(
          "SELECT COUNT(id) FROM executor_test_table WHERE id >= 104"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 2);
}

TEST_F(ExecutorTest, ReadSelectCountIgnoresNullValues) {
  const std::string table_name = uniqueTableName("count_nullable_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + table_name + " ("
      "id int, "
      "note varchar)"));

  {
    Table table = Table::getTable(table_name);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " VALUES (1, 'alpha')"),
                     *wal_);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " (id) VALUES (2)"),
                     *wal_);
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT COUNT(note) FROM " + table_name));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 1);

  executor::drop_table(DropTableParser("DROP TABLE " + table_name));
  EXPECT_FALSE(Table::isPersisted(table_name));
}

TEST_F(ExecutorTest, ReadSelectReturnsCountDistinctForSingleColumn) {
  const std::string table_name = uniqueTableName("count_distinct_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + table_name + " ("
      "id int, "
      "note varchar)"));

  {
    Table table = Table::getTable(table_name);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " VALUES (1, 'alpha')"),
                     *wal_);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " VALUES (2, 'alpha')"),
                     *wal_);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " VALUES (3, 'beta')"),
                     *wal_);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " (id) VALUES (4)"),
                     *wal_);
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT COUNT(DISTINCT note) FROM " + table_name));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 2);

  executor::drop_table(DropTableParser("DROP TABLE " + table_name));
  EXPECT_FALSE(Table::isPersisted(table_name));
}

TEST_F(ExecutorTest, InsertAndGetMultipleRecords) {
  Table& table = *table_;
  std::vector<std::pair<int, std::string>> records = {
      {1, "value1"}, {2, "value-two"}, {10, "value-003"}};

  for (const auto& record : records) {
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO executor_test_table VALUES (" +
                                  std::to_string(record.first) + ", '" +
                                  record.second + "')"),
                     *wal_);
  }

  for (const auto& record : records) {
    std::vector<TypedRow> rows = executor::read(
        *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = " +
                             std::to_string(record.first)));
    ASSERT_EQ(rows.size(), 1u);
    std::string restored = singleVarcharValue(rows.front());
    EXPECT_EQ(record.second, restored);
  }
}

TEST_F(ExecutorTest, InsertAndReadTypedRow) {
  Table& table = *table_;
  executor::insert(*pool_, table,
                   InsertParser(
                       "INSERT INTO executor_test_table VALUES (11, 'typed-value')"),
                   *wal_);

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 11"));
  ASSERT_EQ(rows.size(), 1u);
  TypedRow restored = rows.front();
  ASSERT_EQ(restored.values.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(restored.values[1]));
  EXPECT_EQ(std::get<Column::VarcharType>(restored.values[1]), "typed-value");
}

TEST_F(ExecutorTest, InsertMapsExplicitColumnListToSchemaOrder) {
  Table& table = *table_;

  executor::insert(
      *pool_, table,
      InsertParser(
          "INSERT INTO executor_test_table (value, id) VALUES ('reordered', 11)"),
      *wal_);

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 11"));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 2u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 11);
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[1]), "reordered");
}

TEST_F(ExecutorTest, InsertFillsOmittedColumnsWithNull) {
  const std::string table_name = uniqueTableName("partial_insert_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + table_name + " ("
      "id int, "
      "note varchar)"));

  {
    Table table = Table::getTable(table_name);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name + " (id) VALUES (7)"),
                     *wal_);
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT id, note FROM " + table_name + " WHERE id = 7"));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 2u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 7);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(rows[0].values[1]));

  executor::drop_table(DropTableParser("DROP TABLE " + table_name));
  EXPECT_FALSE(Table::isPersisted(table_name));
}

TEST_F(ExecutorTest, ReadSelectUsesIndexedPredicatePath) {
  SelectParser parser("SELECT value FROM executor_test_table WHERE id = 104");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_104");
}

TEST_F(ExecutorTest, ReadSelectFallsBackToSeqScanForNonIndexedPredicate) {
  SelectParser parser(
      "SELECT value FROM executor_test_table WHERE value = 'row_104'");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_104");
}

TEST_F(ExecutorTest, ReadSelectProjectsIndexedRowForE2ECase) {
  SelectParser parser("SELECT value FROM executor_test_table WHERE id = 104");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_104");
}

TEST_F(ExecutorTest, ReadSelectUsesSeqScanForE2ECase) {
  SelectParser parser(
      "SELECT value FROM executor_test_table WHERE value = 'row_104'");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_104");
}

TEST_F(ExecutorTest, ReadSelectAppliesDefaultAscendingOrderBy) {
  SelectParser parser(
      "SELECT id FROM executor_test_table ORDER BY id");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 101);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 103);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[2].values[0]), 104);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[3].values[0]), 107);
}

TEST_F(ExecutorTest, ReadSelectAppliesDescendingOrderByBeforeProjection) {
  SelectParser parser(
      "SELECT id FROM executor_test_table ORDER BY value DESC");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 107);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 104);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[2].values[0]), 103);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[3].values[0]), 101);
}

TEST_F(ExecutorTest, ReadSelectAppliesExplicitAscendingOrderBy) {
  SelectParser parser(
      "SELECT id FROM executor_test_table ORDER BY id ASC");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 4u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 101);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 103);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[2].values[0]), 104);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[3].values[0]), 107);
}

TEST_F(ExecutorTest, ReadSelectAppliesLimitWithoutOrderBy) {
  SelectParser parser(
      "SELECT id FROM executor_test_table WHERE id >= 103 LIMIT 2");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 103);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 104);
}

TEST_F(ExecutorTest, ReadSelectAppliesLimitAfterOrderBy) {
  SelectParser parser(
      "SELECT id FROM executor_test_table ORDER BY id DESC LIMIT 2");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 107);
  EXPECT_EQ(std::get<Column::IntegerType>(rows[1].values[0]), 104);
}

TEST_F(ExecutorTest, InsertDeleteThenFailToRead) {
  Table& table = *table_;
  const int key = 99;
  executor::insert(*pool_, table,
                   InsertParser(
                       "INSERT INTO executor_test_table VALUES (99, 'transient')"),
                   *wal_);

  executor::remove(
      *pool_, table,
      DeleteParser("DELETE FROM executor_test_table WHERE id = " +
                   std::to_string(key)),
      *wal_);

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = " +
                           std::to_string(key)));
  EXPECT_TRUE(rows.empty());
}

TEST_F(ExecutorTest, DeleteRemovesAllRowsMatchingIndexedPredicate) {
  Table& table = *table_;

  executor::remove(*pool_, table,
                   DeleteParser(
                       "DELETE FROM executor_test_table WHERE id >= 103"),
                   *wal_);

  std::vector<TypedRow> rows101 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 101"));
  ASSERT_EQ(rows101.size(), 1u);
  EXPECT_EQ("row_101", singleVarcharValue(rows101.front()));

  EXPECT_TRUE(executor::read(
                  *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 103"))
                  .empty());
  EXPECT_TRUE(executor::read(
                  *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 104"))
                  .empty());
  EXPECT_TRUE(executor::read(
                  *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 107"))
                  .empty());
}

TEST_F(ExecutorTest, DeleteFallsBackToHeapScanForNonIndexedPredicate) {
  Table& table = *table_;

  executor::remove(*pool_, table,
                   DeleteParser(
                       "DELETE FROM executor_test_table WHERE value = 'row_104'"),
                   *wal_);

    std::vector<TypedRow> rows101 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 101"));
    ASSERT_EQ(rows101.size(), 1u);
    EXPECT_EQ("row_101", singleVarcharValue(rows101.front()));

    std::vector<TypedRow> rows103 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 103"));
    ASSERT_EQ(rows103.size(), 1u);
    EXPECT_EQ("row_103", singleVarcharValue(rows103.front()));

    EXPECT_TRUE(executor::read(
            *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 104"))
            .empty());

    std::vector<TypedRow> rows107 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 107"));
    ASSERT_EQ(rows107.size(), 1u);
    EXPECT_EQ("row_107", singleVarcharValue(rows107.front()));
}

TEST_F(ExecutorTest, UpdateReplacesExistingValue) {
  Table& table = *table_;
  const int key = 123;
  executor::insert(*pool_, table,
                   InsertParser("INSERT INTO executor_test_table VALUES (123, "
                                "'initial-value')"),
                   *wal_);

  executor::update(
      *pool_, table,
      UpdateParser("UPDATE executor_test_table SET value = 'updated-value' "
                   "WHERE id = 123"),
      *wal_);

    std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = " +
                 std::to_string(key)));
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ("updated-value", singleVarcharValue(rows.front()));
}

TEST_F(ExecutorTest, UpdateNonExistingKeyThrows) {
  Table& table = *table_;
  const int key = 777;
  EXPECT_THROW(
      {
        executor::update(
            *pool_, table,
            UpdateParser("UPDATE executor_test_table SET value = "
                         "'does-not-exist' WHERE id = " +
                         std::to_string(key)),
            *wal_);
      },
      std::runtime_error);
}

TEST_F(ExecutorTest, UpdateMatchesAllRowsFromIndexedPredicates) {
  Table& table = *table_;

  executor::update(
      *pool_, table,
      UpdateParser(
          "UPDATE executor_test_table SET value = 'updated-range' WHERE id >= 103"),
      *wal_);

    std::vector<TypedRow> rows101 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 101"));
    ASSERT_EQ(rows101.size(), 1u);
    EXPECT_EQ("row_101", singleVarcharValue(rows101.front()));

    std::vector<TypedRow> rows103 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 103"));
    ASSERT_EQ(rows103.size(), 1u);
    EXPECT_EQ("updated-range", singleVarcharValue(rows103.front()));

    std::vector<TypedRow> rows104 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 104"));
    ASSERT_EQ(rows104.size(), 1u);
    EXPECT_EQ("updated-range", singleVarcharValue(rows104.front()));

    std::vector<TypedRow> rows107 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 107"));
    ASSERT_EQ(rows107.size(), 1u);
    EXPECT_EQ("updated-range", singleVarcharValue(rows107.front()));
}

TEST_F(ExecutorTest, UpdateFallsBackToHeapScanForNonIndexedPredicate) {
  Table& table = *table_;

  executor::update(
      *pool_, table,
      UpdateParser(
          "UPDATE executor_test_table SET value = 'updated-non-index' WHERE value = 'row_104'"),
      *wal_);

    std::vector<TypedRow> rows101 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 101"));
    ASSERT_EQ(rows101.size(), 1u);
    EXPECT_EQ("row_101", singleVarcharValue(rows101.front()));

    std::vector<TypedRow> rows103 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 103"));
    ASSERT_EQ(rows103.size(), 1u);
    EXPECT_EQ("row_103", singleVarcharValue(rows103.front()));

    std::vector<TypedRow> rows104 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 104"));
    ASSERT_EQ(rows104.size(), 1u);
    EXPECT_EQ("updated-non-index", singleVarcharValue(rows104.front()));

    std::vector<TypedRow> rows107 = executor::read(
      *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = 107"));
    ASSERT_EQ(rows107.size(), 1u);
    EXPECT_EQ("row_107", singleVarcharValue(rows107.front()));
}

TEST_F(ExecutorTest, UpdateAppliesSelfPlusLiteralExpression) {
  const std::string table_name = uniqueTableName("update_expression_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + table_name + " ("
      "id int NOT NULL, "
      "amount decimal(12, 2) NOT NULL, "
      "PRIMARY KEY (id))"));

  {
    Table table = Table::getTable(table_name);
    executor::insert(*pool_, table,
                     InsertParser("INSERT INTO " + table_name +
                                  " VALUES (1, 10.5)"),
                     *wal_);

    executor::update(*pool_, table,
                     UpdateParser("UPDATE " + table_name +
                                  " SET amount = amount + 2.5 WHERE id = 1"),
                     *wal_);
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT amount FROM " + table_name +
                           " WHERE id = 1"));
  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::DoubleType>(rows[0].values[0]));
  EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(rows[0].values[0]), 13.0);

  executor::drop_table(DropTableParser("DROP TABLE " + table_name));
  EXPECT_FALSE(Table::isPersisted(table_name));
}

TEST_F(ExecutorTest, InsertPageOverflow) {
  try {
    Table& table = *table_;
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> key_dist(1, 1'000'000);
    std::uniform_int_distribution<int> len_dist(16, 96);
    std::unordered_set<int> used_keys;
    std::unordered_map<int, std::string> expected;

    for (int key : {101, 103, 104, 107}) {
      used_keys.insert(key);
    }

    const size_t max_attempts = 1000;

    for (size_t attempt = 0; attempt < max_attempts; ++attempt) {
      int key;
      do {
        key = key_dist(rng);
      } while (!used_keys.insert(key).second);
      std::cout << "Attempt " << attempt << ": Inserting key=" << key
                << std::endl;

      const int payload_len = len_dist(rng);
      std::string payload(payload_len, '\0');
      for (char& ch : payload) {
        ch = static_cast<char>('a' + (rng() % 26));
      }
      executor::insert(*pool_, table,
                       InsertParser("INSERT INTO executor_test_table VALUES (" +
                                    std::to_string(key) + ", '" + payload + "')"),
                       *wal_);
      expected.emplace(key, payload);
    }

    std::cout << "Start Verifying inserted records..." << std::endl;
    for (const auto& [key, value] : expected) {
      std::vector<TypedRow> rows = executor::read(
          *pool_, SelectParser("SELECT id, value FROM executor_test_table WHERE id = " +
                               std::to_string(key)));
      ASSERT_EQ(rows.size(), 1u) << "missing row for key=" << key;
      std::string restored = singleVarcharValue(rows.front());
      if (restored != value) {
        std::cout << "Mismatch detected for key=" << key
                  << ". Dumping B+tree index state..." << std::endl;
        BTreeCursor::dumpTree(*pool_, table.indexFile()->get(), std::cout);
      }
      EXPECT_EQ(value, restored) << "mismatch for key=" << key;
    }
  } catch (const std::exception& ex) {
    std::cout << "Executor InsertPageOverflow test threw exception: "
              << ex.what() << std::endl;
    std::cout << "Dumping B+tree index state after test failure..."
              << std::endl;
    Table table = Table::getTable(kTableName);
    BTreeCursor::dumpTree(*pool_, table.indexFile()->get(), std::cout);
    throw;
  }
}

TEST_F(ExecutorTest, CreateAndDropTable) {
  const std::string new_table_name = "new_table";
  Table::removeBackingFilesFor(new_table_name);
  CreateTableParser parser("CREATE TABLE new_table (id INTEGER, name VARCHAR)");
  executor::create_table(parser);
  executor::create_index(
      CreateIndexParser("CREATE INDEX idx_new_table_id ON new_table (id)"));

  // Brackets to limit the scope of reopened table and ensure file handles are closed.
  {
    Table new_table = Table::getTable(new_table_name);
    ASSERT_EQ(new_table.name(), new_table_name);
    ASSERT_EQ(new_table.schema().columns().size(), 2u);
    EXPECT_EQ(new_table.schema().columns()[0].getName(), "id");
    EXPECT_EQ(new_table.schema().columns()[0].getType(), Column::Type::Integer);
    EXPECT_EQ(new_table.schema().columns()[1].getName(), "name");
    EXPECT_EQ(new_table.schema().columns()[1].getType(), Column::Type::Varchar);
    EXPECT_EQ(new_table.indexedColumnNames(),
          (std::vector<std::string>{"id"}));
  }

  executor::drop_table(DropTableParser("DROP TABLE new_table"));
  EXPECT_FALSE(Table::isPersisted(new_table_name));
}

TEST_F(ExecutorTest, CreateTableBuildsIndexFromSingleColumnPrimaryKey) {
  const std::string new_table_name = uniqueTableName("primary_key_index_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + new_table_name + " ("
      "id int NOT NULL, "
      "name varchar(10) NOT NULL, "
      "PRIMARY KEY (id))"));

  {
    Table new_table = Table::getTable(new_table_name);
    EXPECT_EQ(new_table.indexedColumnNames(),
          (std::vector<std::string>{"id"}));
    ASSERT_TRUE(new_table.indexFile().has_value());
  }

  executor::drop_table(DropTableParser("DROP TABLE " + new_table_name));
  EXPECT_FALSE(Table::isPersisted(new_table_name));
}

TEST_F(ExecutorTest, CreateTableRejectsDuplicateCompositePrimaryKeyRows) {
  const std::string new_table_name = uniqueTableName("composite_primary_key_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + new_table_name + " ("
      "warehouse_id int NOT NULL, "
      "district_id int NOT NULL, "
      "customer_id int NOT NULL, "
      "balance decimal(12, 2) NOT NULL, "
      "PRIMARY KEY (warehouse_id, district_id, customer_id))"));

    {
    Table table = Table::getTable(new_table_name);
    ASSERT_TRUE(table.indexFile().has_value());
    EXPECT_EQ(table.indexedColumnNames(),
          (std::vector<std::string>{"warehouse_id", "district_id",
                      "customer_id"}));
    executor::insert(*pool_, table,
             InsertParser(
               "INSERT INTO " + new_table_name +
               " VALUES (1, 2, 3, 10.0)"),
             *wal_);

    EXPECT_THROW(
      executor::insert(*pool_, table,
               InsertParser(
                 "INSERT INTO " + new_table_name +
                 " VALUES (1, 2, 3, 20.0)"),
               *wal_),
      std::runtime_error);
    }

  executor::drop_table(DropTableParser("DROP TABLE " + new_table_name));
  EXPECT_FALSE(Table::isPersisted(new_table_name));
}

TEST_F(ExecutorTest, ReadSelectUsesCompositePrimaryKeyPath) {
  const std::string new_table_name = uniqueTableName("composite_primary_key_read_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + new_table_name + " ("
      "warehouse_id int NOT NULL, "
      "district_id int NOT NULL, "
      "customer_id int NOT NULL, "
      "name varchar(16) NOT NULL, "
      "PRIMARY KEY (warehouse_id, district_id, customer_id))"));

  {
    Table table = Table::getTable(new_table_name);
    executor::insert(*pool_, table,
                     InsertParser(
                         "INSERT INTO " + new_table_name +
                         " VALUES (1, 2, 3, 'target')"),
                     *wal_);
    executor::insert(*pool_, table,
                     InsertParser(
                         "INSERT INTO " + new_table_name +
                         " VALUES (1, 2, 4, 'other')"),
                     *wal_);
  }

  std::vector<TypedRow> rows = executor::read(
      *pool_, SelectParser("SELECT name FROM " + new_table_name +
                           " WHERE warehouse_id = 1"
                           " AND district_id = 2"
                           " AND customer_id = 3"));

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "target");

  executor::drop_table(DropTableParser("DROP TABLE " + new_table_name));
  EXPECT_FALSE(Table::isPersisted(new_table_name));
}

TEST_F(ExecutorTest, ReadSelectRoundTripsTraceLikeWarehouseRow) {
  const std::string new_table_name = uniqueTableName("warehouse_read_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + new_table_name + " ("
      "w_id int NOT NULL, "
      "w_ytd decimal(12, 2) NOT NULL, "
      "w_tax decimal(4, 4) NOT NULL, "
      "w_name varchar(10) NOT NULL, "
      "w_state char(2) NOT NULL, "
      "w_zip char(9) NOT NULL, "
      "PRIMARY KEY (w_id))"));

      {
      Table warehouse = Table::getTable(new_table_name);
      ASSERT_EQ(warehouse.schema().columns().size(), 6u);
      EXPECT_EQ(warehouse.schema().columns()[0].getType(), Column::Type::Integer);
      EXPECT_EQ(warehouse.schema().columns()[1].getType(), Column::Type::Double);
      EXPECT_EQ(warehouse.schema().columns()[2].getType(), Column::Type::Double);
      EXPECT_EQ(warehouse.schema().columns()[3].getType(), Column::Type::Varchar);
      EXPECT_EQ(warehouse.schema().columns()[4].getType(), Column::Type::Varchar);
      EXPECT_EQ(warehouse.schema().columns()[5].getType(), Column::Type::Varchar);

      executor::insert(*pool_, warehouse,
               InsertParser(
                 "INSERT INTO " + new_table_name + " VALUES "
                 "(1, 300000.0, 0.1817, 'eiyjz', 'YX', '123456789')"),
               *wal_);

      std::vector<TypedRow> rows = executor::read(
        *pool_, SelectParser(
              "SELECT * FROM " + new_table_name + " WHERE w_id = 1"));

      ASSERT_EQ(rows.size(), 1u);
      ASSERT_EQ(rows[0].values.size(), 6u);
      EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 1);
      EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(rows[0].values[1]), 300000.0);
      EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(rows[0].values[2]), 0.1817);
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[3]), "eiyjz");
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[4]), "YX");
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[5]), "123456789");
      }

  EXPECT_TRUE(Table::isPersisted(new_table_name));
}

TEST_F(ExecutorTest, ReadSelectRoundTripsTraceLikeCustomerRow) {
  const std::string new_table_name = uniqueTableName("customer_read_test");

  executor::create_table(CreateTableParser(
      "CREATE TABLE " + new_table_name + " ("
      "c_w_id int NOT NULL, "
      "c_d_id int NOT NULL, "
      "c_id int NOT NULL, "
      "c_balance decimal(12, 2) NOT NULL, "
      "c_ytd_payment float NOT NULL, "
      "c_since timestamp NOT NULL, "
      "c_first varchar(16) NOT NULL, "
      "c_middle char(2) NOT NULL, "
      "PRIMARY KEY (c_w_id, c_d_id, c_id))"));

      {
      Table customer = Table::getTable(new_table_name);
      ASSERT_EQ(customer.schema().columns().size(), 8u);
      EXPECT_EQ(customer.schema().columns()[0].getType(), Column::Type::Integer);
      EXPECT_EQ(customer.schema().columns()[1].getType(), Column::Type::Integer);
      EXPECT_EQ(customer.schema().columns()[2].getType(), Column::Type::Integer);
      EXPECT_EQ(customer.schema().columns()[3].getType(), Column::Type::Double);
      EXPECT_EQ(customer.schema().columns()[4].getType(), Column::Type::Double);
      EXPECT_EQ(customer.schema().columns()[5].getType(), Column::Type::Varchar);
      EXPECT_EQ(customer.schema().columns()[6].getType(), Column::Type::Varchar);
      EXPECT_EQ(customer.schema().columns()[7].getType(), Column::Type::Varchar);

      executor::insert(*pool_, customer,
               InsertParser(
                 "INSERT INTO " + new_table_name + " VALUES "
                 "(1, 1, 1, -10.0, 10.0, '2026-04-18 04:43:47.487', "
                 "'xzgptnvhrvng', 'OE')"),
               *wal_);

      std::vector<TypedRow> rows = executor::read(
        *pool_, SelectParser(
              "SELECT * FROM " + new_table_name + " "
              "WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1"));

      ASSERT_EQ(rows.size(), 1u);
      ASSERT_EQ(rows[0].values.size(), 8u);
      EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[0]), 1);
      EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[1]), 1);
      EXPECT_EQ(std::get<Column::IntegerType>(rows[0].values[2]), 1);
      EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(rows[0].values[3]), -10.0);
      EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(rows[0].values[4]), 10.0);
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[5]),
            "2026-04-18 04:43:47.487");
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[6]),
            "xzgptnvhrvng");
      EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[7]), "OE");
      }

  EXPECT_TRUE(Table::isPersisted(new_table_name));
}