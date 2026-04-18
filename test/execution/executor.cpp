#include "execution/executor.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/table.h"
#include "execution/create_index_parser.h"
#include "execution/create_table_parser.h"
#include "execution/drop_table_parser.h"
#include "execution/insert_parser.h"
#include "execution/select_parser.h"
#include "execution/update_parser.h"
#include "storage/index/btreecursor.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"

class ExecutorTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "executor_test_table";
  static constexpr const char* kWalPath = "executor_test_table.wal";
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<Table> table_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
    wal_ = WAL::initializeNew(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
    table_ = std::make_unique<Table>(Table::initialize(
        kTableName,
        Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                                   Column("value", Column::Type::Varchar)})));
    table_->createIndex("id");

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
    std::remove(kWalPath);
  }

  static std::string singleVarcharValue(const TypedRow& row) {
    if (row.values.size() != 2 ||
        !std::holds_alternative<Column::VarcharType>(row.values[1])) {
      throw std::runtime_error("Expected row with integer key and varchar value.");
    }
    return std::get<Column::VarcharType>(row.values[1]);
  }
};

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
    std::string restored =
        singleVarcharValue(executor::read(*pool_, table, record.first));
    EXPECT_EQ(record.second, restored);
  }
}

TEST_F(ExecutorTest, InsertAndReadTypedRow) {
  Table& table = *table_;
  executor::insert(*pool_, table,
                   InsertParser(
                       "INSERT INTO executor_test_table VALUES (11, 'typed-value')"),
                   *wal_);

  TypedRow restored = executor::read(*pool_, table, 11);
  ASSERT_EQ(restored.values.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(restored.values[1]));
  EXPECT_EQ(std::get<Column::VarcharType>(restored.values[1]), "typed-value");
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

TEST_F(ExecutorTest, InsertDeleteThenFailToRead) {
  Table& table = *table_;
  const int key = 99;
  executor::insert(*pool_, table,
                   InsertParser(
                       "INSERT INTO executor_test_table VALUES (99, 'transient')"),
                   *wal_);

  executor::remove(*pool_, table, key, *wal_);

  EXPECT_THROW({ executor::read(*pool_, table, key); }, std::runtime_error);
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

  EXPECT_EQ("updated-value",
            singleVarcharValue(executor::read(*pool_, table, key)));
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
      std::string restored =
          singleVarcharValue(executor::read(*pool_, table, key));
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

  Table new_table = Table::getTable(new_table_name);
  ASSERT_EQ(new_table.name(), new_table_name);
  ASSERT_EQ(new_table.schema().columns().size(), 2u);
  EXPECT_EQ(new_table.schema().columns()[0].getName(), "id");
  EXPECT_EQ(new_table.schema().columns()[0].getType(), Column::Type::Integer);
  EXPECT_EQ(new_table.schema().columns()[1].getName(), "name");
  EXPECT_EQ(new_table.schema().columns()[1].getType(), Column::Type::Varchar);
  ASSERT_TRUE(new_table.indexedColumnName().has_value());
  EXPECT_EQ(new_table.indexedColumnName().value(), "id");
  executor::drop_table(DropTableParser("DROP TABLE new_table"));
  EXPECT_FALSE(Table::isPersisted(new_table_name));
}