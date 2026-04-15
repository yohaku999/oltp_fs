#include "execution/executor.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "execution/select_parser.h"
#include "storage/index/btreecursor.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"
#include "catalog/table.h"

class ExecutorTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "executor_test_table";
  static constexpr const char* kE2ETableName = "x";
  static constexpr const char* kWalPath = "executor_test_table.wal";
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    Table::removeBackingFilesFor(kE2ETableName);
    std::remove(kWalPath);
    wal_ = WAL::initializeNew(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
  }

  void TearDown() override {
    pool_.reset();
    wal_.reset();
    Table::removeBackingFilesFor(kTableName);
    Table::removeBackingFilesFor(kE2ETableName);
    std::remove(kWalPath);
  }

  static Table createSingleColumnTable() {
    return Table::initialize(
        kTableName,
        Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                                   Column("value", Column::Type::Varchar)}),
        std::string("id"));
  }

        static Table createE2EProjectionTable() {
          return Table::initialize(
          kE2ETableName,
          Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                     Column("b", Column::Type::Varchar)}),
          std::string("id"));
        }

        static Table createE2ESeqScanTable() {
          return Table::initialize(
          kE2ETableName,
          Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                     Column("name", Column::Type::Varchar)}),
          std::string("id"));
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
  Table table = createSingleColumnTable();
  std::vector<std::pair<int, std::string>> records = {
      {1, "value1"}, {2, "value-two"}, {10, "value-003"}};

  for (const auto& record : records) {
    executor::insert(*pool_, table,
                     TypedRow{{Column::IntegerType(record.first),
                               Column::VarcharType(record.second)}},
                     *wal_);
  }

  for (const auto& record : records) {
    std::string restored =
        singleVarcharValue(executor::read(*pool_, table, record.first));
    EXPECT_EQ(record.second, restored);
  }
}

TEST_F(ExecutorTest, InsertAndReadTypedRow) {
  Table table = createSingleColumnTable();
  TypedRow inserted{
      {Column::IntegerType(11), Column::VarcharType("typed-value")}};
  executor::insert(*pool_, table, inserted, *wal_);

  TypedRow restored = executor::read(*pool_, table, 11);
  ASSERT_EQ(restored.values.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(restored.values[1]));
  EXPECT_EQ(std::get<Column::VarcharType>(restored.values[1]), "typed-value");
}

TEST_F(ExecutorTest, ReadSelectUsesIndexedPredicatePath) {
  Table table = createSingleColumnTable();
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(11),
                             Column::VarcharType("indexed-value")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(12),
                             Column::VarcharType("other-value")}},
                   *wal_);

  SelectParser parser("SELECT value FROM executor_test_table WHERE id = 11");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "indexed-value");
}

TEST_F(ExecutorTest, ReadSelectFallsBackToSeqScanForNonIndexedPredicate) {
  Table table = createSingleColumnTable();
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(11),
                             Column::VarcharType("alpha")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(12),
                             Column::VarcharType("beta")}},
                   *wal_);

  SelectParser parser(
      "SELECT value FROM executor_test_table WHERE value = 'beta'");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "beta");
}

TEST_F(ExecutorTest, ReadSelectProjectsIndexedRowForE2ECase) {
  Table table = createE2EProjectionTable();
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(1),
                             Column::VarcharType("row_b_1")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(3),
                             Column::VarcharType("row_b_3")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(4),
                             Column::VarcharType("row_b_4")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(7),
                             Column::VarcharType("row_b_7")}},
                   *wal_);

  SelectParser parser("SELECT b FROM x WHERE id = 4");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_b_4");
}

TEST_F(ExecutorTest, ReadSelectUsesSeqScanForE2ECase) {
  Table table = createE2ESeqScanTable();
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(1),
                             Column::VarcharType("row_1")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(3),
                             Column::VarcharType("row_3")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(4),
                             Column::VarcharType("row_4")}},
                   *wal_);
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(7),
                             Column::VarcharType("row_7")}},
                   *wal_);

  SelectParser parser("SELECT name FROM x WHERE name = 'row_4'");
  std::vector<TypedRow> rows = executor::read(*pool_, parser);

  ASSERT_EQ(rows.size(), 1u);
  ASSERT_EQ(rows[0].values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(rows[0].values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(rows[0].values[0]), "row_4");
}

TEST_F(ExecutorTest, InsertDeleteThenFailToRead) {
  Table table = createSingleColumnTable();
  const int key = 99;
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(key),
                             Column::VarcharType("transient")}},
                   *wal_);

  executor::remove(*pool_, table, key, *wal_);

  // Once a record is removed, executor::read should no longer surface it.
  EXPECT_THROW({ executor::read(*pool_, table, key); }, std::runtime_error);
}

TEST_F(ExecutorTest, UpdateReplacesExistingValue) {
  Table table = createSingleColumnTable();
  const int key = 123;
  executor::insert(*pool_, table,
                   TypedRow{{Column::IntegerType(key),
                             Column::VarcharType("initial-value")}},
                   *wal_);

  executor::update(*pool_, table,
                   TypedRow{{Column::IntegerType(key),
                             Column::VarcharType("updated-value")}},
                   *wal_);

  EXPECT_EQ("updated-value",
            singleVarcharValue(executor::read(*pool_, table, key)));
}

TEST_F(ExecutorTest, UpdateNonExistingKeyThrows) {
  Table table = createSingleColumnTable();
  const int key = 777;
  EXPECT_THROW(
      {
        executor::update(*pool_, table,
                         TypedRow{{Column::IntegerType(key),
                                   Column::VarcharType("does-not-exist")}},
                         *wal_);
      },
      std::runtime_error);
}

TEST_F(ExecutorTest, InsertPageOverflow) {
  try {
    Table table = createSingleColumnTable();
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> key_dist(1, 1'000'000);
    std::uniform_int_distribution<int> len_dist(16, 96);
    std::unordered_set<int> used_keys;
    std::unordered_map<int, std::string> expected;

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
                       TypedRow{{Column::IntegerType(key),
                                 Column::VarcharType(payload)}},
                       *wal_);
      expected.emplace(key, payload);
    }

    std::cout << "Start Verifying inserted records..." << std::endl;
    // The overflow path only succeeds if every inserted record remains
    // retrievable after the heap and index span many pages.
    for (const auto& [key, value] : expected) {
      std::string restored =
          singleVarcharValue(executor::read(*pool_, table, key));
      if (restored != value) {
        std::cout << "Mismatch detected for key=" << key
                  << ". Dumping B+tree index state..." << std::endl;
        BTreeCursor::dumpTree(*pool_, table.indexFile(), std::cout);
      }
      EXPECT_EQ(value, restored) << "mismatch for key=" << key;
    }
  } catch (const std::exception& ex) {
    std::cout << "Executor InsertPageOverflow test threw exception: "
              << ex.what() << std::endl;
    std::cout << "Dumping B+tree index state after test failure..."
              << std::endl;
    Table table = Table::getTable(kTableName);
    BTreeCursor::dumpTree(*pool_, table.indexFile(), std::cout);
    throw;
  }
}

