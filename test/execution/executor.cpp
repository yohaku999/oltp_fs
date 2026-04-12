#include "execution/executor.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "storage/index/btreecursor.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"
#include "catalog/table.h"

class ExecutorTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "executor_test_table";
  static constexpr const char* kWalPath = "executor_test_table.wal";
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
    wal_ = std::make_unique<WAL>(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
  }

  void TearDown() override {
    pool_.reset();
    wal_.reset();
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
  }

  static Table createSingleColumnTable() {
    return Table::initialize(
        kTableName, Schema(kTableName, std::vector<Column>{Column(
                                           "value", Column::Type::Varchar)}));
  }

  static std::string singleVarcharValue(const TypedRow& row) {
    if (row.values.size() != 1 ||
        !std::holds_alternative<Column::VarcharType>(row.values[0])) {
      throw std::runtime_error("Expected single varchar row.");
    }
    return std::get<Column::VarcharType>(row.values[0]);
  }

};

TEST_F(ExecutorTest, InsertAndGetMultipleRecords) {
  Table table = createSingleColumnTable();
  std::vector<std::pair<int, std::string>> records = {
      {1, "value1"}, {2, "value-two"}, {10, "value-003"}};

  for (const auto& record : records) {
    executor::insert(*pool_, table, record.first,
                     TypedRow{{Column::VarcharType(record.second)}},
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
  TypedRow inserted{{Column::VarcharType("typed-value")}};
  executor::insert(*pool_, table, 11, inserted, *wal_);

  TypedRow restored = executor::read(*pool_, table, 11);
  ASSERT_EQ(restored.values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(restored.values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(restored.values[0]), "typed-value");
}

TEST_F(ExecutorTest, InsertDeleteThenFailToRead) {
  Table table = createSingleColumnTable();
  const int key = 99;
  executor::insert(*pool_, table, key,
                   TypedRow{{Column::VarcharType("transient")}}, *wal_);

  executor::remove(*pool_, table, key, *wal_);

  // Once a record is removed, executor::read should no longer surface it.
  EXPECT_THROW({ executor::read(*pool_, table, key); }, std::runtime_error);
}

TEST_F(ExecutorTest, UpdateReplacesExistingValue) {
  Table table = createSingleColumnTable();
  const int key = 123;
  executor::insert(*pool_, table, key,
                   TypedRow{{Column::VarcharType("initial-value")}}, *wal_);

  executor::update(*pool_, table, key,
                   TypedRow{{Column::VarcharType("updated-value")}}, *wal_);

  EXPECT_EQ("updated-value",
            singleVarcharValue(executor::read(*pool_, table, key)));
}

TEST_F(ExecutorTest, UpdateNonExistingKeyThrows) {
  Table table = createSingleColumnTable();
  const int key = 777;
  EXPECT_THROW(
      {
        executor::update(*pool_, table, key,
                         TypedRow{{Column::VarcharType("does-not-exist")}},
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
      executor::insert(*pool_, table, key,
                       TypedRow{{Column::VarcharType(payload)}}, *wal_);
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

