#include "execution/operators/heap_fetch_operator.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "catalog/table.h"
#include "execution/binder.h"
#include "execution/comparison_predicate.h"
#include "execution/executor.h"
#include "execution/parsers/insert_parser.h"
#include "storage/buffer/bufferpool.h"
#include "storage/disk/file.h"
#include "storage/wal/wal.h"
#include "stub_rid_operator.h"

class HeapFetchOperatorTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "heap_fetch_test_table";
  static constexpr const char* kWalPath = "heap_fetch_test_table.wal";

  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<Table> table_;
  std::vector<RID> inserted_rids_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
    wal_ = WAL::initializeNew(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
    table_ = std::make_unique<Table>(Table::initialize(
        kTableName, Schema(std::vector<Column>{
                        Column("id", Column::Type::Integer),
                        Column("value", Column::Type::Varchar),
                        Column("quantity", Column::Type::Integer)})));

    // Insert test data
    // id=1, value="a", quantity=5
    // id=2, value="b", quantity=10
    // id=3, value="c", quantity=15
    // id=4, value="d", quantity=20
    for (int i = 1; i <= 4; ++i) {
      InsertParser parser("INSERT INTO " + std::string(kTableName) +
                          " VALUES (" + std::to_string(i) + ", '" +
                          char('a' + i - 1) + "', " + std::to_string(i * 5) +
                          ")");
      executor::insert(*pool_, *table_, parser, *wal_);
    }
    inserted_rids_ = table_->heapFile().collectRids(*pool_);
  }

  void TearDown() override {
    table_.reset();
    pool_.reset();
    wal_.reset();
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
  }
};

TEST_F(HeapFetchOperatorTest, NoPredicates) {
  // Without predicates, should return all rows
  auto stub = std::make_unique<StubRidOperator>(inserted_rids_);
  HeapFetchOperator op(std::move(stub), *pool_, table_->heapFile(),
                       table_->schema(),
                       std::vector<BoundComparisonPredicate>());

  op.open();
  std::vector<int> values;
  while (auto row = op.next()) {
    values.push_back(std::get<Column::IntegerType>(row->values[0]));
  }
  op.close();

  ASSERT_EQ(values.size(), 4);
  EXPECT_EQ(values[0], 1);
  EXPECT_EQ(values[1], 2);
  EXPECT_EQ(values[2], 3);
  EXPECT_EQ(values[3], 4);
}

TEST_F(HeapFetchOperatorTest, SinglePredicate_EqualityMatch) {
  // Filter for id = 2
  BoundComparisonPredicate pred{Op::Eq, BoundColumnRef{0, 0},
                                FieldValue{Column::IntegerType(2)}};
  auto stub = std::make_unique<StubRidOperator>(inserted_rids_);
  HeapFetchOperator op(std::move(stub), *pool_, table_->heapFile(),
                       table_->schema(),
                       std::vector<BoundComparisonPredicate>{pred});

  op.open();
  std::vector<int> values;
  while (auto row = op.next()) {
    values.push_back(std::get<Column::IntegerType>(row->values[0]));
  }
  op.close();

  ASSERT_EQ(values.size(), 1);
  EXPECT_EQ(values[0], 2);
}

TEST_F(HeapFetchOperatorTest, SinglePredicate_RangeFilter) {
  // Filter for quantity >= 12 (should match id=3 and id=4, which have qty 15
  // and 20)
  BoundComparisonPredicate pred{Op::Ge, BoundColumnRef{0, 2},
                                FieldValue{Column::IntegerType(12)}};
  auto stub = std::make_unique<StubRidOperator>(inserted_rids_);
  HeapFetchOperator op(std::move(stub), *pool_, table_->heapFile(),
                       table_->schema(),
                       std::vector<BoundComparisonPredicate>{pred});

  op.open();
  std::vector<int> values;
  while (auto row = op.next()) {
    values.push_back(std::get<Column::IntegerType>(row->values[0]));
  }
  op.close();

  ASSERT_EQ(values.size(), 2);
  EXPECT_EQ(values[0], 3);
  EXPECT_EQ(values[1], 4);
}

TEST_F(HeapFetchOperatorTest, MultiplePredicate) {
  // Filter for id > 1 AND quantity < 18
  // Should match id=2 (qty=10) and id=3 (qty=15)
  BoundComparisonPredicate pred1{Op::Gt, BoundColumnRef{0, 0},
                                 FieldValue{Column::IntegerType(1)}};
  BoundComparisonPredicate pred2{Op::Lt, BoundColumnRef{0, 2},
                                 FieldValue{Column::IntegerType(18)}};
  auto stub = std::make_unique<StubRidOperator>(inserted_rids_);
  HeapFetchOperator op(std::move(stub), *pool_, table_->heapFile(),
                       table_->schema(),
                       std::vector<BoundComparisonPredicate>{pred1, pred2});

  op.open();
  std::vector<int> values;
  while (auto row = op.next()) {
    values.push_back(std::get<Column::IntegerType>(row->values[0]));
  }
  op.close();

  ASSERT_EQ(values.size(), 2);
  EXPECT_EQ(values[0], 2);
  EXPECT_EQ(values[1], 3);
}

TEST_F(HeapFetchOperatorTest, PredicateMatchesNone) {
  // Filter for id > 100 (no matches)
  BoundComparisonPredicate pred{Op::Gt, BoundColumnRef{0, 0},
                                FieldValue{Column::IntegerType(100)}};
  auto stub = std::make_unique<StubRidOperator>(inserted_rids_);
  HeapFetchOperator op(std::move(stub), *pool_, table_->heapFile(),
                       table_->schema(),
                       std::vector<BoundComparisonPredicate>{pred});

  op.open();
  std::vector<int> values;
  while (auto row = op.next()) {
    values.push_back(std::get<Column::IntegerType>(row->values[0]));
  }
  op.close();

  EXPECT_EQ(values.size(), 0);
}
