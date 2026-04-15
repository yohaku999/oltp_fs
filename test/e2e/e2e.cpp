#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "execution/executor.h"
#include "execution/filter_operator.h"
#include "execution/heap_fetch.h"
#include "execution/index_scan.h"
#include "execution/projection_operator.h"
#include "execution/seq_scan_operator.h"
#include "execution/select_parser.h"
#include "logging.h"
#include "schema/schema.h"
#include "tuple/typed_row.h"
#include "storage/index/btreecursor.h"
#include "storage/runtime/bufferpool.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/wal/wal.h"
#include "catalog/table.h"

class E2ETest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "x";

  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<WAL> wal;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    std::remove("e2e_test.wal");
    wal = WAL::initializeNew("e2e_test.wal");
    pool = std::make_unique<BufferPool>(*wal);
  }

  void TearDown() override {
    pool.reset();
    wal.reset();
    Table::removeBackingFilesFor(kTableName);
    std::remove("e2e_test.wal");
  }
};

TEST_F(E2ETest, SelectBGreaterEqual4) {
  const std::string sql = "SELECT name FROM x where id >= 4";

  // Populate rows on both sides of the lower bound so the range scan can
  // prove its filtering behavior.
  Schema schema({Column("id", Column::Type::Integer),
                 Column("b", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema, std::string("id"));

  executor::insert(*pool, table,
                   TypedRow{{Column::IntegerType(1), Column::VarcharType("row_b_1")}},
                   *wal);
  executor::insert(*pool, table,
                   TypedRow{{Column::IntegerType(3), Column::VarcharType("row_b_3")}},
                   *wal);
  executor::insert(*pool, table,
                   TypedRow{{Column::IntegerType(4), Column::VarcharType("row_b_4")}},
                   *wal);
  executor::insert(*pool, table,
                   TypedRow{{Column::IntegerType(7), Column::VarcharType("row_b_7")}},
                   *wal);

  const int LOW_KEY = 4;
  const int HIGH_KEY = 10;

  ASSERT_TRUE(table.hasIndexForColumn("id"));
  auto scan =
      IndexScanOperator::fromKeyRange(*pool, table.indexFile(), LOW_KEY, HIGH_KEY);
  HeapFetchOperator fetcher(std::move(scan), *pool, table.heapFile(),
                            table.schema());
  fetcher.open();

  std::vector<Column::IntegerType> seen_ids;
  std::vector<std::string> seen;

  while (auto row = fetcher.next()) {
    ASSERT_EQ(row->values.size(), 2u);
    ASSERT_TRUE(std::holds_alternative<Column::IntegerType>(row->values[0]));
    ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row->values[1]));
    seen_ids.push_back(std::get<Column::IntegerType>(row->values[0]));
    seen.push_back(std::get<Column::VarcharType>(row->values[1]));
  }
  fetcher.close();

  ASSERT_EQ(seen_ids.size(), 2u);
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen_ids[0], 4);
  EXPECT_EQ(seen_ids[1], 7);
  EXPECT_EQ(seen[0], std::string("row_b_4"));
  EXPECT_EQ(seen[1], std::string("row_b_7"));
}

TEST_F(E2ETest, ChoosesSeqScanWhenPredicateHasNoIndexColumn) {
  // initialize table
  Schema schema({Column("id", Column::Type::Integer),
                 Column("name", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema, std::string("id"));

  // insert
  executor::insert(
      *pool, table,
      TypedRow{{Column::IntegerType(1), Column::VarcharType("row_1")}},
      *wal);
  executor::insert(
      *pool, table,
      TypedRow{{Column::IntegerType(3), Column::VarcharType("row_3")}},
      *wal);
  executor::insert(
      *pool, table,
      TypedRow{{Column::IntegerType(4), Column::VarcharType("row_4")}},
      *wal);
  executor::insert(
      *pool, table,
      TypedRow{{Column::IntegerType(7), Column::VarcharType("row_7")}},
      *wal);

  // parse query
  SelectParser parser("SELECT name FROM x where name = 'row_4'");
  std::string table_name = parser.extractTableName();
  std::vector<std::size_t> projection_indices =
      parser.extractProjectionIndices(table.schema());
  std::vector<ComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());
  table = Table::getTable(table_name);
  const int indexed_column_index =
      table.indexedColumnName().has_value()
          ? table.schema().getColumnIndex(table.indexedColumnName().value())
          : -1;
  bool has_index_predicate = false;
  for (const auto& predicate : predicates) {
    if (static_cast<int>(predicate.column_index) == indexed_column_index) {
      has_index_predicate = true;
      break;
    }
  }

  if (has_index_predicate) {
    auto scan = IndexScanOperator::fromKeyRange(*pool, table.indexFile(), 4, 10);
    auto fetcher = std::make_unique<HeapFetchOperator>(
        std::move(scan), *pool, table.heapFile(), table.schema());
    ProjectionOperator projection(std::move(fetcher), projection_indices);
    projection.open();

    std::vector<TypedRow> seen_rows;

    while (auto row = projection.next()) {
      seen_rows.push_back(*row);
    }
    projection.close();

    ASSERT_EQ(seen_rows.size(), 2u);
  } else {
    auto scan = std::make_unique<SeqScanOperator>(*pool, table.heapFile(),
                                                  table.schema());
    auto filter = std::make_unique<FilterOperator>(std::move(scan), predicates);
    ProjectionOperator projection(std::move(filter), projection_indices);
    projection.open();

    std::vector<TypedRow> seen_rows;
    while (auto row = projection.next()) {
      seen_rows.push_back(*row);
    }
    projection.close();

    ASSERT_EQ(seen_rows.size(), 1u);
    ASSERT_EQ(seen_rows[0].values.size(), 1u);
    EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[0].values[0]), "row_4");
  }
  
}