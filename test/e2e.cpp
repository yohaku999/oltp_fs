#include <gtest/gtest.h>

#include <filesystem>
#include <string>

extern "C" {
#include <pg_query.h>
}

#include "executor/executor.h"
#include "executor/heap_fetch.h"
#include "executor/index_scan.h"
#include "logging.h"
#include "schema/schema.h"
#include "schema/typed_row.h"
#include "storage/btreecursor.h"
#include "storage/bufferpool.h"
#include "storage/page.h"
#include "storage/record_cell.h"
#include "storage/record_serializer.h"
#include "storage/wal/wal.h"
#include "table/table.h"

class E2ETest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "x";

  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<WAL> wal;

  PgQueryParseResult result;

  void SetUp() override {
    Table::removeFilesFor(kTableName);
    std::remove("e2e_test.wal");
    wal = std::make_unique<WAL>("e2e_test.wal");
    pool = std::make_unique<BufferPool>(*wal);
  }

  void TearDown() override {
    pool.reset();
    wal.reset();
    Table::removeFilesFor(kTableName);
    std::remove("e2e_test.wal");
    pg_query_free_parse_result(result);
  }
};

TEST_F(E2ETest, SelectBGreaterEqual4) {
  const std::string sql = "SELECT * FROM x where b >= 4";

  result = pg_query_parse(sql.c_str());
  ASSERT_EQ(result.error, nullptr)
      << "parse error: " << (result.error ? result.error->message : "");

  // We don't assert on the exact AST JSON here, but we do ensure it's
  // non-empty.
  ASSERT_NE(result.parse_tree, nullptr);
  ASSERT_GT(std::strlen(result.parse_tree), 0u);

  // Insert a few rows with different b values
  Column col_b("b", Column::Type::Varchar);
  Schema schema("x", {col_b});
  Table table = Table::initialize(kTableName, schema);

  executor::insert(*pool, table, 1,
                   TypedRow{{Column::VarcharType("row_b_1")}});
  executor::insert(*pool, table, 3,
                   TypedRow{{Column::VarcharType("row_b_3")}});
  executor::insert(*pool, table, 4,
                   TypedRow{{Column::VarcharType("row_b_4")}});
  executor::insert(*pool, table, 7,
                   TypedRow{{Column::VarcharType("row_b_7")}});

  const int LOW_KEY = 4;
  const int HIGH_KEY = 10;

    IndexLookup lookup =
      IndexLookup::fromKeyRange(*pool, table.indexFile(), LOW_KEY, HIGH_KEY);
    HeapFetch fetcher(*pool, table.heapFile());

  // We expect to see keys 4 and 7, in that order, with the
  // corresponding payload strings we inserted above.
  std::vector<std::string> seen;

  while (auto rid = lookup.next()) {
    if (!rid) break;
    char* cell_start = fetcher.fetch(rid->heap_page_id, rid->slot_id);
    TypedRow row = RecordCellView(cell_start).getTypedRow(table.schema());
    ASSERT_EQ(row.values.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row.values[0]));
    seen.push_back(std::get<Column::VarcharType>(row.values[0]));
  }

  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0], std::string("row_b_4"));
  EXPECT_EQ(seen[1], std::string("row_b_7"));
}

TEST_F(E2ETest, SelectRangeWithMultiColumnRows) {
  const std::string sql = "SELECT * FROM x where id >= 4";

  result = pg_query_parse(sql.c_str());
  ASSERT_EQ(result.error, nullptr)
      << "parse error: " << (result.error ? result.error->message : "");
  ASSERT_NE(result.parse_tree, nullptr);
  ASSERT_GT(std::strlen(result.parse_tree), 0u);

  Schema schema("x", {Column("id", Column::Type::Integer),
                      Column("name", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema);

  // insert
  executor::insert(
    *pool, table, 1,
    TypedRow{{Column::IntegerType(1), Column::VarcharType("row_1")}});
  executor::insert(
    *pool, table, 3,
    TypedRow{{Column::IntegerType(3), Column::VarcharType("row_3")}});
  executor::insert(
    *pool, table, 4,
    TypedRow{{Column::IntegerType(4), Column::VarcharType("row_4")}});
  executor::insert(
    *pool, table, 7,
    TypedRow{{Column::IntegerType(7), Column::VarcharType("row_7")}});

  // read
  IndexLookup lookup = IndexLookup::fromKeyRange(*pool, table.indexFile(), 4, 10);
  HeapFetch fetcher(*pool, table.heapFile());

  std::vector<TypedRow> seen_rows;

  while (auto rid = lookup.next()) {
    if (!rid) {
      break;
    }
    char* cell_start = fetcher.fetch(rid->heap_page_id, rid->slot_id);
    seen_rows.push_back(RecordCellView(cell_start).getTypedRow(table.schema()));
  }

  ASSERT_EQ(seen_rows.size(), 2u);
  ASSERT_EQ(seen_rows[0].values.size(), 2u);
  ASSERT_EQ(seen_rows[1].values.size(), 2u);

  EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[0].values[0]), 4);
  EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[0].values[1]), "row_4");
  EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[1].values[0]), 7);
  EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[1].values[1]), "row_7");
}
