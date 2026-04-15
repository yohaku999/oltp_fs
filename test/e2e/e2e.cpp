#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

#include "execution/executor.h"
#include "execution/heap_fetch.h"
#include "execution/index_scan.h"
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

  PgQueryParseResult result;

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
    pg_query_free_parse_result(result);
  }
};

TEST_F(E2ETest, SelectBGreaterEqual4) {
  const std::string sql = "SELECT * FROM x where id >= 4";

  result = pg_query_parse(sql.c_str());
  ASSERT_EQ(result.error, nullptr)
      << "parse error: " << (result.error ? result.error->message : "");

  // This test only needs parsing to succeed; it does not depend on the exact
  // AST shape produced by libpg_query.
  ASSERT_NE(result.parse_tree, nullptr);
  ASSERT_GT(std::strlen(result.parse_tree), 0u);

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

TEST_F(E2ETest, SelectRangeWithMultiColumnRows) {
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
  const std::string sql = "SELECT * FROM x where id >= 4";

  result = pg_query_parse(sql.c_str());
  ASSERT_EQ(result.error, nullptr)
      << "parse error: " << (result.error ? result.error->message : "");
  ASSERT_NE(result.parse_tree, nullptr);
  ASSERT_GT(std::strlen(result.parse_tree), 0u);

  // parse
  nlohmann::json parse_tree = nlohmann::json::parse(result.parse_tree);
  std::string table_name =
      parse_tree.at("stmts")
          .at(0)
          .at("stmt")
          .at("SelectStmt")
          .at("fromClause")
          .at(0)
          .at("RangeVar")
          .at("relname")
          .get<std::string>();
  // この辺の条件が複雑なった倍のも対応できるような設計を考える。
  // std::where_column_name = parse_tree.at("stmts")
  //                            .at(0)
  //                            .at("stmt")
  //                            .at("SelectStmt")
  //                            .at("whereClause")
  //                            .at(0)
  //                            .at("A_Expr")
  //                            .at("lexpr")
  //                            .at("ColumnRef")
  //                            .at("fields")
  //                            .at(0)
  //                            .get<std::string>();
  // std::string where_condition = parse_tree.at("stmts")
  //                            .at(0)
  //                            .at("stmt")
  //                            .at("SelectStmt")
  //                            .at("whereClause")
  //                            .at("A_Expr");

  // execute
  // The range [4, 10] should return only the final two inserted rows.
  table = Table::getTable(table_name);
  if(table.hasIndexForColumn("id")){
    auto scan = IndexScanOperator::fromKeyRange(*pool, table.indexFile(), 4, 10);
    HeapFetchOperator fetcher(std::move(scan), *pool, table.heapFile(),
                              table.schema());
    fetcher.open();

    std::vector<TypedRow> seen_rows;

    while (auto row = fetcher.next()) {
      seen_rows.push_back(*row);
    }
    fetcher.close();

    ASSERT_EQ(seen_rows.size(), 2u);
    ASSERT_EQ(seen_rows[0].values.size(), 2u);
    ASSERT_EQ(seen_rows[1].values.size(), 2u);

    EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[0].values[0]), 4);
    EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[0].values[1]), "row_4");
    EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[1].values[0]), 7);
    EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[1].values[1]), "row_7");
  }
  
}