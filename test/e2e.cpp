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
#include "storage/file.h"
#include "storage/page.h"
#include "storage/record_cell.h"
#include "storage/record_serializer.h"

namespace {

std::pair<uint16_t, uint16_t> insertTypedRow(BufferPool& pool, File& indexFile,
                                             File& heapFile, int key,
                                             const Schema& schema,
                                             const TypedRow& row) {
  RecordSerializer serializer(schema, row);

  int target_page_id = heapFile.getMaxPageID();
  Page* heap_page = pool.getPage(target_page_id, heapFile);
  auto inserted_slot_id = heap_page->insertCell(serializer.serializedBytes());
  if (!inserted_slot_id.has_value()) {
    pool.unpin(heap_page, heapFile);
    target_page_id = pool.createNewPage(true, heapFile);
    heap_page = pool.getPage(target_page_id, heapFile);
    inserted_slot_id = heap_page->insertCell(serializer.serializedBytes());
    if (!inserted_slot_id.has_value()) {
      throw std::runtime_error(
          "Failed to insert serialized record into heap page.");
    }
  }
  pool.unpin(heap_page, heapFile);

  BTreeCursor::insertIntoIndex(pool, indexFile, key,
                               static_cast<uint16_t>(target_page_id),
                               static_cast<uint16_t>(inserted_slot_id.value()));
  return {static_cast<uint16_t>(target_page_id),
          static_cast<uint16_t>(inserted_slot_id.value())};
}

}  // namespace

class E2ETest : public ::testing::Test {
 protected:
  static constexpr const char* kIndexPath = "x.index";
  static constexpr const char* kHeapPath = "x.db";

  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<File> testFile;
  std::unique_ptr<File> heapFile;

  PgQueryParseResult result;

  void SetUp() override {
    // delete garbage
    std::error_code ec;
    std::filesystem::remove(kIndexPath, ec);
    std::filesystem::remove(kHeapPath, ec);

    // initialize
    pool = std::make_unique<BufferPool>();
    // Initialize B+ tree structure for the index file.
    testFile = std::make_unique<File>(kIndexPath);
    heapFile = std::make_unique<File>(kHeapPath);
    uint16_t leaf_page_id = pool->createNewPage(true, *testFile);
    LOG_INFO("Initialized leaf page id {} for index {}", leaf_page_id,
             kIndexPath);
    ASSERT_EQ(leaf_page_id, 1);
    pool->getPage(0, *testFile)->setRightMostChildPageId(1);
  }

  void TearDown() override {
    // Destroy in-memory resources before deleting backing files
    pool.reset();
    testFile.reset();
    heapFile.reset();

    std::error_code ec;
    std::filesystem::remove(kIndexPath, ec);
    std::filesystem::remove(kHeapPath, ec);

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

  const char* v1 = "row_b_1";
  executor::insert(*pool, *testFile, *heapFile, 1, const_cast<char*>(v1),
                   std::strlen(v1));
  const char* v2 = "row_b_3";
  executor::insert(*pool, *testFile, *heapFile, 3, const_cast<char*>(v2),
                   std::strlen(v2));

  const char* v3 = "row_b_4";
  executor::insert(*pool, *testFile, *heapFile, 4, const_cast<char*>(v3),
                   std::strlen(v3));

  const char* v4 = "row_b_7";
  executor::insert(*pool, *testFile, *heapFile, 7, const_cast<char*>(v4),
                   std::strlen(v4));

  const int LOW_KEY = 4;
  const int HIGH_KEY = 10;

  IndexLookup lookup =
      IndexLookup::fromKeyRange(*pool, *testFile, LOW_KEY, HIGH_KEY);
  HeapFetch fetcher(*pool, *heapFile);

  // We expect to see keys 4 and 7, in that order, with the
  // corresponding payload strings we inserted above.
  std::vector<std::string> seen;

  while (auto rid = lookup.next()) {
    if (!rid) break;
    char* cell_start = fetcher.fetch(rid->heap_page_id, rid->slot_id);
    TypedRow row = RecordCellView(cell_start).getTypedRow(schema);
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

  insertTypedRow(
      *pool, *testFile, *heapFile, 1, schema,
      TypedRow{{Column::IntegerType(1), Column::VarcharType("row_1")}});
  insertTypedRow(
      *pool, *testFile, *heapFile, 3, schema,
      TypedRow{{Column::IntegerType(3), Column::VarcharType("row_3")}});
  insertTypedRow(
      *pool, *testFile, *heapFile, 4, schema,
      TypedRow{{Column::IntegerType(4), Column::VarcharType("row_4")}});
  insertTypedRow(
      *pool, *testFile, *heapFile, 7, schema,
      TypedRow{{Column::IntegerType(7), Column::VarcharType("row_7")}});

  IndexLookup lookup = IndexLookup::fromKeyRange(*pool, *testFile, 4, 10);
  HeapFetch fetcher(*pool, *heapFile);

  std::vector<TypedRow> seen_rows;

  while (auto rid = lookup.next()) {
    if (!rid) {
      break;
    }
    char* cell_start = fetcher.fetch(rid->heap_page_id, rid->slot_id);
    seen_rows.push_back(RecordCellView(cell_start).getTypedRow(schema));
  }

  ASSERT_EQ(seen_rows.size(), 2u);
  ASSERT_EQ(seen_rows[0].values.size(), 2u);
  ASSERT_EQ(seen_rows[1].values.size(), 2u);

  EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[0].values[0]), 4);
  EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[0].values[1]), "row_4");
  EXPECT_EQ(std::get<Column::IntegerType>(seen_rows[1].values[0]), 7);
  EXPECT_EQ(std::get<Column::VarcharType>(seen_rows[1].values[1]), "row_7");
}
