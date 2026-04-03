#include <gtest/gtest.h>
#include <string>

extern "C" {
#include <pg_query.h>
}

#include "storage/bufferpool.h"
#include "storage/file.h"
#include "executor/executor.h"
#include "executor/index_scan.h"
#include "executor/heap_fetch.h"

// Very small end-to-end style test that exercises:
// - libpg_query parsing of a simple SELECT
// - executor::insert to populate heap + index
// - IndexLookup range scan + HeapFetch to read back rows
//
// This is essentially the same flow as src/sql/select_sample.cpp,
// but expressed as a test so it can be run in CI/TDD style.

TEST(E2E, SelectBGreaterEqual4)
{
    const std::string sql = "SELECT * FROM x where b >= 4";

    PgQueryParseResult result = pg_query_parse(sql.c_str());
    ASSERT_EQ(result.error, nullptr) << "parse error: "
                                     << (result.error ? result.error->message : "");

    // We don't assert on the exact AST JSON here, but we do ensure it's non-empty.
    ASSERT_NE(result.parse_tree, nullptr);
    ASSERT_GT(std::strlen(result.parse_tree), 0u);

    BufferPool pool;
    File indexFile("x.index");
    File heapFile("x.db");

    // Insert a few rows with different b values
    const char *v1 = "row_b_1";
    executor::insert(pool, indexFile, heapFile, 1,
                     const_cast<char *>(v1), std::strlen(v1) + 1);

    const char *v2 = "row_b_3";
    executor::insert(pool, indexFile, heapFile, 3,
                     const_cast<char *>(v2), std::strlen(v2) + 1);

    const char *v3 = "row_b_4";
    executor::insert(pool, indexFile, heapFile, 4,
                     const_cast<char *>(v3), std::strlen(v3) + 1);

    const char *v4 = "row_b_7";
    executor::insert(pool, indexFile, heapFile, 7,
                     const_cast<char *>(v4), std::strlen(v4) + 1);

    const int LOW_KEY = 4;
    const int HIGH_KEY = 1'000'000;

    IndexLookup lookup = IndexLookup::fromKeyRange(pool, indexFile, LOW_KEY, HIGH_KEY);
    HeapFetch fetcher(pool, heapFile);

    // We expect to see keys 4 and 7, in that order, with the
    // corresponding payload strings we inserted above.
    std::vector<std::string> seen;

    while (auto rid = lookup.next()) {
        char *value = fetcher.fetch(rid->heap_page_id, rid->slot_id);
        seen.emplace_back(value);
    }

    pg_query_free_parse_result(result);

    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], std::string("row_b_4"));
    EXPECT_EQ(seen[1], std::string("row_b_7"));
}
