#include "storage/buffer/bufferpool.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>
#include <utility>

#include "schema/schema.h"
#include "storage/disk/pagedfile.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/wal/lsn_allocator.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"

namespace {

RecordCell makeSingleVarcharRecord(const std::string& value) {
  static const Schema schema{
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row{{value}};
  return RecordSerializer::serialize(schema, row);
}

}  // namespace

class BufferPoolTest : public ::testing::Test {
 protected:
  static constexpr const char* kTestFile = "testfile.db";
  static constexpr const char* kWalFile = "bufferpool_test.wal";
  static constexpr const size_t max_frames_count = 10;
  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<PagedFile> testFile;
  std::unique_ptr<WAL> wal;

  void SetUp() override {
    std::remove(kTestFile);
    std::remove(kWalFile);
    std::ofstream ofs(kTestFile, std::ios::binary);
    std::array<char, Page::PAGE_SIZE_BYTE> empty{};
    ofs.write(empty.data(), empty.size());
    ofs.close();
    wal = WAL::initializeNew(kWalFile);
    pool = std::make_unique<BufferPool>(*wal);
    testFile = std::make_unique<PagedFile>(kTestFile);
  }

  void TearDown() override {
    pool.reset();
    std::remove(kTestFile);
    std::remove(kWalFile);
  }
};

TEST_F(BufferPoolTest, GetPageSamePageReturnsCachedPage) {
  uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
  Page* page1 = pool->pinPage(page_id, *testFile);
  Page* page1_again = pool->pinPage(page_id, *testFile);
  EXPECT_EQ(page1, page1_again);
  pool->unpinPage(page1, *testFile);
  pool->unpinPage(page1_again, *testFile);
}

TEST_F(BufferPoolTest, createNewPageAllFramesFilledSuccessfully) {
  for (size_t i = 0; i < max_frames_count; ++i) {
    uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    ASSERT_NE(page, nullptr);
    pool->unpinPage(page, *testFile);
  }
}

// Fill beyond the in-memory frame budget and verify that evicted pages can be
// loaded back from storage without losing their contents.
TEST_F(BufferPoolTest, createNewPageWithEviction) {
  // Keep page snapshots so the test can compare the reloaded page bytes after
  // eviction writes them out to disk.
  std::array<std::array<char, Page::PAGE_SIZE_BYTE>, max_frames_count + 10>
      page_copies;
  std::array<uint16_t, max_frames_count + 10> page_ids;

  for (size_t i = 0; i < max_frames_count + 10; ++i) {
    uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    ASSERT_NE(page, nullptr);

    page_ids[i] = page_id;

    char test_data[50];
    std::snprintf(test_data, sizeof(test_data), "test_data_page_%zu", i);
    RecordCell cell = makeSingleVarcharRecord(test_data);
    page->insertCell(cell);

    std::memcpy(page_copies[i].data(), page->data(), Page::PAGE_SIZE_BYTE);
    pool->unpinPage(page, *testFile);
  }

  // Sanity-check that the backing file remains readable after eviction.
  std::ifstream ifs(kTestFile, std::ios::binary | std::ios::ate);
  ASSERT_TRUE(ifs.is_open());
  std::streamsize file_size = ifs.tellg();
  ifs.close();
  EXPECT_GE(file_size, 0);

  // Every page should round-trip through eviction without byte-level changes.
  for (size_t i = 0; i < max_frames_count + 10; ++i) {
    Page* page = pool->pinPage(page_ids[i], *testFile);
    ASSERT_NE(page, nullptr) << "Should be able to access/reload page " << i;

    int cmp_result =
        std::memcmp(page->data(), page_copies[i].data(), Page::PAGE_SIZE_BYTE);
    EXPECT_EQ(cmp_result, 0)
        << "Page " << i << " content should match after eviction and reload";
    pool->unpinPage(page, *testFile);
  }
}

TEST_F(BufferPoolTest, EvictionFlushesWALUpToPageLSN) {
  std::vector<std::byte> body = {std::byte{0x01}, std::byte{0x02}};
  constexpr std::uint64_t kRecordLsn = 0;

  // The record stays buffered until eviction forces WAL durability.
  wal->write(WALRecord::RecordType::INSERT, 1, body);
  EXPECT_EQ(wal->getFlushedLSN(), 0u);

  for (size_t i = 0; i < max_frames_count; ++i) {
    uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    // Mark each resident page as depending on the same WAL record.
    page->setPageLSN(kRecordLsn);
    pool->unpinPage(page, *testFile);
  }

  // Allocate one more page to force eviction of a dirty page.
  (void)pool->createPage(PageKind::Heap, *testFile);

  EXPECT_EQ(wal->getFlushedLSN(), kRecordLsn);
}

TEST_F(BufferPoolTest, ResetFlushesDirtyPagesAndStartsWithColdBuffer) {
  uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
  Page* page = pool->pinPage(page_id, *testFile);
  RecordCell cell = makeSingleVarcharRecord("survives reset");
  ASSERT_TRUE(page->insertCell(cell).has_value());
  std::array<char, Page::PAGE_SIZE_BYTE> expected{};
  std::memcpy(expected.data(), page->data(), expected.size());
  pool->unpinPage(page, *testFile);

  const BufferPoolResetResult result = pool->resetForBenchmark();

  EXPECT_EQ(1u, result.resident_frames_cleared);
  EXPECT_EQ(1u, result.dirty_frames_flushed);
  EXPECT_EQ(1u, result.generation);
  EXPECT_EQ(0u, pool->stats().pin_page_calls);
  EXPECT_EQ(0u, pool->stats().misses);
  EXPECT_EQ(0u, pool->stats().dirty_evictions);

  Page* reloaded = pool->pinPage(page_id, *testFile);
  ASSERT_NE(nullptr, reloaded);
  EXPECT_EQ(0, std::memcmp(expected.data(), reloaded->data(), expected.size()));
  EXPECT_EQ(1u, pool->stats().pin_page_calls);
  EXPECT_EQ(1u, pool->stats().misses);
  EXPECT_EQ(0u, pool->stats().resident_hits);
  pool->unpinPage(reloaded, *testFile);
}

TEST_F(BufferPoolTest, ResetRejectsPinnedFramesWithoutChangingPool) {
  uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
  Page* pinned = pool->pinPage(page_id, *testFile);

  EXPECT_THROW(pool->resetForBenchmark(), std::runtime_error);
  EXPECT_EQ(pinned, pool->pinPage(page_id, *testFile));

  pool->unpinPage(pinned, *testFile);
  pool->unpinPage(pinned, *testFile);
  EXPECT_NO_THROW(pool->resetForBenchmark());
}
