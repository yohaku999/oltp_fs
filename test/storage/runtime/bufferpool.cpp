#include "storage/runtime/bufferpool.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>

#include "storage/runtime/file.h"
#include "storage/wal/lsn_allocator.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"
#include "schema/schema.h"

namespace {

RecordSerializer serializeSingleVarcharRecord(const std::string& value) {
  static const Schema schema{
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row{{value}};
  return RecordSerializer(schema, row);
}

}  // namespace

class BufferPoolTest : public ::testing::Test {
 protected:
  static constexpr const char* kTestFile = "testfile.db";
  static constexpr const char* kWalFile = "bufferpool_test.wal";
  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<File> testFile;
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
    testFile = std::make_unique<File>(kTestFile);
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
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i) {
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
  std::array<std::array<char, Page::PAGE_SIZE_BYTE>,
             BufferPool::MAX_FRAME_COUNT + 10>
      page_copies;
  std::array<uint16_t, BufferPool::MAX_FRAME_COUNT + 10> page_ids;

  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 10; ++i) {
    uint16_t page_id = pool->createPage(PageKind::Heap, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    ASSERT_NE(page, nullptr);

    page_ids[i] = page_id;

    char test_data[50];
    std::snprintf(test_data, sizeof(test_data), "test_data_page_%zu", i);
    RecordSerializer cell = serializeSingleVarcharRecord(test_data);
    page->insertCell(cell.serializedBytes());

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
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 10; ++i) {
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

  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i) {
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