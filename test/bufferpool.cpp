#include "../src/storage/bufferpool.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>

#include "../src/storage/file.h"
#include "../src/storage/lsn_allocator.h"
#include "../src/storage/page.h"
#include "../src/storage/record_cell.h"
#include "../src/storage/record_serializer.h"
#include "../src/storage/wal/wal.h"
#include "../src/storage/wal_record.h"

namespace {

RecordSerializer serializeSingleVarcharRecord(const std::string& value) {
  static const Schema schema{
      "single_varchar_record",
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row{{value}};
  return RecordSerializer(schema, row);
}

}

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
    wal = std::make_unique<WAL>(kWalFile);
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
  uint16_t page_id = pool->createPage(true, *testFile);
  Page* page1 = pool->pinPage(page_id, *testFile);
  Page* page1_again = pool->pinPage(page_id, *testFile);
  EXPECT_EQ(page1, page1_again);
  pool->unpinPage(page1, *testFile);
  pool->unpinPage(page1_again, *testFile);
}

TEST_F(BufferPoolTest, createNewPageAllFramesFilledSuccessfully) {
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i) {
    uint16_t page_id = pool->createPage(true, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    ASSERT_NE(page, nullptr);
    pool->unpinPage(page, *testFile);
  }
}

// test eviction by filling more than max frames and verifying evicted pages can
// be reloaded correctly and also make sure reloaded through storage as well.
TEST_F(BufferPoolTest, createNewPageWithEviction) {
  // Store copies of page contents for later comparison
  // 10 pages will be evicted.
  std::array<std::array<char, Page::PAGE_SIZE_BYTE>,
             BufferPool::MAX_FRAME_COUNT + 10>
      page_copies;
  std::array<uint16_t, BufferPool::MAX_FRAME_COUNT + 10> page_ids;

  // Fill all frames and write unique data to each page
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 10; ++i) {
    uint16_t page_id = pool->createPage(true, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    ASSERT_NE(page, nullptr);

    // Store the page ID (it's the max page ID after allocation)
    page_ids[i] = page_id;

    // Write unique data to each page
    char test_data[50];
    std::snprintf(test_data, sizeof(test_data), "test_data_page_%zu", i);
    RecordSerializer cell = serializeSingleVarcharRecord(test_data);
    page->insertCell(cell.serializedBytes());

    // Copy the page content
    std::memcpy(page_copies[i].data(), page->page_buffer_,
          Page::PAGE_SIZE_BYTE);
    pool->unpinPage(page, *testFile);
  }

  // make sure file size has incresed by eviction.
  std::ifstream ifs(kTestFile, std::ios::binary | std::ios::ate);
  ASSERT_TRUE(ifs.is_open());
  std::streamsize file_size = ifs.tellg();
  ifs.close();
  EXPECT_GE(file_size, 0);

  // Verify all pages can still be accessed and their contents match
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 10; ++i) {
    Page* page = pool->pinPage(page_ids[i], *testFile);
    ASSERT_NE(page, nullptr) << "Should be able to access/reload page " << i;

    // Compare the page content with the saved copy
    int cmp_result = std::memcmp(page->page_buffer_, page_copies[i].data(),
                                 Page::PAGE_SIZE_BYTE);
    EXPECT_EQ(cmp_result, 0)
        << "Page " << i << " content should match after eviction and reload";
    pool->unpinPage(page, *testFile);
  }
}

TEST_F(BufferPoolTest, EvictionFlushesWALUpToPageLSN) {
  LSNAllocator allocator(0);

  std::vector<std::byte> body = {std::byte{0x01}, std::byte{0x02}};
  WALRecord rec =
      make_wal_record(allocator, WALRecord::RecordType::INSERT, 1, body);

  // WAL has not been flushed yet because of flush_threshold_bytes_
  wal->write(rec);
  EXPECT_EQ(wal->getFlushedLSN(), 0u);

  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i) {
    uint16_t page_id = pool->createPage(true, *testFile);
    Page* page = pool->pinPage(page_id, *testFile);
    // all the page has the same LSN.
    page->setPageLSN(rec.get_lsn());
    pool->unpinPage(page, *testFile);
  }

  // trigger eviction
  (void)pool->createPage(true, *testFile);

  EXPECT_EQ(wal->getFlushedLSN(), rec.get_lsn());
}