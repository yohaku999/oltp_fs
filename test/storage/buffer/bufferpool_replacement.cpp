#include "storage/buffer/bufferpool.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <fstream>
#include <memory>
#include <vector>

#include "storage/disk/file.h"
#include "storage/page/page.h"
#include "storage/wal/wal.h"

class BufferPoolReplacementTest : public ::testing::Test {
 protected:
  static constexpr const char* kTestFile = "bufferpool_replacement_test.db";
  static constexpr const char* kWalFile = "bufferpool_replacement_test.wal";

  std::unique_ptr<BufferPool> pool;
  std::unique_ptr<File> test_file;
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
    test_file = std::make_unique<File>(kTestFile);
  }

  void TearDown() override {
    pool.reset();
    test_file.reset();
    wal.reset();
    std::remove(kTestFile);
    std::remove(kWalFile);
  }
};

TEST_F(BufferPoolReplacementTest, ResidentHitProtectsPageFromNextEviction) {
  std::vector<uint16_t> page_ids;
  page_ids.reserve(BufferPool::MAX_FRAME_COUNT + 2);

  // fill up buffer pool
  for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i) {
    page_ids.push_back(pool->createPage(PageKind::Heap, *test_file));
  }

  // create hot page
  const uint16_t hot_page_id = page_ids.front();
  Page* hot_page = pool->pinPage(hot_page_id, *test_file);
  pool->unpinPage(hot_page, *test_file);

  const auto hits_after_hot_access = pool->stats().resident_hits;
  const auto misses_after_hot_access = pool->stats().misses;

  // trigger eviciton
  page_ids.push_back(pool->createPage(PageKind::Heap, *test_file));
  page_ids.push_back(pool->createPage(PageKind::Heap, *test_file));

  // should read hot page from ache
  Page* hot_page_again = pool->pinPage(hot_page_id, *test_file);
  pool->unpinPage(hot_page_again, *test_file);

  EXPECT_EQ(hits_after_hot_access + 1, pool->stats().resident_hits);
  EXPECT_EQ(misses_after_hot_access, pool->stats().misses);
}
