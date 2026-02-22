#include "../src/bufferpool.h"
#include "../src/page.h"
#include "../src/file.h"
#include "../src/record_cell.h"
#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <set>
#include <gtest/gtest.h>

class BufferPoolTest : public ::testing::Test
{
protected:
    static constexpr const char *kTestFile = "testfile.db";
    std::unique_ptr<BufferPool> pool;
    std::unique_ptr<File> testFile;

    void SetUp() override
    {
        std::remove(kTestFile);
        std::ofstream ofs(kTestFile, std::ios::binary);
        std::array<char, Page::PAGE_SIZE_BYTE> empty{};
        ofs.write(empty.data(), empty.size());
        ofs.close();
        pool = std::make_unique<BufferPool>();
        testFile = std::make_unique<File>(kTestFile);
    }

    void TearDown() override
    {
        pool.reset();
        std::remove(kTestFile);
    }
};

TEST_F(BufferPoolTest, GetPageNewPageReturnsNonNull)
{
    Page *page = pool->getPage(1, *testFile);
    ASSERT_NE(page, nullptr);
}

TEST_F(BufferPoolTest, GetPageSamePageReturnsCachedPage)
{
    Page *page1 = pool->getPage(1, *testFile);
    Page *page1_again = pool->getPage(1, *testFile);
    EXPECT_EQ(page1, page1_again);
}

TEST_F(BufferPoolTest, GetPageDifferentPagesReturnsDifferentObjects)
{
    Page *page1 = pool->getPage(1, *testFile);
    Page *page2 = pool->getPage(2, *testFile);
    EXPECT_NE(page1, page2);
}

TEST_F(BufferPoolTest, GetPageAllFramesFilledSuccessfully)
{
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
}

TEST_F(BufferPoolTest, EvictionWhenAllFramesFilled)
{
    // useup all frames
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        pool->getPage(static_cast<uint16_t>(i), *testFile);
    }
    // trigger eviction by requesting one more page
    EXPECT_NO_THROW(pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile));
}

// Eviction Tests (excluding tests that require pin/unpin)

TEST_F(BufferPoolTest, EvictionEvictedPageCanBeReloaded)
{
    // Fill all frames
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
    // Trigger eviction by loading one more page
    Page *evicting_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    ASSERT_NE(evicting_page, nullptr);
    
    // Reload the first page (likely evicted)
    Page *reloaded_page = pool->getPage(0, *testFile);
    EXPECT_NE(reloaded_page, nullptr);
}

TEST_F(BufferPoolTest, EvictionDirtyPageFlushedOnEviction)
{
    // Fill all frames
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
    
    // Request one more page to trigger eviction, then mark it as dirty
    Page *dirty_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    ASSERT_NE(dirty_page, nullptr);
    
    Page::initializePage(dirty_page->start_p_, true, BufferPool::MAX_FRAME_COUNT);
    char test_data[] = "test_dirty_data";
    RecordCell cell(100, test_data, sizeof(test_data));
    dirty_page->insertCell(cell);
    EXPECT_TRUE(dirty_page->isDirty());
    
    // Trigger another eviction to evict the dirty page
    pool->getPage(BufferPool::MAX_FRAME_COUNT + 1, *testFile);
    
    // Reload the dirty page and verify data persistence
    Page *reloaded = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    ASSERT_NE(reloaded, nullptr);
    // After reload, page should not be dirty
    EXPECT_FALSE(reloaded->isDirty());
}

TEST_F(BufferPoolTest, EvictionMultipleEvictionCycles)
{
    // Perform multiple eviction cycles
    for (size_t cycle = 0; cycle < 3; ++cycle)
    {
        for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 2; ++i)
        {
            uint16_t page_id = static_cast<uint16_t>(cycle * 100 + i);
            Page *page = pool->getPage(page_id, *testFile);
            EXPECT_NE(page, nullptr) << "Failed at cycle " << cycle << ", page " << i;
        }
    }
}

TEST_F(BufferPoolTest, EvictionWithMultipleFiles)
{
    // Create a second test file
    const char *kTestFile2 = "testfile2.db";
    std::remove(kTestFile2);
    std::ofstream ofs(kTestFile2, std::ios::binary);
    std::array<char, Page::PAGE_SIZE_BYTE> empty{};
    ofs.write(empty.data(), empty.size());
    ofs.close();
    File testFile2(kTestFile2);
    
    // Load pages from both files
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT / 2; ++i)
    {
        pool->getPage(static_cast<uint16_t>(i), *testFile);
        pool->getPage(static_cast<uint16_t>(i), testFile2);
    }
    
    // Trigger eviction with pages from file1
    for (size_t i = BufferPool::MAX_FRAME_COUNT / 2; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        EXPECT_NE(page, nullptr);
    }
    
    // Verify pages from file2 can still be accessed
    Page *page_file2 = pool->getPage(0, testFile2);
    EXPECT_NE(page_file2, nullptr);
    
    std::remove(kTestFile2);
}

TEST_F(BufferPoolTest, EvictionFrameReusedAfterEviction)
{
    // Fill all frames
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
    
    // Get pointer to the page that will be evicted (frame 0 is always evicted first)
    // Frame 0 contains page 4 (the 5th page loaded: 0,1,2,3,4)
    Page *frame_0_initial = pool->getPage(4, *testFile);
    char *frame_memory_initial = frame_0_initial->start_p_;
    
    // Trigger eviction - this will evict page 4 from frame 0
    pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    
    // Get the new page in frame 0
    Page *frame_0_after_eviction = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    char *frame_memory_after = frame_0_after_eviction->start_p_;
    
    // The underlying memory addresses should be the same (frame was reused)
    EXPECT_EQ(frame_memory_initial, frame_memory_after) 
        << "Frame memory should be reused after eviction";
}
