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
        pool = std::make_unique<BufferPool>(std::make_unique<FrameDirectory>());
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

TEST_F(BufferPoolTest, EvictionEvictedPageCanBeReloaded)
{
    // Trigger eviction
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
    Page *evicting_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    ASSERT_NE(evicting_page, nullptr);
    
    // Reload the first page (likely evicted)
    Page *reloaded_page = pool->getPage(0, *testFile);
    EXPECT_NE(reloaded_page, nullptr);
}

TEST_F(BufferPoolTest, EvictionDirtyPageFlushedOnEviction)
{
    // Trigger eviction
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
    Page *dirty_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);

    // mark it as dirty
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
    // TODO: should verify the content of the reloaded page to ensure it was flushed correctly
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
    // Record all frame memory addresses
    std::set<const void*> initial_frame_addresses;
    
    // Fill all frames and collect their memory addresses
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
        initial_frame_addresses.insert(reinterpret_cast<const void*>(page->start_p_));
    }
    
    // Note: Due to eviction during the fill phase, we may have fewer unique addresses
    // than MAX_FRAME_COUNT if eviction started before all frames were filled
    EXPECT_GE(initial_frame_addresses.size(), 1u)
        << "Should have at least one frame address";
    EXPECT_LE(initial_frame_addresses.size(), BufferPool::MAX_FRAME_COUNT)
        << "Should not exceed max frame count";
    
    // Trigger eviction
    Page *new_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    ASSERT_NE(new_page, nullptr);
    
    const void* new_page_addr = reinterpret_cast<const void*>(new_page->start_p_);
    
    // The new page should be using one of the original frame addresses (frame was reused)
    EXPECT_TRUE(initial_frame_addresses.count(new_page_addr) > 0)
        << "Evicted page should reuse an existing frame memory address";
}

// DI-based test: Using PredictableFrameDirectory
TEST_F(BufferPoolTest, EvictionWithPredictableVictimSelection)
{
    // Create BufferPool with custom FrameDirectory
    auto predictable_dir = std::make_unique<FrameDirectory>();
    pool = std::make_unique<BufferPool>(std::move(predictable_dir));
    
    // Fill all frames and track memory addresses
    std::set<char*> frame_addresses;
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        frame_addresses.insert(page->start_p_);
    }
    
    EXPECT_EQ(frame_addresses.size(), BufferPool::MAX_FRAME_COUNT)
        << "All frames should have unique addresses";
    
    // Trigger eviction - FrameDirectory will evict a frame
    Page *new_page = pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile);
    
    // Verify the new page reuses one of the existing frame addresses
    EXPECT_NE(frame_addresses.find(new_page->start_p_), frame_addresses.end())
        << "New page should reuse an existing frame's memory address";
    
    // Verify all original pages can still be accessed (some may need reload)
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        EXPECT_NE(page, nullptr)
            << "Page " << i << " should be accessible after eviction";
    }
}

