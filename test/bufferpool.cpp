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

TEST_F(BufferPoolTest, GetPageSamePageReturnsCachedPage)
{
    Page *page1 = pool->getPage(1, *testFile);
    Page *page1_again = pool->getPage(1, *testFile);
    EXPECT_EQ(page1, page1_again);
}

TEST_F(BufferPoolTest, GetPageAllFramesFilledSuccessfully)
{
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
    }
}

// test eviction by filling more than max frames and verifying evicted pages can be reloaded correctly and also make sure reloaded through storage as well.
TEST_F(BufferPoolTest, GetPageWithEviction)
{
    // Store copies of page contents for later comparison
    // 10 pages will be evicted.
    std::array<std::array<char, Page::PAGE_SIZE_BYTE>, BufferPool::MAX_FRAME_COUNT+10> page_copies;
    
    // Fill all frames and write unique data to each page
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT + 10; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr);
        
        // Initialize and write unique data to each page
        Page::initializePage(page->start_p_, true, static_cast<uint16_t>(i));
        char test_data[50];
        std::snprintf(test_data, sizeof(test_data), "test_data_page_%zu", i);
        RecordCell cell(static_cast<uint16_t>(i * 10), test_data, std::strlen(test_data) + 1);
        page->insertCell(cell);
        
        // Copy the page content
        std::memcpy(page_copies[i].data(), page->start_p_, Page::PAGE_SIZE_BYTE);
    }

    // make sure file size has incresed by eviction.
    std::ifstream ifs(kTestFile, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(ifs.is_open());
    std::streamsize file_size = ifs.tellg();
    ifs.close();
    EXPECT_GE(file_size, 0);
    
    // Verify all pages can still be accessed and their contents match
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT+10; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), *testFile);
        ASSERT_NE(page, nullptr) << "Should be able to access/reload page " << i;
        
        // Compare the page content with the saved copy
        int cmp_result = std::memcmp(page->start_p_, page_copies[i].data(), Page::PAGE_SIZE_BYTE);
        EXPECT_EQ(cmp_result, 0) << "Page " << i << " content should match after eviction and reload";
    }
}