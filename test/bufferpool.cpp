#include "../src/bufferpool.h"
#include "../src/page.h"
#include "../src/file.h"
#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
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

TEST_F(BufferPoolTest, GetPageNoFreeFrameThrowsException)
{
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        pool->getPage(static_cast<uint16_t>(i), *testFile);
    }
    EXPECT_THROW(pool->getPage(BufferPool::MAX_FRAME_COUNT, *testFile), std::runtime_error);
}
