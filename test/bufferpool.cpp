#include "../src/bufferpool.h"
#include "../src/page.h"
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

    void SetUp() override
    {
        std::remove(kTestFile);
        std::ofstream ofs(kTestFile, std::ios::binary);
        std::array<char, Page::PAGE_SIZE_BYTE> empty{};
        ofs.write(empty.data(), empty.size());
        ofs.close();
        pool = std::make_unique<BufferPool>();
    }

    void TearDown() override
    {
        pool.reset();
        std::remove(kTestFile);
    }
};

TEST_F(BufferPoolTest, GetPageNewPageReturnsNonNull)
{
    Page *page = pool->getPage(1, kTestFile);
    ASSERT_NE(page, nullptr);
}

TEST_F(BufferPoolTest, GetPageSamePageReturnsCachedPage)
{
    Page *page1 = pool->getPage(1, kTestFile);
    Page *page1_again = pool->getPage(1, kTestFile);
    EXPECT_EQ(page1, page1_again);
}

TEST_F(BufferPoolTest, GetPageDifferentPagesReturnsDifferentObjects)
{
    Page *page1 = pool->getPage(1, kTestFile);
    Page *page2 = pool->getPage(2, kTestFile);
    EXPECT_NE(page1, page2);
}

TEST_F(BufferPoolTest, GetPageAllFramesFilledSuccessfully)
{
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        Page *page = pool->getPage(static_cast<uint16_t>(i), kTestFile);
        ASSERT_NE(page, nullptr);
    }
}

TEST_F(BufferPoolTest, GetPageNoFreeFrameThrowsException)
{
    for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
    {
        pool->getPage(static_cast<uint16_t>(i), kTestFile);
    }
    EXPECT_THROW(pool->getPage(BufferPool::MAX_FRAME_COUNT, kTestFile), std::runtime_error);
}
