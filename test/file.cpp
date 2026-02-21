#include "../src/file.h"
#include "../src/page.h"
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>
#include <gtest/gtest.h>

class FileTest : public ::testing::Test
{
protected:
    const std::string test_file_path_ = "file_test_temp.db";
    std::unique_ptr<File> file_p;

    void SetUp() override
    {
        file_p.reset();
        std::remove(test_file_path_.c_str());
        std::ofstream ofs(test_file_path_, std::ios::binary | std::ios::trunc);
        ofs.close();
        file_p = std::make_unique<File>(test_file_path_);
    }

    void TearDown() override
    {
        file_p.reset();
        std::remove(test_file_path_.c_str());
    }
};

TEST_F(FileTest, AllocateAndUsage)
{
    EXPECT_TRUE(file_p->isPageIDUsed(0));
    uint16_t next = file_p->allocateNextPageId();
    EXPECT_EQ(1u, next);
    EXPECT_TRUE(file_p->isPageIDUsed(1));
    EXPECT_FALSE(file_p->isPageIDUsed(2));
}

TEST_F(FileTest, WriteAndLoadPage)
{
    std::vector<char> write_buffer(Page::PAGE_SIZE_BYTE);
    for (size_t i = 0; i < write_buffer.size(); ++i)
    {
        write_buffer[i] = static_cast<char>(i % 256);
    }

    file_p->writePageOnFile(1, write_buffer.data());

    std::vector<char> read_buffer(Page::PAGE_SIZE_BYTE);
    file_p->loadPageOnFrame(1, read_buffer.data());

    EXPECT_EQ(0, std::memcmp(write_buffer.data(), read_buffer.data(), Page::PAGE_SIZE_BYTE));
}

TEST_F(FileTest, LoadMaxPageIdFromHeader)
{
    constexpr uint16_t persisted_max = 42;

    file_p.reset();

    std::vector<char> header(File::HEADDER_SIZE_BYTE, 0);
    std::memcpy(header.data(), &persisted_max, sizeof(persisted_max));
    std::ofstream ofs(test_file_path_, std::ios::binary | std::ios::trunc);
    ofs.write(header.data(), header.size());
    ofs.close();

    file_p = std::make_unique<File>(test_file_path_);
    EXPECT_TRUE(file_p->isPageIDUsed(persisted_max));
    EXPECT_FALSE(file_p->isPageIDUsed(persisted_max + 1));
    uint16_t next = file_p->allocateNextPageId();
    EXPECT_EQ(static_cast<uint16_t>(persisted_max + 1), next);
    EXPECT_TRUE(file_p->isPageIDUsed(next));
}
