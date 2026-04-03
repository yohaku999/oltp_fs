#include "../src/storage/btreecursor.h"
#include "../src/storage/bufferpool.h"
#include "../src/storage/file.h"
#include "../src/storage/page.h"

#include <array>
#include <cstdio>
#include <memory>
#include <string>

#include <gtest/gtest.h>

class BTreeCursorTest : public ::testing::Test
{
protected:
    std::unique_ptr<BufferPool> pool_;
    std::unique_ptr<File> index_file_;
    const std::string index_path_ = "btreecursor_test.index";

    void SetUp() override
    {
        pool_ = std::make_unique<BufferPool>();
        std::remove(index_path_.c_str());
        index_file_ = std::make_unique<File>(index_path_);
        initializeLeafPage(*index_file_);
    }

    void TearDown() override
    {
        index_file_.reset();
        std::remove(index_path_.c_str());
    }

    static void initializeLeafPage(File &file)
    {
        std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
        auto page = std::make_unique<Page>(buffer.data(), true, 0, 0);
        file.writePageOnFile(0, buffer.data());
    }
};

TEST_F(BTreeCursorTest, InsertIntoIndexAndFindRecordLocation)
{
    const uint16_t heap_page_id = 5;
    const uint16_t slot_id = 3;

    std::vector<int> keys = {1, 42, 100};

    for (int key : keys)
    {
        BTreeCursor::insertIntoIndex(*pool_, *index_file_, key, heap_page_id, slot_id);
    }

    for (int key : keys)
    {
        auto location = BTreeCursor::findRecordLocation(*pool_, *index_file_, key);
        ASSERT_TRUE(location.has_value());
        EXPECT_EQ(location->first, heap_page_id);
        EXPECT_EQ(location->second, slot_id);
    }
}