#include "../src/btreecursor.h"
#include "../src/bufferpool.h"
#include "../src/file.h"
#include "../src/page.h"
#include <array>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <gtest/gtest.h>

class BTreeCursorTest : public ::testing::Test
{
protected:
    BufferPool pool_;
    std::unique_ptr<File> index_file_;
    std::unique_ptr<File> heap_file_;
    const std::string index_path_ = "btreecursor_test.index";
    const std::string heap_path_ = "btreecursor_test.db";

    void SetUp() override
    {
        std::remove(index_path_.c_str());
        std::remove(heap_path_.c_str());
        index_file_ = std::make_unique<File>(index_path_);
        heap_file_ = std::make_unique<File>(heap_path_);
        initializeLeafPage(*index_file_);
        initializeLeafPage(*heap_file_);
    }

    void TearDown() override
    {
        index_file_.reset();
        heap_file_.reset();
        std::remove(index_path_.c_str());
        std::remove(heap_path_.c_str());
    }

    static void initializeLeafPage(File &file)
    {
        std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
        Page::initializePage(buffer.data(), true, 0);
        file.writePageOnFile(0, buffer.data());
    }
};

TEST_F(BTreeCursorTest, InsertAndGetRecord)
{
    std::string payload = "value1";

    BTreeCursor::insert(pool_, *index_file_, *heap_file_, 1, payload.data(), payload.size());
    char *stored = BTreeCursor::getRecord(pool_, *index_file_, *heap_file_, 1);
    std::string restored(stored, payload.size());
    EXPECT_EQ(payload, restored);
}