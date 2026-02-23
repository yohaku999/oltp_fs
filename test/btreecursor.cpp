#include "../src/btreecursor.h"
#include "../src/bufferpool.h"
#include "../src/file.h"
#include "../src/page.h"
#include <array>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>
#include <gtest/gtest.h>

class BTreeCursorTest : public ::testing::Test
{
protected:
    std::unique_ptr<BufferPool> pool_;
    std::unique_ptr<File> index_file_;
    std::unique_ptr<File> heap_file_;
    const std::string index_path_ = "btreecursor_test.index";
    const std::string heap_path_ = "btreecursor_test.db";

    void SetUp() override
    {
        pool_ = std::make_unique<BufferPool>();
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
        new Page(buffer.data(), true, 0, 0);
        file.writePageOnFile(0, buffer.data());
    }
};

TEST_F(BTreeCursorTest, InsertAndGetMultipleRecords)
{
    std::vector<std::pair<int, std::string>> records = {
        {1, "value1"},
        {2, "value-two"},
        {10, "value-003"}
    };

    // insert
    for (const auto &record : records)
    {
        char *value_ptr = const_cast<char *>(record.second.data());
        BTreeCursor::insert(*pool_, *index_file_, *heap_file_, record.first, value_ptr, record.second.size());
    }

    // read
    for (const auto &record : records)
    {
        char *stored = BTreeCursor::read(*pool_, *index_file_, *heap_file_, record.first);
        std::string restored(stored, record.second.size());
        EXPECT_EQ(record.second, restored);
    }
}

TEST_F(BTreeCursorTest, InsertDeleteThenFailToRead)
{
    const int key = 99;
    std::string payload = "transient";

    char *value_ptr = payload.data();
    BTreeCursor::insert(*pool_, *index_file_, *heap_file_, key, value_ptr, payload.size());

    // delete the record
    BTreeCursor::remove(*pool_, *index_file_, *heap_file_, key);

    // reading should now fail because the slot has been invalidated
    EXPECT_THROW({ BTreeCursor::read(*pool_, *index_file_, *heap_file_, key); }, std::runtime_error);
}

// TEST_F(BTreeCursorTest, InsertPageOverflow)
// {
//     std::mt19937 rng(0xC0FFEE);
//     std::uniform_int_distribution<int> key_dist(1, 1'000'000);
//     std::uniform_int_distribution<int> len_dist(16, 96);
//     std::unordered_set<int> used_keys;

//     const size_t max_attempts = 20000;

//     for (size_t attempt = 0; attempt < max_attempts; ++attempt)
//     {
//         int key;
//         do
//         {
//             key = key_dist(rng);
//         } while (!used_keys.insert(key).second);
//         std::cout << "Attempt " << attempt << ": Inserting key=" << key << std::endl;


//         const int payload_len = len_dist(rng);
//         std::string payload(payload_len, '\0');
//         for (char &ch : payload)
//         {
//             ch = static_cast<char>('a' + (rng() % 26));
//         }
//         EXPECT_NO_THROW(BTreeCursor::insert(*pool_, *index_file_, *heap_file_, key, payload.data(), payload.size()));
//     }
// }