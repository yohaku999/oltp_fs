#include "../src/storage/btreecursor.h"
#include "../src/storage/bufferpool.h"
#include "../src/storage/file.h"
#include "../src/storage/page.h"
#include "../src/storage/lsn_allocator.h"
#include "../src/storage/wal/wal.h"
#include <array>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>

class BTreeCursorTest : public ::testing::Test
{
protected:
    std::unique_ptr<BufferPool> pool_;
    std::unique_ptr<File> index_file_;
    std::unique_ptr<File> heap_file_;
    std::unique_ptr<LSNAllocator> lsn_;
    std::unique_ptr<WAL> wal_;
    const std::string index_path_ = "btreecursor_test.index";
    const std::string heap_path_ = "btreecursor_test.db";
    const std::string wal_path_ = "btreecursor_test.wal";

    void SetUp() override
    {
        pool_ = std::make_unique<BufferPool>();
        std::remove(index_path_.c_str());
        std::remove(heap_path_.c_str());
        std::remove(wal_path_.c_str());
        index_file_ = std::make_unique<File>(index_path_);
        heap_file_ = std::make_unique<File>(heap_path_);
        lsn_ = std::make_unique<LSNAllocator>(0);
        wal_ = std::make_unique<WAL>(wal_path_);
        initializeLeafPage(*index_file_);
        initializeLeafPage(*heap_file_);
    }

    void TearDown() override
    {
        index_file_.reset();
        heap_file_.reset();
        wal_.reset();
        lsn_.reset();
        std::remove(index_path_.c_str());
        std::remove(heap_path_.c_str());
        std::remove(wal_path_.c_str());
    }

    std::size_t walFileSize() const
    {
        struct stat st;
        if (stat(wal_path_.c_str(), &st) != 0)
        {
            return 0;
        }
        return static_cast<std::size_t>(st.st_size);
    }

    static void initializeLeafPage(File &file)
    {
        std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
        auto page = std::make_unique<Page>(buffer.data(), true, 0, 0);
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
        BTreeCursor::insert(*pool_, *index_file_, *heap_file_,
                           record.first, value_ptr, record.second.size(),
                           *lsn_, *wal_);
    }

    // read
    for (const auto &record : records)
    {
        char *stored = BTreeCursor::read(*pool_, *index_file_, *heap_file_, record.first);
        std::string restored(stored, record.second.size());
        EXPECT_EQ(record.second, restored);
    }

    // Just verify that INSERTs generated some WAL records on disk for now.
    wal_->flush();
    EXPECT_GT(walFileSize(), 0u);
}

TEST_F(BTreeCursorTest, InsertDeleteThenFailToRead)
{
    const int key = 99;
    std::string payload = "transient";

    char *value_ptr = payload.data();
    BTreeCursor::insert(*pool_, *index_file_, *heap_file_, key, value_ptr, payload.size(),
                       *lsn_, *wal_);

    BTreeCursor::remove(*pool_, *index_file_, *heap_file_, key, *lsn_, *wal_);

    // reading should now fail because the slot has been invalidated
    EXPECT_THROW({ BTreeCursor::read(*pool_, *index_file_, *heap_file_, key); }, std::runtime_error);
}

TEST_F(BTreeCursorTest, UpdateReplacesExistingValue)
{
    const int key = 123;
    std::string initial = "initial-value";
    std::string updated = "updated-value";

    char* initial_ptr = initial.data();
    BTreeCursor::insert(*pool_, *index_file_, *heap_file_, key, initial_ptr, initial.size(),
                       *lsn_, *wal_);

    char* update_ptr = updated.data();
    BTreeCursor::update(*pool_, *index_file_, *heap_file_, key, update_ptr, updated.size(),
                       *lsn_, *wal_);

    char* stored = BTreeCursor::read(*pool_, *index_file_, *heap_file_, key);
    std::string restored(stored, updated.size());
    EXPECT_EQ(updated, restored);
}

TEST_F(BTreeCursorTest, UpdateNonExistingKeyThrows)
{
    const int key = 777;
    std::string payload = "does-not-exist";

    char* value_ptr = payload.data();
    EXPECT_THROW(
        {
            BTreeCursor::update(*pool_, *index_file_, *heap_file_, key, value_ptr, payload.size(),
                               *lsn_, *wal_);
        },
        std::runtime_error);
}

TEST_F(BTreeCursorTest, InsertPageOverflow)
{
    try
    {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> key_dist(1, 1'000'000);
        std::uniform_int_distribution<int> len_dist(16, 96);
        std::unordered_set<int> used_keys;
        std::unordered_map<int, std::string> expected;

        const size_t max_attempts = 1000;

        for (size_t attempt = 0; attempt < max_attempts; ++attempt)
        {
            int key;
            do
            {
                key = key_dist(rng);
            } while (!used_keys.insert(key).second);
            std::cout << "Attempt " << attempt << ": Inserting key=" << key << std::endl;

            const int payload_len = len_dist(rng);
            std::string payload(payload_len, '\0');
            for (char &ch : payload)
            {
                ch = static_cast<char>('a' + (rng() % 26));
            }
            BTreeCursor::insert(*pool_, *index_file_, *heap_file_, key, payload.data(), payload.size(),
                               *lsn_, *wal_);
            expected.emplace(key, payload);
        }

        std::cout << "Start Verifying inserted records..." << std::endl;
        // Verify that all inserted records can be read back correctly.
        for (const auto& [key, value] : expected)
        {
            char* stored = BTreeCursor::read(*pool_, *index_file_, *heap_file_, key);
            std::string restored(stored, value.size());
            if (restored != value)
            {
                std::cout << "Mismatch detected for key=" << key << ". Dumping B+tree index state..." << std::endl;
                BTreeCursor::dumpTree(*pool_, *index_file_, std::cout);
            }
            EXPECT_EQ(value, restored) << "mismatch for key=" << key;
        }
    }
    catch (const std::exception& ex)
    {
        std::cout << "InsertPageOverflow test threw exception: " << ex.what() << std::endl;
        std::cout << "Dumping B+tree index state after test failure..." << std::endl;
        BTreeCursor::dumpTree(*pool_, *index_file_, std::cout);
        throw;
    }
}