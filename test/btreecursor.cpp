#include "../src/storage/btreecursor.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "../src/storage/bufferpool.h"
#include "../src/storage/file.h"
#include "../src/storage/wal/wal.h"
#include "../src/storage/page.h"

class BTreeCursorTest : public ::testing::Test {
 protected:
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<File> index_file_;
  std::unique_ptr<WAL> wal_;
  const std::string index_path_ = "btreecursor_test.index";
  const std::string wal_path_ = "btreecursor_test.wal";

  void SetUp() override {
    std::remove(index_path_.c_str());
    std::remove(wal_path_.c_str());
    wal_ = std::make_unique<WAL>(wal_path_);
    pool_ = std::make_unique<BufferPool>(*wal_);
    index_file_ = std::make_unique<File>(index_path_);
    initializeLeafPage(*index_file_);
  }

  void TearDown() override {
    index_file_.reset();
    wal_.reset();
    std::remove(index_path_.c_str());
    std::remove(wal_path_.c_str());
  }

  static void initializeLeafPage(File& file) {
    std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
    auto page = std::make_unique<Page>(buffer.data(), true, 0, 0);
    file.writePageOnFile(0, buffer.data());
  }
};

// NOTE: Missing WAL/pageLSN invariants tests
//
// At this stage we intentionally do not test that the WAL record's payload
// (RecordType, page_id, slot_id, body bytes) matches the logical operation.
//
// Reason: the current WAL API does not expose a stable way to inspect the
// last written WALRecord from tests without coupling BTreeCursorTest tightly
// to WAL's internal serialization format. For now we rely on higher-level
// tests (functional correctness + WAL file growth) and will add more precise
// pageLSN/WAL invariants once the recovery path and WAL inspection helpers
// are in place.

TEST_F(BTreeCursorTest, InsertIntoIndexAndFindRecordLocation) {
  const uint16_t heap_page_id = 5;
  const uint16_t slot_id = 3;

  std::vector<int> keys = {1, 42, 100};

  for (int key : keys) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_, key, heap_page_id,
                                 slot_id);
  }

  for (int key : keys) {
    auto location = BTreeCursor::findRecordLocation(*pool_, *index_file_, key);
    ASSERT_TRUE(location.has_value());
    EXPECT_EQ(location->first, heap_page_id);
    EXPECT_EQ(location->second, slot_id);
  }
}

TEST_F(BTreeCursorTest, InsertManyKeysTriggersSplitAndIsSearchable) {
  const uint16_t heap_page_id = 7;
  const uint16_t slot_id = 9;

  const int initial_max_page = index_file_->getMaxPageID();

  const int num_keys = 500;
  for (int key = 0; key < num_keys; ++key) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_, key, heap_page_id,
                                 slot_id);
  }

  const int max_page_after = index_file_->getMaxPageID();
  EXPECT_GT(max_page_after, initial_max_page)
      << "index did not allocate any new pages";

  for (int key = 0; key < num_keys; ++key) {
    auto location = BTreeCursor::findRecordLocation(*pool_, *index_file_, key);
    ASSERT_TRUE(location.has_value()) << "missing index entry for key=" << key;
    EXPECT_EQ(location->first, heap_page_id);
    EXPECT_EQ(location->second, slot_id);
  }
}