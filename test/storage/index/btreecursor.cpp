#include "storage/index/btreecursor.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "storage/runtime/bufferpool.h"
#include "storage/runtime/file.h"
#include "storage/page/page.h"
#include "storage/index/index_key.h"
#include "storage/wal/wal.h"

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
    wal_ = WAL::initializeNew(wal_path_);
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
    auto page = std::make_unique<Page>(
        Page::initializeNew(buffer.data(), PageKind::LeafIndex, 0, 0));
    file.writePageFromBuffer(0, buffer.data());
  }
};

namespace {

std::string encodeCustomerPrimaryKey(int warehouse_id, int district_id,
                                     int customer_id) {
  std::string key;
  key.reserve(15);
  key.push_back('I');
  key += index_key::encodeInteger(warehouse_id);
  key.push_back('I');
  key += index_key::encodeInteger(district_id);
  key.push_back('I');
  key += index_key::encodeInteger(customer_id);
  return key;
}

}  // namespace

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
    BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                                 index_key::encodeInteger(key),
                                 heap_page_id, slot_id);
  }

  for (int key : keys) {
    auto rid =
        BTreeCursor::findRID(*pool_, *index_file_, index_key::encodeInteger(key));
    ASSERT_TRUE(rid.has_value());
    EXPECT_EQ(rid->heap_page_id, heap_page_id);
    EXPECT_EQ(rid->slot_id, slot_id);
  }
}

TEST_F(BTreeCursorTest, InsertManyKeysTriggersSplitAndIsSearchable) {
  const uint16_t heap_page_id = 7;
  const uint16_t slot_id = 9;

  const int initial_max_page = index_file_->getMaxPageID();

  const int num_keys = 500;
  for (int key = 0; key < num_keys; ++key) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                                 index_key::encodeInteger(key),
                                 heap_page_id, slot_id);
  }

  const int max_page_after = index_file_->getMaxPageID();
  EXPECT_GT(max_page_after, initial_max_page)
      << "index did not allocate any new pages";

  for (int key = 0; key < num_keys; ++key) {
    auto rid =
        BTreeCursor::findRID(*pool_, *index_file_, index_key::encodeInteger(key));
    ASSERT_TRUE(rid.has_value()) << "missing index entry for key=" << key;
    EXPECT_EQ(rid->heap_page_id, heap_page_id);
    EXPECT_EQ(rid->slot_id, slot_id);
  }
}

TEST_F(BTreeCursorTest, InsertCompositeKeysAcrossInternalSplitsRemainsSearchable) {
  const uint16_t heap_page_id = 11;

  const int num_keys = 30000;
  for (int customer_id = 1; customer_id <= num_keys; ++customer_id) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                                 encodeCustomerPrimaryKey(1, 7, customer_id),
                                 heap_page_id,
                                 static_cast<uint16_t>(customer_id % 65535));
  }

  for (int customer_id : {1, 2, 127, 4096, 16384, 2297, 30000}) {
    auto rid = BTreeCursor::findRID(*pool_, *index_file_,
                                    encodeCustomerPrimaryKey(1, 7, customer_id));
    ASSERT_TRUE(rid.has_value()) << "missing composite index entry for customer_id="
                                 << customer_id;
    EXPECT_EQ(rid->heap_page_id, heap_page_id);
  }
}