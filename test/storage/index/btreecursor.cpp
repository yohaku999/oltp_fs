#include "storage/index/btreecursor.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "storage/buffer/bufferpool.h"
#include "storage/disk/file.h"
#include "storage/index/index_key.h"
#include "storage/index/index_page.h"
#include "storage/index/leaf_cell.h"
#include "storage/page/page.h"
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
        Page::initializeNew(buffer.data(), PageKind::LeafIndex,
                            LeafIndexPage::NO_RIGHT_SIBLING, 0));
    file.writePageFromBuffer(0, buffer.data());
  }
};

namespace {

std::string encodeIntKey(int value) {
  return index_key::encodeFieldValue(
      FieldValue{static_cast<Column::IntegerType>(value)},
      Column::Type::Integer);
}

std::string encodeVarcharKey(const std::string& value) {
  return index_key::encodeFieldValue(FieldValue{value}, Column::Type::Varchar);
}

std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> boundaries(
    const std::string& left_key, bool left_is_inclusive,
    const std::string& right_key, bool right_is_inclusive) {
  return {BTreeCursor::Boundary{left_key, left_is_inclusive},
          BTreeCursor::Boundary{right_key, right_is_inclusive}};
}

std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> exactBoundary(
    const std::string& key) {
  return boundaries(key, true, key, true);
}

std::vector<RID> entryRIDs(const std::vector<IndexEntry>& entries) {
  std::vector<RID> rids;
  rids.reserve(entries.size());
  for (const IndexEntry& entry : entries) {
    rids.push_back(entry.rid);
  }
  return rids;
}

std::vector<RID> findIntRIDs(BufferPool& pool, File& index_file, int key) {
  return entryRIDs(BTreeCursor::findEntries(
      pool, index_file, exactBoundary(encodeIntKey(key)), false));
}

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

std::string encodeOrderLinePrimaryKeyPrefix(int warehouse_id, int district_id,
                                            int order_id) {
  std::string key;
  key.reserve(15);
  key.push_back('I');
  key += index_key::encodeInteger(warehouse_id);
  key.push_back('I');
  key += index_key::encodeInteger(district_id);
  key.push_back('I');
  key += index_key::encodeInteger(order_id);
  return key;
}

std::string encodeOrderLinePrimaryKey(int warehouse_id, int district_id,
                                      int order_id, int line_number) {
  std::string key =
      encodeOrderLinePrimaryKeyPrefix(warehouse_id, district_id, order_id);
  key.push_back('I');
  key += index_key::encodeInteger(line_number);
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
    BTreeCursor::insertIntoIndex(*pool_, *index_file_, encodeIntKey(key),
                                 heap_page_id, slot_id);
  }

  for (int key : keys) {
    auto rids = findIntRIDs(*pool_, *index_file_, key);
    ASSERT_FALSE(rids.empty());
    EXPECT_EQ(rids.front().heap_page_id, heap_page_id);
    EXPECT_EQ(rids.front().slot_id, slot_id);
  }
}

TEST_F(BTreeCursorTest, InsertManyKeysTriggersSplitAndIsSearchable) {
  const uint16_t heap_page_id = 7;

  const int initial_max_page = index_file_->getMaxPageID();

  const int num_keys = 500;
  for (int key = 0; key < num_keys; ++key) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_, encodeIntKey(key),
                                 heap_page_id, static_cast<uint16_t>(key));
  }

  const int max_page_after = index_file_->getMaxPageID();
  EXPECT_GT(max_page_after, initial_max_page)
      << "index did not allocate any new pages";

  for (int key = 0; key < num_keys; ++key) {
    auto rids = findIntRIDs(*pool_, *index_file_, key);
    ASSERT_FALSE(rids.empty()) << "missing index entry for key=" << key;
    EXPECT_EQ(rids.front().heap_page_id, heap_page_id);
    EXPECT_EQ(rids.front().slot_id, key);
  }

  std::vector<IndexEntry> entries = BTreeCursor::findEntries(
      *pool_, *index_file_, boundaries("", true, "", true), false);
  ASSERT_EQ(entries.size(), static_cast<std::size_t>(num_keys));
  for (int key = 0; key < num_keys; ++key) {
    EXPECT_EQ(entries[static_cast<std::size_t>(key)].key, encodeIntKey(key));
    EXPECT_EQ(entries[static_cast<std::size_t>(key)].rid.heap_page_id,
              heap_page_id);
    EXPECT_EQ(entries[static_cast<std::size_t>(key)].rid.slot_id, key);
  }
}

TEST_F(BTreeCursorTest, FindEntriesSupportsRangeOperatorsWithinLeaf) {
  for (int key = 1; key <= 5; ++key) {
    BTreeCursor::insertIntoIndex(*pool_, *index_file_, encodeIntKey(key),
                                 static_cast<uint16_t>(key), 1);
  }

  auto heap_page_ids = [](const std::vector<RID>& rids) {
    std::vector<uint16_t> values;
    values.reserve(rids.size());
    for (const RID& rid : rids) {
      values.push_back(rid.heap_page_id);
    }
    return values;
  };

  EXPECT_EQ((std::vector<uint16_t>{3, 4, 5}),
            heap_page_ids(entryRIDs(BTreeCursor::findEntries(
                *pool_, *index_file_,
                boundaries(encodeIntKey(2), false, "", true), false))));
  EXPECT_EQ((std::vector<uint16_t>{3, 4, 5}),
            heap_page_ids(entryRIDs(BTreeCursor::findEntries(
                *pool_, *index_file_,
                boundaries(encodeIntKey(3), true, "", true), false))));
  EXPECT_EQ((std::vector<uint16_t>{1, 2}),
            heap_page_ids(entryRIDs(BTreeCursor::findEntries(
                *pool_, *index_file_,
                boundaries("", true, encodeIntKey(3), false), false))));
  EXPECT_EQ((std::vector<uint16_t>{1, 2}),
            heap_page_ids(entryRIDs(BTreeCursor::findEntries(
                *pool_, *index_file_,
                boundaries("", true, encodeIntKey(2), true), false))));
}

TEST_F(BTreeCursorTest, FindEntriesSupportsCompositePrefixEquality) {
  BTreeCursor::insertIntoIndex(
      *pool_, *index_file_, encodeOrderLinePrimaryKey(1, 7, 3000, 15), 10, 1);
  BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                               encodeOrderLinePrimaryKey(1, 7, 3001, 1), 11, 1);
  BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                               encodeOrderLinePrimaryKey(1, 7, 3001, 2), 12, 1);
  BTreeCursor::insertIntoIndex(*pool_, *index_file_,
                               encodeOrderLinePrimaryKey(1, 7, 3002, 1), 13, 1);

  std::vector<RID> rids = entryRIDs(BTreeCursor::findEntries(
      *pool_, *index_file_,
      exactBoundary(encodeOrderLinePrimaryKeyPrefix(1, 7, 3001)), false));

  ASSERT_EQ(rids.size(), 2u);
  EXPECT_EQ(rids[0].heap_page_id, 11);
  EXPECT_EQ(rids[1].heap_page_id, 12);
}

TEST_F(BTreeCursorTest, InsertCompactsFullyInvalidLeafBeforeSplitting) {
  Page* root_page = pool_->pinPage(index_file_->getRootPageID(), *index_file_);
  int inserted_stale_keys = 0;
  while (true) {
    const std::string key =
        encodeVarcharKey("stale_key_" + std::string(96, 'x') +
                         std::to_string(inserted_stale_keys));
    LeafCell cell(key, 1, static_cast<uint16_t>(inserted_stale_keys));
    std::optional<int> slot_id = root_page->insertCell(cell);
    if (!slot_id.has_value()) {
      break;
    }

    root_page->invalidateSlot(static_cast<uint16_t>(slot_id.value()));
    ++inserted_stale_keys;
  }
  pool_->unpinPage(root_page, *index_file_);
  ASSERT_GT(inserted_stale_keys, 0);

  const std::string live_value = "live_key_" + std::string(96, 'y');
  const std::string live_key = encodeVarcharKey(live_value);
  BTreeCursor::insertIntoIndex(*pool_, *index_file_, live_key, 7, 9);

  std::vector<RID> rids = entryRIDs(BTreeCursor::findEntries(
      *pool_, *index_file_, exactBoundary(live_key), false));
  ASSERT_FALSE(rids.empty());
  EXPECT_EQ(rids.front().heap_page_id, 7);
  EXPECT_EQ(rids.front().slot_id, 9);
}

TEST_F(BTreeCursorTest,
       InsertCompositeKeysAcrossInternalSplitsRemainsSearchable) {
  const uint16_t heap_page_id = 11;

  const int num_keys = 30000;
  for (int customer_id = 1; customer_id <= num_keys; ++customer_id) {
    BTreeCursor::insertIntoIndex(
        *pool_, *index_file_, encodeCustomerPrimaryKey(1, 7, customer_id),
        heap_page_id, static_cast<uint16_t>(customer_id % 65535));
  }

  for (int customer_id : {1, 2, 127, 4096, 16384, 2297, 30000}) {
    auto rids = entryRIDs(BTreeCursor::findEntries(
        *pool_, *index_file_,
        exactBoundary(encodeCustomerPrimaryKey(1, 7, customer_id)), false));
    ASSERT_FALSE(rids.empty())
        << "missing composite index entry for customer_id=" << customer_id;
    EXPECT_EQ(rids.front().heap_page_id, heap_page_id);
  }
}
