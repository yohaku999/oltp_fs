#include "storage/page/page.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/index_key.h"
#include "storage/index/index_page.h"
#include "storage/page/cell.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"

namespace {

RecordCell makeSingleVarcharRecord(const std::string& value) {
  static const Schema schema{
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row{{value}};
  return RecordSerializer::serialize(schema, row);
}

std::string encodeIntKey(int value) {
  return index_key::encodeFieldValue(
      FieldValue{static_cast<Column::IntegerType>(value)},
      Column::Type::Integer);
}

BTreeCursor::Boundary inclusiveBoundary(const std::string& key) {
  return BTreeCursor::Boundary{key, true};
}

std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> exactBoundaries(
    const std::string& key) {
  return {inclusiveBoundary(key), inclusiveBoundary(key)};
}

}  // namespace

TEST(PageTest, InsertLeafPageAndFind) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);
  EXPECT_TRUE(page->isDirty());
  struct Entry {
    int key;
    uint16_t heap_page_id;
    uint16_t slot_id;
  } entries[] = {{22222, 500, 2}, {11111, 999, 15}, {33333, 123, 7}};
  int expected_slot_ids[] = {0, 0, 2};

  for (int i = 0; i < static_cast<int>(std::size(entries)); ++i) {
    const Entry& entry = entries[i];
    auto slot_id_opt = page->insertCell(
        LeafCell(encodeIntKey(entry.key), entry.heap_page_id, entry.slot_id));
    ASSERT_TRUE(slot_id_opt.has_value());
    EXPECT_TRUE(page->isDirty());
    int slot_id = slot_id_opt.value();
    EXPECT_EQ(expected_slot_ids[i], slot_id);

    LeafIndexPage leaf(*page);
    const auto [left_boundary, right_boundary] =
        exactBoundaries(encodeIntKey(entry.key));
    auto [next_page, entries] =
        leaf.findEntries(left_boundary, right_boundary, false);
    EXPECT_EQ(LeafIndexPage::NO_RIGHT_SIBLING, next_page);
    ASSERT_FALSE(entries.empty());
    EXPECT_EQ(entry.heap_page_id, entries.front().rid.heap_page_id);
    EXPECT_EQ(entry.slot_id, entries.front().rid.slot_id);
  }

  LeafIndexPage leaf(*page);
  EXPECT_EQ(encodeIntKey(11111), leaf.cellAt(0).key());
  EXPECT_EQ(encodeIntKey(22222), leaf.cellAt(1).key());
  EXPECT_EQ(encodeIntKey(33333), leaf.cellAt(2).key());
}

TEST(PageTest, TransferCellsToCompactsSourcePage) {
  std::array<char, Page::PAGE_SIZE_BYTE> src_data{};
  std::array<char, Page::PAGE_SIZE_BYTE> dst_data{};

  auto src_page = std::make_unique<Page>(src_data.data(), PageKind::LeafIndex,
                                         LeafIndexPage::NO_RIGHT_SIBLING, 1);
  auto dst_page = std::make_unique<Page>(dst_data.data(), PageKind::LeafIndex,
                                         LeafIndexPage::NO_RIGHT_SIBLING, 2);

  for (int key = 1; key <= 4; ++key) {
    auto slot_id_opt = src_page->insertCell(LeafCell(encodeIntKey(key), 0, 0));
    ASSERT_TRUE(slot_id_opt.has_value());
  }

  // Use key 3 as the split boundary and verify that compaction leaves the
  // source and destination pages in the expected partitioned state.
  LeafIndexPage src_leaf(*src_page);
  LeafIndexPage dst_leaf(*dst_page);
  src_leaf.transferAndCompactTo(dst_leaf, 3);

  EXPECT_FALSE(dst_leaf.hasKey(encodeIntKey(1)));
  EXPECT_FALSE(dst_leaf.hasKey(encodeIntKey(2)));
  EXPECT_FALSE(dst_leaf.hasKey(encodeIntKey(3)));
  EXPECT_TRUE(dst_leaf.hasKey(encodeIntKey(4)));

  EXPECT_TRUE(src_leaf.hasKey(encodeIntKey(1)));
  EXPECT_TRUE(src_leaf.hasKey(encodeIntKey(2)));
  EXPECT_TRUE(src_leaf.hasKey(encodeIntKey(3)));
  EXPECT_FALSE(src_leaf.hasKey(encodeIntKey(4)));

  // Layout: byte 0 = node type, byte 1 = slot count.
  uint8_t src_slot_count = static_cast<uint8_t>(src_data[1]);
  EXPECT_EQ(3, src_slot_count);

  uint8_t dst_slot_count = static_cast<uint8_t>(dst_data[1]);
  EXPECT_EQ(1, dst_slot_count);

  EXPECT_TRUE(src_page->isDirty());
  EXPECT_TRUE(dst_page->isDirty());
}

// when page runs out of space, insertCell should return nullopt and not modify
// the page.
TEST(PageTest, InsertLeafPageRunsOutOfSpace) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);
  std::array<char, Page::PAGE_SIZE_BYTE> page_snapshot{};

  size_t successful_inserts = 0;
  bool saw_nullopt = false;
  const size_t max_attempts = Page::PAGE_SIZE_BYTE;

  for (size_t attempt = 0; attempt < max_attempts; ++attempt) {
    std::memcpy(page_snapshot.data(), page->data(), Page::PAGE_SIZE_BYTE);
    LeafCell cell(encodeIntKey(100000 + static_cast<int>(attempt)),
                  static_cast<uint16_t>(attempt),
                  static_cast<uint16_t>(attempt));
    auto slot_id_opt = page->insertCell(cell);
    if (!slot_id_opt.has_value()) {
      saw_nullopt = true;
      EXPECT_EQ(0, std::memcmp(page->data(), page_snapshot.data(),
                               Page::PAGE_SIZE_BYTE));
      break;
    }
    EXPECT_TRUE(page->isDirty());
    ++successful_inserts;
  }

  EXPECT_TRUE(saw_nullopt);
  EXPECT_GT(successful_inserts, 0u);
}

TEST(PageTest, InsertOversizedHeapRecordDoesNotCorruptPage) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::Heap, 0, 1);
  std::array<char, Page::PAGE_SIZE_BYTE> page_snapshot{};

  std::string oversized_payload(Page::PAGE_SIZE_BYTE, 'x');
  RecordCell record = makeSingleVarcharRecord(oversized_payload);

  std::memcpy(page_snapshot.data(), page->data(), Page::PAGE_SIZE_BYTE);
  auto slot_id_opt = page->insertCell(record);

  EXPECT_FALSE(slot_id_opt.has_value());
  EXPECT_EQ(
      0, std::memcmp(page->data(), page_snapshot.data(), Page::PAGE_SIZE_BYTE));
}

TEST(PageTest, InsertIntermediatePageAndFind) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  uint16_t right_most_child_page_id = 999;
  auto page = std::make_unique<Page>(page_data.data(), PageKind::InternalIndex,
                                     right_most_child_page_id, 1);
  EXPECT_TRUE(page->isDirty());
  struct Entry {
    int key;
    uint16_t page_id;
  } entries[] = {{10000, 63}, {30000, 21}, {20000, 42}};

  for (const Entry& entry : entries) {
    auto slot_id_opt = page->insertCell(
        IntermediateCell(entry.page_id, encodeIntKey(entry.key)));
    ASSERT_TRUE(slot_id_opt.has_value());
    EXPECT_TRUE(page->isDirty());
  }

  InternalIndexPage internal(*page);

  EXPECT_EQ(encodeIntKey(10000), internal.cellAt(0).key());
  EXPECT_EQ(encodeIntKey(20000), internal.cellAt(1).key());
  EXPECT_EQ(encodeIntKey(30000), internal.cellAt(2).key());

  for (const Entry& entry : entries) {
    uint16_t child_page = internal.findChildPage(encodeIntKey(entry.key));
    EXPECT_EQ(entry.page_id, child_page);
  }

  uint16_t child_page_for_large_key =
      internal.findChildPage(encodeIntKey(entries[2].key + 1));
  EXPECT_EQ(entries[1].page_id, child_page_for_large_key);

  uint16_t child_page_for_small_key =
      internal.findChildPage(encodeIntKey(entries[2].key - 1));
  EXPECT_EQ(entries[2].page_id, child_page_for_small_key);

  uint16_t child_page_for_largest_key =
      internal.findChildPage(encodeIntKey(entries[1].key + 1));
  EXPECT_EQ(right_most_child_page_id, child_page_for_largest_key);

  ASSERT_TRUE(internal.replaceChildPageId(entries[2].page_id, 77));
  EXPECT_EQ(77, internal.findChildPage(encodeIntKey(entries[2].key - 1)));

  ASSERT_TRUE(internal.replaceChildPageId(right_most_child_page_id, 88));
  EXPECT_EQ(88, internal.findChildPage(encodeIntKey(entries[1].key + 1)));
}

TEST(PageTest, FindChildPageUsesLastMatchingSeparator) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::InternalIndex,
                                     999, 1);

  ASSERT_TRUE(page->insertCell(IntermediateCell(10, encodeIntKey(100))));
  ASSERT_TRUE(page->insertCell(IntermediateCell(20, encodeIntKey(200))));
  ASSERT_TRUE(page->insertCell(IntermediateCell(21, encodeIntKey(200))));
  ASSERT_TRUE(page->insertCell(IntermediateCell(30, encodeIntKey(300))));

  InternalIndexPage internal(*page);
  EXPECT_EQ(10, internal.findChildPage(encodeIntKey(50)));
  EXPECT_EQ(20, internal.findChildPage(encodeIntKey(150)));
  EXPECT_EQ(21, internal.findChildPage(encodeIntKey(200)));
  EXPECT_EQ(30, internal.findChildPage(encodeIntKey(250)));
  EXPECT_EQ(999, internal.findChildPage(encodeIntKey(350)));
}

TEST(PageTest, FindChildPageSkipsInvalidSeparators) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::InternalIndex,
                                     999, 1);

  ASSERT_TRUE(page->insertCell(IntermediateCell(10, encodeIntKey(100))));
  auto key_200_slot =
      page->insertCell(IntermediateCell(20, encodeIntKey(200)));
  ASSERT_TRUE(key_200_slot.has_value());
  ASSERT_TRUE(page->insertCell(IntermediateCell(30, encodeIntKey(300))));
  auto key_400_slot =
      page->insertCell(IntermediateCell(40, encodeIntKey(400)));
  ASSERT_TRUE(key_400_slot.has_value());
  ASSERT_TRUE(page->insertCell(IntermediateCell(50, encodeIntKey(500))));

  page->invalidateSlot(key_200_slot.value());
  page->invalidateSlot(key_400_slot.value());
  ASSERT_TRUE(page->insertCell(IntermediateCell(15, encodeIntKey(150))));

  InternalIndexPage internal(*page);
  EXPECT_EQ(10, internal.findChildPage(encodeIntKey(50)));
  EXPECT_EQ(15, internal.findChildPage(encodeIntKey(150)));
  EXPECT_EQ(30, internal.findChildPage(encodeIntKey(200)));
  EXPECT_EQ(30, internal.findChildPage(encodeIntKey(300)));
  EXPECT_EQ(50, internal.findChildPage(encodeIntKey(400)));
  EXPECT_EQ(999, internal.findChildPage(encodeIntKey(600)));
}

TEST(PageTest, InvalidateSlotSetsFlag) {
  // Verify invalidation through the public Page API for index and record cells.
  auto assertInvalidation = [](PageKind kind, const Cell& cell) {
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    auto page = std::make_unique<Page>(page_data.data(), kind, 0, 1);
    auto slot_id_opt = page->insertCell(cell);
    ASSERT_TRUE(slot_id_opt.has_value());
    uint16_t slot_id = slot_id_opt.value();

    uint16_t cell_offset = 0;
    std::memcpy(&cell_offset,
                page_data.data() + Page::HEADDER_SIZE_BYTE +
                    Page::CELL_POINTER_SIZE * slot_id,
                sizeof(uint16_t));
    char* cell_data = page_data.data() + cell_offset;
    ASSERT_TRUE(Cell::isValid(cell_data));

    page->invalidateSlot(slot_id);
    EXPECT_TRUE(page->isDirty());

    EXPECT_FALSE(Cell::isValid(cell_data));
  };

  assertInvalidation(PageKind::LeafIndex, LeafCell(encodeIntKey(42), 100, 7));
  std::string payload = "page-record";
  RecordCell record = makeSingleVarcharRecord(payload);
  assertInvalidation(PageKind::Heap, record);
}

TEST(PageTest, InvalidateSlotMarksPreviouslyCleanPageDirty) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);

  auto slot_id_opt = page->insertCell(LeafCell(encodeIntKey(123), 4, 5));
  ASSERT_TRUE(slot_id_opt.has_value());

  page->clearDirty();
  EXPECT_FALSE(page->isDirty());

  page->invalidateSlot(slot_id_opt.value());

  EXPECT_TRUE(page->isDirty());

  LeafIndexPage leaf(*page);
  const auto [left_boundary, right_boundary] =
      exactBoundaries(encodeIntKey(123));
  auto [next_page, entries] =
      leaf.findEntries(left_boundary, right_boundary, false);
  EXPECT_EQ(LeafIndexPage::NO_RIGHT_SIBLING, next_page);
  EXPECT_TRUE(entries.empty());
}

TEST(PageTest, LeafSearchSkipsInvalidEntries) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);

  auto slot_id_opt = page->insertCell(LeafCell(encodeIntKey(123), 1, 1));
  page->invalidateSlot(slot_id_opt.value());

  LeafIndexPage leaf(*page);
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(123)));
  const auto [left_boundary, right_boundary] =
      exactBoundaries(encodeIntKey(123));
  auto [next_page, entries] =
      leaf.findEntries(left_boundary, right_boundary, false);
  EXPECT_EQ(LeafIndexPage::NO_RIGHT_SIBLING, next_page);
  EXPECT_TRUE(entries.empty());
}

TEST(PageTest, LeafHasKeyUsesBinarySearchAcrossInvalidSlots) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);
  LeafIndexPage leaf(*page);
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(10)));

  ASSERT_TRUE(page->insertCell(LeafCell(encodeIntKey(10), 1, 1)));
  auto key_20_slot = page->insertCell(LeafCell(encodeIntKey(20), 1, 2));
  ASSERT_TRUE(key_20_slot.has_value());
  ASSERT_TRUE(page->insertCell(LeafCell(encodeIntKey(30), 1, 3)));
  auto key_40_slot = page->insertCell(LeafCell(encodeIntKey(40), 1, 4));
  ASSERT_TRUE(key_40_slot.has_value());
  ASSERT_TRUE(page->insertCell(LeafCell(encodeIntKey(50), 1, 5)));

  page->invalidateSlot(key_20_slot.value());
  page->invalidateSlot(key_40_slot.value());
  ASSERT_TRUE(page->insertCell(LeafCell(encodeIntKey(15), 1, 6)));

  EXPECT_TRUE(leaf.hasKey(encodeIntKey(10)));
  EXPECT_TRUE(leaf.hasKey(encodeIntKey(15)));
  EXPECT_TRUE(leaf.hasKey(encodeIntKey(30)));
  EXPECT_TRUE(leaf.hasKey(encodeIntKey(50)));
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(20)));
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(25)));
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(40)));
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(60)));
}

TEST(PageTest, LeafInsertInvalidateReuseSlot) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::LeafIndex,
                                     LeafIndexPage::NO_RIGHT_SIBLING, 1);
  EXPECT_TRUE(page->isDirty());

  auto first_slot = page->insertCell(LeafCell(encodeIntKey(1), 1, 1));
  ASSERT_TRUE(first_slot.has_value());
  page->invalidateSlot(first_slot.value());
  LeafIndexPage leaf(*page);
  EXPECT_FALSE(leaf.hasKey(encodeIntKey(1)));

  auto second_slot = page->insertCell(LeafCell(encodeIntKey(1), 1, 1));
  ASSERT_TRUE(second_slot.has_value());
  EXPECT_TRUE(leaf.hasKey(encodeIntKey(1)));

  const auto [left_boundary, right_boundary] = exactBoundaries(encodeIntKey(1));
  auto [next_page, entries] =
      leaf.findEntries(left_boundary, right_boundary, false);
  EXPECT_EQ(LeafIndexPage::NO_RIGHT_SIBLING, next_page);
  ASSERT_FALSE(entries.empty());
  EXPECT_EQ(entries.front().rid.heap_page_id, 1);
  EXPECT_EQ(entries.front().rid.slot_id, 1);
}

TEST(PageTest, HeapInsertInvalidateReuseSlot) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::Heap, 0, 1);

  std::string payload = "heap-value";
  RecordCell first_record = makeSingleVarcharRecord(payload);
  auto first_slot = page->insertCell(first_record);
  ASSERT_TRUE(first_slot.has_value());
  page->invalidateSlot(first_slot.value());

  RecordCell second_record = makeSingleVarcharRecord(payload);
  auto second_slot = page->insertCell(second_record);
  ASSERT_TRUE(second_slot.has_value());
  EXPECT_NE(first_slot.value(), second_slot.value());

  char* cell_start = page->getSlotCellStart(second_slot.value());
  static const Schema schema{
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row = RecordCellView(cell_start).getTypedRow(schema);
  ASSERT_EQ(row.values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row.values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(row.values[0]), payload);
}

TEST(PageTest, UpdateFixedRecordRegionPreservesOtherFields) {
  const Schema schema(std::vector<Column>{
      Column("id", Column::Type::Integer),
      Column("note", Column::Type::Varchar),
      Column("amount", Column::Type::Double),
      Column("count", Column::Type::Integer),
  });
  const TypedRow original_row{{Column::IntegerType{1}, std::string("original"),
                               Column::DoubleType{10.5},
                               Column::IntegerType{2}}};
  const TypedRow after_row{{Column::IntegerType{999}, std::string("changed"),
                            Column::DoubleType{12.5}, Column::IntegerType{7}}};

  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  Page page(page_data.data(), PageKind::Heap, 0, 1);
  const auto slot =
      page.insertCell(RecordSerializer::serialize(schema, original_row));
  ASSERT_TRUE(slot.has_value());
  page.clearDirty();

  const std::size_t amount_offset =
      RecordCellLayout::HEADER_SIZE_BYTE + sizeof(Column::IntegerType);
  page.updateFixedRecordRegion(
      *slot, RecordSerializer::serialize(schema, after_row), amount_offset,
      sizeof(Column::DoubleType) + sizeof(Column::IntegerType));

  const TypedRow stored_row =
      RecordCellView(page.getSlotCellStart(*slot)).getTypedRow(schema);
  EXPECT_EQ(std::get<Column::IntegerType>(stored_row.values[0]), 1);
  EXPECT_EQ(std::get<Column::VarcharType>(stored_row.values[1]), "original");
  EXPECT_DOUBLE_EQ(std::get<Column::DoubleType>(stored_row.values[2]), 12.5);
  EXPECT_EQ(std::get<Column::IntegerType>(stored_row.values[3]), 7);
  EXPECT_TRUE(page.isDirty());
}

TEST(PageTest, UpdateFixedRecordRegionCanChangeNullState) {
  const Schema schema(std::vector<Column>{
      Column("id", Column::Type::Integer),
      Column("count", Column::Type::Integer),
      Column("note", Column::Type::Varchar),
  });
  const TypedRow original_row{
      {Column::IntegerType{1}, std::monostate{}, std::string("original")}};
  const TypedRow after_row{
      {Column::IntegerType{1}, Column::IntegerType{8}, std::string("changed")}};

  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  Page page(page_data.data(), PageKind::Heap, 0, 1);
  const auto slot =
      page.insertCell(RecordSerializer::serialize(schema, original_row));
  ASSERT_TRUE(slot.has_value());

  const std::size_t fixed_region_end =
      RecordCellLayout::HEADER_SIZE_BYTE + 2 * sizeof(Column::IntegerType);
  page.updateFixedRecordRegion(
      *slot, RecordSerializer::serialize(schema, after_row),
      RecordCellLayout::NULL_BITMAP_OFFSET,
      fixed_region_end - RecordCellLayout::NULL_BITMAP_OFFSET);

  const TypedRow stored_row =
      RecordCellView(page.getSlotCellStart(*slot)).getTypedRow(schema);
  EXPECT_EQ(std::get<Column::IntegerType>(stored_row.values[0]), 1);
  EXPECT_EQ(std::get<Column::IntegerType>(stored_row.values[1]), 8);
  EXPECT_EQ(std::get<Column::VarcharType>(stored_row.values[2]), "original");
}

TEST(PageTest, SetPageLSNUpdatesHeaderAndMarksDirty) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::Heap, 0, 1);

  page->clearDirty();
  EXPECT_FALSE(page->isDirty());

  const std::uint64_t lsn = 42;
  page->setPageLSN(lsn);

  EXPECT_EQ(page->getPageLSN(), lsn);
  EXPECT_TRUE(page->isDirty());
}

TEST(PageTest, NewPageInitializesPageLSNToZero) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), PageKind::Heap, 0, 1);

  EXPECT_EQ(page->getPageLSN(), 0u);
  EXPECT_TRUE(page->isDirty());
}
TEST(PageTest, KindRoundTripsForAllPageKinds) {
  std::array<char, Page::PAGE_SIZE_BYTE> heap_data{};
  std::array<char, Page::PAGE_SIZE_BYTE> leaf_data{};
  std::array<char, Page::PAGE_SIZE_BYTE> internal_data{};

  Page heap(heap_data.data(), PageKind::Heap, 0, 1);
  Page leaf(leaf_data.data(), PageKind::LeafIndex, 0, 2);
  Page internal(internal_data.data(), PageKind::InternalIndex, 0, 3);

  EXPECT_EQ(heap.kind(), PageKind::Heap);
  EXPECT_EQ(leaf.kind(), PageKind::LeafIndex);
  EXPECT_EQ(internal.kind(), PageKind::InternalIndex);
  EXPECT_TRUE(heap.isLeaf());
  EXPECT_TRUE(leaf.isLeaf());
  EXPECT_FALSE(internal.isLeaf());

  Page wrapped_heap(heap_data.data(), 1);
  Page wrapped_leaf(leaf_data.data(), 2);
  Page wrapped_internal(internal_data.data(), 3);

  EXPECT_EQ(wrapped_heap.kind(), PageKind::Heap);
  EXPECT_EQ(wrapped_leaf.kind(), PageKind::LeafIndex);
  EXPECT_EQ(wrapped_internal.kind(), PageKind::InternalIndex);
}
