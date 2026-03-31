#include "../src/storage/page.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>
#include <optional>
#include <string>

#include "../src/storage/cell.h"
#include "../src/storage/index_page.h"
#include "../src/storage/record_cell.h"
#include "../src/storage/record_serializer.h"

namespace {

RecordSerializer serializeSingleVarcharRecord(const std::string& value) {
  static const Schema schema{
      "single_varchar_record",
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row{{value}};
  return RecordSerializer(schema, row);
}

}  // namespace

TEST(PageTest, InsertLeafPageAndFind) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);
  EXPECT_TRUE(page->isDirty());
  struct Entry {
    int key;
    uint16_t heap_page_id;
    uint16_t slot_id;
  } entries[] = {{11111, 999, 15}, {22222, 500, 2}, {33333, 123, 7}};

  for (int i = 0; i < static_cast<int>(std::size(entries)); ++i) {
    const Entry& entry = entries[i];
    auto slot_id_opt = page->insertCell(
        LeafCell(entry.key, entry.heap_page_id, entry.slot_id));
    ASSERT_TRUE(slot_id_opt.has_value());
    EXPECT_TRUE(page->isDirty());
    int slot_id = slot_id_opt.value();
    EXPECT_EQ(i, slot_id);

    LeafIndexPage leaf(*page);
    auto ref_opt = leaf.findRef(entry.key, false);
    ASSERT_TRUE(ref_opt.has_value());
    auto [heap_page_id, slot_id_ref] = ref_opt.value();
    EXPECT_EQ(entry.heap_page_id, heap_page_id);
    EXPECT_EQ(entry.slot_id, slot_id_ref);
  }
}

TEST(PageTest, TransferCellsToCompactsSourcePage) {
  std::array<char, Page::PAGE_SIZE_BYTE> src_data{};
  std::array<char, Page::PAGE_SIZE_BYTE> dst_data{};

  auto src_page = std::make_unique<Page>(src_data.data(), true, 0, 1);
  auto dst_page = std::make_unique<Page>(dst_data.data(), true, 0, 2);

  // Insert 4 keys: 1,2,3,4
  for (int key = 1; key <= 4; ++key) {
    auto slot_id_opt = src_page->insertCell(LeafCell(key, 0, 0));
    ASSERT_TRUE(slot_id_opt.has_value());
  }

  // Split with separate_key = 3: keys < 3 (1,2) should move to dst_page
  LeafCell separate_cell(3, 0, 0);
  std::vector<std::byte> separate_serialized = separate_cell.serialize();
  LeafIndexPage src_leaf(*src_page);
  LeafIndexPage dst_leaf(*dst_page);
  src_leaf.transferAndCompactTo(
      dst_leaf, reinterpret_cast<char*>(separate_serialized.data()));

  // Destination page should have keys 1 and 2
  EXPECT_TRUE(dst_leaf.hasKey(1));
  EXPECT_TRUE(dst_leaf.hasKey(2));
  EXPECT_TRUE(dst_leaf.hasKey(3));
  EXPECT_FALSE(dst_leaf.hasKey(4));

  // Source page should keep keys 3 and 4 only, and be compacted
  EXPECT_FALSE(src_leaf.hasKey(1));
  EXPECT_FALSE(src_leaf.hasKey(2));
  EXPECT_FALSE(src_leaf.hasKey(3));
  EXPECT_TRUE(src_leaf.hasKey(4));

  // Layout: byte 0 = node type, byte 1 = slot count.
  uint8_t src_slot_count = static_cast<uint8_t>(src_data[1]);
  EXPECT_EQ(1, src_slot_count);

  uint8_t dst_slot_count = static_cast<uint8_t>(dst_data[1]);
  EXPECT_EQ(3, dst_slot_count);

  EXPECT_TRUE(src_page->isDirty());
  EXPECT_TRUE(dst_page->isDirty());
}

// when page runs out of space, insertCell should return nullopt and not modify
// the page.
TEST(PageTest, InsertLeafPageRunsOutOfSpace) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);

  size_t successful_inserts = 0;
  bool saw_nullopt = false;
  const size_t max_attempts = Page::PAGE_SIZE_BYTE;

  for (size_t attempt = 0; attempt < max_attempts; ++attempt) {
    LeafCell cell(100000 + static_cast<int>(attempt),
                  static_cast<uint16_t>(attempt),
                  static_cast<uint16_t>(attempt));
    auto slot_id_opt = page->insertCell(cell);
    if (!slot_id_opt.has_value()) {
      saw_nullopt = true;
      break;
    }
    EXPECT_TRUE(page->isDirty());
    ++successful_inserts;
  }

  EXPECT_TRUE(saw_nullopt);
  EXPECT_GT(successful_inserts, 0u);
}

TEST(PageTest, InsertIntermediatePageAndFind) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  uint16_t right_most_child_page_id = 999;
  auto page = std::make_unique<Page>(page_data.data(), false,
                                     right_most_child_page_id, 1);
  EXPECT_TRUE(page->isDirty());
  struct Entry {
    int key;
    uint16_t page_id;
  } entries[] = {{10000, 63}, {30000, 21}, {20000, 42}};

  for (const Entry& entry : entries) {
    auto slot_id_opt =
        page->insertCell(IntermediateCell(entry.page_id, entry.key));
    ASSERT_TRUE(slot_id_opt.has_value());
    EXPECT_TRUE(page->isDirty());
  }

  InternalIndexPage internal(*page);

  for (const Entry& entry : entries) {
    uint16_t child_page = internal.findChildPage(entry.key);
    EXPECT_EQ(entry.page_id, child_page);
  }

  uint16_t child_page_for_large_key =
      internal.findChildPage(entries[2].key + 1);
  EXPECT_EQ(entries[1].page_id, child_page_for_large_key);

  uint16_t child_page_for_small_key =
      internal.findChildPage(entries[2].key - 1);
  EXPECT_EQ(entries[2].page_id, child_page_for_small_key);

  uint16_t child_page_for_largest_key =
      internal.findChildPage(entries[1].key + 1);
  EXPECT_EQ(right_most_child_page_id, child_page_for_largest_key);
}

// TODO: invalidation should be done via page API. we will implement later.
TEST(PageTest, InvalidateSlotSetsFlag) {
  auto assertInvalidation = [](bool is_leaf, const Cell& cell) {
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    auto page = std::make_unique<Page>(page_data.data(), is_leaf, 0, 1);
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

  auto assertSerializedInvalidation = [](bool is_leaf,
                                         const std::vector<std::byte>& bytes) {
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    auto page = std::make_unique<Page>(page_data.data(), is_leaf, 0, 1);
    auto slot_id_opt = page->insertCell(bytes);
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

  assertInvalidation(true, LeafCell(42, 100, 7));
  std::string payload = "page-record";
  RecordSerializer record = serializeSingleVarcharRecord(payload);
  assertSerializedInvalidation(true, record.serializedBytes());
}

TEST(PageTest, LeafSearchSkipsInvalidEntries) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);

  auto slot_id_opt = page->insertCell(LeafCell(123, 1, 1));
  page->invalidateSlot(slot_id_opt.value());

  LeafIndexPage leaf(*page);
  EXPECT_FALSE(leaf.hasKey(123));
  EXPECT_FALSE(leaf.findRef(123, false).has_value());
}

TEST(PageTest, LeafInsertInvalidateReuseSlot) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);
  EXPECT_TRUE(page->isDirty());

  auto first_slot = page->insertCell(LeafCell(1, 1, 1));
  ASSERT_TRUE(first_slot.has_value());
  page->invalidateSlot(first_slot.value());
  LeafIndexPage leaf(*page);
  EXPECT_FALSE(leaf.hasKey(1));

  auto second_slot = page->insertCell(LeafCell(1, 1, 1));
  ASSERT_TRUE(second_slot.has_value());
  EXPECT_TRUE(leaf.hasKey(1));

  auto ref = leaf.findRef(1, false);
  ASSERT_TRUE(ref.has_value());
  EXPECT_EQ(ref->first, 1);
  EXPECT_EQ(ref->second, 1);
}

TEST(PageTest, HeapInsertInvalidateReuseSlot) {
  std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
  auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);

  std::string payload = "heap-value";
  RecordSerializer first_record = serializeSingleVarcharRecord(payload);
  auto first_slot = page->insertCell(first_record.serializedBytes());
  ASSERT_TRUE(first_slot.has_value());
  page->invalidateSlot(first_slot.value());

  RecordSerializer second_record = serializeSingleVarcharRecord(payload);
  auto second_slot = page->insertCell(second_record.serializedBytes());
  ASSERT_TRUE(second_slot.has_value());
  EXPECT_NE(first_slot.value(), second_slot.value());

  char* cell_start = page->getXthSlotCellStart(second_slot.value());
  static const Schema schema{
      "single_varchar_record",
      std::vector<Column>{Column("value", Column::Type::Varchar)}};
  TypedRow row = RecordCellView(cell_start).getTypedRow(schema);
  ASSERT_EQ(row.values.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row.values[0]));
  EXPECT_EQ(std::get<Column::VarcharType>(row.values[0]), payload);
}

TEST(PageTest, SetPageLSNUpdatesHeaderAndMarksDirty)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);

    page->clearDirty();
    EXPECT_FALSE(page->isDirty());

    const std::uint64_t lsn = 42;
    page->setPageLSN(lsn);

    EXPECT_EQ(page->getPageLSN(), lsn);
    EXPECT_TRUE(page->isDirty());
}

TEST(PageTest, NewPageInitializesPageLSNToZero)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    auto page = std::make_unique<Page>(page_data.data(), true, 0, 1);

    EXPECT_EQ(page->getPageLSN(), 0u);
    EXPECT_TRUE(page->isDirty());
}