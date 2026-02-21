#include "../src/page.h"
#include "../src/cell.h"
#include "../src/record_cell.h"
#include <array>
#include <optional>
#include <cstring>
#include <string>
#include <gtest/gtest.h>

TEST(PageTest, InsertLeafPageAndFind)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    Page *page = Page::initializePage(page_data.data(), true, 0);
    struct Entry
    {
        int key;
        uint16_t heap_page_id;
        uint16_t slot_id;
    } entries[] = {
        {11111, 999, 15},
        {22222, 500, 2},
        {33333, 123, 7}
    };

    for (int i = 0; i < static_cast<int>(std::size(entries)); ++i)
    {
        const Entry &entry = entries[i];
        auto slot_id_opt = page->insertCell(LeafCell(entry.key, entry.heap_page_id, entry.slot_id));
        ASSERT_TRUE(slot_id_opt.has_value());
        int slot_id = slot_id_opt.value();
        EXPECT_EQ(i, slot_id);

        auto ref_opt = page->findLeafRef(entry.key);
        ASSERT_TRUE(ref_opt.has_value());
        auto [heap_page_id, slot_id_ref] = ref_opt.value();
        EXPECT_EQ(entry.heap_page_id, heap_page_id);
        EXPECT_EQ(entry.slot_id, slot_id_ref);
    }
}

TEST(PageTest, InsertLeafPageRunsOutOfSpace)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    Page *page = Page::initializePage(page_data.data(), true, 0);

    size_t successful_inserts = 0;
    bool saw_nullopt = false;
    const size_t max_attempts = Page::PAGE_SIZE_BYTE;

    for (size_t attempt = 0; attempt < max_attempts; ++attempt)
    {
        LeafCell cell(100000 + static_cast<int>(attempt), static_cast<uint16_t>(attempt), static_cast<uint16_t>(attempt));
        auto slot_id_opt = page->insertCell(cell);
        if (!slot_id_opt.has_value())
        {
            saw_nullopt = true;
            break;
        }
        ++successful_inserts;
    }

    EXPECT_TRUE(saw_nullopt);
    EXPECT_GT(successful_inserts, 0u);
}

TEST(PageTest, InsertIntermediatePageAndFind)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    uint16_t right_most_child_page_id = 999;
    Page *page = Page::initializePage(page_data.data(), false, right_most_child_page_id);
    struct Entry
    {
        int key;
        uint16_t page_id;
    } entries[] = {
        {10000, 63},
        {30000, 21},
        {20000, 42}
    };

    for (const Entry &entry : entries)
    {
        auto slot_id_opt = page->insertCell(IntermediateCell(entry.page_id, entry.key));
        ASSERT_TRUE(slot_id_opt.has_value());
    }

    for (const Entry &entry : entries)
    {
        uint16_t child_page = page->findChildPage(entry.key);
        EXPECT_EQ(entry.page_id, child_page);
    }

    uint16_t child_page_for_large_key = page->findChildPage(entries[2].key + 1);
    EXPECT_EQ(entries[1].page_id, child_page_for_large_key);

    uint16_t child_page_for_small_key = page->findChildPage(entries[2].key - 1);
    EXPECT_EQ(entries[2].page_id, child_page_for_small_key);

    uint16_t child_page_for_largest_key = page->findChildPage(entries[1].key + 1);
    EXPECT_EQ(right_most_child_page_id, child_page_for_largest_key);
}

TEST(PageTest, InvalidateSlotSetsFlag)
{
    auto assertInvalidation = [](bool is_leaf, auto make_cell) {
        std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
        Page *page = Page::initializePage(page_data.data(), is_leaf, 0);
        auto slot_id_opt = page->insertCell(make_cell());
        ASSERT_TRUE(slot_id_opt.has_value());
        uint16_t slot_id = slot_id_opt.value();

        uint16_t cell_offset = 0;
        std::memcpy(&cell_offset,
                    page_data.data() + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * slot_id,
                    sizeof(uint16_t));
        char *cell_data = page_data.data() + cell_offset;
        ASSERT_TRUE(Cell::isValid(cell_data));

        page->invalidateSlot(slot_id);

        EXPECT_FALSE(Cell::isValid(cell_data));
    };

    assertInvalidation(true, []() { return LeafCell(42, 100, 7); });
    std::string payload = "page-record";
    assertInvalidation(true, [&payload]() { return RecordCell(99, payload.data(), payload.size()); });
}

TEST(PageTest, LeafSearchSkipsInvalidEntries)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    Page *page = Page::initializePage(page_data.data(), true, 0);

    auto slot_id_opt = page->insertCell(LeafCell(123, 1, 1));
    page->invalidateSlot(slot_id_opt.value());

    EXPECT_FALSE(page->hasKey(123));
    EXPECT_FALSE(page->findLeafRef(123).has_value());
}

TEST(PageTest, LeafInsertInvalidateReuseSlot)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    Page *page = Page::initializePage(page_data.data(), true, 0);

    auto first_slot = page->insertCell(LeafCell(1, 1, 1));
    ASSERT_TRUE(first_slot.has_value());
    page->invalidateSlot(first_slot.value());
    EXPECT_FALSE(page->hasKey(1));

    auto second_slot = page->insertCell(LeafCell(1, 1, 1));
    ASSERT_TRUE(second_slot.has_value());
    EXPECT_TRUE(page->hasKey(1));

    auto ref = page->findLeafRef(1);
    ASSERT_TRUE(ref.has_value());
    EXPECT_EQ(ref->first, 1);
    EXPECT_EQ(ref->second, 1);
}

TEST(PageTest, HeapInsertInvalidateReuseSlot)
{
    std::array<char, Page::PAGE_SIZE_BYTE> page_data{};
    Page *page = Page::initializePage(page_data.data(), true, 0);

    std::string payload = "heap-value";
    auto first_slot = page->insertCell(RecordCell(10, payload.data(), payload.size()));
    ASSERT_TRUE(first_slot.has_value());
    page->invalidateSlot(first_slot.value());

    auto second_slot = page->insertCell(RecordCell(10, payload.data(), payload.size()));
    ASSERT_TRUE(second_slot.has_value());
    EXPECT_NE(first_slot.value(), second_slot.value());

    char *value_ptr = page->getXthSlotValue(second_slot.value());
    EXPECT_EQ(std::string(value_ptr, payload.size()), payload);
}