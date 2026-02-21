#include "../src/page.h"
#include "../src/cell.h"
#include <array>
#include <optional>
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
