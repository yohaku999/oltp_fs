#include "../src/page.h"
#include "../src/cell.h"
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <optional>

class PageTest
{
public:
    void runAll()
    {
        testInsertLeafPageAndFind();
        testInsertLeafPageRunsOutOfSpace();
        testInsertIntermediatePageAndFind();
        std::cout << "All Page tests passed!" << std::endl;
    }

    void testInsertLeafPageAndFind()
    {
        char page_data[Page::PAGE_SIZE_BYTE] = {};
        Page *page = Page::initializePage(page_data, true);
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
            assert(slot_id_opt.has_value());
            int slot_id = slot_id_opt.value();
            assert(slot_id == i);

            auto ref_opt = page->findLeafRef(entry.key);
            assert(ref_opt.has_value());
            auto [heap_page_id, slot_id_ref] = ref_opt.value();
            assert(heap_page_id == entry.heap_page_id);
            assert(slot_id_ref == entry.slot_id);
        }
        std::cout << "testInsertLeafPageAndFind: OK" << std::endl;
    }

    void testInsertLeafPageRunsOutOfSpace()
    {
        char page_data[Page::PAGE_SIZE_BYTE] = {};
        Page *page = Page::initializePage(page_data, true);

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

        assert(saw_nullopt);
        assert(successful_inserts > 0);
        std::cout << "testInsertLeafPageRunsOutOfSpace: OK after " << successful_inserts << " inserts" << std::endl;
    }

    void testInsertIntermediatePageAndFind()
    {
        char page_data[Page::PAGE_SIZE_BYTE] = {};
        Page *page = Page::initializePage(page_data, false);
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
            assert(slot_id_opt.has_value());
        }

        for (const Entry &entry : entries)
        {
            uint16_t child_page = page->findChildPage(entry.key);
            assert(child_page == entry.page_id);
        }

        uint16_t child_page_for_large_key = page->findChildPage(entries[2].key + 1);
        assert(child_page_for_large_key == entries[1].page_id);

        uint16_t child_page_for_small_key = page->findChildPage(entries[2].key - 1);
        assert(child_page_for_small_key == entries[2].page_id);

        uint16_t child_page_for_largest_key = page->findChildPage(entries[2].key + 1);
        // what should i return. read book.
        // TODO: have to ensure all the entries are soreted?t

        std::cout << "testInsertIntermediatePageAndFind: OK" << std::endl;
    }
};

int main()
{
    PageTest test;
    test.runAll();
    return 0;
}
