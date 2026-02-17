#include "../src/page.h"
#include "../src/cell.h"
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>

class PageTest
{
public:
    void runAll()
    {
        testInsertLeafPageAndFind();
        std::cout << "All Page tests passed!" << std::endl;
    }

    // TODO: insert till fails
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
            int slot_id = page->insertCell(LeafCell(entry.key, entry.heap_page_id, entry.slot_id));
            assert(slot_id == i);

            std::pair<uint16_t, uint16_t> ref = page->findLeafRef(entry.key);
            assert(ref.first == entry.heap_page_id);
            assert(ref.second == entry.slot_id);
        }
        std::cout << "testInsertLeafPageAndFind: OK" << std::endl;
    }
};

int main()
{
    PageTest test;
    test.runAll();
    return 0;
}
