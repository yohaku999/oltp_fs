#include "../src/page.h"
#include "../src/cell.h"
#include <cassert>
#include <iostream>
#include <cstring>

class PageTest
{
public:
    void runAll()
    {
        testInsertLeafPageAndFind();
        std::cout << "All Page tests passed!" << std::endl;
    }

    void testInsertLeafPageAndFind()
    {
        LeafCell cell(11111, 999, 15);
        char page_data[Page::PAGE_SIZE_BYTE] = {};
        Page *page = Page::initializePage(page_data, true);
        int slot_id = page->insertCell(cell);
        assert(slot_id == 0);
        std::pair<uint16_t, uint16_t> ref = page->findLeafRef(11111);
        assert(ref.first == 999);
        assert(ref.second == 15);
        std::cout << "testInsertLeafPageAndFind: OK" << std::endl;
    }
};

int main()
{
    PageTest test;
    test.runAll();
    return 0;
}
