#include "../src/intermediate_cell.h"
#include "../src/leaf_cell.h"
#include <cassert>
#include <iostream>
#include <cstring>

class CellTest
{
public:
    void runAll()
    {
        intermediateCell_encodeAndDecode_RoundTrip();
        leafCell_encodeAndDecode_RoundTrip();

        std::cout << "All Cell tests passed!" << std::endl;
    }

    // ========== IntermediateCell tests ==========

    void intermediateCell_encodeAndDecode_RoundTrip()
    {
        IntermediateCell original;
        original.keySize = 4;
        original.pageID = 500;
        original.key = 77777;

        char buffer[8] = {0};
        original.encodeCell(buffer);

        IntermediateCell decoded = IntermediateCell::decodeCell(buffer);

        assert(decoded.keySize == original.keySize);
        assert(decoded.pageID == original.pageID);
        assert(decoded.key == original.key);

        std::cout << "intermediateCell_encodeAndDecode_RoundTrip: OK" << std::endl;
    }

    // ========== LeafCell tests ==========

    void leafCell_encodeAndDecode_RoundTrip()
    {
        LeafCell original;
        original.keySize = 4;
        original.heapPageID = 999;
        original.slotID = 15;
        original.key = 11111;

        char buffer[10] = {0};
        original.encodeCell(buffer);

        LeafCell decoded = LeafCell::decodeCell(buffer);

        assert(decoded.keySize == original.keySize);
        assert(decoded.heapPageID == original.heapPageID);
        assert(decoded.slotID == original.slotID);
        assert(decoded.key == original.key);

        std::cout << "leafCell_encodeAndDecode_RoundTrip: OK" << std::endl;
    }
};

int main()
{
    CellTest test;
    test.runAll();
    return 0;
}
