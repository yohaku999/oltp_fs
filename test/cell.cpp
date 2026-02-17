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
        IntermediateCell original(500, 77777);
        std::vector<std::byte> serialized = original.serialize();

        IntermediateCell decoded = IntermediateCell::decodeCell(reinterpret_cast<char*>(serialized.data()));

        assert(decoded.key_size() == original.key_size());
        assert(decoded.page_id() == original.page_id());
        assert(decoded.key() == original.key());

        std::cout << "intermediateCell_encodeAndDecode_RoundTrip: OK" << std::endl;
    }

    // ========== LeafCell tests ==========

    void leafCell_encodeAndDecode_RoundTrip()
    {
        LeafCell original(11111, 999, 15);
        std::vector<std::byte> serialized = original.serialize();
        LeafCell decoded = LeafCell::decodeCell(reinterpret_cast<char*>(serialized.data()));

        assert(decoded.heap_page_id() == original.heap_page_id());
        assert(decoded.slot_id() == original.slot_id());
        assert(decoded.key() == original.key());

        std::cout << "leafCell_encodeAndDecode_RoundTrip: OK" << std::endl;
    }
};

int main()
{
    CellTest test;
    test.runAll();
    return 0;
}
