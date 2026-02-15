#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class LeafCell
{
private:
    // The value range of pageID, slotID, cell key is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t PAGE_ID_SIZE = sizeof(uint16_t);
    static constexpr size_t SLOT_ID_SIZE = sizeof(uint16_t);
    static constexpr size_t CELL_KEY_SIZE = sizeof(uint16_t);

public:
    uint16_t keySize;
    uint16_t heapPageID;
    uint16_t slotID;
    int key;
    static LeafCell decodeCell(char *data_p);
    void encodeCell(char *data_p) const;
};
