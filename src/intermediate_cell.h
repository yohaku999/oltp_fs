#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class IntermediateCell
{
private:
    // The value range of cell key, pageID is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t PAGE_ID_SIZE = sizeof(uint16_t);
    static constexpr size_t CELL_KEY_SIZE = sizeof(uint16_t);

public:
    uint16_t keySize;
    uint16_t pageID;
    int key;
    static IntermediateCell decodeCell(char *data_p);
    void encodeCell(char *data_p) const;
};