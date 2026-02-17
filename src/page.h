#pragma once
#include "intermediate_cell.h"
#include "leaf_cell.h"
#include "cell.h"
#include <span>
#include <utility>

/**
 * The structure of page is as follows:
 * | header (256 bytes) | cell pointer array (2 bytes per cell) | cells (variable size) |
 * The header contains the following information:
 * - slot count (1 byte): the number of cells in the page.
 * - slot directory offset (2 bytes): the offset of the slot directory, which is the
 */
class Page
{
private:
    static constexpr size_t NODE_TYPE_FLAG_BYTE = 0;
    static constexpr size_t SLOT_COUNT_SIZE_BYTE = 1;
    static constexpr size_t SLOT_DIRECTORY_OFFSET_BYTE = 2;
    IntermediateCell getIntermediateCellOnXthPointer(int x);
    LeafCell getLeafCellOnXthPointer(int x);
    uint16_t getCellOffsetOnXthPointer(int x);
    uint8_t getSlotCount();
    uint16_t getSlotDirectoryOffset();
    void updateSlotCount(uint8_t new_count);
    void updateSlotDirectoryOffset(uint16_t new_offset);
    void updateNodeTypeFlag(bool is_leaf);
    

public:
    static constexpr size_t HEADDER_SIZE_BYTE = 256;
    bool isLeaf() const;
    // The value range of cell is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t PAGE_SIZE_BYTE = 4096;
    static constexpr size_t CELL_POINTER_SIZE = sizeof(uint16_t);
    char *start_p_;
    std::pair<uint16_t, uint16_t> findLeafRef(int key);
    uint16_t findChildPage(int key);
    char *getXthSlotValue(int x);
    bool hasKey(int key);
    int insertCell(const Cell &cell);
    static Page* initializePage(char *start_p, bool is_leaf);
    static Page* wrap(char *start_p);
    Page(char *start_p) : start_p_(start_p) {};
};