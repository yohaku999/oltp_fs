#pragma once
#include "intermediate_cell.h"
#include "leaf_cell.h"
#include "cell.h"
#include <span>
#include <utility>
#include <optional>
/**
 * The structure of page is as follows:
 * | header (256 bytes) | cell pointer array (2 bytes per cell) | cells (variable size) |
 * The header contains the following information:
 * - node type flag (1 byte): 0 for intermediate page, 1 for leaf page.
 * - slot count (1 byte): the number of cells in the page.
 * - slot directory offset (2 bytes): the offset of the slot directory, which is the
 * - right-most child pointer for intermediate page, which is 0 for leaf page.
 */
class Page
{
private:
    static constexpr size_t NODE_TYPE_FLAG_OFFSET = 0;
    static constexpr size_t SLOT_COUNT_OFFSET = NODE_TYPE_FLAG_OFFSET + sizeof(uint8_t);
    static constexpr size_t SLOT_DIRECTORY_OFFSET = SLOT_COUNT_OFFSET + sizeof(uint16_t);
    static constexpr size_t RIGHT_MOST_CHILD_POINTER_OFFSET = SLOT_DIRECTORY_OFFSET + sizeof(uint16_t);
    IntermediateCell getIntermediateCellOnXthPointer(int x);
    LeafCell getLeafCellOnXthPointer(int x);
    uint16_t getCellOffsetOnXthPointer(int x);
    uint8_t getSlotCount();
    uint16_t getSlotDirectoryOffset();
    void updateSlotCount(uint8_t new_count);
    void updateSlotDirectoryOffset(uint16_t new_offset);
    void updateNodeTypeFlag(bool is_leaf);
    uint16_t rightMostChildPageId() const;
    void setRightMostChildPageId(uint16_t page_id);
    bool is_dirty_ = false;
    int pageID_ = -1;
    int parentPageID_ = -1;
    

public:
    static constexpr size_t HEADDER_SIZE_BYTE = 256;
    void markDirty(){
        is_dirty_ = true;
    };
    void clearDirty(){
        is_dirty_ = false;
    };
    bool isDirty(){
        return is_dirty_;
    };
    int getPageID() const {
        return pageID_;
    };
    int getParentPageID() const {
        return parentPageID_;
    };
    void setParentPageID(int parent_page_id) {
        parentPageID_ = parent_page_id;
    };
    bool isLeaf() const;
    char* getSeparateKey();
    void transferCellsTo(Page* new_page, char* separate_key);
    // The value range of cell is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t PAGE_SIZE_BYTE = 4096;
    static constexpr size_t CELL_POINTER_SIZE = sizeof(uint16_t);
    char *start_p_;
    std::optional<std::pair<uint16_t, uint16_t>> findLeafRef(int key, bool do_invalidate=false);
    uint16_t findChildPage(int key);
    char *getXthSlotValue(int x);
    bool hasKey(int key);
    std::optional<int> insertCell(const Cell &cell);
    void invalidateSlot(uint16_t slot_id);
    
    // Constructor for creating a new page
    Page(char *start_p, bool is_leaf, uint16_t rightMostChildPageId, uint16_t page_id);
    
    // Constructor for wrapping existing page data
    Page(char *start_p, uint16_t page_id);
};