#include "page.h"
#include "leaf_cell.h"
#include "intermediate_cell.h"
#include <cstdint>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <optional>
#include <utility>
#include <vector>
#include <algorithm>

Page* Page::initializePage(char *start_p, bool is_leaf, uint16_t rightMostChildPageId){
    Page *page = new Page(start_p);
    page->updateNodeTypeFlag(is_leaf);
    page->updateSlotCount(0);
    page->updateSlotDirectoryOffset(Page::PAGE_SIZE_BYTE);
    page->setRightMostChildPageId(rightMostChildPageId);
    return page;
}

Page* Page::wrap(char *start_p){
    Page *page = new Page(start_p);
    return page;
};

bool Page::hasKey(int key)
{   
    if(!isLeaf()){
        throw std::logic_error("hasKey should only be called for leaf node.");
    }

    for (int cell_pointer_index = 0; cell_pointer_index < getSlotCount(); cell_pointer_index++){
        LeafCell cell = getLeafCellOnXthPointer(cell_pointer_index);
        if (cell.key() == key)
        {
            return true;
        }
    }
    return false;
}

std::optional<std::pair<uint16_t, uint16_t>> Page::findLeafRef(int key)
{
    if (!isLeaf())
    {
        throw std::logic_error("findLeafRef called on non-leaf page");
    }

    /**
     * PERFORMANCE:Since both reads and inserts in a B+ tree must traverse from the root to a leaf (performing an in-page search at each level),
     * every insert inherently includes a traversal phase. 
     * Therefore, average performance is sensitive to the number of key comparisons within a page, 
     * and using binary search to reduce in-page search from O(N_page) to O(log N_page) can be algorithmically advantageous. In practice,
     * however, the benefit may be small depending on N_page and key-comparison cost (fixed vs. variable length),
     * so benchmarking is required.
     */
    for (int idx = 0; idx < getSlotCount(); ++idx)
    {
        LeafCell cell = getLeafCellOnXthPointer(idx);
        if (cell.key() == key)
        {
            return std::make_pair(cell.heap_page_id(), cell.slot_id());
        }
    }
    spdlog::info("key {} not found in this page.", key);
    return std::nullopt;
}

uint16_t Page::findChildPage(int key)
{
    if (isLeaf())
    {
        throw std::logic_error("findChildPage called on leaf page");
    }

    // PERFORMANCE: binary search can be implemented here.
    std::vector<IntermediateCell> cells;
    cells.reserve(getSlotCount());
    for (int idx = 0; idx < getSlotCount(); ++idx)
    {
        cells.push_back(getIntermediateCellOnXthPointer(idx));
    }
    std::sort(cells.begin(), cells.end(), [](const IntermediateCell &a, const IntermediateCell &b) {
        return a.key() < b.key();
    });
    for (const IntermediateCell &cell : cells)
    {
        if (cell.key() >= key)
        {
            return cell.page_id();
        }
    }
    return rightMostChildPageId();
}

/**
 * returns the slot ID of the inserted cell.
 */
std::optional<int> Page::insertCell(const Cell &cell)
{
    // check if the page has enough space to insert the new cell.
    uint16_t new_cell_offset = getSlotDirectoryOffset() - cell.payloadSize();
    char *cell_data_p = start_p_ + new_cell_offset;
    char *cell_ptr_end_p = start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * (getSlotCount()+1);
    if (!(cell_data_p > cell_ptr_end_p))
    {
        spdlog::info("This page does not have enough space to insert the cell anymore.");
        return std::nullopt;
    }

    // serialize cell and copy to the page.
    std::vector<std::byte> serialized_data = cell.serialize();
    std::memcpy(cell_data_p, serialized_data.data(), serialized_data.size());

    // update cell pointer.
    // cell pointers should be sored by key in ascending order? For now, we just insert the new cell pointer to the end of cell pointers, so the cell pointers are not sorted by key, but we can implement the sorting in the future if needed.
    char *cell_ptr_p = start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * getSlotCount();
    std::memcpy(cell_ptr_p, &new_cell_offset, sizeof(uint16_t));

    // update headder.
    updateSlotDirectoryOffset(new_cell_offset);
    updateSlotCount(getSlotCount() + 1);

    spdlog::info("Inserted a new cell with key {} into page. New slot count: {}, new slot directory offset: {}", cell.key(), getSlotCount(), getSlotDirectoryOffset());
    return getSlotCount() - 1;
}

// private methods
IntermediateCell Page::getIntermediateCellOnXthPointer(int x)
{
    char *data_p = start_p_ + getCellOffsetOnXthPointer(x);
    return IntermediateCell::decodeCell(data_p);
}

uint16_t Page::getCellOffsetOnXthPointer(int x)
{
    char *cell_ptr_p = start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * x;
    return readValue<uint16_t>(cell_ptr_p);
}

LeafCell Page::getLeafCellOnXthPointer(int x)
{
    char *data_p = start_p_ + getCellOffsetOnXthPointer(x);
    return LeafCell::decodeCell(data_p);
}

char* Page::getXthSlotValue(int x)
{
    char* cell_data = start_p_ + getCellOffsetOnXthPointer(x);
    char *cursor = cell_data + Cell::FLAG_FIELD_SIZE;
    // forward the pointer to the value part of the cell.
    int stored_key = readValue<int>(cursor);
    cursor += sizeof(int);
    size_t value_size = readValue<size_t>(cursor);
    (void)value_size;
    cursor += sizeof(size_t);
    return cursor;
}

uint8_t Page::getSlotCount()
{
    return readValue<uint8_t>(start_p_ + SLOT_COUNT_OFFSET);
}

uint16_t Page::getSlotDirectoryOffset()
{
    return readValue<uint16_t>(start_p_ + SLOT_DIRECTORY_OFFSET);
}

void Page::updateSlotCount(uint8_t new_count)
{
    std::memcpy(start_p_ + SLOT_COUNT_OFFSET, &new_count, sizeof(uint8_t));
}

void Page::updateSlotDirectoryOffset(uint16_t new_offset)
{
    std::memcpy(start_p_ + SLOT_DIRECTORY_OFFSET, &new_offset, sizeof(uint16_t));
}

void Page::updateNodeTypeFlag(bool is_leaf)
{
    uint8_t flag = is_leaf ? 1 : 0;
    std::memcpy(start_p_ + NODE_TYPE_FLAG_OFFSET, &flag, sizeof(uint8_t));
}

bool Page::isLeaf() const
{
    return readValue<uint8_t>(start_p_ + NODE_TYPE_FLAG_OFFSET) == 1;
}

uint16_t Page::rightMostChildPageId() const
{
    return readValue<uint16_t>(start_p_ + RIGHT_MOST_CHILD_POINTER_OFFSET);
}

void Page::setRightMostChildPageId(uint16_t page_id)
{
    std::memcpy(start_p_ + RIGHT_MOST_CHILD_POINTER_OFFSET, &page_id, sizeof(uint16_t));
}