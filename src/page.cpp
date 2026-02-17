#include "page.h"
#include "leaf_cell.h"
#include "intermediate_cell.h"
#include <cstdint>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <optional>
#include <utility>

Page* Page::initializePage(char *start_p, bool is_leaf){
    Page *page = new Page(start_p);
    page->updateNodeTypeFlag(is_leaf);
    page->updateSlotCount(0);
    page->updateSlotDirectoryOffset(Page::PAGE_SIZE_BYTE);
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

    for (int idx = 0; idx < getSlotCount(); ++idx)
    {
        IntermediateCell cell = getIntermediateCellOnXthPointer(idx);
        if (cell.key() <= key)
        {
            return cell.page_id();
        }
    }
    spdlog::info("key {} is greater than all keys in the internal page, returning the last child.", key);
    return 0; //TODO:
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
    return start_p_ + getCellOffsetOnXthPointer(x);
}

uint8_t Page::getSlotCount()
{
    return readValue<uint8_t>(start_p_ + SLOT_COUNT_SIZE_BYTE);
}

uint16_t Page::getSlotDirectoryOffset()
{
    return readValue<uint16_t>(start_p_ + SLOT_DIRECTORY_OFFSET_BYTE);
}

void Page::updateSlotCount(uint8_t new_count)
{
    std::memcpy(start_p_ + SLOT_COUNT_SIZE_BYTE, &new_count, sizeof(uint8_t));
}

void Page::updateSlotDirectoryOffset(uint16_t new_offset)
{
    std::memcpy(start_p_ + SLOT_DIRECTORY_OFFSET_BYTE, &new_offset, sizeof(uint16_t));
}

void Page::updateNodeTypeFlag(bool is_leaf)
{
    uint8_t flag = is_leaf ? 1 : 0;
    std::memcpy(start_p_ + NODE_TYPE_FLAG_BYTE, &flag, sizeof(uint8_t));
}

bool Page::isLeaf() const
{
    return readValue<uint8_t>(start_p_ + NODE_TYPE_FLAG_BYTE) == 1;
}