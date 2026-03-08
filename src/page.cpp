#include "page.h"
#include "leaf_cell.h"
#include "intermediate_cell.h"
#include <cstdint>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include "logging.h"
#include <optional>
#include <utility>
#include <vector>
#include <algorithm>
#include <ostream>
#include <limits>

// Constructor for creating a new page
Page::Page(char *start_p, bool is_leaf, uint16_t rightMostChildPageId, uint16_t page_id)
    : start_p_(start_p), pageID_(page_id), parentPageID_(-1), is_dirty_(false)
{
    updateNodeTypeFlag(is_leaf);
    updateSlotCount(0);
    updateSlotDirectoryOffset(Page::PAGE_SIZE_BYTE);
    setRightMostChildPageId(rightMostChildPageId);
    markDirty();
}

// Constructor for wrapping existing page data
Page::Page(char *start_p, uint16_t page_id)
    : start_p_(start_p), pageID_(page_id), parentPageID_(-1), is_dirty_(false)
{
    // Existing page data is already initialized, so we don't need to do anything
}


/**
 * returns the slot ID of the inserted cell.
 */
std::optional<int> Page::insertCell(const Cell &cell)
{
    LOG_INFO("Attempting to insert cell with key {} into page ID {}", cell.key(), getPageID());
    // check if the page has enough space to insert the new cell.
    uint16_t new_cell_offset = getSlotDirectoryOffset() - cell.payloadSize();
    char *cell_data_p = start_p_ + new_cell_offset;
    char *cell_ptr_end_p = start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * (getSlotCount()+1);
    if (!(cell_data_p > cell_ptr_end_p))
    {
        LOG_INFO("This page does not have enough space to insert the cell anymore.");
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

    this->markDirty();

    LOG_INFO("Inserted a new cell with key {} into page. New slot count: {}, new slot directory offset: {}", cell.key(), getSlotCount(), getSlotDirectoryOffset());
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
    if (!Cell::isValid(cell_data))
    {
        throw std::runtime_error("This slot has been invalidated.");
    }
    char *cursor = cell_data + Cell::FLAG_FIELD_SIZE;
    // forward the pointer to the value part of the cell.
    int stored_key = readValue<int>(cursor);
    cursor += sizeof(int);
    size_t value_size = readValue<size_t>(cursor);
    (void)value_size;
    cursor += sizeof(size_t);
    return cursor;
}

char* Page::getSeparateKey(){
    // Since the cell pointers are sorted, just return the key value of the cell that the middle slot pointer points to.
    int middle_slot_index = getSlotCount() / 2;
    while (true){
        char *cell_data = start_p_ + getCellOffsetOnXthPointer(middle_slot_index);
        if (Cell::isValid(cell_data))
        {
            return cell_data;
        }
        middle_slot_index++;
        if (middle_slot_index >= getSlotCount()){
            // TODO: this is a corner case, but we should deal with this.
            throw std::runtime_error("All cells in this page are invalid.");
        }
    }
}

void Page::invalidateSlot(uint16_t slot_id)
{
    char *cell_data = start_p_ + getCellOffsetOnXthPointer(slot_id);
    Cell::markInvalid(cell_data);
}

uint16_t Page::getSlotCount()
{
    return readValue<uint16_t>(start_p_ + SLOT_COUNT_OFFSET);
}

uint16_t Page::getSlotDirectoryOffset()
{
    return readValue<uint16_t>(start_p_ + SLOT_DIRECTORY_OFFSET);
}

void Page::updateSlotCount(uint16_t new_count)
{
    std::memcpy(start_p_ + SLOT_COUNT_OFFSET, &new_count, sizeof(uint16_t));
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

void Page::dump(std::ostream& os)
{
    os << "=== Page " << pageID_
       << " (" << (isLeaf() ? "leaf" : "internal") << ") ===\n";
    os << "parent=" << parentPageID_
       << " slotCount=" << static_cast<int>(getSlotCount());
    if (!isLeaf())
    {
        os << " rightMostChild=" << rightMostChildPageId();
    }
    os << "\n";

    for (int i = 0; i < getSlotCount(); ++i)
    {
        char* cell_data = start_p_ + getCellOffsetOnXthPointer(i);
        if (!Cell::isValid(cell_data))
        {
            os << "  [" << i << "] <invalid>\n";
            continue;
        }

        if (isLeaf())
        {
            LeafCell c = getLeafCellOnXthPointer(i);
            os << "  [" << i << "] Leaf  key=" << c.key()
               << " heapPage=" << c.heap_page_id()
               << " slot=" << c.slot_id() << "\n";
        }
        else
        {
            IntermediateCell c = getIntermediateCellOnXthPointer(i);
            os << "  [" << i << "] Inter key=" << c.key()
               << " childPage=" << c.page_id() << "\n";
        }
    }
    os << "\n";
}