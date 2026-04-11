#include "page.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "logging.h"
#include "record_cell.h"

namespace {

}  // namespace

Page Page::initializeNew(char* page_buffer, PageKind kind,
                         uint16_t right_most_child_page_id, uint16_t page_id) {
  return Page(page_buffer, kind, right_most_child_page_id, page_id);
}

Page Page::wrapExisting(char* page_buffer, uint16_t page_id) {
  return Page(page_buffer, page_id);
}

Page::Page(char* page_buffer, PageKind kind, uint16_t right_most_child_page_id,
           uint16_t page_id)
    : page_buffer_(page_buffer),
      page_id_(page_id),
      parent_page_id_(-1),
      is_dirty_(false) {
  updateNodeTypeFlag(kind);
  updateSlotCount(0);
  updateSlotDirectoryOffset(Page::PAGE_SIZE_BYTE);
  setRightMostChildPageId(right_most_child_page_id);
  updatePageLSN(0);
  markDirty();
}

Page::Page(char* page_buffer, uint16_t page_id)
    : page_buffer_(page_buffer),
      page_id_(page_id),
      parent_page_id_(-1),
      is_dirty_(false) {}

/**
 * Attempts to append a serialized cell to this page.
 *
 * @param serialized_cell Serialized cell bytes to place in the page payload.
 * @return Slot ID of the inserted cell when enough free space remains;
 *         `std::nullopt` otherwise.
 *
 * On success, this method updates the slot directory, advances the payload
 * boundary, and marks the page dirty.
 */
std::optional<int> Page::insertCell(
    const std::vector<std::byte>& serialized_cell) {
  LOG_INFO("Attempting to insert serialized cell into page ID {}", getPageID());

  uint16_t new_cell_offset = getSlotDirectoryOffset() - serialized_cell.size();
  char* cell_data_start = page_buffer_ + new_cell_offset;
  char* next_slot_pointer = page_buffer_ + Page::HEADDER_SIZE_BYTE +
                            Page::CELL_POINTER_SIZE * (getSlotCount() + 1);
  const bool has_space_for_new_cell = cell_data_start > next_slot_pointer;
  if (!has_space_for_new_cell) {
    LOG_INFO(
        "This page does not have enough space to insert the cell anymore.");
    return std::nullopt;
  }

  std::memcpy(cell_data_start, serialized_cell.data(), serialized_cell.size());

  // update cell pointer.
  // cell pointers should be sored by key in ascending order? For now, we just
  // insert the new cell pointer to the end of cell pointers, so the cell
  // pointers are not sorted by key, but we can implement the sorting in the
  // future if needed.
  char* slot_pointer = page_buffer_ + Page::HEADDER_SIZE_BYTE +
                       Page::CELL_POINTER_SIZE * getSlotCount();
  std::memcpy(slot_pointer, &new_cell_offset, sizeof(uint16_t));

  // update headder.
  updateSlotDirectoryOffset(new_cell_offset);
  updateSlotCount(getSlotCount() + 1);

  this->markDirty();

  LOG_INFO(
      "Inserted a new cell into page. New slot count: {}, new slot directory "
      "offset: {}",
      getSlotCount(), getSlotDirectoryOffset());
  return getSlotCount() - 1;
}

std::optional<int> Page::insertCell(const Cell& cell) {
  LOG_INFO("Attempting to insert {} cell into page ID {}",
           static_cast<int>(cell.kind()), getPageID());
  std::vector<std::byte> serialized_data = cell.serialize();
  return insertCell(serialized_data);
}

// private methods
uint16_t Page::getCellOffsetOnXthPointer(int x) {
  char* slot_pointer =
      page_buffer_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * x;
  return readValue<uint16_t>(slot_pointer);
}

char* Page::getSlotCellStart(int slot_id) {
  char* cell_data = page_buffer_ + getCellOffsetOnXthPointer(slot_id);
  if (!Cell::isValid(cell_data)) {
    throw std::runtime_error("This slot has been invalidated.");
  }
  return cell_data;
}

char* Page::getSplitKeyCellStart() {
  // Since the cell pointers are sorted, just return the key value of the cell
  // that the middle slot pointer points to.
  int middle_slot_index = getSlotCount() / 2;
  while (true) {
    char* cell_data =
        page_buffer_ + getCellOffsetOnXthPointer(middle_slot_index);
    if (Cell::isValid(cell_data)) {
      return cell_data;
    }
    middle_slot_index++;
    if (middle_slot_index >= getSlotCount()) {
      // TODO: this is a corner case, but we should deal with this.
      throw std::runtime_error("All cells in this page are invalid.");
    }
  }
}

void Page::invalidateSlot(uint16_t slot_id) {
  char* cell_data = page_buffer_ + getCellOffsetOnXthPointer(slot_id);
  Cell::markInvalid(cell_data);
}

uint16_t Page::getSlotCount() {
  return readValue<uint16_t>(page_buffer_ + SLOT_COUNT_OFFSET);
}

uint16_t Page::getSlotDirectoryOffset() {
  return readValue<uint16_t>(page_buffer_ + SLOT_DIRECTORY_OFFSET);
}

void Page::updateSlotCount(uint16_t new_count) {
  std::memcpy(page_buffer_ + SLOT_COUNT_OFFSET, &new_count, sizeof(uint16_t));
}

void Page::updateSlotDirectoryOffset(uint16_t new_offset) {
  std::memcpy(page_buffer_ + SLOT_DIRECTORY_OFFSET, &new_offset,
              sizeof(uint16_t));
}

void Page::updateNodeTypeFlag(PageKind kind) {
  uint8_t flag = 0;
  switch (kind) {
    case PageKind::Heap:
    case PageKind::LeafIndex:
      flag = 1;
      break;
    case PageKind::InternalIndex:
      flag = 0;
      break;
  }
  std::memcpy(page_buffer_ + NODE_TYPE_FLAG_OFFSET, &flag, sizeof(uint8_t));
}

bool Page::isLeaf() const {
  return readValue<uint8_t>(page_buffer_ + NODE_TYPE_FLAG_OFFSET) == 1;
}

uint16_t Page::slotCount() const {
  return readValue<uint16_t>(page_buffer_ + SLOT_COUNT_OFFSET);
}

char* Page::slotCellStartUnchecked(int slot_id) {
  return page_buffer_ + getCellOffsetOnXthPointer(slot_id);
}

const char* Page::slotCellStartUnchecked(int slot_id) const {
  return page_buffer_ + readValue<uint16_t>(page_buffer_ + Page::HEADDER_SIZE_BYTE +
                                            Page::CELL_POINTER_SIZE * slot_id);
}

uint16_t Page::rightMostChildPageId() const {
  return readValue<uint16_t>(page_buffer_ + RIGHT_MOST_CHILD_POINTER_OFFSET);
}

void Page::setRightMostChildPageId(uint16_t page_id) {
  std::memcpy(page_buffer_ + RIGHT_MOST_CHILD_POINTER_OFFSET, &page_id,
              sizeof(uint16_t));
}

std::uint64_t Page::getPageLSN() const {
  return readValue<std::uint64_t>(page_buffer_ + PAGE_LSN_OFFSET);
}

void Page::updatePageLSN(std::uint64_t lsn) {
  std::memcpy(page_buffer_ + PAGE_LSN_OFFSET, &lsn, sizeof(std::uint64_t));
}