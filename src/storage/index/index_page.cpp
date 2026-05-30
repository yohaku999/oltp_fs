#include "index_page.h"

#include <algorithm>
#include <cstring>
#include <vector>

#include "index_key.h"
#include "logging.h"

LeafCell LeafIndexPage::cellAt(int slot_id) const {
  return LeafCell::decodeCell(page_.getSlotCellStart(slot_id));
}

bool LeafIndexPage::hasKey(const std::string& key) const {
  for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    LeafCell cell = cellAt(idx);
    if (index_key::compare(cell.key(), key) == 0) {
      return true;
    }
  }
  return false;
}

std::pair<uint16_t, std::vector<RID>> LeafIndexPage::findRef(const std::string& key,
                                          bool do_invalidate, Op op) {
  /**
   * PERFORMANCE:Same consideration as Page::findLeafRef; kept verbatim.
   */
  std::vector<RID> matching_rids;
  bool need_to_scan_next_page = op != Op::Eq;
  for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      dbfs_log::index().debug("LeafIndexPage::findRef skipping invalid slot {}", idx);
      continue;
    }
    LeafCell cell = cellAt(idx);
    const int compare_result = index_key::compare(cell.key(), key);
    if (index_key::matches(cell.key(), key, op)) {
      if (do_invalidate) {
        dbfs_log::index().debug("LeafIndexPage::findRef invalidating slot {} for key {}", idx,
                  index_key::formatForDebug(key));
        page_.invalidateSlot(idx);
      }
      matching_rids.push_back(RID{cell.heap_page_id(), cell.slot_id()});
    }
    if(op == Op::Lt || op == Op::Le) {
      // Since the cells are sorted by key, we can stop once the upper bound is reached.
      if ((op == Op::Lt && compare_result >= 0) ||
          (op == Op::Le && compare_result > 0)) {
        need_to_scan_next_page = false;
        break;
      }
    } else if (op == Op::Eq) {
      if (compare_result > 0) {
        need_to_scan_next_page = false;
        break;
      }
    }
  }
  if (matching_rids.empty()) {
    dbfs_log::index().debug("LeafIndexPage::findRef key {} not found in this page.",
              index_key::formatForDebug(key));
  }
  
  
  if (need_to_scan_next_page) {
    dbfs_log::index().debug("LeafIndexPage::findRef scanning next page for key {}.", index_key::formatForDebug(key));
    // or should i just return page ID here?
    return std::make_pair(this->getRightSiblingPageId(), matching_rids);  
  }
  else{
    return std::make_pair(LeafIndexPage::NO_RIGHT_SIBLING, matching_rids);
  }
  
}

void LeafIndexPage::compact() {
  const uint16_t old_slot_count = page_.getSlotCount();
  std::vector<LeafCell> cells;
  cells.reserve(old_slot_count);
  for (uint16_t idx = 0; idx < old_slot_count; ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    cells.push_back(cellAt(idx));
  }

  const uint16_t new_slot_count = static_cast<uint16_t>(cells.size());
  uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
  for (uint16_t idx = 0; idx < new_slot_count; ++idx) {
    const LeafCell& cell = cells[idx];
    write_offset =
        static_cast<uint16_t>(write_offset - cell.payloadSize());
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer = page_.data() + Page::HEADDER_SIZE_BYTE +
                         Page::CELL_POINTER_SIZE * idx;
    std::memcpy(slot_pointer, &write_offset, sizeof(uint16_t));
  }

  page_.updateSlotCount(new_slot_count);
  page_.updateSlotDirectoryOffset(write_offset);
  page_.markDirty();
}

void LeafIndexPage::transferAndCompactTo(LeafIndexPage& dst,
                                         const std::string& separate_key) {

  const int slot_count = page_.getSlotCount();
  for (int idx = 0; idx < slot_count; ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }

    const std::string cell_key = LeafCell::getKey(cell_data);
    if (index_key::compare(cell_key, separate_key) <= 0) {
      dst.page_.insertCell(LeafCell::decodeCell(cell_data));
      page_.invalidateSlot(idx);
    }
  }

  uint16_t old_slot_count = page_.getSlotCount();

  std::vector<LeafCell> cells;
  cells.reserve(old_slot_count);
  for (int i = 0; i < old_slot_count; ++i) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(i);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    cells.push_back(cellAt(i));
  }

  const uint16_t new_slot_count = static_cast<uint16_t>(cells.size());
  if (new_slot_count == 0) {
    throw std::logic_error(
        "LeafIndexPage::transferAndCompactTo: new_slot_count == 0 (not "
        "implemented)");
  }

  uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
  for (uint16_t idx = 0; idx < new_slot_count; ++idx) {
    const LeafCell& cell = cells[idx];
    const uint16_t payload_size = static_cast<uint16_t>(cell.payloadSize());
    write_offset = static_cast<uint16_t>(write_offset - payload_size);
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer = page_.data() + Page::HEADDER_SIZE_BYTE +
                         Page::CELL_POINTER_SIZE * idx;
    std::memcpy(slot_pointer, &write_offset, sizeof(uint16_t));
  }

  page_.updateSlotCount(new_slot_count);
  page_.updateSlotDirectoryOffset(write_offset);
  page_.markDirty();

    dbfs_log::index().info(
      "Completed transfer and compaction of LeafIndexPage. New slot count: {}, "
      "new slot directory offset: {}",
      new_slot_count, write_offset);
}

IntermediateCell InternalIndexPage::cellAt(int slot_id) const {
  return IntermediateCell::decodeCell(page_.getSlotCellStart(slot_id));
}

uint16_t InternalIndexPage::rightMostChildPageId() const {
  return page_.rightMostChildPageId();
}

uint16_t InternalIndexPage::findChildPage(const std::string& key) {
  std::vector<IntermediateCell> cells;
  cells.reserve(page_.getSlotCount());
  for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    cells.push_back(cellAt(idx));
  }

  std::sort(cells.begin(), cells.end(),
            [](const IntermediateCell& a, const IntermediateCell& b) {
              return index_key::compare(a.key(), b.key()) < 0;
            });

  for (const IntermediateCell& cell : cells) {
    if (index_key::compare(cell.key(), key) >= 0) {
      return cell.page_id();
    }
  }
  dbfs_log::index().debug("InternalIndexPage::findChildPage: Going to right most child {}.",
            rightMostChildPageId());
  return rightMostChildPageId();
}

void InternalIndexPage::compact() {
  const uint16_t old_slot_count = page_.getSlotCount();
  std::vector<IntermediateCell> cells;
  cells.reserve(old_slot_count);
  for (uint16_t idx = 0; idx < old_slot_count; ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    cells.push_back(cellAt(idx));
  }

  const uint16_t new_slot_count = static_cast<uint16_t>(cells.size());
  uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
  for (uint16_t idx = 0; idx < new_slot_count; ++idx) {
    const IntermediateCell& cell = cells[idx];
    write_offset =
        static_cast<uint16_t>(write_offset - cell.payloadSize());
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer = page_.data() + Page::HEADDER_SIZE_BYTE +
                         Page::CELL_POINTER_SIZE * idx;
    std::memcpy(slot_pointer, &write_offset, sizeof(uint16_t));
  }

  page_.updateSlotCount(new_slot_count);
  page_.updateSlotDirectoryOffset(write_offset);
  page_.markDirty();
}

void InternalIndexPage::transferAndCompactTo(InternalIndexPage& dst,
                                             const std::string& separate_key) {
  const uint16_t original_right_most_child = page_.rightMostChildPageId();
  std::optional<IntermediateCell> separator_cell;

  const int slot_count = page_.getSlotCount();
  for (int idx = 0; idx < slot_count; ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }

    const std::string cell_key = IntermediateCell::getKey(cell_data);
    const int compare = index_key::compare(cell_key, separate_key);
    if (compare < 0) {
      dst.page_.insertCell(IntermediateCell::decodeCell(cell_data));
      page_.invalidateSlot(idx);
    } else if (compare == 0) {
      separator_cell = IntermediateCell::decodeCell(cell_data);
      page_.invalidateSlot(idx);
    }
  }

  if (!separator_cell.has_value()) {
    throw std::logic_error(
        "InternalIndexPage::transferAndCompactTo: separator cell not found");
  }

  dst.page_.setRightMostChildPageId(separator_cell->page_id());
  dst.page_.markDirty();
  page_.setRightMostChildPageId(original_right_most_child);

  uint16_t old_slot_count = page_.getSlotCount();

  std::vector<IntermediateCell> cells;
  cells.reserve(old_slot_count);
  for (int i = 0; i < old_slot_count; ++i) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(i);
    if (!Cell::isValid(cell_data)) {
      continue;
    }
    cells.push_back(cellAt(i));
  }

  const uint16_t new_slot_count = static_cast<uint16_t>(cells.size());
  if (new_slot_count == 0) {
    throw std::logic_error(
        "InternalIndexPage::transferAndCompactTo: new_slot_count == 0 (not "
        "implemented)");
  }

  uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
  for (uint16_t idx = 0; idx < new_slot_count; ++idx) {
    const IntermediateCell& cell = cells[idx];
    const uint16_t payload_size = static_cast<uint16_t>(cell.payloadSize());
    write_offset = static_cast<uint16_t>(write_offset - payload_size);
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer = page_.data() + Page::HEADDER_SIZE_BYTE +
                         Page::CELL_POINTER_SIZE * idx;
    std::memcpy(slot_pointer, &write_offset, sizeof(uint16_t));
  }

  page_.updateSlotCount(new_slot_count);
  page_.updateSlotDirectoryOffset(write_offset);
  page_.markDirty();

    dbfs_log::index().info(
      "Completed transfer and compaction of InternalIndexPage. New slot count: "
      "{}, new slot directory offset: {}",
      new_slot_count, write_offset);
}
