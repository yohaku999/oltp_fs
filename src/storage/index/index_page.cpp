#include "index_page.h"

#include <cstring>
#include <vector>

#include "index_key.h"
#include "logging.h"
#include "storage/index/btreecursor.h"

LeafCell LeafIndexPage::cellAt(int slot_id) const {
  return LeafCell::decodeCell(page_.getSlotCellStart(slot_id));
}

bool LeafIndexPage::hasKey(const std::string& key) const {
  const int slot_count = page_.getSlotCount();
  int lower = 0;
  int upper = slot_count - 1;
  while (lower <= upper) {
    const int middle = lower + (upper - lower) / 2;
    int probe = middle;
    const char* cell_data = nullptr;
    while (probe <= upper) {
      cell_data = page_.data() + page_.getCellOffsetOnXthPointer(probe);
      if (Cell::isValid(cell_data)) {
        break;
      }
      ++probe;
    }

    if (probe > upper) {
      upper = middle - 1;
      continue;
    }

    const int compare = index_key::compare(LeafCell::keyView(cell_data), key);
    if (compare < 0) {
      lower = probe + 1;
    } else if (compare > 0) {
      upper = middle - 1;
    } else {
      return true;
    }
  }

  return false;
}

// Appends exact entries from this leaf page based on sorted unique keys.
// Returns the lookup-key position from which the next leaf page should resume.
std::size_t LeafIndexPage::appendExactEntries(
    const std::vector<std::string>& sorted_unique_keys,
    std::size_t lookup_key_pos, std::size_t lookup_key_end,
    std::vector<IndexEntry>& matching_entries) const {
  for (uint16_t leaf_slot = 0; leaf_slot < page_.slotCount(); ++leaf_slot) {
    const char* cell_data = page_.slotCellStartUnchecked(leaf_slot);
    if (!Cell::isValid(cell_data)) {
      continue;
    }

    const std::string_view indexed_key = LeafCell::keyView(cell_data);
    while (lookup_key_pos < lookup_key_end) {
      const int lookup_vs_indexed = index_key::compare(
          sorted_unique_keys[lookup_key_pos], indexed_key);
      if (lookup_vs_indexed < 0) {
        ++lookup_key_pos;
        continue;
      }
      if (lookup_vs_indexed == 0) {
        matching_entries.push_back(
            IndexEntry{std::string(indexed_key), LeafCell::readRID(cell_data)});
      }
      break;
    }
    if (lookup_key_pos == lookup_key_end) {
      break;
    }
  }
  return lookup_key_pos;
}

/**
 * Find the index entries inside right_boundary.
 * Invalidates the corresponding slots if do_invalidate is true.
 * @return a pair of (next_page_id, matching_entries). next_page_id is the
 * right sibling page ID if the right_boundary is not found in the current page
 * and we need to continue searching; otherwise, it is NO_RIGHT_SIBLING
 * indicating we can stop searching.
 *
 * If target_rid is provided, only the entries with the matching RID will be returned/deleted.
 *
 */
std::pair<uint16_t, std::vector<IndexEntry>> LeafIndexPage::findEntries(
    const BTreeCursor::Boundary& left_boundary,
    const BTreeCursor::Boundary& right_boundary, bool do_invalidate,
    std::optional<RID> target_rid) {
  std::vector<IndexEntry> matching_entries;
  bool need_to_scan_next_page = true;
  for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      dbfs_log::index().debug(
          "LeafIndexPage::findEntries skipping invalid slot {}", idx);
      continue;
    }
    const std::string_view key = LeafCell::keyView(cell_data);
    if (!BTreeCursor::isInsideBoundary(key, right_boundary, false)) {
      need_to_scan_next_page = false;
      break;
    }
    if (!BTreeCursor::isInsideBoundary(key, left_boundary, true)) {
      continue;
    }

    const RID rid = LeafCell::readRID(cell_data);
    if (target_rid.has_value() &&
        (rid.heap_page_id != target_rid->heap_page_id ||
         rid.slot_id != target_rid->slot_id)) {
      continue;
    }
    if (do_invalidate) {
      page_.invalidateSlot(idx);
    }
    matching_entries.push_back(IndexEntry{std::string(key), rid});
  }
  if (need_to_scan_next_page) {
    return std::make_pair(this->getRightSiblingPageId(), matching_entries);
  } else {
    return std::make_pair(LeafIndexPage::NO_RIGHT_SIBLING, matching_entries);
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
    write_offset = static_cast<uint16_t>(write_offset - cell.payloadSize());
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer =
        page_.data() + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
    std::memcpy(slot_pointer, &write_offset, sizeof(uint16_t));
  }

  page_.updateSlotCount(new_slot_count);
  page_.updateSlotDirectoryOffset(write_offset);
  page_.markDirty();
}

void LeafIndexPage::transferAndCompactTo(LeafIndexPage& dst,
                                         uint16_t split_slot_index) {
  for (uint16_t slot_index = split_slot_index;
       slot_index < page_.slotCount(); ++slot_index) {
    char* cell_data =
        page_.data() + page_.getCellOffsetOnXthPointer(slot_index);
    if (!Cell::isValid(cell_data)) {
      continue;
    }

    dst.page_.insertCell(LeafCell::decodeCell(cell_data));
    page_.invalidateSlot(slot_index);
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

    char* slot_pointer =
        page_.data() + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
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

/**
 * This function returns the child page ID to follow for a given key.
 * Each separator is the inclusive upper bound of its child page.
 * It returns the last child whose separator equals the key, or the first child
 * whose separator is greater. If neither exists, it returns the rightmost
 * child page ID.
 */
uint16_t InternalIndexPage::findChildPage(const std::string& key) {
  const int slot_count = page_.getSlotCount();
  int lower = 0;
  int upper = slot_count;
  while (lower < upper) {
    const int middle = lower + (upper - lower) / 2;
    int probe = middle;
    const char* cell_data = nullptr;
    while (probe < upper) {
      cell_data = page_.data() + page_.getCellOffsetOnXthPointer(probe);
      if (Cell::isValid(cell_data)) {
        break;
      }
      ++probe;
    }

    if (probe == upper) {
      upper = middle;
      continue;
    }

    const std::string_view cell_key = IntermediateCell::keyView(cell_data);
    if (index_key::compare(cell_key, key) <= 0) {
      lower = probe + 1;
    } else {
      upper = middle;
    }
  }

  int first_greater_slot = lower;
  const char* first_greater_cell_data = nullptr;
  while (first_greater_slot < slot_count) {
    first_greater_cell_data =
        page_.data() + page_.getCellOffsetOnXthPointer(first_greater_slot);
    if (Cell::isValid(first_greater_cell_data)) {
      break;
    }
    ++first_greater_slot;
  }

  int previous_slot = first_greater_slot - 1;
  while (previous_slot >= 0) {
    const char* previous_cell_data =
        page_.data() + page_.getCellOffsetOnXthPointer(previous_slot);
    if (!Cell::isValid(previous_cell_data)) {
      --previous_slot;
      continue;
    }

    const std::string_view previous_key =
        IntermediateCell::keyView(previous_cell_data);
    if (index_key::compare(previous_key, key) == 0) {
      return IntermediateCell::readPageID(previous_cell_data);
    }
    break;
  }

  if (first_greater_slot < slot_count) {
    return IntermediateCell::readPageID(first_greater_cell_data);
  }

  dbfs_log::index().debug(
      "InternalIndexPage::findChildPage: Going to right most child {}.",
      rightMostChildPageId());
  return rightMostChildPageId();
}

/**
 * Replace Cell which points to old_page_id from new_page_id
 */
bool InternalIndexPage::replaceChildPageId(uint16_t old_page_id,
                                           uint16_t new_page_id) {
  if (page_.rightMostChildPageId() == old_page_id) {
    page_.setRightMostChildPageId(new_page_id);
    page_.markDirty();
    return true;
  }

  for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
    char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
    if (!Cell::isValid(cell_data)) {
      continue;
    }

    char* page_id_data = cell_data + Cell::FLAG_FIELD_SIZE + sizeof(uint16_t);
    if (readValue<uint16_t>(page_id_data) != old_page_id) {
      continue;
    }

    std::memcpy(page_id_data, &new_page_id, sizeof(uint16_t));
    page_.markDirty();
    return true;
  }

  return false;
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
    write_offset = static_cast<uint16_t>(write_offset - cell.payloadSize());
    char* cell_destination = page_.data() + write_offset;
    std::vector<std::byte> serialized = cell.serialize();
    std::memcpy(cell_destination, serialized.data(), serialized.size());

    char* slot_pointer =
        page_.data() + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
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

    const std::string_view cell_key = IntermediateCell::keyView(cell_data);
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

    char* slot_pointer =
        page_.data() + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
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
