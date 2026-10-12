#pragma once

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "index_key.h"
#include "intermediate_cell.h"
#include "leaf_cell.h"
#include "rid.h"
#include "storage/index/btreecursor.h"
#include "storage/page/page.h"

// LeafIndexPage assume the underlying Page buffer/layout is
// already initialized and do not own the storage.

class LeafIndexPage {
 public:
  static constexpr uint16_t NO_RIGHT_SIBLING =
      65535;  // special page ID indicating no right sibling for leaf pages.
  explicit LeafIndexPage(Page& page) : page_(page) {
    if (!page_.isLeaf()) {
      throw std::logic_error("LeafIndexPage constructed with non-leaf Page");
    }
  }

  Page& page() { return page_; }
  const Page& page() const { return page_; }

  LeafCell cellAt(int slot_id) const;
  bool hasKey(const std::string& key) const;
  std::size_t appendExactEntries(
      const std::vector<std::string>& sorted_unique_keys,
      std::size_t lookup_key_pos, std::size_t lookup_key_end,
      std::vector<IndexEntry>& matching_entries) const;
  std::pair<uint16_t, std::vector<IndexEntry>> findEntries(
      const BTreeCursor::Boundary& left_boundary,
      const BTreeCursor::Boundary& right_boundary, bool do_invalidate,
      std::optional<RID> target_rid = std::nullopt);
  void compact();
  void getRightSidePageID();

  void transferAndCompactTo(LeafIndexPage& dst, uint16_t split_slot_index);

  uint16_t getRightSiblingPageId() {
    return this->page_.rightMostChildPageId();
  }
  void setRightSiblingPageId(uint16_t page_id) {
    this->page_.setRightMostChildPageId(page_id);
    this->page_.markDirty();
  }

 private:
  Page& page_;
};

class InternalIndexPage {
 public:
  explicit InternalIndexPage(Page& page) : page_(page) {
    if (page_.isLeaf()) {
      throw std::logic_error("InternalIndexPage constructed with leaf Page");
    }
  }

  Page& page() { return page_; }
  const Page& page() const { return page_; }

  IntermediateCell cellAt(int slot_id) const;
  uint16_t rightMostChildPageId() const;
  uint16_t findChildPage(const std::string& key);
  bool replaceChildPageId(uint16_t old_page_id, uint16_t new_page_id);
  void compact();
  void transferAndCompactTo(InternalIndexPage& dst,
                            const std::string& separate_key);
  uint16_t leftMostChildPageId() const {
    std::optional<std::string_view> leftmost_key;
    uint16_t leftmost_page_id = 0;

    for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
      char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
      if (!Cell::isValid(cell_data)) {
        continue;
      }

      const std::string_view key = IntermediateCell::keyView(cell_data);
      if (!leftmost_key.has_value() ||
          index_key::compare(key, leftmost_key.value()) < 0) {
        leftmost_key = key;
        leftmost_page_id = IntermediateCell::readPageID(cell_data);
      }
    }

    if (!leftmost_key.has_value()) {
      throw std::logic_error(
          "InternalIndexPage::leftMostChildPageId: internal page has no valid "
          "separator cell");
    }

    return leftmost_page_id;
  }

 private:
  Page& page_;
};
