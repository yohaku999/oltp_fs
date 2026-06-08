#pragma once

#include <optional>
#include <stdexcept>
#include <string>

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
  std::pair<uint16_t, std::vector<IndexEntry>> findEntries(
      BTreeCursor::Boundary left_boundary, BTreeCursor::Boundary right_boundary,
      bool do_invalidate);
  void compact();
  void getRightSidePageID();

  void transferAndCompactTo(LeafIndexPage& dst,
                            const std::string& separate_key);

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
    std::optional<IntermediateCell> leftmost;

    for (int idx = 0; idx < page_.getSlotCount(); ++idx) {
      char* cell_data = page_.data() + page_.getCellOffsetOnXthPointer(idx);
      if (!Cell::isValid(cell_data)) {
        continue;
      }

      IntermediateCell cell = cellAt(idx);
      if (!leftmost.has_value() ||
          index_key::compare(cell.key(), leftmost->key()) < 0) {
        leftmost = cell;
      }
    }

    if (!leftmost.has_value()) {
      throw std::logic_error(
          "InternalIndexPage::leftMostChildPageId: internal page has no valid "
          "separator cell");
    }

    return leftmost->page_id();
  }

 private:
  Page& page_;
};
