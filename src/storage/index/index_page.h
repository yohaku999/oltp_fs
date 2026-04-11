#pragma once

#include <optional>

#include "intermediate_cell.h"
#include "leaf_cell.h"
#include "storage/page/page.h"
#include "rid.h"

// LeafIndexPage assume the underlying Page buffer/layout is
// already initialized and do not own the storage.

class LeafIndexPage {
 public:
  explicit LeafIndexPage(Page& page) : page_(page) {
    if (!page_.isLeaf()) {
      throw std::logic_error("LeafIndexPage constructed with non-leaf Page");
    }
  }

  Page& page() { return page_; }
  const Page& page() const { return page_; }

  LeafCell cellAt(int slot_id) const;
  bool hasKey(int key) const;
  std::optional<RID> findRef(int key, bool do_invalidate);

  void transferAndCompactTo(LeafIndexPage& dst, char* separate_key);

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
  uint16_t findChildPage(int key);
  void transferAndCompactTo(InternalIndexPage& dst, char* separate_key);

 private:
  Page& page_;
};
