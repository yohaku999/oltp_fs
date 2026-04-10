#pragma once
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "cell.h"
#include "intermediate_cell.h"
#include "leaf_cell.h"

class LeafIndexPage;
class InternalIndexPage;
/**
 * The structure of page is as follows:
 * | header (256 bytes) | cell pointer array (2 bytes per cell) | cells
 * (variable size) | The header contains the following information in order:
 * - node type flag (1 byte): 0 for intermediate page, 1 for leaf page.
 * - slot count (2 bytes): the number of cells in the page.
 * - slot directory offset (2 bytes): the offset of the start of the cell area.
 * - right-most child pointer (2 bytes): valid for intermediate pages, 0 for
 * leaf pages.
 * - page LSN (8 bytes): LSN of the latest WAL record whose effects are
 * reflected in this page (used for WAL / recovery coordination). The remaining
 * bytes in the 256-byte header are reserved for future use.
 */
class Page {
 private:
  static constexpr size_t NODE_TYPE_FLAG_OFFSET = 0;
  static constexpr size_t SLOT_COUNT_OFFSET =
      NODE_TYPE_FLAG_OFFSET + sizeof(uint8_t);
  static constexpr size_t SLOT_DIRECTORY_OFFSET =
      SLOT_COUNT_OFFSET + sizeof(uint16_t);
  static constexpr size_t RIGHT_MOST_CHILD_POINTER_OFFSET =
      SLOT_DIRECTORY_OFFSET + sizeof(uint16_t);
  static constexpr size_t PAGE_LSN_OFFSET =
      RIGHT_MOST_CHILD_POINTER_OFFSET + sizeof(uint16_t);
  IntermediateCell getIntermediateCellOnXthPointer(int x);
  LeafCell getLeafCellOnXthPointer(int x);
  uint16_t getCellOffsetOnXthPointer(int x);
  uint16_t getSlotCount();
  uint16_t getSlotDirectoryOffset();
  void updateSlotCount(uint16_t new_count);
  void updateSlotDirectoryOffset(uint16_t new_offset);
  void updateNodeTypeFlag(bool is_leaf);
  uint16_t rightMostChildPageId() const;
  void setRightMostChildPageId(uint16_t page_id);
  void updatePageLSN(std::uint64_t lsn);
  bool is_dirty_ = false;
  int page_id_ = -1;
  int parent_page_id_ = HAS_NO_PARENT;

  friend class LeafIndexPage;
  friend class InternalIndexPage;

 public:
  static constexpr int HAS_NO_PARENT = -1;
  static constexpr size_t HEADDER_SIZE_BYTE = 256;
  void markDirty() { is_dirty_ = true; };
  void clearDirty() { is_dirty_ = false; };
  bool isDirty() { return is_dirty_; };
  int getPageID() const { return page_id_; };
  int getParentPageID() const { return parent_page_id_; };
  // NOTE : Relation between PageID will be collected on traversal for now. This
  // can be bothring later.
  void setParentPageID(int parent_page_id) {
    parent_page_id_ = parent_page_id;
  };
  bool isLeaf() const;
  char* getSplitKeyCellStart();
  // The value range of cell is 0~4095 for now, so we can use uint16_t to store
  // them.
  static constexpr size_t PAGE_SIZE_BYTE = 4096;
  static constexpr size_t CELL_POINTER_SIZE = sizeof(uint16_t);
  char* start_p_;
  char* getSlotValueStart(int slot_id);
  std::optional<int> insertCell(const std::vector<std::byte>& serialized_cell);
  std::optional<int> insertCell(const Cell& cell);
  void invalidateSlot(uint16_t slot_id);
  std::uint64_t getPageLSN() const;
  void setPageLSN(std::uint64_t lsn) {
    updatePageLSN(lsn);
    markDirty();
  }
  char* getSlotCellStart(int slot_id);

  // Constructor for creating a new page
  Page(char* start_p, bool is_leaf, uint16_t right_most_child_page_id,
       uint16_t page_id);

  // Constructor for wrapping existing page data
  Page(char* start_p, uint16_t page_id);

  void dump(std::ostream& os);
};