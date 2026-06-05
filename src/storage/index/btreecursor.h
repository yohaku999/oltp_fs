#pragma once
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "storage/buffer/bufferpool.h"
#include "storage/index/rid.h"

class IntermediateCell;

/**
 * BTreeCursor is an arbitration layer that coordinates BufferPool and File to
 * execute B-Tree index traversal and modification of index page as a single
 * transactional sequence.
 *
 * A path-generation helper centralizes file naming under
 * ./data/<table>.{index,db}. File is responsible for page-ID allocation and
 * persistence (e.g., maintaining the high-water mark), while BufferPool
 * provides page caching.
 */
class BTreeCursor {
  
 public:
  struct Boundary {
    std::string composite_key;
    bool is_inclusive;
  };
  static std::vector<IndexEntry> findEntries(
      BufferPool& pool, File& indexFile,
      std::pair<Boundary, Boundary> boundaries, bool do_invalidate);
  static int findLeafPageID(BufferPool& pool, File& indexFile,
                            const std::string& key);
  static IntermediateCell splitPage(BufferPool& pool, File& indexFile, Page* old_page, Page* parent_page);
  static void insertIntoIndex(BufferPool& pool, File& indexFile,
                              const std::string& key,
                              uint16_t heap_page_id, uint16_t slot_id);
  static IntermediateCell splitLeafPage(BufferPool& pool, File& index_file,
                                        Page& old_page,
                                        const std::string& separate_key);
  static IntermediateCell splitInternalPage(BufferPool& pool, File& index_file,
                                            Page& old_page,
                                            const std::string& separate_key);
  static Page* ensureParentPage(BufferPool& pool, File& index_file,
                                Page& old_page);
  static bool isInsideBoundary(std::string_view key, Boundary boundary, bool is_boundary_left);

 public:
  static void dumpTree(BufferPool& pool, File& indexFile, std::ostream& os);
};
