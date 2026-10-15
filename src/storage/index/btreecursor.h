#pragma once
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "storage/buffer/bufferpool.h"
#include "storage/index/intermediate_cell.h"
#include "storage/index/rid.h"

/**
 * BTreeCursor is an arbitration layer that coordinates BufferPool and
 * PagedFile to execute B-Tree index traversal and modification of index pages
 * as a single transactional sequence.
 *
 * A path-generation helper centralizes file naming under
 * ./data/<table>.{index,db}. PagedFile is responsible for page-ID allocation
 * and persistence (e.g., maintaining the high-water mark), while BufferPool
 * provides page caching.
 */
class BTreeCursor {
 public:
  struct Boundary {
    std::string composite_key;
    bool is_inclusive;
  };
  struct SplitResult {
    IntermediateCell separator_cell;
    uint16_t right_page_id;
  };
  static std::vector<IndexEntry> findEntries(
      BufferPool& pool, PagedFile& indexFile,
      const std::pair<Boundary, Boundary>& boundaries, bool do_invalidate,
      std::optional<RID> target_rid = std::nullopt,
      std::optional<std::size_t> limit_count = std::nullopt);
  // Keys must be strictly increasing. Entries are returned in key order.
  static std::vector<IndexEntry> findExactEntries(
      BufferPool& pool, PagedFile& indexFile,
      const std::vector<std::string>& sorted_unique_keys);
  static int findLeafPageID(BufferPool& pool, PagedFile& indexFile,
                            const std::string& key);
  static bool containsKey(BufferPool& pool, PagedFile& indexFile,
                          const std::string& key);
  static SplitResult splitPage(BufferPool& pool, PagedFile& indexFile,
                               Page* old_page, Page* parent_page);
  static void insertIntoIndex(BufferPool& pool, PagedFile& indexFile,
                              const std::string& key, uint16_t heap_page_id,
                              uint16_t slot_id);
  static void replaceRID(BufferPool& pool, PagedFile& indexFile,
                         const std::string& key, const RID& old_rid,
                         const RID& new_rid);
  static void removeRID(BufferPool& pool, PagedFile& indexFile,
                        const std::string& key, const RID& rid);
  static SplitResult splitLeafPage(BufferPool& pool, PagedFile& index_file,
                                   Page& old_page,
                                   const std::string& separate_key);
  static SplitResult splitInternalPage(BufferPool& pool, PagedFile& index_file,
                                       Page& old_page,
                                       const std::string& separate_key);
  static Page* ensureParentPage(BufferPool& pool, PagedFile& index_file,
                                Page& old_page);
  static bool isInsideBoundary(std::string_view key, const Boundary& boundary,
                               bool is_boundary_left);

 public:
  static void dumpTree(BufferPool& pool, PagedFile& indexFile,
                       std::ostream& os);
};
