#pragma once
#include <optional>
#include <ostream>
#include <string>

#include "storage/runtime/bufferpool.h"
#include "storage/index/rid.h"

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
  static std::optional<RID> findRID(BufferPool& pool, File& indexFile,
                                    const std::string& key,
                                    bool do_invalidate = false);
  static int findLeafPageID(BufferPool& pool, File& indexFile,
                            const std::string& key);
  static void splitPage(BufferPool& pool, File& indexFile, Page* old_page);
  static void insertIntoIndex(BufferPool& pool, File& indexFile,
                              const std::string& key,
                              uint16_t heap_page_id, uint16_t slot_id);
  static void splitLeafPage(BufferPool& pool, File& index_file, Page& old_page,
                            Page& parent_page,
                            const std::string& separate_key);
  static void splitInternalPage(BufferPool& pool, File& index_file,
                                Page& old_page, Page& parent_page,
                                const std::string& separate_key);
  static Page* ensureParentPage(BufferPool& pool, File& index_file,
                                Page& old_page);

 public:
  static void dumpTree(BufferPool& pool, File& indexFile, std::ostream& os);
};