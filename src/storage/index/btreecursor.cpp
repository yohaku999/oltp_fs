#include "btreecursor.h"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>

#include "storage/runtime/bufferpool.h"
#include "storage/page/cell.h"
#include "index_key.h"
#include "index_page.h"
#include "logging.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"

namespace {

void dumpIndexPage(const Page& page, std::ostream& os) {
  os << "=== Page " << page.getPageID() << " ("
     << (page.isLeaf() ? "leaf" : "internal") << ") ===\n";
  os << "parent=" << page.getParentPageID()
    << " slotCount=" << static_cast<int>(page.slotCount());
  if (!page.isLeaf()) {
    os << " rightMostChild="
       << InternalIndexPage(const_cast<Page&>(page)).rightMostChildPageId();
  }
  os << "\n";

  if (page.isLeaf()) {
    LeafIndexPage leaf(const_cast<Page&>(page));
    for (int i = 0; i < page.slotCount(); ++i) {
      const char* cell_data = page.slotCellStartUnchecked(i);
      if (!Cell::isValid(cell_data)) {
        os << "  [" << i << "] <invalid>\n";
        continue;
      }

      LeafCell cell = leaf.cellAt(i);
      os << "  [" << i << "] Leaf  key="
        << index_key::formatForDebug(cell.key())
         << " heapPage=" << cell.heap_page_id()
         << " slot=" << cell.slot_id() << "\n";
    }
  } else {
    InternalIndexPage internal(const_cast<Page&>(page));
    for (int i = 0; i < page.slotCount(); ++i) {
      const char* cell_data = page.slotCellStartUnchecked(i);
      if (!Cell::isValid(cell_data)) {
        os << "  [" << i << "] <invalid>\n";
        continue;
      }

      IntermediateCell cell = internal.cellAt(i);
    os << "  [" << i << "] Inter key="
      << index_key::formatForDebug(cell.key())
         << " childPage=" << cell.page_id() << "\n";
    }
  }

  os << "\n";
}

}  // namespace

/**
 * Traverses the B-tree to find the leaf page that may contain `key`.
 *
 * @param pool Buffer pool used to pin and unpin index pages during traversal.
 * @param indexFile Index file whose root page seeds the traversal.
 * @param key Search key to route through internal nodes.
 * @return Page ID of the leaf page where `key` should reside.
 *
 * As a side effect, each visited page is annotated with its parent page ID for
 * later split handling.
 */
int BTreeCursor::findLeafPageID(BufferPool& pool, File& indexFile,
                               const std::string& key) {
  int page_id = indexFile.getRootPageID();
  int parent_page_id = Page::HAS_NO_PARENT;
  while (true) {
    dbfs_log::index().debug(
      "Traversing to find leaf page for key {}: currently at page ID {} in "
      "index file {}.",
      index_key::formatForDebug(key), page_id, indexFile.getFilePath());
    Page* page = pool.pinPage(page_id, indexFile);
    page->setParentPageID(parent_page_id);
    if (page->isLeaf()) {
      dbfs_log::index().debug("Found leaf page ID {} for key {} in index {}", page_id,
               index_key::formatForDebug(key), indexFile.getFilePath());
      pool.unpinPage(page, indexFile);
      break;
    }

    InternalIndexPage internal(*page);
    int child_page_id = internal.findChildPage(key);
    pool.unpinPage(page, indexFile);
    dbfs_log::index().debug("The child page ID of page ID {} for key {} is {}", page_id,
          index_key::formatForDebug(key), child_page_id);
    parent_page_id = page_id;
    page_id = child_page_id;
  }
  return page_id;
}

std::optional<RID> BTreeCursor::findRID(BufferPool& pool, File& indexFile,
                                        const std::string& key,
                                        bool do_invalidate) {
  dbfs_log::index().debug("Finding RID for key {} in index file {}.",
            index_key::formatForDebug(key), indexFile.getFilePath());
  // NOTE: we decided not to invalidate intermediate nodes during traversal for
  // now. we will come back to this when we start to support concurrency.
  int page_id = findLeafPageID(pool, indexFile, key);
  Page* leaf_page = pool.pinPage(page_id, indexFile);
  LeafIndexPage leaf(*leaf_page);
  std::optional<RID> rid = leaf.findRef(key, do_invalidate);
  pool.unpinPage(leaf_page, indexFile);
  if (!rid.has_value()) {
    dbfs_log::index().debug("Key {} not found in leaf page ID {} of index file {}.",
          index_key::formatForDebug(key), page_id,
          indexFile.getFilePath());
    return std::nullopt;
  }

    dbfs_log::index().debug(
      "Found RID for key {} in leaf page ID {} of index file {}: "
      "heap page ID {}, slot ID {}.",
      index_key::formatForDebug(key), page_id, indexFile.getFilePath(),
      rid->heap_page_id, rid->slot_id);
  return rid;
}

void BTreeCursor::insertIntoIndex(BufferPool& pool, File& indexFile,
                                  const std::string& key,
                                  uint16_t heap_page_id, uint16_t slot_id) {
    dbfs_log::index().debug(
      "Inserting index entry for key {} pointing to heap page ID {}, slot ID "
      "{} into index file {}.",
      index_key::formatForDebug(key), heap_page_id, slot_id,
      indexFile.getFilePath());
  int target_page_id = findLeafPageID(pool, indexFile, key);
  Page* target_page = pool.pinPage(target_page_id, indexFile);
  std::unique_ptr<Cell> cell_to_insert =
      std::make_unique<LeafCell>(key, heap_page_id, slot_id);

  while (true) {
    auto inserted_slot_id = target_page->insertCell(*cell_to_insert);
    if (inserted_slot_id.has_value()) {
      pool.unpinPage(target_page, indexFile);
      break;
    }

    Page* parent_page = ensureParentPage(pool, indexFile, *target_page);
    IntermediateCell separator_cell =
        splitPage(pool, indexFile, target_page, parent_page);

    // find page to be inserted by comparing the separator key with the key to be inserted, and insert into the page. Note that the separator key is guaranteed to be greater than all keys in the old page, so if the key to be inserted is less than or equal to the separator key, it should be inserted into the old page instead of traversing the whole tree to find correct page to be inserted.
    Page* retry_page = target_page;
    Page* new_page = nullptr;
    if (index_key::compare(cell_to_insert->key(), separator_cell.key()) <=
        0) {
      new_page = pool.pinPage(separator_cell.page_id(), indexFile);
      retry_page = new_page;
    }

    inserted_slot_id = retry_page->insertCell(*cell_to_insert);
    if (new_page != nullptr) {
      pool.unpinPage(new_page, indexFile);
    }
    if (!inserted_slot_id.has_value()) {
      pool.unpinPage(parent_page, indexFile);
      pool.unpinPage(target_page, indexFile);
      throw std::logic_error(
          "BTreeCursor::insertIntoIndex: insertion failed even after split");
    }

    pool.unpinPage(target_page, indexFile);
    cell_to_insert = std::make_unique<IntermediateCell>(separator_cell);
    target_page = parent_page;
  }

    dbfs_log::index().info(
      "Inserted index entry for key {} pointing to heap page ID {}, slot ID "
      "{}.",
      index_key::formatForDebug(key), heap_page_id, slot_id);
}

/**
 * @brief creates parent page if the old_page is the initial page of the btree.
 * @param txn The transaction performing the insert.
 * @return parent page of old_page, which is the new root page of the btree.
 */
Page* BTreeCursor::ensureParentPage(BufferPool& pool, File& index_file,
                                    Page& old_page) {
  int parent_page_id;
  if (old_page.getParentPageID() == Page::HAS_NO_PARENT) {
    dbfs_log::index().debug(
      "Initialized Parent Page for page ID {} because it has no parent but "
      "root page.",
      old_page.getPageID());
    int new_root_page_id = pool.createPage(PageKind::InternalIndex, index_file,
                         old_page.getPageID());
    index_file.setRootPageID(new_root_page_id);
    old_page.setParentPageID(new_root_page_id);
    parent_page_id = new_root_page_id;
  } else {
    parent_page_id = old_page.getParentPageID();
  }

  Page* parent_page = pool.pinPage(parent_page_id, index_file);
  return parent_page;
}

/**
 * @brief splits the old_page
 * create new page and move half of the cells from old_page to the new page, then insert a separator cell into the parent page.
 * @param pool Buffer pool used to pin and unpin index pages during split.
 * @param index_file Index file to which the pages belong.
 * @param old_page The page to split.
 * @param separate_key The key to separate the old page and the new page.
 * @return the separator cell to be inserted into the parent page.
 */
IntermediateCell BTreeCursor::splitLeafPage(BufferPool& pool, File& index_file,
                                            Page& old_page,
                                            const std::string& separate_key) {
  int new_page_id = pool.createPage(PageKind::LeafIndex, index_file);
  Page* new_page = pool.pinPage(new_page_id, index_file);

  LeafIndexPage old_leaf(old_page);
  LeafIndexPage new_leaf(*new_page);
  old_leaf.transferAndCompactTo(new_leaf, separate_key);

  pool.unpinPage(new_page, index_file);
  return IntermediateCell(new_page_id, separate_key);
}

IntermediateCell BTreeCursor::splitInternalPage(BufferPool& pool,
                                                File& index_file,
                                                Page& old_page,
                                                const std::string& separate_key) {
  int new_page_id = pool.createPage(PageKind::InternalIndex, index_file);
  Page* new_page = pool.pinPage(new_page_id, index_file);

  InternalIndexPage old_internal(old_page);
  InternalIndexPage new_internal(*new_page);
  old_internal.transferAndCompactTo(new_internal, separate_key);

  pool.unpinPage(new_page, index_file);
  return IntermediateCell(new_page_id, separate_key);
}

IntermediateCell BTreeCursor::splitPage(BufferPool& pool, File& index_file,
                            Page* old_page, Page*parent_page) {
  dbfs_log::index().debug("Split old page and rewire pointer.");
  if (old_page->isLeaf()) {
    const std::string separate_key =
        LeafCell::getKey(old_page->getSplitKeyCellStart());
    return splitLeafPage(pool, index_file, *old_page, separate_key);
  } else {
    const std::string separate_key =
        IntermediateCell::getKey(old_page->getSplitKeyCellStart());
    return splitInternalPage(pool, index_file, *old_page, separate_key);
  }
}

void BTreeCursor::dumpTree(BufferPool& pool, File& indexFile,
                           std::ostream& os) {
  const uint16_t max_page_id = indexFile.getMaxPageID();
  for (uint16_t page_id = 0; page_id <= max_page_id; ++page_id) {
    if (!indexFile.isPageIDUsed(page_id)) {
      continue;
    }

    Page* page = pool.pinPage(page_id, indexFile);
    dumpIndexPage(*page, os);
    pool.unpinPage(page, indexFile);
  }
}
