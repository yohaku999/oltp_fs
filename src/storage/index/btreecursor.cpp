#include "btreecursor.h"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>

#include "storage/runtime/bufferpool.h"
#include "storage/page/cell.h"
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
      os << "  [" << i << "] Leaf  key=" << cell.key()
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
      os << "  [" << i << "] Inter key=" << cell.key()
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
int BTreeCursor::findLeafPageID(BufferPool& pool, File& indexFile, int key) {
  int page_id = indexFile.getRootPageID();
  int parent_page_id = Page::HAS_NO_PARENT;
  while (true) {
    LOG_INFO(
        "Traversing to find leaf page for key {}: currently at page ID {} in "
        "index file {}.",
        key, page_id, indexFile.getFilePath());
    Page* page = pool.pinPage(page_id, indexFile);
    page->setParentPageID(parent_page_id);
    if (page->isLeaf()) {
      LOG_INFO("Found leaf page ID {} for key {} in index {}", page_id, key,
               indexFile.getFilePath());
      pool.unpinPage(page, indexFile);
      break;
    } else {
      InternalIndexPage internal(*page);
      int child_page_id = internal.findChildPage(key);
      pool.unpinPage(page, indexFile);
      LOG_INFO("The child page ID of page ID {} for key {} is {}", page_id, key,
               child_page_id);
      parent_page_id = page_id;
      page_id = child_page_id;
    }
  }
  return page_id;
}

std::optional<RID> BTreeCursor::findRecordLocation(BufferPool& pool,
                           File& indexFile, int key,
                           bool do_invalidate) {
  LOG_INFO("Finding record location for key {} in index file {}.", key,
           indexFile.getFilePath());
  // NOTE: we decided not to invalidate intermediate nodes during traversal for
  // now. we will come back to this when we start to support concurrency.
  int page_id = findLeafPageID(pool, indexFile, key);
  Page* leaf_page = pool.pinPage(page_id, indexFile);
  LeafIndexPage leaf(*leaf_page);
  std::optional<RID> rid = leaf.findRef(key, do_invalidate);
  pool.unpinPage(leaf_page, indexFile);
  if (!rid.has_value()) {
    LOG_INFO("Key {} not found in leaf page ID {} of index file {}.", key,
             page_id, indexFile.getFilePath());
    return std::nullopt;
  } else {
    LOG_INFO(
        "Found record location for key {} in leaf page ID {} of index file {}: "
        "heap page ID {}, slot ID {}.",
        key, page_id, indexFile.getFilePath(), rid->heap_page_id,
        rid->slot_id);
  }
  return rid;
}

void BTreeCursor::insertIntoIndex(BufferPool& pool, File& indexFile, int key,
                                  uint16_t heap_page_id, uint16_t slot_id) {
  LOG_INFO(
      "Inserting index entry for key {} pointing to heap page ID {}, slot ID "
      "{} into index file {}.",
      key, heap_page_id, slot_id, indexFile.getFilePath());
  // find the leaf page to insert the new record.
  // OPTIMIZE : currently we are traversing the same path twice and can be
  // omitted.
  int leaf_page_id = findLeafPageID(pool, indexFile, key);
  Page* leaf_page = pool.pinPage(leaf_page_id, indexFile);
  auto leaf_slot_id =
      leaf_page->insertCell(LeafCell(key, heap_page_id, slot_id));
  if (!leaf_slot_id.has_value()) {
    splitPage(pool, indexFile, leaf_page);
    pool.unpinPage(leaf_page, indexFile);
    insertIntoIndex(pool, indexFile, key, heap_page_id, slot_id);
  } else {
    pool.unpinPage(leaf_page, indexFile);
  }
  LOG_INFO(
      "Inserted index entry for key {} pointing to heap page ID {}, slot ID "
      "{}.",
      key, heap_page_id, slot_id);
}

Page* BTreeCursor::ensureParentPage(BufferPool& pool, File& index_file,
                                    Page& old_page) {
  int parent_page_id;
  if (old_page.getParentPageID() == Page::HAS_NO_PARENT) {
    LOG_INFO(
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

void BTreeCursor::splitLeafPage(BufferPool& pool, File& index_file,
                                Page& old_page, Page& parent_page,
                                char* separate_key) {
  int separate_key_value = LeafCell::getKey(separate_key);
  int new_page_id = pool.createPage(PageKind::LeafIndex, index_file);
  Page* new_page = pool.pinPage(new_page_id, index_file);

  LeafIndexPage old_leaf(old_page);
  LeafIndexPage new_leaf(*new_page);
  old_leaf.transferAndCompactTo(new_leaf, separate_key);

  parent_page.insertCell(IntermediateCell(new_page_id, separate_key_value));

  pool.unpinPage(new_page, index_file);
}

void BTreeCursor::splitInternalPage(BufferPool& pool, File& index_file,
                                    Page& old_page, Page& parent_page,
                                    char* separate_key) {
  int separate_key_value = IntermediateCell::getKey(separate_key);
  int new_page_id = pool.createPage(PageKind::InternalIndex, index_file,
                                    old_page.getPageID());
  Page* new_page = pool.pinPage(new_page_id, index_file);

  InternalIndexPage old_internal(old_page);
  InternalIndexPage new_internal(*new_page);
  old_internal.transferAndCompactTo(new_internal, separate_key);

  parent_page.insertCell(IntermediateCell(new_page_id, separate_key_value));

  pool.unpinPage(new_page, index_file);
}

void BTreeCursor::splitPage(BufferPool& pool, File& index_file,
                            Page* old_page) {
  LOG_INFO("splitPage called on page ID {}.", old_page->getPageID());

  auto parent_page = ensureParentPage(pool, index_file, *old_page);

  LOG_INFO("Split old page and rewire pointer.");
  char* separate_key = old_page->getSplitKeyCellStart();

  if (old_page->isLeaf()) {
    splitLeafPage(pool, index_file, *old_page, *parent_page, separate_key);
  } else {
    splitInternalPage(pool, index_file, *old_page, *parent_page, separate_key);
  }

  pool.unpinPage(parent_page, index_file);
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