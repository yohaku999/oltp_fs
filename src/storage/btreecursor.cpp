#include "btreecursor.h"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>

#include "bufferpool.h"
#include "cell.h"
#include "index_page.h"
#include "logging.h"
#include "page.h"
#include "record_cell.h"

int BTreeCursor::findLeafPageID(BufferPool& pool, File& indexFile, int key) {
  // traverse the btree from the root page until we find the leaf page that may
  // contain the key.
  int page_id = indexFile.getRootPageID();
  int parent_page_id = Page::HAS_NO_PARENT;
  while (true) {
    LOG_INFO(
        "Traversing to find leaf page for key {}: currently at page ID {} in "
        "index file {}.",
        key, page_id, indexFile.getFilePath());
    Page* page = pool.pinPage(page_id, indexFile);
    // if the page has parent page id of -1, the page is the root page or just
    // not memoed yet.
    page->setParentPageID(parent_page_id);
    if (page->isLeaf()) {
      LOG_INFO("Found leaf page ID {} for key {} in index {}", page_id, key,
               indexFile.getFilePath());
      pool.unpinPage(page, indexFile);
      break;
    }
    // zero outしているので、はじめは必ずintermediate扱いになる
    InternalIndexPage internal(*page);
    // 実装を簡単にするため、B木の初期化時点でPageID 0のintermediate
    // pageのrightmostpageIDがPageID 1のLeaf pageを指すようにしている。
    // ここでrightmostpageIDの初期値が0なので困っている。
    // rightmostの下にleafまでpage0,1,2で初期化するか
    // rightmostを1にするなら、その先を辿っていくことになり、ページ1として、leafを初期化すればいい。
    //
    // leafを0ノードとしてつくるか、みたいなところで悩んでいる。
    // そもそもrightmostpageIDのsplitってちゃんとできるんだっけ
    // 現在のsplitの実装がうまく走るように初期化を構築したい。
    int child_page_id = internal.findChildPage(key);
    pool.unpinPage(page, indexFile);
    LOG_INFO("The child page ID of page ID {} for key {} is {}", page_id, key,
             child_page_id);
    parent_page_id = page_id;
    page_id = child_page_id;
  }
  return page_id;
}

std::optional<std::pair<uint16_t, uint16_t>> BTreeCursor::findRecordLocation(
    BufferPool& pool, File& indexFile, int key, bool do_invalidate) {
  LOG_INFO("Finding record location for key {} in index file {}.", key,
           indexFile.getFilePath());
  // NOTE: we decided not to invalidate intermediate nodes during traversal for
  // now. we will come back to this when we start to support concurrency.
  int page_id = findLeafPageID(pool, indexFile, key);
  Page* leaf_page = pool.pinPage(page_id, indexFile);
  LeafIndexPage leaf(*leaf_page);
  auto result = leaf.findRef(key, do_invalidate);
  pool.unpinPage(leaf_page, indexFile);
  if (!result.has_value()) {
    LOG_INFO("Key {} not found in leaf page ID {} of index file {}.", key,
             page_id, indexFile.getFilePath());
  } else {
    LOG_INFO(
        "Found record location for key {} in leaf page ID {} of index file {}: "
        "heap page ID {}, slot ID {}.",
        key, page_id, indexFile.getFilePath(), result->first, result->second);
  }
  return result;
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
    int new_root_page_id =
        pool.createPage(false, index_file, old_page.getPageID());
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
  int new_page_id = pool.createPage(true, index_file);
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
  int new_page_id = pool.createPage(false, index_file, old_page.getPageID());
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
    page->dump(os);
    pool.unpinPage(page, indexFile);
  }
}