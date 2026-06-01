#include "btreecursor.h"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/runtime/bufferpool.h"
#include "storage/page/cell.h"
#include "index_key.h"
#include "logging.h"
#include "storage/page/page.h"
#include "storage/index/index_page.h"
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

void compactIndexPage(Page& page) {
  if (page.isLeaf()) {
    LeafIndexPage(page).compact();
    return;
  }
  InternalIndexPage(page).compact();
}

bool hasInvalidCell(const Page& page) {
  for (uint16_t slot_id = 0; slot_id < page.slotCount(); ++slot_id) {
    if (!Cell::isValid(page.slotCellStartUnchecked(slot_id))) {
      return true;
    }
  }
  return false;
}

std::optional<int> insertCellWithCompaction(Page& page, const Cell& cell) {
  std::optional<int> inserted_slot_id = page.insertCell(cell);
  if (inserted_slot_id.has_value()) {
    return inserted_slot_id;
  }

  if (!hasInvalidCell(page)) {
    return std::nullopt;
  }

  compactIndexPage(page);
  return page.insertCell(cell);
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

/**
 * indexFileで定義される単一のindexキーに対して、"key"の値をopに基づいて比較し、一致しているRIDを返す。
 * 一致はeq、gt、ge、lt、leのそれぞれに対応している。
 * 
 * この関数を複数キー、複数条件に対応させる。また、一部のキーに対しても対応させる。
 * なぜ、index上でキーのdescなどが重要なのか？
 */

// TODO:引数にフィリタリングも追加
std::vector<RID> BTreeCursor::findRIDs(BufferPool& pool, File& indexFile,
                                       bool do_invalidate, const std::vector<std::vector<BoundComparisonPredicate>>& ordered_predicates, const std::vector<std::size_t>& key_order_indexes) {
  // 走査を開始するleafnodeを見つける。
  // 走査は必ず左から右に行う。
  // 1st_key < xの場合、走査開始は最も左のleafnode, 1st_key >= xの場合、走査開始はxを含むleafnode、場合によっては1st_key < yによって走査終了となる。
  
  // 2nd key以降は、1st_keyがeqでない限り、ページ内部を地道に舐めるために使用される。
  // 1st_keyがeqであれば、2nd_keyは走査開始のleafnodeを特定するために使用される。

  // めんどくさいなこのロジック。もっと簡単に考えられないか？抽象のレイヤを分けられないか？
  // NOTE: we decided not to invalidate intermediate nodes during traversal for
  // now. we will come back to this when we start to support concurrency.

  // no left buondary : Boundary("", true)
  // find traversal start point based on left boundary. if left boundary is empty, start from the left most leaf page. 
  // otherwise, start from the leaf page that may contain the left boundary key.
  
  // define traversal boundary based on ordered predicates.
  Boundary left_boundary{"", true};
  Boundary right_boundary{"", true};
  for (size_t i = 0; i < ordered_predicates.size(); ++i) {
    const auto& predicates_for_key = ordered_predicates[i];
    if (predicates_for_key.empty() && i == 0) {
       throw std::logic_error("The leading key must have at least one predicate for index lookup.");
    }
    if (predicates_for_key.empty() && i != 0) {
      // if non-leading key has no predicate, we will not be able to use it for boundary for latter keys and define boundary here.
      break;
    }
    const auto& eq_pred_it = std::find_if(predicates_for_key.begin(), predicates_for_key.end(), [](const BoundComparisonPredicate& predicate) { return predicate.op == Op::Eq; });
    if (eq_pred_it != predicates_for_key.end()) {
      // if predicate is eq, it can be both left and right boundary.
      const auto& eq_pred = *eq_pred_it;
      const auto& value = std::get<FieldValue>(std::get_if<BoundColumnRef>(&eq_pred.left) ? eq_pred.right : eq_pred.left);
      const auto& column_type = std::get<BoundColumnRef>(std::get_if<BoundColumnRef>(&eq_pred.left) ? eq_pred.left : eq_pred.right).type;
      // ここにカラムタイプ情報が必要。
      left_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      right_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      continue;
    }
    const auto& gt_pred_it = std::find_if(predicates_for_key.begin(), predicates_for_key.end(), [](const BoundComparisonPredicate& predicate) { return predicate.op == Op::Gt; });
    if (gt_pred_it != predicates_for_key.end()) {
      // if predicate is gt, it can only be left boundary.
      const auto& gt_pred = *gt_pred_it;
      const auto& value = std::get<FieldValue>(std::get_if<BoundColumnRef>(&gt_pred.left) ? gt_pred.right : gt_pred.left);
      const auto& column_type = std::get<BoundColumnRef>(std::get_if<BoundColumnRef>(&gt_pred.left) ? gt_pred.left : gt_pred.right).type;
      left_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      left_boundary.is_inclusive = false;
      continue;
    }
    const auto& ge_pred_it = std::find_if(predicates_for_key.begin(), predicates_for_key.end(), [](const BoundComparisonPredicate& predicate) { return predicate.op == Op::Ge; });
    if (ge_pred_it != predicates_for_key.end()) {
      // if predicate is ge, it can only be left boundary.
      const auto& ge_pred = *ge_pred_it;
      const auto& value = std::get<FieldValue>(std::get_if<BoundColumnRef>(&ge_pred.left) ? ge_pred.right : ge_pred.left);
      const auto& column_type = std::get<BoundColumnRef>(std::get_if<BoundColumnRef>(&ge_pred.left) ? ge_pred.left : ge_pred.right).type;
      left_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      continue;
    }
    const auto& lt_pred_it = std::find_if(predicates_for_key.begin(), predicates_for_key.end(), [](const BoundComparisonPredicate& predicate) { return predicate.op == Op::Lt; });
    if (lt_pred_it != predicates_for_key.end()) {
      // if predicate is lt, it can only be right boundary.
      const auto& lt_pred = *lt_pred_it;
      const auto& value = std::get<FieldValue>(std::get_if<BoundColumnRef>(&lt_pred.left) ? lt_pred.right : lt_pred.left);
      const auto& column_type = std::get<BoundColumnRef>(std::get_if<BoundColumnRef>(&lt_pred.left) ? lt_pred.left : lt_pred.right).type;
      right_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      right_boundary.is_inclusive = false;
      continue;
    }
    const auto& le_pred_it = std::find_if(predicates_for_key.begin(), predicates_for_key.end(), [](const BoundComparisonPredicate& predicate) { return predicate.op == Op::Le; });
    if (le_pred_it != predicates_for_key.end()) {
      // if predicate is le, it can only be right boundary.
      const auto& le_pred = *le_pred_it;
      const auto& value = std::get<FieldValue>(std::get_if<BoundColumnRef>(&le_pred.left) ? le_pred.right : le_pred.left);
      const auto& column_type = std::get<BoundColumnRef>(std::get_if<BoundColumnRef>(&le_pred.left) ? le_pred.left : le_pred.right).type;
      right_boundary.composite_key += index_key::encodeFieldValue(value, column_type);
      continue;
    }
  }

  
  // find traversal start point based on left boundary.
  int page_id;
  if(left_boundary.composite_key.empty()) {
    page_id = findLeafPageID(pool, indexFile, "");
  }else{
    page_id = findLeafPageID(pool, indexFile, left_boundary.composite_key);
  }

  // flatten
  std::vector<BoundComparisonPredicate> flattened_predicates;
  for(const auto& predicates_for_key : ordered_predicates){
    flattened_predicates.insert(flattened_predicates.end(), predicates_for_key.begin(), predicates_for_key.end());
  }
  // leaf page traversal
  std::vector<RID> matching_rids;
  while (page_id != LeafIndexPage::NO_RIGHT_SIBLING) {
    Page* leaf_page = pool.pinPage(page_id, indexFile);
    LeafIndexPage leaf(*leaf_page);
    auto [next_page, rids] = leaf.findRef(right_boundary, do_invalidate, flattened_predicates);
    pool.unpinPage(leaf_page, indexFile);
    matching_rids.insert(matching_rids.end(), rids.begin(), rids.end());
    page_id = next_page;
  }
  
  return matching_rids;
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
    auto inserted_slot_id =
        insertCellWithCompaction(*target_page, *cell_to_insert);
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

    inserted_slot_id = insertCellWithCompaction(*retry_page, *cell_to_insert);
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
  LeafIndexPage new_leaf(*new_page);
  new_leaf.setRightSiblingPageId(old_page.getPageID());
  

  LeafIndexPage old_leaf(old_page);
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
