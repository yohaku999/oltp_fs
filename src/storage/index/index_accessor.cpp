#include "storage/index/index_accessor.h"

#include "storage/buffer/bufferpool.h"
#include "storage/index/btreecursor.h"
#include "storage/local_storage_manager.h"

IndexAccessor::IndexAccessor(BufferPool& pool, LocalStorageManager& storage)
    : pool_(pool), storage_(storage) {}

std::vector<IndexEntry> IndexAccessor::find(const IndexMetadata& index,
                                            const Boundaries& boundaries) {
  const auto btree_boundaries =
      std::make_pair(BTreeCursor::Boundary{boundaries.first.composite_key,
                                           boundaries.first.is_inclusive},
                     BTreeCursor::Boundary{boundaries.second.composite_key,
                                           boundaries.second.is_inclusive});
  return BTreeCursor::findEntries(pool_, storage_.open(index.file_name),
                                  btree_boundaries, false);
}

bool IndexAccessor::contains(const IndexMetadata& index,
                             const std::string& key) {
  return BTreeCursor::containsKey(pool_, storage_.open(index.file_name), key);
}

void IndexAccessor::insert(const IndexMetadata& index, const std::string& key,
                           const RID& rid) {
  BTreeCursor::insertIntoIndex(pool_, storage_.open(index.file_name), key,
                               rid.heap_page_id, rid.slot_id);
}

void IndexAccessor::remove(const IndexMetadata& index, const std::string& key,
                           const RID& rid) {
  BTreeCursor::removeRID(pool_, storage_.open(index.file_name), key, rid);
}

void IndexAccessor::replaceRID(const IndexMetadata& index,
                               const std::string& key, const RID& old_rid,
                               const RID& new_rid) {
  BTreeCursor::replaceRID(pool_, storage_.open(index.file_name), key, old_rid,
                          new_rid);
}
