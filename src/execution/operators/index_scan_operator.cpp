#include "index_scan_operator.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include "storage/index/btreecursor.h"
#include "storage/index/index_key.h"

namespace {
std::vector<BoundComparisonPredicate> flattenPredicates(
    const std::vector<std::vector<BoundComparisonPredicate>>&
        ordered_predicates) {
  std::vector<BoundComparisonPredicate> flattened;
  for (const auto& predicates_for_key : ordered_predicates) {
    flattened.insert(flattened.end(), predicates_for_key.begin(),
                     predicates_for_key.end());
  }
  return flattened;
}
}  // namespace

IndexScanOperator::IndexScanOperator(
    BufferPool& pool, File& index_file,
    std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> boundaries,
    std::vector<std::vector<BoundComparisonPredicate>> index_ordered_predicates)
    : pool_(pool),
      indexFile_(index_file),
      boundaries_(boundaries),
      index_ordered_predicates_(std::move(index_ordered_predicates)),
      lookup_done_(false) {}

void IndexScanOperator::open() {
  lookup_done_ = false;
  rid_pos_ = 0;
  current_rids_.clear();
  logger_.open();
  logger_.setMetric("predicates", index_ordered_predicates_.size());
}

// `findRIDs` returns the complete RID set for the current index predicate.
std::optional<RID> IndexScanOperator::next() {
  if (!lookup_done_) {
    logger_.recordInput();
    const std::vector<BoundComparisonPredicate> index_predicates =
        flattenPredicates(index_ordered_predicates_);
    const std::vector<IndexEntry> entries =
        BTreeCursor::findEntries(pool_, indexFile_, boundaries_, false);
    current_rids_.clear();
    current_rids_.reserve(entries.size());
    for (const IndexEntry& entry : entries) {
      TypedRow typed_key = index_key::decodeToTypedRow(entry.key);
      if (passesPredicates(typed_key, index_predicates)) {
        current_rids_.push_back(entry.rid);
      }
    }
    lookup_done_ = true;
    rid_pos_ = 0;
  }

  if (rid_pos_ < current_rids_.size()) {
    logger_.recordOutput();
    return current_rids_[rid_pos_++];
  }

  return std::nullopt;
}

void IndexScanOperator::close() { logger_.close(); }
