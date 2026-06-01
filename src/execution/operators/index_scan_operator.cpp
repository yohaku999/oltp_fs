#include "index_scan_operator.h"

#include <stdexcept>
#include <utility>

#include "storage/index/btreecursor.h"

IndexScanOperator::IndexScanOperator(
    BufferPool& pool, File& index_file,
    std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates,
    std::vector<std::size_t> key_order_indexes)
    : pool_(pool),
      indexFile_(index_file),
      ordered_predicates_(std::move(ordered_predicates)),
      key_order_indexes_(std::move(key_order_indexes)),
      lookup_done_(false) {
  if (ordered_predicates_.empty()) {
    throw std::runtime_error("Index scan requires at least one encoded lookup key.");
  }
}

void IndexScanOperator::open() {
  lookup_done_ = false;
  rid_pos_ = 0;
  current_rids_.clear();
  logger_.open();
  logger_.setMetric("predicates", ordered_predicates_.size());
}

// `findRIDs` returns the complete RID set for the current index predicate.
std::optional<RID> IndexScanOperator::next() {
  if (!lookup_done_) {
    logger_.recordInput();
    current_rids_ =
        BTreeCursor::findRIDs(pool_, indexFile_, false, ordered_predicates_, key_order_indexes_);
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
