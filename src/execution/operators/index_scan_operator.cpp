#include "index_scan_operator.h"

#include <stdexcept>

#include "storage/index/btreecursor.h"

IndexScanOperator::IndexScanOperator(BufferPool& pool, File& index_file,
                                     std::vector<std::string> encoded_keys, Op op)
    : pool_(pool),
      indexFile_(index_file),
      encoded_keys_(std::move(encoded_keys)),
      op_(op),
      pos_(0) {
  if (encoded_keys_.empty()) {
    throw std::runtime_error("Index scan requires at least one encoded lookup key.");
  }
}

void IndexScanOperator::open() {
  pos_ = 0;
  rid_pos_ = 0;
  current_rids_.clear();
  logger_.open();
  logger_.setMetric("lookup_keys", encoded_keys_.size());
}

// OPTIMIZE: we can do better then just traversing the btree again and again with random keys.
std::optional<RID> IndexScanOperator::next() {
  while (true) {
    if (rid_pos_ < current_rids_.size()) {
      logger_.recordOutput();
      return current_rids_[rid_pos_++];
    }

    if (pos_ >= encoded_keys_.size()) {
      return std::nullopt;
    }

    logger_.recordInput();
    current_rids_ =
        BTreeCursor::findRIDs(pool_, indexFile_, encoded_keys_[pos_++], false, op_);
    rid_pos_ = 0;
  }
}

void IndexScanOperator::close() { logger_.close(); }
