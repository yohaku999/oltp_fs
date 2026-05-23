#include "index_scan_operator.h"

#include <stdexcept>

#include "storage/index/btreecursor.h"

IndexScanOperator::IndexScanOperator(BufferPool& pool, File& index_file,
                                     std::vector<std::string> encoded_keys)
    : pool_(pool),
      indexFile_(index_file),
      encoded_keys_(std::move(encoded_keys)),
      pos_(0) {
  if (encoded_keys_.empty()) {
    throw std::runtime_error("Index scan requires at least one encoded lookup key.");
  }
}

void IndexScanOperator::open() {
  pos_ = 0;
  logger_.open();
  logger_.setMetric("lookup_keys", encoded_keys_.size());
}

// OPTIMIZE: we can do better then just traversing the btree again and again with random keys.
std::optional<RID> IndexScanOperator::next() {
  while (pos_ < encoded_keys_.size()) {
    logger_.recordInput();
    std::optional<RID> rid =
        BTreeCursor::findRID(pool_, indexFile_, encoded_keys_[pos_++]);
    if (!rid.has_value()) {
      continue;
    }

    logger_.recordOutput();
    return rid;
  }

  return std::nullopt;
}

void IndexScanOperator::close() { logger_.close(); }