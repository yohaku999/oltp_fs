#include "index_scan.h"

#include "storage/index/btreecursor.h"
#include "logging.h"

IndexScanOperator::IndexScanOperator(BufferPool& pool, File& index_file,
                                     std::vector<int> keys)
    : pool_(pool),
      indexFile_(index_file),
      keys_(std::move(keys)),
      pos_(0),
      mode_(Mode::Keys) {}

std::unique_ptr<IndexScanOperator> IndexScanOperator::fromKey(
    BufferPool& pool, File& index_file, int key) {
  std::vector<int> keys;
  keys.push_back(key);
  return std::make_unique<IndexScanOperator>(pool, index_file,
                                             std::move(keys));
}

std::unique_ptr<IndexScanOperator> IndexScanOperator::fromKeys(
    BufferPool& pool, File& index_file, std::vector<int> keys) {
  return std::make_unique<IndexScanOperator>(pool, index_file,
                                             std::move(keys));
}

std::unique_ptr<IndexScanOperator> IndexScanOperator::fromKeyRange(
    BufferPool& pool, File& index_file, int low_key, int high_key) {
  LOG_INFO("Starting index scan for keys in range [{}, {}]", low_key, high_key);
  auto lookup =
      std::make_unique<IndexScanOperator>(pool, index_file, std::vector<int>{});
  lookup->mode_ = Mode::Range;

  if (low_key <= high_key) {
    lookup->low_key_ = low_key;
    lookup->high_key_ = high_key;
    lookup->current_key_ = low_key;
  } else {
    // Represent an empty range so next() immediately returns std::nullopt.
    lookup->low_key_ = low_key;
    lookup->high_key_ = high_key;
    lookup->current_key_ = high_key + 1;
  }
  LOG_INFO("Finished index scan for keys in range [{}, {}]", low_key, high_key);
  return lookup;
}

void IndexScanOperator::open() {
  pos_ = 0;
  if (mode_ == Mode::Range) {
    current_key_ = low_key_;
    if (low_key_ > high_key_) {
      current_key_ = high_key_ + 1;
    }
  }
}

std::optional<RID> IndexScanOperator::next() {
  if (mode_ == Mode::Keys) {
    while (pos_ < keys_.size()) {
      int key = keys_[pos_++];
      std::optional<RID> rid = BTreeCursor::findRID(pool_, indexFile_, key);
      if (!rid.has_value()) {
        continue;
      }

      return rid;
    }
  } else {
    while (current_key_ <= high_key_) {
      int key = current_key_++;
      std::optional<RID> rid = BTreeCursor::findRID(pool_, indexFile_, key);
      if (!rid.has_value()) {
        continue;
      }

      return rid;
    }
  }

  return std::nullopt;
}

void IndexScanOperator::close() {}