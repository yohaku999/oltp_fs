#include "index_scan.h"

#include "storage/index/btreecursor.h"
#include "logging.h"

IndexScan::IndexScan(BufferPool& pool, File& indexFile,
                         std::vector<int> keys)
    : pool_(pool),
      indexFile_(indexFile),
      keys_(std::move(keys)),
      pos_(0),
      mode_(Mode::Keys) {}

IndexScan IndexScan::fromKey(BufferPool& pool, File& indexFile, int key) {
  std::vector<int> keys;
  keys.push_back(key);
  return IndexScan(pool, indexFile, std::move(keys));
}

IndexScan IndexScan::fromKeys(BufferPool& pool, File& indexFile,
                                  std::vector<int> keys) {
  return IndexScan(pool, indexFile, std::move(keys));
}

IndexScan IndexScan::fromKeyRange(BufferPool& pool, File& indexFile,
                                      int low_key, int high_key) {
  LOG_INFO("Starting index scan for keys in range [{}, {}]", low_key, high_key);
  IndexScan lookup(pool, indexFile, std::vector<int>{});
  lookup.mode_ = Mode::Range;

  if (low_key <= high_key) {
    lookup.low_key_ = low_key;
    lookup.high_key_ = high_key;
    lookup.current_key_ = low_key;
  } else {
    // Represent an empty range so next() immediately returns std::nullopt.
    lookup.low_key_ = low_key;
    lookup.high_key_ = high_key;
    lookup.current_key_ = high_key + 1;
  }
  LOG_INFO("Finished index scan for keys in range [{}, {}]", low_key, high_key);
  return lookup;
}

std::optional<RID> IndexScan::next() {
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