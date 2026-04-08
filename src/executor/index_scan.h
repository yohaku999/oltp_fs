#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

class BufferPool;
class File;

#include "../storage/rid.h"

class IndexLookup {
 public:
  IndexLookup(BufferPool& pool, File& indexFile, std::vector<int> keys);
  // named constructors
  static IndexLookup fromKey(BufferPool& pool, File& indexFile, int key);
  static IndexLookup fromKeys(BufferPool& pool, File& indexFile,
                              std::vector<int> keys);
  static IndexLookup fromKeyRange(BufferPool& pool, File& indexFile,
                                  int low_key, int high_key);

  std::optional<RID> next();

 private:
  enum class Mode { Keys, Range };

  BufferPool& pool_;
  File& indexFile_;
  std::vector<int> keys_;
  std::size_t pos_ = 0;

  Mode mode_ = Mode::Keys;
  int low_key_ = 0;
  int high_key_ = -1;
  int current_key_ = 0;
};