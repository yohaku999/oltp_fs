#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

class BufferPool;
class File;

#include "execution/rid_operator.h"

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(BufferPool& pool, File& index_file,
                    std::vector<int> keys);
  static std::unique_ptr<IndexScanOperator> fromKey(BufferPool& pool,
                                                    File& index_file,
                                                    int key);
  static std::unique_ptr<IndexScanOperator> fromKeys(
      BufferPool& pool, File& index_file, std::vector<int> keys);
  static std::unique_ptr<IndexScanOperator> fromKeyRange(
      BufferPool& pool, File& index_file, int low_key, int high_key);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

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