#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

class BufferPool;
class File;

#include "execution/operators/rid_operator.h"

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(BufferPool& pool, File& index_file,
                    std::vector<std::string> encoded_keys);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& indexFile_;
  std::vector<std::string> encoded_keys_;
  std::size_t pos_ = 0;
};