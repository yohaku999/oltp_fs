#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

class BufferPool;
class File;

#include "execution/comparison_predicate.h"
#include "execution/operator.h"

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(BufferPool& pool, File& index_file,
                    std::vector<std::string> encoded_keys, Op op);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& indexFile_;
  std::vector<std::string> encoded_keys_;
  Op op_;
  OperatorExecutionLogger logger_{"IndexScanOperator"};
  std::size_t pos_ = 0;
  std::vector<RID> current_rids_;
  std::size_t rid_pos_ = 0;
};
