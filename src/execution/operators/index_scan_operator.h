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
                    std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates,
                    std::vector<std::size_t> key_order_indexes);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& indexFile_;
  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates_;
  std::vector<std::size_t> key_order_indexes_;
  OperatorExecutionLogger logger_{"IndexScanOperator"};
  bool lookup_done_ = false;
  std::vector<RID> current_rids_;
  std::size_t rid_pos_ = 0;
};
