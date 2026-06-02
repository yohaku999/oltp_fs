#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

class BufferPool;
class File;

#include "execution/comparison_predicate.h"
#include "storage/index/btreecursor.h"
#include "execution/operator.h"

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(BufferPool& pool, File& index_file, std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> boundaries,
                    std::vector<std::vector<BoundComparisonPredicate>> index_ordered_predicates);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& indexFile_;
  std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> boundaries_;
  std::vector<std::vector<BoundComparisonPredicate>> index_ordered_predicates_;
  OperatorExecutionLogger logger_{"IndexScanOperator"};
  bool lookup_done_ = false;
  std::vector<RID> current_rids_;

  std::size_t rid_pos_ = 0;
};
