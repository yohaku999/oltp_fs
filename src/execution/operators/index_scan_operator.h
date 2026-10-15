#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "execution/operator.h"
#include "storage/index/index_accessor.h"

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(IndexAccessor& index_accessor, IndexMetadata index,
                    IndexAccessor::Boundaries boundaries,
                    std::vector<std::vector<BoundComparisonPredicate>>
                        index_ordered_predicates,
                    std::optional<std::size_t> limit_count);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  IndexAccessor& index_accessor_;
  IndexMetadata index_;
  IndexAccessor::Boundaries boundaries_;
  std::vector<std::vector<BoundComparisonPredicate>> index_ordered_predicates_;
  std::optional<std::size_t> limit_count_;
  OperatorExecutionLogger logger_{"IndexScanOperator"};
  bool lookup_done_ = false;
  std::vector<RID> current_rids_;

  std::size_t rid_pos_ = 0;
};
