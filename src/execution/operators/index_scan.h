#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

class BufferPool;
class File;

#include "execution/comparison_predicate.h"
#include "execution/operators/rid_operator.h"

// This scan only supports predicates over a discrete integer index key.
// Range predicates are executed as repeated point lookups over the integer
// key space, so callers must pass predicates already normalized for a single
// indexed integer column.
struct DiscreteIntegerIndexPredicates {
  std::vector<UnboundComparisonPredicate> predicates;
};

class IndexScanOperator : public RidOperator {
 public:
  IndexScanOperator(BufferPool& pool, File& index_file,
                    DiscreteIntegerIndexPredicates discrete_integer_predicates);

  void open() override;
  std::optional<RID> next() override;
  void close() override;

 private:
  enum class Mode { Keys, Range };

  void configureScanRangeFromPredicates(
      const std::vector<UnboundComparisonPredicate>& predicates);
  void setDiscreteIntegerRange(int low_key, int high_key);

  BufferPool& pool_;
  File& indexFile_;
  std::vector<int> keys_;
  std::size_t pos_ = 0;

  Mode mode_ = Mode::Keys;
  int low_key_ = 0;
  int high_key_ = -1;
  int current_key_ = 0;
};