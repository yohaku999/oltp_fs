#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"
#include "storage/heap/heapfile.h"
#include "storage/heap/heapscan.h"

class BufferPool;
class Schema;

class SeqScanOperator : public TypedRowOperator {
 public:
  SeqScanOperator(BufferPool& pool, HeapFile heap_file, const Schema& schema,
                  std::vector<BoundComparisonPredicate> predicates = {});

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  const Schema& schema_;
  std::vector<BoundComparisonPredicate> predicates_;
  OperatorExecutionLogger logger_{"SeqScanOperator"};
  bool is_open_ = false;
  HeapFile heap_file_;
  HeapScan scan_;
};
