#pragma once

#include <cstdint>
#include <optional>
#include <vector>
#include "execution/comparison_predicate.h"
#include "execution/operator.h"

class BufferPool;
class File;
class Schema;


class SeqScanOperator : public TypedRowOperator {
 public:
  SeqScanOperator(BufferPool& pool, File& heap_file, const Schema& schema, std::vector<BoundComparisonPredicate> predicates = {});

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& heap_file_;
  const Schema& schema_;
  std::vector<BoundComparisonPredicate> predicates_;
  OperatorExecutionLogger logger_{"SeqScanOperator"};
  uint16_t current_page_id_ = 0;
  uint16_t current_slot_id_ = 0;
  bool is_open_ = false;
};