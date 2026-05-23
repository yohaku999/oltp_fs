#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

class BufferPool;
class File;
class Schema;
struct BoundComparisonPredicate;

#include "execution/operator.h"

class HeapFetchOperator : public TypedRowOperator {
 public:
  HeapFetchOperator(std::unique_ptr<RidOperator> child, BufferPool& pool,
                    File& heap_file, const Schema& schema,
                    std::vector<BoundComparisonPredicate> predicates = {});
  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  std::unique_ptr<RidOperator> child_;
  BufferPool& pool_;
  File& heap_file_;
  const Schema& schema_;
  std::vector<BoundComparisonPredicate> predicates_;
  OperatorExecutionLogger logger_{"HeapFetchOperator"};
};