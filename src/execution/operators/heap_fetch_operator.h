#pragma once

#include <cstdint>
#include <memory>
#include <optional>

class BufferPool;
class File;
class Schema;

#include "execution/operator.h"

class HeapFetchOperator : public TypedRowOperator {
 public:
  HeapFetchOperator(std::unique_ptr<RidOperator> child, BufferPool& pool,
                    File& heap_file, const Schema& schema);
  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  std::unique_ptr<RidOperator> child_;
  BufferPool& pool_;
  File& heap_file_;
  const Schema& schema_;
};