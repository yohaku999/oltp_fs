#pragma once

#include <cstdint>
#include <optional>

#include "execution/operator.h"

class BufferPool;
class File;
class Schema;

class OrderByOperator : public Operator {
 public:
  OrderByOperator(BufferPool& pool, File& heap_file, const Schema& schema);

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  BufferPool& pool_;
  File& heap_file_;
  const Schema& schema_;
  uint16_t current_page_id_ = 0;
  uint16_t current_slot_id_ = 0;
  bool is_open_ = false;
};