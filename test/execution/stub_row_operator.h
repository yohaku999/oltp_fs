#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "execution/operator.h"

class StubRowOperator : public Operator {
 public:
  explicit StubRowOperator(std::vector<TypedRow> rows)
      : rows_(std::move(rows)) {}

  void open() override {
    is_open_ = true;
    cursor_ = 0;
  }

  std::optional<TypedRow> next() override {
    if (!is_open_ || cursor_ >= rows_.size()) {
      return std::nullopt;
    }
    return rows_[cursor_++];
  }

  void close() override { is_open_ = false; }

 private:
  std::vector<TypedRow> rows_;
  std::size_t cursor_ = 0;
  bool is_open_ = false;
};

inline TypedRow makeStubRow(Column::IntegerType id, const std::string& name,
                            Column::IntegerType score) {
  return TypedRow{{id, Column::VarcharType(name), score}};
}