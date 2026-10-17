#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "execution/operator.h"

class StubRowOperator : public TypedRowOperator {
 public:
  explicit StubRowOperator(std::vector<TypedRow> rows,
                           OutputProperties output_properties = {})
      : rows_(std::move(rows)),
        output_properties_(std::move(output_properties)) {}

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

  const OutputProperties& outputProperties() const override {
    return output_properties_;
  }

 private:
  std::vector<TypedRow> rows_;
  OutputProperties output_properties_;
  std::size_t cursor_ = 0;
  bool is_open_ = false;
};

inline TypedRow makeStubRow(Column::IntegerType id, const std::string& name,
                            Column::IntegerType score) {
  return TypedRow{{id, Column::VarcharType(name), score}};
}
