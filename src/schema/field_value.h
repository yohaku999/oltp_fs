#pragma once

#include <variant>

#include "column.h"

using FieldValue =
    std::variant<std::monostate, Column::IntegerType, Column::VarcharType>;

inline bool isNullFieldValue(const FieldValue& value) {
  return std::holds_alternative<std::monostate>(value);
}