#pragma once

#include <variant>

#include "schema/column.h"

using FieldValue =
  std::variant<std::monostate, Column::IntegerType, Column::DoubleType,
         Column::VarcharType>;

inline bool isNullFieldValue(const FieldValue& value) {
  return std::holds_alternative<std::monostate>(value);
}