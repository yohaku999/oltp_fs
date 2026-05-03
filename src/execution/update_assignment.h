#pragma once

#include <cstddef>
#include <string>
#include <variant>
#include <vector>

#include "schema/column.h"
#include "tuple/field_value.h"

enum class UpdateBinaryOperator {
  Add,
  Subtract,
};

struct UnboundSelfArithmeticUpdate {
  UpdateBinaryOperator op;
  FieldValue literal;
};

using UnboundUpdateValue =
    std::variant<FieldValue, UnboundSelfArithmeticUpdate>;

struct UnboundUpdateAssignment {
  std::string target_column_name;
  UnboundUpdateValue value;
};

struct BoundSelfArithmeticUpdate {
  std::size_t target_column_index;
  UpdateBinaryOperator op;
  FieldValue literal;
};

using BoundUpdateValue = std::variant<FieldValue, BoundSelfArithmeticUpdate>;

struct BoundUpdateAssignment {
  std::size_t target_column_index;
  BoundUpdateValue value;
};