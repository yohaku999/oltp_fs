#pragma once

#include <cstddef>
#include <string>
#include <variant>
#include <vector>

#include "parser/query_parameters.h"
#include "schema/column.h"
#include "tuple/field_value.h"

enum class UpdateBinaryOperator {
  Add,
  Subtract,
};

struct PreparedUnboundSelfArithmeticUpdate {
  UpdateBinaryOperator op;
  PreparedFieldValue literal;
};

using PreparedUnboundUpdateValue =
    std::variant<PreparedFieldValue, PreparedUnboundSelfArithmeticUpdate>;

struct PreparedUnboundUpdateAssignment {
  std::string target_column_name;
  PreparedUnboundUpdateValue value;
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
