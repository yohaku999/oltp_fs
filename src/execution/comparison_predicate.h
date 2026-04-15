#pragma once

#include <cstddef>

#include "tuple/field_value.h"

struct ComparisonPredicate {
  std::size_t column_index;
  enum class Op { Eq, Gt, Ge, Lt, Le };
  Op op;
  FieldValue value;
};