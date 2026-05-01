#pragma once

#include <cstddef>
#include <string>
#include <variant>

#include "tuple/field_value.h"

struct TypedRow;

struct ColumnRef {
  std::string table_name;
  std::string column_name;
};

using UnboundOperand = std::variant<ColumnRef, FieldValue>;

enum class Op { Eq, Gt, Ge, Lt, Le };
struct UnboundComparisonPredicate {
  Op op;
  UnboundOperand left;
  UnboundOperand right;
};

struct BoundColumnRef {
  std::size_t source_index;
  std::size_t column_index;
};

using BoundOperand = std::variant<std::monostate, BoundColumnRef, FieldValue>;
struct BoundComparisonPredicate {
  Op op;
  BoundOperand left;
  BoundOperand right;
};

FieldValue resolveBoundOperand(const BoundOperand& operand,
                              const TypedRow& row);
