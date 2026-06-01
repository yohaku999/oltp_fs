#pragma once

#include <cstddef>
#include <string>
#include <variant>
#include <vector>

#include "tuple/field_value.h"
#include "schema/column.h"

struct TypedRow;

struct ColumnRef {
  std::string table_name;
  std::string column_name;
};

using UnboundOperand = std::variant<ColumnRef, FieldValue>;

enum class Op { Eq, Gt, Ge, Lt, Le };

/**
 * UnboundComparisonPredicate represents a comparison predicate with unbound operands, which can be either column references or literal values.
 * It is produced by the parser and later converted into BoundComparisonPredicate by binding column references to actual column indices based on the input schema.
 */
struct UnboundComparisonPredicate {
  Op op;
  UnboundOperand left;
  UnboundOperand right;
};

/**
 * BoundColumnRef represents a column reference with a resolved source and column index.
 */
struct BoundColumnRef {
  std::size_t source_index;
  std::size_t column_index;
  Column::Type type;
};

using BoundOperand = std::variant<BoundColumnRef, FieldValue>;

/**
 * BoundComparisonPredicate represents a comparison predicate with bound operands, where column references are resolved to actual column indices.
 */
struct BoundComparisonPredicate {
  Op op;
  BoundOperand left;
  BoundOperand right;
};

FieldValue resolveBoundOperand(const BoundOperand& operand,
                              const TypedRow& row);

std::vector<std::vector<BoundComparisonPredicate>>
align_predicates_with_key_order(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<std::size_t>& key_order_indexes);

/**
 * matchesPredicates checks if a given row satisfies all the provided comparison predicates.
 */
bool matchesPredicates(const TypedRow& row,
                       const std::vector<BoundComparisonPredicate>& predicates);
