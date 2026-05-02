#include "execution/comparison_predicate.h"

#include <stdexcept>

#include "tuple/typed_row.h"

FieldValue resolveBoundOperand(const BoundOperand& operand,
                              const TypedRow& row) {
  if (const auto* column_ref = std::get_if<BoundColumnRef>(&operand)) {
    if (column_ref->source_index != 0) {
      throw std::runtime_error(
          "Single-table predicate references a non-zero source index.");
    }
    if (column_ref->column_index >= row.values.size()) {
      throw std::runtime_error("Predicate column index is out of range.");
    }
    return row.values[column_ref->column_index];
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  throw std::logic_error("Unsupported bound predicate operand.");
}

bool matchesPredicates(const TypedRow& row,
                       const std::vector<BoundComparisonPredicate>& predicates) {
  for (const auto& predicate : predicates) {
    const FieldValue left = resolveBoundOperand(predicate.left, row);
    const FieldValue right = resolveBoundOperand(predicate.right, row);
    switch (predicate.op) {
      case Op::Eq:
        if (left != right) {
          return false;
        }
        break;
      case Op::Gt:
        if (left <= right) {
          return false;
        }
        break;
      case Op::Ge:
        if (left < right) {
          return false;
        }
        break;
      case Op::Lt:
        if (left >= right) {
          return false;
        }
        break;
      case Op::Le:
        if (left > right) {
          return false;
        }
        break;
    }
  }

  return true;
}