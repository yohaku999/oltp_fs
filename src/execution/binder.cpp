#include "execution/binder.h"

#include <optional>
#include <stdexcept>

namespace binder {

namespace {

bool isNumericType(Column::Type type) {
  return type == Column::Type::Integer || type == Column::Type::Double;
}

/**
 * Tries to bind the given column reference to a column in the specified tables.
 * If the column reference does not specify a table name,
 * then it will be bound to the first matching column found in the tables.
 * If multiple matching columns are found for an unqualified reference, an
 * exception is thrown due to ambiguity. If no matching column is found,
 * std::nullopt is returned.
 */
std::optional<BoundColumnRef> tryBindColumnRef(
    const ColumnRef& column_ref, const std::vector<Table>& tables) {
  const bool has_table_name = !column_ref.table_name.empty();
  std::size_t joined_column_offset = 0;
  std::optional<BoundColumnRef> matched_column;

  for (const auto& table : tables) {
    const std::size_t table_column_count = table.schema().columns().size();
    const bool table_matches =
        !has_table_name || column_ref.table_name == table.name();
    if (!table_matches) {
      // if the table name doesn't match, skip all columns of this table and
      // move on to the next table
      joined_column_offset += table_column_count;
      continue;
    }

    const int column_index =
        table.schema().getColumnIndex(column_ref.column_name);
    if (column_index >= 0) {
      const BoundColumnRef bound_column{
          0, joined_column_offset + static_cast<std::size_t>(column_index),
          table.schema().columns().at(column_index).getType()};

      if (has_table_name) {
        return bound_column;
      }

      if (matched_column.has_value()) {
        // if the column reference is unqualified and we've already found a
        // matching column before, then this reference is ambiguous.
        throw std::runtime_error("Ambiguous column: " + column_ref.column_name);
      }

      matched_column = bound_column;
    }

    joined_column_offset += table_column_count;
  }

  return matched_column;
}

std::optional<BoundOperand> tryBindOperand(const UnboundOperand& operand,
                                           const std::vector<Table>& tables) {
  // when the operand is a column reference
  if (const auto* column_ref = std::get_if<ColumnRef>(&operand)) {
    const std::optional<BoundColumnRef> bound_column =
        tryBindColumnRef(*column_ref, tables);
    if (!bound_column.has_value()) {
      return std::nullopt;
    }
    return BoundOperand{*bound_column};
  }

  // when the operand is a literal value
  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return BoundOperand{*value};
  }

  throw std::logic_error("Unsupported unbound operand.");
}

}  // namespace

BoundColumnRef bindColumnRef(const ColumnRef& column_ref,
                             const std::vector<Table>& tables) {
  const std::optional<BoundColumnRef> bound_column =
      tryBindColumnRef(column_ref, tables);
  if (bound_column.has_value()) {
    return *bound_column;
  }

  if (!column_ref.table_name.empty()) {
    throw std::runtime_error("Unknown column on table " +
                             column_ref.table_name + ": " +
                             column_ref.column_name);
  }
  throw std::runtime_error("Unknown column: " + column_ref.column_name);
}

BoundOperand bindOperand(const UnboundOperand& operand,
                         const std::vector<Table>& tables) {
  if (const auto* column_ref = std::get_if<ColumnRef>(&operand)) {
    return bindColumnRef(*column_ref, tables);
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  throw std::logic_error("Unsupported unbound operand.");
}

std::vector<BoundSelectItem> bindSelectItems(
    const std::vector<UnboundSelectItem>& select_items,
    const std::vector<Table>& tables) {
  std::vector<BoundSelectItem> bound_select_items;

  std::size_t joined_column_offset = 0;
  for (const auto& item : select_items) {
    if (std::holds_alternative<SelectAllItem>(item)) {
      joined_column_offset = 0;
      for (const auto& table : tables) {
        const std::size_t column_count = table.schema().columns().size();
        for (std::size_t column_index = 0; column_index < column_count;
             ++column_index) {
          bound_select_items.push_back(BoundColumnRef{
              0, joined_column_offset + column_index,
              table.schema().columns().at(column_index).getType()});
        }
        joined_column_offset += column_count;
      }
      continue;
    }

    if (const auto* column_ref = std::get_if<ColumnRef>(&item)) {
      bound_select_items.push_back(bindColumnRef(*column_ref, tables));
      continue;
    }

    const auto& aggregate_call = std::get<UnboundAggregateCall>(item);
    if (const auto* column_ref =
            std::get_if<ColumnRef>(&aggregate_call.argument)) {
      bound_select_items.push_back(BoundAggregateCall{
          aggregate_call.function, bindColumnRef(*column_ref, tables),
          aggregate_call.is_distinct});
    } else {
      // This is an aggregate call with the special argument meaning "all
      // columns".
      bound_select_items.push_back(BoundAggregateCall{
          aggregate_call.function, AggregateAllColumnsArgument{},
          aggregate_call.is_distinct});
    }
  }

  return bound_select_items;
}

std::vector<BoundComparisonPredicate> bindPredicates(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  std::vector<BoundComparisonPredicate> bound_predicates;
  bound_predicates.reserve(predicates.size());

  for (const auto& predicate : predicates) {
    bound_predicates.push_back(BoundComparisonPredicate{
        predicate.op, bindOperand(predicate.left, tables),
        bindOperand(predicate.right, tables)});
  }

  return bound_predicates;
}

/**
 * Binds as many predicates as possible using only the specified table, and
 * returns the bound predicates. Predicates that cannot be fully bound using the
 * specified table are silently dropped.
 */
std::vector<BoundComparisonPredicate> bindPredicatesResolvableByTable(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const Table& table) {
  std::vector<BoundComparisonPredicate> bound_predicates;
  bound_predicates.reserve(predicates.size());

  for (const auto& predicate : predicates) {
    std::optional<BoundOperand> left =
        tryBindOperand(predicate.left, std::vector<Table>{table});
    if (!left.has_value()) {
      continue;
    }

    std::optional<BoundOperand> right =
        tryBindOperand(predicate.right, std::vector<Table>{table});
    if (!right.has_value()) {
      continue;
    }

    bound_predicates.push_back(
        BoundComparisonPredicate{predicate.op, *left, *right});
  }

  return bound_predicates;
}

std::vector<BoundUpdateAssignment> bindUpdateAssignments(
    const std::vector<UnboundUpdateAssignment>& assignments,
    const Table& table) {
  std::vector<BoundUpdateAssignment> bound_assignments;
  bound_assignments.reserve(assignments.size());

  for (const auto& assignment : assignments) {
    const int column_index =
        table.schema().getColumnIndex(assignment.target_column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown UPDATE target column: " +
                               assignment.target_column_name);
    }

    const std::size_t target_column_index =
        static_cast<std::size_t>(column_index);
    const Column::Type target_type =
        table.schema().columns().at(target_column_index).getType();

    if (const auto* literal = std::get_if<FieldValue>(&assignment.value)) {
      bound_assignments.push_back(
          BoundUpdateAssignment{target_column_index, *literal});
      continue;
    }

    if (!isNumericType(target_type)) {
      throw std::runtime_error(
          "UPDATE arithmetic requires a numeric target column.");
    }

    const auto& arithmetic =
        std::get<UnboundSelfArithmeticUpdate>(assignment.value);
    if (!std::holds_alternative<Column::IntegerType>(arithmetic.literal) &&
        !std::holds_alternative<Column::DoubleType>(arithmetic.literal)) {
      throw std::runtime_error("UPDATE arithmetic requires a numeric literal.");
    }

    bound_assignments.push_back(BoundUpdateAssignment{
        target_column_index,
        BoundSelfArithmeticUpdate{target_column_index, arithmetic.op,
                                  arithmetic.literal}});
  }

  return bound_assignments;
}

}  // namespace binder
