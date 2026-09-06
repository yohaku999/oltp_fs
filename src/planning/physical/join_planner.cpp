#include "planning/physical/join_planner.h"

#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "planning/binder.h"

namespace physical {

namespace {

std::optional<JoinColumnPair> findHashJoinKeyForTwoTableJoin(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  if (tables.size() != 2) {
    return std::nullopt;
  }

  const std::size_t outer_column_count = tables[0].schema().columns().size();
  const std::size_t inner_column_count = tables[1].schema().columns().size();
  const std::size_t joined_column_count =
      outer_column_count + inner_column_count;

  for (const auto& predicate : predicates) {
    if (predicate.op != Op::Eq) {
      continue;
    }

    const auto* left_column = std::get_if<BoundColumnRef>(&predicate.left);
    const auto* right_column = std::get_if<BoundColumnRef>(&predicate.right);
    if (left_column == nullptr || right_column == nullptr ||
        left_column->type != right_column->type) {
      continue;
    }

    const auto to_outer_index =
        [&](std::size_t column_index) -> std::optional<std::size_t> {
      if (column_index < outer_column_count) {
        return column_index;
      }
      return std::nullopt;
    };
    const auto to_inner_index =
        [&](std::size_t column_index) -> std::optional<std::size_t> {
      if (column_index >= outer_column_count &&
          column_index < joined_column_count) {
        return column_index - outer_column_count;
      }
      return std::nullopt;
    };

    const std::optional<std::size_t> left_outer =
        to_outer_index(left_column->column_index);
    const std::optional<std::size_t> left_inner =
        to_inner_index(left_column->column_index);
    const std::optional<std::size_t> right_outer =
        to_outer_index(right_column->column_index);
    const std::optional<std::size_t> right_inner =
        to_inner_index(right_column->column_index);

    if (left_outer.has_value() && right_inner.has_value()) {
      return JoinColumnPair{left_outer.value(), right_inner.value()};
    }
    if (right_outer.has_value() && left_inner.has_value()) {
      return JoinColumnPair{right_outer.value(), left_inner.value()};
    }
  }

  return std::nullopt;
}

std::optional<IndexLookupJoinPlan> findIndexLookupJoinPlanForTwoTableJoin(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  if (tables.size() != 2 || tables[1].indexes().empty()) {
    return std::nullopt;
  }

  const std::size_t outer_column_count = tables[0].schema().columns().size();
  const std::size_t inner_column_count = tables[1].schema().columns().size();
  const std::size_t joined_column_count =
      outer_column_count + inner_column_count;

  const auto to_outer_index =
      [&](std::size_t column_index) -> std::optional<std::size_t> {
    if (column_index < outer_column_count) {
      return column_index;
    }
    return std::nullopt;
  };
  const auto to_inner_index =
      [&](std::size_t column_index) -> std::optional<std::size_t> {
    if (column_index >= outer_column_count &&
        column_index < joined_column_count) {
      return column_index - outer_column_count;
    }
    return std::nullopt;
  };

  std::optional<IndexLookupJoinPlan> best_plan;
  std::size_t best_index_column_count = 0;
  for (std::size_t index_position = 0;
       index_position < tables[1].indexes().size(); ++index_position) {
    IndexLookupJoinPlan plan{index_position, {}, {}};
    std::vector<bool> covered_inner_columns(inner_column_count, false);
    const auto mark_covered = [&](std::size_t inner_column_index) {
      covered_inner_columns[inner_column_index] = true;
    };

    for (const auto& predicate : predicates) {
      if (predicate.op != Op::Eq) {
        continue;
      }

      const auto* left_column = std::get_if<BoundColumnRef>(&predicate.left);
      const auto* right_column =
          std::get_if<BoundColumnRef>(&predicate.right);
      const auto* left_value = std::get_if<FieldValue>(&predicate.left);
      const auto* right_value = std::get_if<FieldValue>(&predicate.right);

      if (left_column != nullptr && right_column != nullptr &&
          left_column->type == right_column->type) {
        const std::optional<std::size_t> left_outer =
            to_outer_index(left_column->column_index);
        const std::optional<std::size_t> left_inner =
            to_inner_index(left_column->column_index);
        const std::optional<std::size_t> right_outer =
            to_outer_index(right_column->column_index);
        const std::optional<std::size_t> right_inner =
            to_inner_index(right_column->column_index);

        if (left_outer.has_value() && right_inner.has_value()) {
          plan.join_keys.push_back(
              JoinColumnPair{left_outer.value(), right_inner.value()});
          mark_covered(right_inner.value());
          continue;
        }
        if (right_outer.has_value() && left_inner.has_value()) {
          plan.join_keys.push_back(
              JoinColumnPair{right_outer.value(), left_inner.value()});
          mark_covered(left_inner.value());
          continue;
        }
      }

      const BoundColumnRef* column_ref =
          left_column != nullptr ? left_column : right_column;
      const FieldValue* value =
          left_value != nullptr ? left_value : right_value;
      if (column_ref == nullptr || value == nullptr) {
        continue;
      }

      const std::optional<std::size_t> inner_index =
          to_inner_index(column_ref->column_index);
      if (!inner_index.has_value()) {
        continue;
      }

      plan.constants.push_back(ColumnValuePair{inner_index.value(), *value});
      mark_covered(inner_index.value());
    }

    const std::vector<std::size_t> index_column_indexes =
        tables[1].indexedColumnIndexes(
            tables[1].indexes()[index_position]);
    const bool index_is_covered =
        std::all_of(index_column_indexes.begin(), index_column_indexes.end(),
                    [&](std::size_t column_index) {
                      return covered_inner_columns[column_index];
                    });
    if (!plan.join_keys.empty() && index_is_covered &&
        index_column_indexes.size() > best_index_column_count) {
      best_index_column_count = index_column_indexes.size();
      best_plan = std::move(plan);
    }
  }
  return best_plan;
}

}  // namespace

JoinPlan planJoin(const binder::BoundSelectQuery& query) {
  if (query.tables.size() == 1) {
    return NoJoinPlan{};
  }

  if (std::optional<IndexLookupJoinPlan> plan =
          findIndexLookupJoinPlanForTwoTableJoin(query.predicates,
                                                 query.tables);
      plan.has_value()) {
    return std::move(plan.value());
  }

  if (std::optional<JoinColumnPair> key =
          findHashJoinKeyForTwoTableJoin(query.predicates, query.tables);
      key.has_value()) {
    return HashJoinPlan{key.value()};
  }

  return LoopJoinPlan{};
}

}  // namespace physical
