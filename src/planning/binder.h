#pragma once

#include <cstddef>
#include <optional>
#include <variant>
#include <vector>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "execution/order_by_spec.h"
#include "execution/select_item.h"
#include "execution/update_assignment.h"
#include "parser/query_parameters.h"

class SelectParser;

namespace binder {

using PreparedOperand =
    std::variant<BoundColumnRef, FieldValue, QueryParameterRef>;

struct PreparedComparisonPredicate {
  Op op;
  PreparedOperand left;
  PreparedOperand right;
};

struct PreparedSelfArithmeticUpdate {
  std::size_t target_column_index;
  UpdateBinaryOperator op;
  PreparedFieldValue literal;
};

using PreparedUpdateValue =
    std::variant<PreparedFieldValue, PreparedSelfArithmeticUpdate>;

struct PreparedUpdateAssignment {
  std::size_t target_column_index;
  PreparedUpdateValue value;
};

struct PreparedSelectQuery {
  std::vector<Table> tables;
  std::vector<BoundSelectItem> select_items;
  std::vector<PreparedComparisonPredicate> predicates;
  std::vector<std::vector<PreparedComparisonPredicate>> table_predicates;
  std::vector<OrderBySpec> order_by_specs;
  std::optional<std::variant<std::size_t, QueryParameterRef>> limit;
};

struct BoundSelectQuery {
  const std::vector<Table>& tables;
  const std::vector<BoundSelectItem>& select_items;
  std::vector<BoundComparisonPredicate> predicates;
  std::vector<std::vector<BoundComparisonPredicate>> table_predicates;
  const std::vector<OrderBySpec>& order_by_specs;
  std::optional<std::size_t> limit_count;
};

PreparedSelectQuery prepareSelectQuery(const SelectParser& parser);

BoundSelectQuery bindSelectQuery(const PreparedSelectQuery& prepared,
                                 const QueryParameters& parameters = {});

std::vector<PreparedComparisonPredicate> preparePredicates(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const std::vector<Table>& tables);

std::vector<BoundComparisonPredicate> bindPreparedPredicates(
    const std::vector<PreparedComparisonPredicate>& predicates,
    const QueryParameters& parameters = {});

std::vector<PreparedUpdateAssignment> prepareUpdateAssignments(
    const std::vector<PreparedUnboundUpdateAssignment>& assignments,
    const Table& table);

std::vector<BoundUpdateAssignment> bindPreparedUpdateAssignments(
    const std::vector<PreparedUpdateAssignment>& assignments,
    const QueryParameters& parameters = {});

BoundColumnRef bindColumnRef(const ColumnRef& column_ref,
                             const std::vector<Table>& tables);

std::vector<BoundSelectItem> bindSelectItems(
    const std::vector<UnboundSelectItem>& select_items,
    const std::vector<Table>& tables);

}  // namespace binder
