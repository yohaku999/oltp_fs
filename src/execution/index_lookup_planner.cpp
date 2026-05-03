#include "execution/index_lookup_planner.h"

#include "catalog/table.h"
#include "../logging.h"

namespace {

std::optional<ExactMatchIndexColumnValue> tryExtractExactMatchLookupValue(
    const UnboundComparisonPredicate& predicate) {
  if (predicate.op != Op::Eq) {
    LOG_DEBUG("skipping non-equality predicate for index lookup");
    return std::nullopt;
  }

  const auto* column_ref = std::get_if<ColumnRef>(&predicate.left);
  const auto* value = std::get_if<FieldValue>(&predicate.right);
  if (column_ref == nullptr || value == nullptr) {
    column_ref = std::get_if<ColumnRef>(&predicate.right);
    value = std::get_if<FieldValue>(&predicate.left);
  }
  if (column_ref == nullptr || value == nullptr) {
    return std::nullopt;
  }

  return ExactMatchIndexColumnValue{column_ref->column_name, *value};
}

}  // namespace

IndexLookupPlan IndexLookupPlanner::plan(
    const Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  std::vector<ExactMatchIndexColumnValue> exact_match_values;
  exact_match_values.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    const auto exact_match_value = tryExtractExactMatchLookupValue(predicate);
    if (!exact_match_value.has_value()) {
      continue;
    }

    exact_match_values.push_back(std::move(exact_match_value.value()));
  }

  const std::optional<std::string> exact_match_key =
      table.tryBuildExactMatchIndexKey(exact_match_values);
  if (!exact_match_key.has_value()) {
    return {};
  }

  IndexLookupPlan plan;
  plan.kind = IndexLookupPlan::Kind::PointLookups;
  plan.encoded_keys.push_back(exact_match_key.value());
  return plan;
}