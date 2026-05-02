#pragma once

#include <optional>
#include <string>
#include <vector>

class Schema;

#include "execution/comparison_predicate.h"
#include "execution/order_by_spec.h"
#include "execution/parsers/pg_query_json_parser.h"
#include "execution/select_item.h"

class SelectParser : private PgQueryJsonParser {
 public:
  explicit SelectParser(std::string sql);
  ~SelectParser() = default;

  std::vector<std::string> extractTableNames() const;
  std::vector<UnboundSelectItem> extractSelectItems() const;
  std::vector<OrderBySpec> extractOrderBySpecs(const Schema& schema) const;
  std::optional<std::size_t> extractLimitCount() const;
  std::vector<UnboundComparisonPredicate> extractComparisonPredicates(
      const Schema& schema) const;
};