#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>

class Schema;

#include "execution/comparison_predicate.h"
#include "execution/order_by_spec.h"
#include "execution/select_item.h"
#include "parser/pg_query_json_parser.h"
#include "parser/query_parameters.h"

class SelectParser : private PgQueryJsonParser {
 public:
  using PgQueryJsonParser::sqlTemplate;

  explicit SelectParser(std::string sql);
  ~SelectParser() = default;

  std::vector<std::string> extractTableNames() const;
  std::vector<UnboundSelectItem> extractSelectItems() const;
  std::vector<std::optional<std::string>> extractSelectAliases() const;
  std::vector<OrderBySpec> extractOrderBySpecs(const Schema& schema) const;
  std::optional<std::variant<std::size_t, QueryParameterRef>>
  extractPreparedLimit() const;
  std::vector<UnboundComparisonPredicate> extractPreparedComparisonPredicates(
      const Schema& schema) const;
};
