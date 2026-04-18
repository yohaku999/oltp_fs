#pragma once

#include <string>
#include <vector>

class Schema;

#include "execution/comparison_predicate.h"
#include "execution/order_by_spec.h"
#include "execution/parsers/pg_query_json_parser.h"

class SelectParser : private PgQueryJsonParser {
 public:
  explicit SelectParser(std::string sql);
  ~SelectParser() = default;

  std::string extractTableName() const;
  std::vector<std::size_t> extractProjectionIndices(const Schema& schema) const;
    std::vector<OrderBySpec> extractOrderBySpecs(const Schema& schema) const;
  std::vector<ComparisonPredicate> extractComparisonPredicates(
      const Schema& schema) const;
};