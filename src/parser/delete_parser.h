#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "parser/pg_query_json_parser.h"
#include "parser/query_parameters.h"

class Schema;

class DeleteParser : private PgQueryJsonParser {
 public:
  explicit DeleteParser(std::string sql);
  ~DeleteParser() = default;

  std::string extractTableName() const;
  std::vector<UnboundComparisonPredicate> extractComparisonPredicates(
      const Schema& schema, const QueryParameters& parameters = {}) const;

 private:
  const nlohmann::json& deleteStatement() const;
};
