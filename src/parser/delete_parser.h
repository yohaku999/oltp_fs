#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "parser/pg_query_json_parser.h"

class Schema;

class DeleteParser : private PgQueryJsonParser {
 public:
  using PgQueryJsonParser::sqlTemplate;

  explicit DeleteParser(std::string sql);
  ~DeleteParser() = default;

  std::string extractTableName() const;
  std::vector<UnboundComparisonPredicate> extractPreparedComparisonPredicates(
      const Schema& schema) const;

 private:
  const nlohmann::json& deleteStatement() const;
};
