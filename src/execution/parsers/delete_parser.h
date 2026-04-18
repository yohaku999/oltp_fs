#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/parsers/pg_query_json_parser.h"

class Schema;

class DeleteParser : private PgQueryJsonParser {
 public:
  explicit DeleteParser(std::string sql);
  ~DeleteParser() = default;

  std::string extractTableName() const;
  std::vector<ComparisonPredicate> extractComparisonPredicates(
      const Schema& schema) const;

 private:
  const nlohmann::json& deleteStatement() const;
};