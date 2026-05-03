#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/update_assignment.h"
#include "execution/parsers/pg_query_json_parser.h"

class Schema;

class UpdateParser : private PgQueryJsonParser {
 public:
  explicit UpdateParser(std::string sql);
  ~UpdateParser() = default;

  std::string extractTableName() const;

  std::vector<UnboundComparisonPredicate> extractComparisonPredicates(
      const Schema& schema) const;
  std::vector<UnboundUpdateAssignment> extractAssignments(
    const Schema& schema) const;

 private:
  const nlohmann::json& updateStatement() const;
};