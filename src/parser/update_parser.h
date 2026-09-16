#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/update_assignment.h"
#include "parser/pg_query_json_parser.h"

class Schema;

class UpdateParser : private PgQueryJsonParser {
 public:
  using PgQueryJsonParser::sqlTemplate;

  explicit UpdateParser(std::string sql);
  ~UpdateParser() = default;

  std::string extractTableName() const;

  std::vector<UnboundComparisonPredicate> extractPreparedComparisonPredicates(
      const Schema& schema) const;
  std::vector<PreparedUnboundUpdateAssignment> extractPreparedAssignments(
      const Schema& schema) const;

 private:
  const nlohmann::json& updateStatement() const;
};
