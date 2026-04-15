#pragma once

#include <vector>

#include <nlohmann/json.hpp>

class Schema;

#include "execution/comparison_predicate.h"

class ComparisonPredicateParser {
 public:
  static std::vector<ComparisonPredicate> parse(const nlohmann::json& node,
                                                const Schema& schema);

 private:
  static void extractFromNode(const nlohmann::json& node, const Schema& schema,
                              std::vector<ComparisonPredicate>& out);
  static ComparisonPredicate::Op parseComparisonOp(const nlohmann::json& expr);
  static FieldValue parsePredicateValue(const nlohmann::json& expr);
};