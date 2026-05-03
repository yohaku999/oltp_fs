#pragma once

#include <string>
#include <vector>

#include "execution/comparison_predicate.h"

class Table;

struct IndexLookupPlan {
  enum class Kind { None, PointLookups };

  Kind kind = Kind::None;
  std::vector<std::string> encoded_keys;

  bool canUseIndex() const {
    return kind == Kind::PointLookups && !encoded_keys.empty();
  }
};

class IndexLookupPlanner {
 public:
  static IndexLookupPlan plan(
      const Table& table,
      const std::vector<UnboundComparisonPredicate>& predicates);
};