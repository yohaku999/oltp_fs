#pragma once

#include <cstddef>
#include <variant>
#include <vector>

#include "tuple/field_value.h"

namespace physical {

struct JoinColumnPair {
  std::size_t outer_column_index;
  std::size_t inner_column_index;
};

struct ColumnValuePair {
  std::size_t column_index;
  FieldValue value;
};

struct NoJoinPlan {};

struct IndexLookupJoinPlan {
  std::size_t inner_index_position;
  std::vector<JoinColumnPair> join_keys;
  std::vector<ColumnValuePair> constants;
};

struct HashJoinPlan {
  JoinColumnPair join_key;
};

struct LoopJoinPlan {};

using JoinPlan =
    std::variant<NoJoinPlan, IndexLookupJoinPlan, HashJoinPlan, LoopJoinPlan>;

}  // namespace physical
