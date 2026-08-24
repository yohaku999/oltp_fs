#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "tuple/field_value.h"

namespace physical {

enum class JoinType { None, IndexLookup, Hash, Loop };

struct JoinColumnPair {
  std::size_t outer_column_index;
  std::size_t inner_column_index;
};

struct ColumnValuePair {
  std::size_t column_index;
  FieldValue value;
};

struct JoinPlan {
  JoinType type;
  std::optional<JoinColumnPair> hash_join_key;
  std::vector<JoinColumnPair> index_lookup_join_keys;
  std::vector<ColumnValuePair> index_lookup_constants;
};

}  // namespace physical
