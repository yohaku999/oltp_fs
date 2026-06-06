#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "execution/operator.h"

struct HashJoinKey {
  std::size_t outer_column_index;
  std::size_t inner_column_index;
};

class HashJoinOperator : public TypedRowOperator {
 public:
  explicit HashJoinOperator(std::unique_ptr<TypedRowOperator> outer_child,
                            std::unique_ptr<TypedRowOperator> inner_child,
                            HashJoinKey join_key);

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  std::unique_ptr<TypedRowOperator> outer_child_;
  std::unique_ptr<TypedRowOperator> inner_child_;
  std::unordered_multimap<FieldValue, TypedRow> hash_table;
  HashJoinKey join_key_;
  std::optional<TypedRow> current_outer_row_;
  std::unordered_multimap<FieldValue, TypedRow>::iterator current_inner_it_;
  std::unordered_multimap<FieldValue, TypedRow>::iterator current_inner_end_;
  mutable OperatorExecutionLogger logger_{"HashJoinOperator"};
};
