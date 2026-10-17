#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "execution/operator.h"
#include "planning/physical/join_plan.h"

class HashJoinOperator : public TypedRowOperator {
 public:
  explicit HashJoinOperator(std::unique_ptr<TypedRowOperator> outer_child,
                            std::unique_ptr<TypedRowOperator> inner_child,
                            physical::JoinColumnPair join_key);

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;
  const OutputProperties& outputProperties() const override {
    return outer_child_->outputProperties();
  }

 private:
  std::unique_ptr<TypedRowOperator> outer_child_;
  std::unique_ptr<TypedRowOperator> inner_child_;
  std::unordered_multimap<FieldValue, TypedRow> hash_table;
  physical::JoinColumnPair join_key_;
  std::optional<TypedRow> current_outer_row_;
  std::unordered_multimap<FieldValue, TypedRow>::iterator current_inner_it_;
  std::unordered_multimap<FieldValue, TypedRow>::iterator current_inner_end_;
  mutable OperatorExecutionLogger logger_{"HashJoinOperator"};
};
