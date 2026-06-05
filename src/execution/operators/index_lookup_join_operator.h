#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"
#include "tuple/field_value.h"

class BufferPool;
class Table;

struct IndexLookupJoinKey {
  std::size_t outer_column_index;
  std::size_t inner_column_index;
};

struct IndexLookupJoinConstantKey {
  std::size_t inner_column_index;
  FieldValue value;
};

class IndexLookupJoinOperator : public TypedRowOperator {
 public:
  IndexLookupJoinOperator(
      std::unique_ptr<TypedRowOperator> outer_child, BufferPool& pool,
      Table& inner_table, std::vector<IndexLookupJoinKey> join_keys,
      std::vector<IndexLookupJoinConstantKey> constant_keys,
      std::vector<BoundComparisonPredicate> inner_predicates = {});

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  std::optional<std::string> buildLookupKey(const TypedRow& outer_row) const;
  std::vector<TypedRow> lookupInnerRows(const TypedRow& outer_row);

  std::unique_ptr<TypedRowOperator> outer_child_;
  BufferPool& pool_;
  Table& inner_table_;
  std::vector<IndexLookupJoinKey> join_keys_;
  std::vector<IndexLookupJoinConstantKey> constant_keys_;
  std::vector<BoundComparisonPredicate> inner_predicates_;
  std::optional<TypedRow> current_outer_row_;
  std::vector<TypedRow> current_inner_rows_;
  std::size_t current_inner_pos_ = 0;
  std::size_t index_lookups_ = 0;
  mutable OperatorExecutionLogger logger_{"IndexLookupJoinOperator"};
};
