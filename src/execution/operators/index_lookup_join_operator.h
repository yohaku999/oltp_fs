#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"
#include "planning/physical/join_plan.h"
#include "schema/schema.h"
#include "storage/disk/pagedfile.h"
#include "storage/heap/heapfile.h"
#include "tuple/field_value.h"

class BufferPool;

class IndexLookupJoinOperator : public TypedRowOperator {
 public:
  IndexLookupJoinOperator(
      std::unique_ptr<TypedRowOperator> outer_child, BufferPool& pool,
      HeapFile inner_heap_file, PagedFile inner_index_file,
      Schema inner_schema,
      std::vector<std::size_t> indexed_column_indexes,
      std::vector<physical::JoinColumnPair> join_keys,
      std::vector<physical::ColumnValuePair> constant_keys,
      std::vector<BoundComparisonPredicate> inner_predicates = {});

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  std::optional<std::string> buildLookupKey(const TypedRow& outer_row) const;
  std::vector<TypedRow> lookupInnerRows(const TypedRow& outer_row);

  std::unique_ptr<TypedRowOperator> outer_child_;
  BufferPool& pool_;
  HeapFile inner_heap_file_;
  PagedFile inner_index_file_;
  Schema inner_schema_;
  std::vector<std::size_t> indexed_column_indexes_;
  std::vector<physical::JoinColumnPair> join_keys_;
  std::vector<physical::ColumnValuePair> constant_keys_;
  std::vector<BoundComparisonPredicate> inner_predicates_;
  std::optional<TypedRow> current_outer_row_;
  std::vector<TypedRow> current_inner_rows_;
  std::size_t current_inner_pos_ = 0;
  std::size_t index_lookups_ = 0;
  mutable OperatorExecutionLogger logger_{"IndexLookupJoinOperator"};
};
