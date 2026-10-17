#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"
#include "planning/physical/join_plan.h"
#include "schema/schema.h"
#include "storage/heap/heapfile.h"
#include "storage/index/index_accessor.h"
#include "tuple/field_value.h"

class BufferPool;

/**
 * Joins each outer row with inner rows found through an index lookup.
 *
 * It batches outer rows, sorts and deduplicates their lookup keys, fetches the
 * matching heap records through a shared index traversal, applies the residual
 * predicates, and emits joined rows in the original outer-row order.
 */
class IndexLookupJoinOperator : public TypedRowOperator {
 public:
  IndexLookupJoinOperator(
      std::unique_ptr<TypedRowOperator> outer_child, BufferPool& pool,
      HeapFile inner_heap_file, IndexAccessor& index_accessor,
      IndexMetadata inner_index, Schema inner_schema,
      std::vector<std::size_t> indexed_column_indexes,
      std::vector<physical::JoinColumnPair> join_keys,
      std::vector<physical::ColumnValuePair> constant_keys,
      std::vector<BoundComparisonPredicate> inner_predicates = {});

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;
  const OutputProperties& outputProperties() const override {
    return outer_child_->outputProperties();
  }

 private:
  static constexpr std::size_t kLookupBatchSize = 512;

  std::optional<std::string> buildLookupKey(const TypedRow& outer_row) const;
  bool prepareNextBatch();
  std::vector<std::vector<TypedRow>> lookupInnerRows(
      const std::vector<std::string>& lookup_keys);

  std::unique_ptr<TypedRowOperator> outer_child_;
  BufferPool& pool_;
  HeapFile inner_heap_file_;
  IndexAccessor& index_accessor_;
  IndexMetadata inner_index_;
  Schema inner_schema_;
  std::vector<std::size_t> indexed_column_indexes_;
  std::vector<physical::JoinColumnPair> join_keys_;
  std::vector<physical::ColumnValuePair> constant_keys_;
  std::vector<BoundComparisonPredicate> inner_predicates_;
  std::vector<TypedRow> outer_row_batch_;
  std::vector<std::optional<std::size_t>> inner_row_group_index_by_outer_row_;
  std::vector<std::vector<TypedRow>> matched_inner_row_groups_;
  std::size_t current_outer_pos_ = 0;
  std::size_t current_inner_pos_ = 0;
  std::size_t index_lookups_ = 0;
  mutable OperatorExecutionLogger logger_{"IndexLookupJoinOperator"};
};
