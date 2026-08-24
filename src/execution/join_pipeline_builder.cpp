#include "execution/join_pipeline_builder.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include "execution/operators/hash_join_operator.h"
#include "execution/operators/index_lookup_join_operator.h"
#include "execution/operators/loop_join_operator.h"
#include "planning/binder.h"
#include "storage/buffer/bufferpool.h"

namespace execution {

std::unique_ptr<TypedRowOperator> buildJoinPipeline(
    physical::JoinPlan plan,
    std::vector<std::unique_ptr<TypedRowOperator>> sources, BufferPool& pool,
    binder::BoundSelectQuery& query) {
  switch (plan.type) {
    case physical::JoinType::None:
      return std::move(sources.front());

    case physical::JoinType::IndexLookup: {
      std::vector<IndexLookupJoinKey> join_keys;
      join_keys.reserve(plan.index_lookup_join_columns.size());
      for (const auto& columns : plan.index_lookup_join_columns) {
        join_keys.push_back(IndexLookupJoinKey{
            columns.outer_column_index, columns.inner_column_index});
      }

      std::vector<IndexLookupJoinConstantKey> constant_keys;
      constant_keys.reserve(plan.index_lookup_constants.size());
      for (const auto& constant : plan.index_lookup_constants) {
        constant_keys.push_back(IndexLookupJoinConstantKey{
            constant.inner_column_index, constant.value});
      }

      return std::make_unique<IndexLookupJoinOperator>(
          std::move(sources[0]), pool, query.tables[1], std::move(join_keys),
          std::move(constant_keys), query.table_predicates[1]);
    }

    case physical::JoinType::Hash: {
      const physical::JoinColumnPair& columns =
          plan.hash_join_columns.value();
      return std::make_unique<HashJoinOperator>(
          std::move(sources[0]), std::move(sources[1]),
          HashJoinKey{columns.outer_column_index, columns.inner_column_index});
    }

    case physical::JoinType::Loop:
      return std::make_unique<LoopJoinOperator>(std::move(sources));
  }

  throw std::logic_error("Unsupported physical join type.");
}

}  // namespace execution
