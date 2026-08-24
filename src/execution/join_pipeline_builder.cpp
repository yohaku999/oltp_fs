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
      return std::make_unique<IndexLookupJoinOperator>(
          std::move(sources[0]), pool, query.tables[1],
          std::move(plan.index_lookup_join_keys),
          std::move(plan.index_lookup_constants),
          query.table_predicates[1]);
    }

    case physical::JoinType::Hash: {
      return std::make_unique<HashJoinOperator>(
          std::move(sources[0]), std::move(sources[1]),
          plan.hash_join_key.value());
    }

    case physical::JoinType::Loop:
      return std::make_unique<LoopJoinOperator>(std::move(sources));
  }

  throw std::logic_error("Unsupported physical join type.");
}

}  // namespace execution
