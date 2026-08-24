#include "execution/join_pipeline_builder.h"

#include <utility>
#include <variant>

#include "execution/operators/hash_join_operator.h"
#include "execution/operators/index_lookup_join_operator.h"
#include "execution/operators/loop_join_operator.h"
#include "planning/binder.h"
#include "storage/buffer/bufferpool.h"

namespace execution {

namespace {

template <class... Visitors>
struct Overloaded : Visitors... {
  using Visitors::operator()...;
};

template <class... Visitors>
Overloaded(Visitors...) -> Overloaded<Visitors...>;

}  // namespace

std::unique_ptr<TypedRowOperator> buildJoinPipeline(
    physical::JoinPlan plan,
    std::vector<std::unique_ptr<TypedRowOperator>> sources, BufferPool& pool,
    binder::BoundSelectQuery& query) {
  return std::visit(
      Overloaded{
          [&](physical::NoJoinPlan&) -> std::unique_ptr<TypedRowOperator> {
            return std::move(sources.front());
          },
          [&](physical::IndexLookupJoinPlan& index_plan)
              -> std::unique_ptr<TypedRowOperator> {
            return std::make_unique<IndexLookupJoinOperator>(
                std::move(sources[0]), pool, query.tables[1],
                std::move(index_plan.join_keys),
                std::move(index_plan.constants), query.table_predicates[1]);
          },
          [&](physical::HashJoinPlan& hash_plan)
              -> std::unique_ptr<TypedRowOperator> {
            return std::make_unique<HashJoinOperator>(
                std::move(sources[0]), std::move(sources[1]),
                hash_plan.join_key);
          },
          [&](physical::LoopJoinPlan&) -> std::unique_ptr<TypedRowOperator> {
            return std::make_unique<LoopJoinOperator>(std::move(sources));
          }},
      plan);
}

}  // namespace execution
