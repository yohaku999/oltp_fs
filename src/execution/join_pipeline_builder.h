#pragma once

#include <memory>
#include <vector>

#include "execution/operator.h"
#include "planning/physical/join_plan.h"

class BufferPool;
class LocalStorageManager;

namespace binder {
struct BoundSelectQuery;
}

namespace execution {

std::unique_ptr<TypedRowOperator> buildJoinPipeline(
    physical::JoinPlan plan,
    std::vector<std::unique_ptr<TypedRowOperator>> sources, BufferPool& pool,
    const LocalStorageManager& storage,
    binder::BoundSelectQuery& query);

}  // namespace execution
