#pragma once

#include "planning/physical/join_plan.h"

namespace binder {
struct BoundSelectQuery;
}

namespace physical {

JoinPlan planJoin(const binder::BoundSelectQuery& query);

}  // namespace physical
