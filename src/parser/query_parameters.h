#pragma once

#include <cstddef>
#include <variant>
#include <vector>

#include "schema/column.h"
#include "tuple/field_value.h"

using QueryParameters = std::vector<FieldValue>;

struct QueryParameterRef {
  std::size_t index;
  Column::Type type;
};

using PreparedFieldValue = std::variant<FieldValue, QueryParameterRef>;
