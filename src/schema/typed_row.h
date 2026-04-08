#pragma once

#include <vector>

#include "field_value.h"

struct TypedRow {
  std::vector<FieldValue> values;
};