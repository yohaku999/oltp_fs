#pragma once

#include <cstddef>

enum class OrderByDirection {
  Asc,
  Desc,
};

struct OrderBySpec {
  std::size_t column_index;
  OrderByDirection direction;
};