#pragma once

#include <optional>
#include "tuple/typed_row.h"

class Operator {
 public:
  virtual ~Operator() = default;
  virtual void open() = 0;
  virtual std::optional<TypedRow> next() = 0;
  virtual void close() = 0;
};