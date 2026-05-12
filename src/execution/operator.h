#pragma once

#include <optional>

#include "storage/index/rid.h"
#include "tuple/typed_row.h"

template <typename T>
class Operator {
 public:
  virtual ~Operator() = default;
  virtual void open() = 0;
  virtual std::optional<T> next() = 0;
  virtual void close() = 0;
};

template <typename T>
using TypedOperator = Operator<T>;

using TypedRowOperator = Operator<TypedRow>;
using RidOperator = Operator<RID>;