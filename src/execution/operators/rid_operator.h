#pragma once

#include <optional>

#include "storage/index/rid.h"

class RidOperator {
 public:
  virtual ~RidOperator() = default;
  virtual void open() = 0;
  virtual std::optional<RID> next() = 0;
  virtual void close() = 0;
};