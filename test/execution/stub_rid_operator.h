#pragma once

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "execution/operator.h"

class StubRidOperator : public RidOperator {
 public:
  explicit StubRidOperator(std::vector<RID> rids) : rids_(std::move(rids)) {}

  void open() override {
    is_open_ = true;
    cursor_ = 0;
  }

  std::optional<RID> next() override {
    if (!is_open_ || cursor_ >= rids_.size()) {
      return std::nullopt;
    }
    return rids_[cursor_++];
  }

  void close() override { is_open_ = false; }

 private:
  std::vector<RID> rids_;
  std::size_t cursor_ = 0;
  bool is_open_ = false;
};