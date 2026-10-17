#pragma once

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "execution/operator.h"

class StubRidOperator : public RidOperator {
 public:
  explicit StubRidOperator(std::vector<RID> rids,
                           OutputProperties output_properties = {})
      : rids_(std::move(rids)),
        output_properties_(std::move(output_properties)) {}

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

  const OutputProperties& outputProperties() const override {
    return output_properties_;
  }

 private:
  std::vector<RID> rids_;
  OutputProperties output_properties_;
  std::size_t cursor_ = 0;
  bool is_open_ = false;
};
