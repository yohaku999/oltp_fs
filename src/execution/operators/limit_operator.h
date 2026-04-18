#pragma once

#include <cstddef>
#include <memory>
#include <optional>

#include "execution/operator.h"

class LimitOperator : public Operator {
 public:
  LimitOperator(std::unique_ptr<Operator> child, std::size_t limit)
      : child_(std::move(child)), limit_(limit), emitted_count_(0) {}

  void open() override {
    emitted_count_ = 0;
    child_->open();
  }

  std::optional<TypedRow> next() override {
    if (emitted_count_ >= limit_) {
      return std::nullopt;
    }

    std::optional<TypedRow> row = child_->next();
    if (!row.has_value()) {
      return std::nullopt;
    }

    ++emitted_count_;
    return row;
  }

  void close() override {
    emitted_count_ = 0;
    child_->close();
  }

 private:
  std::unique_ptr<Operator> child_;
  std::size_t limit_;
  std::size_t emitted_count_;
};