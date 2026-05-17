#pragma once

#include <cstddef>
#include <memory>
#include <optional>

#include "execution/operator.h"

class LimitOperator : public TypedRowOperator {
 public:
  LimitOperator(std::unique_ptr<TypedRowOperator> child, std::size_t limit)
      : child_(std::move(child)), limit_(limit), emitted_count_(0) {}

  void open() override {
    emitted_count_ = 0;
    logger_.open();
    logger_.setMetric("limit", limit_);
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

    logger_.recordInput();
    ++emitted_count_;
    logger_.recordOutput();
    return row;
  }

  void close() override {
    emitted_count_ = 0;
    child_->close();
    logger_.close();
  }

 private:
  std::unique_ptr<TypedRowOperator> child_;
  std::size_t limit_;
  std::size_t emitted_count_;
  OperatorExecutionLogger logger_{"LimitOperator"};
};