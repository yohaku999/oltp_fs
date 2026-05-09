#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "execution/operators/closable_queue.h"

template <typename T>
class ExchangeConsumerOperator {
 public:
  explicit ExchangeConsumerOperator(ClosableQueue<T>& queue)
      : queue_(queue), current_batch_index_(0) {}

  void open() {
    current_batch_.clear();
    current_batch_index_ = 0;
  }

  std::optional<T> next() {
    if (current_batch_index_ < current_batch_.size()) {
      return current_batch_[current_batch_index_++];
    }

    std::optional<std::vector<T>> batch = queue_.pop();
    if (!batch.has_value()) {
      return std::nullopt;
    }

    current_batch_ = std::move(batch.value());
    current_batch_index_ = 0;
    return current_batch_[current_batch_index_++];
  }

  void close() {}

 private:
  ClosableQueue<T>& queue_;
  std::vector<T> current_batch_;
  size_t current_batch_index_;
};