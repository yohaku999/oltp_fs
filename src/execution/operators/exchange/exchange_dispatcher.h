#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "execution/operators/closable_queue.h"

template <typename T>
class ExchangeDispatcher {
 public:
  virtual ~ExchangeDispatcher() = default;
  virtual void dispatch(
      const std::vector<T>& batch,
      std::vector<std::unique_ptr<ClosableQueue<T>>>& queues) = 0;
};

template <typename T>
class RoundRobinDispatcher : public ExchangeDispatcher<T> {
 public:
  void dispatch(
      const std::vector<T>& batch,
      std::vector<std::unique_ptr<ClosableQueue<T>>>& queues) override {
    queues[next_queue_index_]->pushBatch(batch);
    next_queue_index_ = (next_queue_index_ + 1) % queues.size();
  }

 private:
  size_t next_queue_index_ = 0;
};

template <typename T>
class HashPartitionDispatcher : public ExchangeDispatcher<T> {
 public:
  explicit HashPartitionDispatcher(
      std::function<size_t(const T&)> partition_hash_fn)
      : partition_hash_fn_(std::move(partition_hash_fn)) {}

  void dispatch(
      const std::vector<T>& batch,
      std::vector<std::unique_ptr<ClosableQueue<T>>>& queues) override {
    std::vector<std::vector<T>> partitioned_batches(queues.size());
    for (const auto& row : batch) {
      const size_t partition_id = partition_hash_fn_(row) % queues.size();
      partitioned_batches[partition_id].push_back(row);
    }

    for (size_t partition_id = 0; partition_id < partitioned_batches.size();
         ++partition_id) {
      if (!partitioned_batches[partition_id].empty()) {
        queues[partition_id]->pushBatch(partitioned_batches[partition_id]);
      }
    }
  }

 private:
  std::function<size_t(const T&)> partition_hash_fn_;
};