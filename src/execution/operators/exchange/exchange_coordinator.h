#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include "execution/operators/closable_queue.h"
#include "execution/operators/exchange/exchange_consumer_operator.h"
#include "execution/operators/exchange/exchange_producer_operator.h"
#include "execution/operators/exchange/exchange_sink.h"

template <typename T>
class ExchangeCoordinator {
 public:
  enum class DispatchRule {
    RoundRobin,
    HashPartition,
  };

  ExchangeCoordinator(size_t batch_capacity,
                      size_t producer_threads_count,
                      size_t consumer_threads_count,
                      DispatchRule dispatch_rule,
                      std::function<std::unique_ptr<Operator>(size_t)> child_factory,
                      std::optional<std::function<size_t(const T&)>> partition_hash_fn = std::nullopt) {
    queues_.reserve(consumer_threads_count);
    consumers_.reserve(consumer_threads_count);
    for (size_t i = 0; i < consumer_threads_count; i++) {
      auto queue = std::make_unique<ClosableQueue<T>>();
      consumers_.push_back(std::make_unique<ExchangeConsumerOperator<T>>(*queue));
      queues_.push_back(std::move(queue));
    }

    std::shared_ptr<ExchangeSink<T>> sink;
    if (dispatch_rule == DispatchRule::RoundRobin) {
      auto dispatcher = std::make_unique<RoundRobinDispatcher<T>>();
      sink = std::make_shared<ExchangeSink<T>>(std::move(dispatcher), queues_, producer_threads_count);
    } else if (dispatch_rule == DispatchRule::HashPartition) {
      if (!partition_hash_fn.has_value()) {
        throw std::invalid_argument("partition_hash_fn is required for HashPartition dispatch");
      }
      auto dispatcher = std::make_unique<HashPartitionDispatcher<T>>(std::move(*partition_hash_fn));
      sink = std::make_shared<ExchangeSink<T>>(std::move(dispatcher), queues_, producer_threads_count);
    }

    producers_.reserve(producer_threads_count);
    for (size_t i = 0; i < producer_threads_count; i++) {
      auto child = child_factory(i);
      producers_.emplace_back(std::move(child), sink, batch_capacity);
    }
  }

  ~ExchangeCoordinator() {
    join();
  }

  void start() {
    std::call_once(start_once_, [this]() {
      if (producers_.empty()) {
        for (const auto& queue : queues_) {
          queue->close();
        }
        return;
      }

      producer_threads_.reserve(producers_.size());
      for (auto& producer : producers_) {
        auto* producer_ptr = &producer;
        producer_threads_.emplace_back([producer_ptr]() {
          producer_ptr->run();
        });
      }
    });
  }

  void join() {
    for (auto& producer_thread : producer_threads_) {
      if (producer_thread.joinable()) {
        producer_thread.join();
      }
    }
  }

  size_t consumerCount() const {
    return consumers_.size();
  }

  ExchangeConsumerOperator<T>& consumerAt(size_t index) {
    if (index >= consumers_.size()) {
      throw std::out_of_range("consumer index is out of range");
    }
    return *consumers_[index];
  }

 private:
  std::once_flag start_once_;
  std::vector<ExchangeProducerOperator<T>> producers_;
  std::vector<std::thread> producer_threads_;
  std::vector<std::unique_ptr<ExchangeConsumerOperator<T>>> consumers_;
  std::vector<std::unique_ptr<ClosableQueue<T>>> queues_;
};