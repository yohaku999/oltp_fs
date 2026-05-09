#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>

#include "execution/operator.h"
#include "execution/operators/exchange/exchange_coordinator.h"

class ExchangeOperator : public Operator {
 public:
  using DispatchRule = ExchangeCoordinator<TypedRow>::DispatchRule;
  using PartitionHashFunction = std::function<size_t(const TypedRow&)>;
  using ChildFactory = std::function<std::unique_ptr<Operator>(size_t)>;

  // TODO: Unify RID-oriented operators with the main Operator abstraction and allow this
  // wrapper to work with arbitrary types. Decide how to handle cases where the number of
  // consumers equals the number of producers; this may be solvable by configuration alone,
  // or it may require introducing a batch-oriented operator abstraction.
  // If this remains a thin wrapper over the runtime, reconsider whether the internal pieces
  // should be called "operators" at all. Check how the original paper uses that terminology.
  ExchangeOperator(size_t batch_capacity,
                   size_t producer_threads_count,
                   size_t consumer_threads_count,
                   DispatchRule dispatch_rule,
                   ChildFactory child_factory,
                   std::optional<PartitionHashFunction> partition_hash_fn = std::nullopt,
                   size_t consumer_index = 0)
      : coordinator_(batch_capacity,
                     producer_threads_count,
                     consumer_threads_count,
                     dispatch_rule,
                     std::move(child_factory),
                     std::move(partition_hash_fn)),
        consumer_index_(consumer_index) {
    if (consumer_threads_count != 1) {
      throw std::invalid_argument(
          "ExchangeOperator currently supports exactly one consumer thread");
    }
    if (consumer_index_ >= coordinator_.consumerCount()) {
      throw std::out_of_range("consumer index is out of range");
    }
  }

  void open() override {
    coordinator_.consumerAt(consumer_index_).open();
    coordinator_.start();
  }

  std::optional<TypedRow> next() override {
    return coordinator_.consumerAt(consumer_index_).next();
  }

  void close() override {
    coordinator_.consumerAt(consumer_index_).close();
    coordinator_.join();
  }

 private:
  ExchangeCoordinator<TypedRow> coordinator_;
  size_t consumer_index_;
};