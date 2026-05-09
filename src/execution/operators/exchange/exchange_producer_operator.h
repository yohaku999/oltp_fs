#pragma once

#include <memory>
#include <vector>

#include "execution/operator.h"
#include "execution/operators/exchange/exchange_sink.h"

template <typename T>
class ExchangeProducerOperator {
 public:
  ExchangeProducerOperator(std::unique_ptr<Operator> child,
                           std::shared_ptr<ExchangeSink<T>> sink,
                           size_t batch_capacity)
      : child_(std::move(child)), sink_(sink), batch_capacity_(batch_capacity) {}

  void run() {
    child_->open();
    std::vector<T> batch;
    while (auto row = child_->next()) {
      batch.push_back(*row);
      if (batch.size() >= batch_capacity_) {
        sink_->emitBatch(batch);
        batch.clear();
      }
    }

    if (!batch.empty()) {
      sink_->emitBatch(batch);
    }

    child_->close();
    sink_->producerFinished();
  }

 private:
  std::unique_ptr<Operator> child_;
  std::shared_ptr<ExchangeSink<T>> sink_;
  size_t batch_capacity_;
};