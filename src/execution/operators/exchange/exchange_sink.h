#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "execution/operators/exchange/exchange_dispatcher.h"

template <typename T>
class ExchangeSink {
 public:
  ExchangeSink(std::unique_ptr<ExchangeDispatcher<T>> dispatcher,
               std::vector<std::unique_ptr<ClosableQueue<T>>>& queues,
               size_t producer_count)
      : dispatcher_(std::move(dispatcher)),
        queues_(queues),
        remaining_producers_(producer_count) {}

  void emitBatch(const std::vector<T>& batch) {
    dispatcher_->dispatch(batch, queues_);
  }

  void producerFinished() {
    if (remaining_producers_.fetch_sub(1) == 1) {
      for (const auto& queue : queues_) {
        queue->close();
      }
    }
  }

 private:
  std::unique_ptr<ExchangeDispatcher<T>> dispatcher_;
  std::vector<std::unique_ptr<ClosableQueue<T>>>& queues_;
  std::atomic<size_t> remaining_producers_;
};