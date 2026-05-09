#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>

template <typename T>
class ClosableQueue {
 public:
  void pushBatch(const std::vector<T>& batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(batch);
    cond_var_.notify_one();
  }

  std::optional<std::vector<T>> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_var_.wait(lock, [this] { return !queue_.empty() || closed_; });
    if (queue_.empty()) {
      return std::nullopt;
    }
    std::vector<T> batch = queue_.front();
    queue_.pop();
    return batch;
  }

  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cond_var_.notify_all();
  }

 private:
  std::queue<std::vector<T>> queue_;
  bool closed_ = false;
  std::mutex mutex_;
  std::condition_variable cond_var_;
};