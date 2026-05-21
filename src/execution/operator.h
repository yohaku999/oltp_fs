#pragma once

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "logging.h"
#include "storage/index/rid.h"
#include "tuple/typed_row.h"

class OperatorExecutionLogger {
 public:
  explicit OperatorExecutionLogger(std::string operator_name)
      : operator_name_(std::move(operator_name)) {}

  ~OperatorExecutionLogger() { close(); }

  void open() {
    if (!enabled()) {
      return;
    }

    input_rows_ = 0;
    output_rows_ = 0;
    metrics_.clear();
    opened_ = true;
  }

  void recordInput(std::size_t count = 1) {
    if (!opened_) {
      return;
    }
    input_rows_ += count;
  }

  void recordOutput(std::size_t count = 1) {
    if (!opened_) {
      return;
    }
    output_rows_ += count;
  }

  void setMetric(std::string_view key, std::size_t value) {
    if (!opened_) {
      return;
    }

    for (auto& metric : metrics_) {
      if (metric.first == key) {
        metric.second = value;
        return;
      }
    }

    metrics_.emplace_back(key, value);
  }

  void close() {
    if (!opened_) {
      return;
    }

    logSnapshot("close");
    opened_ = false;
  }

  static bool enabled() {
    static const bool value = [] {
      const char* env = std::getenv("DBFS_OPERATOR_LOG_ROWS");
      if (env == nullptr) {
        return false;
      }

      const std::string_view raw(env);
      return !raw.empty() && raw != "0" && raw != "false" && raw != "FALSE";
    }();
    return value;
  }

 private:
  void logSnapshot(std::string_view event,
                   std::string_view milestone = std::string_view()) const {
    if (!opened_) {
      return;
    }

    std::string rendered_metrics;
    for (const auto& [key, value] : metrics_) {
      rendered_metrics += " ";
      rendered_metrics += key;
      rendered_metrics += "=";
      rendered_metrics += std::to_string(value);
    }

    appendFileLine(renderFileLine(event, milestone, rendered_metrics));
  }

  std::string renderFileLine(std::string_view event, std::string_view milestone,
                             const std::string& rendered_metrics) const {
    std::string line = "operator=";
    line += operator_name_;
    line += " event=";
    line += event;
    if (!milestone.empty()) {
      line += " milestone=";
      line += milestone;
    }
    line += " input_rows=";
    line += std::to_string(input_rows_);
    line += " output_rows=";
    line += std::to_string(output_rows_);
    line += rendered_metrics;
    return line;
  }

  static const std::string& logFilePath() {
    static const std::string value = [] {
      const char* env = std::getenv("DBFS_OPERATOR_LOG_FILE");
      if (env == nullptr) {
        return std::string();
      }
      return std::string(env);
    }();
    return value;
  }

  struct FileSink {
    explicit FileSink(const std::string& path) {
      if (path.empty()) {
        return;
      }

      const std::filesystem::path file_path(path);
      if (file_path.has_parent_path()) {
        std::filesystem::create_directories(file_path.parent_path());
      }

      stream.open(path, std::ios::app);
    }

    std::mutex mutex;
    std::ofstream stream;
  };

  static FileSink& fileSink() {
    static FileSink sink(logFilePath());
    return sink;
  }

  static void appendFileLine(const std::string& line) {
    const std::string& path = logFilePath();
    if (path.empty()) {
      return;
    }

    FileSink& sink = fileSink();
    if (!sink.stream.is_open()) {
      return;
    }

    std::lock_guard<std::mutex> lock(sink.mutex);
    sink.stream << line << '\n';
    sink.stream.flush();
  }

  std::string operator_name_;
  std::size_t input_rows_ = 0;
  std::size_t output_rows_ = 0;
  std::vector<std::pair<std::string, std::size_t>> metrics_;
  bool opened_ = false;
};

template <typename T>
class Operator {
 public:
  virtual ~Operator() = default;
  virtual void open() = 0;
  virtual std::optional<T> next() = 0;
  virtual void close() = 0;
};

template <typename T>
using TypedOperator = Operator<T>;

using TypedRowOperator = Operator<TypedRow>;
using RidOperator = Operator<RID>;