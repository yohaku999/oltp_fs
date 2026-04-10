#include "logging.h"

namespace {
struct LogInitializer {
  LogInitializer() {
    spdlog::set_pattern("%^[%Y-%m-%d %H:%M:%S.%e] [%l] - %v%$");
  }
};

static LogInitializer log_initializer;
}  // namespace
