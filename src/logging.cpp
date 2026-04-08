#include "logging.h"

// initialize global log pattern at program startup
namespace {
struct LogInitializer {
  LogInitializer() {
    // Do not include file/line in the pattern. The helper wrappers
    // prepend the function/method name into the message itself,
    // so keep the pattern concise (timestamp, level, message).
    spdlog::set_pattern("%^[%Y-%m-%d %H:%M:%S.%e] [%l] - %v%$");
  }
};

static LogInitializer log_initializer;
}  // namespace
