#include "logging.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr const char* kPattern = "%^[%Y-%m-%d %H:%M:%S.%e] [%l] [%n] %v%$";

spdlog::level::level_enum parseLevel(const char* value,
                                     spdlog::level::level_enum fallback) {
  if (value == nullptr || *value == '\0') {
    return fallback;
  }

  const spdlog::level::level_enum level = spdlog::level::from_str(value);
  return level == spdlog::level::off && std::string_view(value) != "off"
             ? fallback
             : level;
}

spdlog::level::level_enum defaultLevel() {
  return parseLevel(std::getenv("DBFS_LOG_LEVEL"), spdlog::level::info);
}

spdlog::level::level_enum loggerLevel(const char* env_name) {
  return parseLevel(std::getenv(env_name), defaultLevel());
}

std::shared_ptr<spdlog::sinks::sink> consoleSink() {
  static auto sink = [] {
    auto created = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    created->set_pattern(kPattern);
    return created;
  }();
  return sink;
}

std::shared_ptr<spdlog::sinks::sink> fileSink(const char* path) {
  std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }
  auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path, true);
  sink->set_pattern(kPattern);
  return sink;
}

spdlog::logger& makeLogger(const std::string& name,
                           spdlog::level::level_enum level,
                           std::vector<spdlog::sink_ptr> sinks) {
  auto logger =
      std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
  logger->set_level(level);
  logger->flush_on(spdlog::level::warn);
  spdlog::register_logger(logger);
  return *logger;
}

spdlog::logger& consoleLogger(const std::string& name,
                              spdlog::level::level_enum level) {
  return makeLogger(name, level, {consoleSink()});
}

}  // namespace

namespace dbfs_log {

spdlog::logger& catalog() {
  static spdlog::logger& logger =
      consoleLogger("catalog", loggerLevel("DBFS_CATALOG_LOG_LEVEL"));
  return logger;
}

spdlog::logger& execution() {
  static spdlog::logger& logger =
      consoleLogger("execution", loggerLevel("DBFS_EXECUTION_LOG_LEVEL"));
  return logger;
}

spdlog::logger& index() {
  static spdlog::logger& logger =
      consoleLogger("index", loggerLevel("DBFS_INDEX_LOG_LEVEL"));
  return logger;
}

spdlog::logger& server() {
  static spdlog::logger& logger = []() -> spdlog::logger& {
    const char* server_log_file = std::getenv("DBFS_SERVER_LOG_FILE");
    if (server_log_file != nullptr && *server_log_file != '\0') {
      return makeLogger("server", loggerLevel("DBFS_SERVER_LOG_LEVEL"),
                        {consoleSink(), fileSink(server_log_file)});
    }
    return consoleLogger("server", loggerLevel("DBFS_SERVER_LOG_LEVEL"));
  }();
  return logger;
}

spdlog::logger& storage() {
  static spdlog::logger& logger =
      consoleLogger("storage", loggerLevel("DBFS_STORAGE_LOG_LEVEL"));
  return logger;
}

}  // namespace dbfs_log
