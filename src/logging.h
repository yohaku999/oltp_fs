#pragma once
#include <spdlog/spdlog.h>

#include <string_view>

// Helper to extract "Class::method" (or just "function") from
// __PRETTY_FUNCTION__.
inline std::string_view dbfs_extract_func_name(const char* pretty_func) {
  std::string_view sv(pretty_func);
  auto paren = sv.find('(');
  if (paren != std::string_view::npos) {
    sv = sv.substr(0, paren);
  }
  auto space = sv.rfind(' ');
  if (space != std::string_view::npos) {
    sv = sv.substr(space + 1);
  }
  return sv;
}

// Core macro that prepends "Class::method" to the log message.
#define DBFS_LOG(level, fmt, ...)                                              \
  spdlog::log(level, "{} - " fmt, dbfs_extract_func_name(__PRETTY_FUNCTION__), \
              ##__VA_ARGS__)

// User-facing macros.
#define LOG_INFO(fmt, ...) DBFS_LOG(spdlog::level::info, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) DBFS_LOG(spdlog::level::debug, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) DBFS_LOG(spdlog::level::warn, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) DBFS_LOG(spdlog::level::err, fmt, ##__VA_ARGS__)
