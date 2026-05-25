#pragma once

#include <spdlog/logger.h>

namespace dbfs_log {

spdlog::logger& catalog();
spdlog::logger& execution();
spdlog::logger& index();
spdlog::logger& server();
spdlog::logger& storage();

}  // namespace dbfs_log
