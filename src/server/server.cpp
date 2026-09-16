#include "server.h"

#include <arpa/inet.h>
#include <execinfo.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "catalog/table.h"
#include "execution/executor.h"
#include "execution/prepared_statement_cache.h"
#include "execution/select_item.h"
#include "logging.h"
#include "parser/create_index_parser.h"
#include "parser/create_table_parser.h"
#include "parser/delete_parser.h"
#include "parser/drop_table_parser.h"
#include "parser/insert_parser.h"
#include "parser/select_parser.h"
#include "parser/update_parser.h"
#include "storage/buffer/bufferpool.h"
#include "storage/index/index_accessor.h"
#include "storage/local_storage_manager.h"
#include "storage/table_metadata_store.h"
#include "storage/wal/wal.h"

namespace {

QueryParameters queryParametersFromJson(const nlohmann::json& values) {
  QueryParameters parameters;
  parameters.reserve(values.size());
  for (const nlohmann::json& value : values) {
    if (value.is_null()) {
      parameters.emplace_back(std::monostate{});
    } else if (value.is_number_integer() || value.is_number_unsigned()) {
      parameters.emplace_back(value.get<Column::IntegerType>());
    } else if (value.is_number_float()) {
      parameters.emplace_back(value.get<Column::DoubleType>());
    } else if (value.is_string()) {
      parameters.emplace_back(value.get<Column::VarcharType>());
    } else {
      throw std::runtime_error("Unsupported JSON parameter type.");
    }
  }
  return parameters;
}

template <typename Parser>
class ParsedStatementCache {
 public:
  const Parser& get(const std::string& sql_template) {
    const auto cached = parsers_.find(sql_template);
    if (cached != parsers_.end()) {
      return *cached->second;
    }

    auto parser = std::make_unique<Parser>(sql_template);
    const Parser& result = *parser;
    parsers_.emplace(sql_template, std::move(parser));
    return result;
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<Parser>> parsers_;
};

std::string leadingKeyword(const std::string& sql) {
  const std::size_t first = sql.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }

  const std::size_t last = sql.find_first_of(" \t\r\n", first);
  std::string keyword = sql.substr(
      first, last == std::string::npos ? std::string::npos : last - first);
  for (char& ch : keyword) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return keyword;
}

nlohmann::json fieldValueToJson(const FieldValue& value) {
  if (std::holds_alternative<std::monostate>(value)) {
    return nullptr;
  }
  if (std::holds_alternative<Column::IntegerType>(value)) {
    return std::get<Column::IntegerType>(value);
  }
  if (std::holds_alternative<Column::DoubleType>(value)) {
    return std::get<Column::DoubleType>(value);
  }
  return std::get<Column::VarcharType>(value);
}

Schema joinedSchemaForTables(const std::vector<Table>& tables) {
  std::vector<Column> columns;
  for (const Table& table : tables) {
    const auto& table_columns = table.schema().columns();
    columns.insert(columns.end(), table_columns.begin(), table_columns.end());
  }
  return Schema(std::move(columns));
}

std::vector<std::string> selectColumnNames(const SelectParser& parser) {
  const std::vector<std::string> table_names = parser.extractTableNames();
  std::vector<Table> tables;
  tables.reserve(table_names.size());
  for (const std::string& table_name : table_names) {
    tables.push_back(TableMetadataStore::read(table_name).value());
  }

  const Schema joined_schema = joinedSchemaForTables(tables);
  std::vector<std::string> columns;
  const std::vector<UnboundSelectItem> select_items =
      parser.extractSelectItems();
  const std::vector<std::optional<std::string>> aliases =
      parser.extractSelectAliases();
  for (std::size_t idx = 0; idx < select_items.size(); ++idx) {
    if (idx < aliases.size() && aliases[idx].has_value()) {
      columns.push_back(aliases[idx].value());
      continue;
    }

    const UnboundSelectItem& item = select_items[idx];
    if (std::holds_alternative<SelectAllItem>(item)) {
      for (const Column& column : joined_schema.columns()) {
        columns.push_back(column.getName());
      }
      continue;
    }

    if (const auto* column_ref = std::get_if<ColumnRef>(&item)) {
      columns.push_back(column_ref->column_name);
      continue;
    }

    const auto& aggregate = std::get<UnboundAggregateCall>(item);
    std::string name;
    switch (aggregate.function) {
      case AggregateFunction::Count:
        name = "count";
        break;
      case AggregateFunction::Sum:
        name = "sum";
        break;
      case AggregateFunction::Max:
        name = "max";
        break;
    }
    if (const auto* column_ref = std::get_if<ColumnRef>(&aggregate.argument)) {
      name += "(" + column_ref->column_name + ")";
    } else {
      name += "(*)";
    }
    columns.push_back(name);
  }
  return columns;
}

void recvAll(int fd, void* buffer, size_t length) {
  char* p = static_cast<char*>(buffer);
  size_t received = 0;
  while (received < length) {
    ssize_t byte_read = ::recv(fd, p + received, length - received, 0);
    if (byte_read == 0) {
      throw std::runtime_error("client closed connection");
    }
    if (byte_read < 0) {
      throw std::runtime_error(std::string("recv failed: ") +
                               std::strerror(errno));
    }
    received += static_cast<size_t>(byte_read);
  }
}

void sendAll(int fd, const void* buffer, size_t length) {
  const char* p = static_cast<const char*>(buffer);
  size_t sent = 0;
  while (sent < length) {
    ssize_t byte_sent = ::send(fd, p + sent, length - sent, 0);
    if (byte_sent <= 0) {
      throw std::runtime_error(std::string("send failed: ") +
                               std::strerror(errno));
    }
    sent += static_cast<size_t>(byte_sent);
  }
}

}  // namespace

struct Server::ParserCache {
  ParsedStatementCache<SelectParser> select;
  ParsedStatementCache<InsertParser> insert;
  ParsedStatementCache<UpdateParser> update;
  ParsedStatementCache<DeleteParser> remove;
  ParsedStatementCache<CreateTableParser> create_table;
  ParsedStatementCache<CreateIndexParser> create_index;
  ParsedStatementCache<DropTableParser> drop_table;
};

Server::Server(int port) : port_(port) {
  std::filesystem::create_directories("data");
  const std::string wal_path = "data/server.wal";
  wal_ = std::filesystem::exists(wal_path) ? WAL::openExisting(wal_path)
                                           : WAL::initializeNew(wal_path);
  pool_ = std::make_unique<BufferPool>(*wal_);
  storage_ = std::make_unique<LocalStorageManager>();
  index_accessor_ = std::make_unique<IndexAccessor>(*pool_, *storage_);
  parser_cache_ = std::make_unique<ParserCache>();
  statement_cache_ = std::make_unique<executor::PreparedStatementCache>();
}

Server::~Server() = default;

void Server::start() {
  int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    throw std::runtime_error(std::string("socket failed: ") +
                             std::strerror(errno));
  }

  int opt = 1;
  if (::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) <
      0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("setsockopt failed: ") +
                             std::strerror(errno));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port_));
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("bind failed: ") +
                             std::strerror(errno));
  }

  if (::listen(server_fd, 16) < 0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("listen failed: ") +
                             std::strerror(errno));
  }

  for (;;) {
    int client_fd = ::accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }

    int tcp_no_delay = 1;
    if (::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &tcp_no_delay,
                     sizeof(tcp_no_delay)) < 0) {
      ::close(client_fd);
      throw std::runtime_error(std::string("setsockopt TCP_NODELAY failed: ") +
                               std::strerror(errno));
    }

    std::thread([this, client_fd]() {
      for (;;) {
        std::string request;
        try {
          request = readFrame(client_fd);
          std::string response;
          {
            std::lock_guard<std::mutex> lock(request_mutex_);
            response = handleRequest(request);
          }
          writeFrame(client_fd, response);
        } catch (const std::exception& e) {
          const std::string what = e.what();
          if (what.find("client closed connection") != std::string::npos) {
            dbfs_log::server().debug("client closed connection");
            break;
          }

          // Log request and exception (backtrace) through the logging system
          try {
            if (!request.empty()) {
              const std::size_t maxlen = 2000;
              if (request.size() > maxlen) {
                dbfs_log::server().error(
                    "caught exception processing request (truncated): {}",
                    request.substr(0, maxlen));
              } else {
                dbfs_log::server().error(
                    "caught exception processing request: {}", request);
              }
            } else {
              dbfs_log::server().error(
                  "caught exception processing request: <empty request>");
            }

            dbfs_log::server().error("exception: {}", what);

            // backtrace symbols at debug level
            void* bt[64];
            int bt_size = backtrace(bt, 64);
            char** bt_syms = backtrace_symbols(bt, bt_size);
            if (bt_syms) {
              for (int i = 0; i < bt_size; ++i) {
                dbfs_log::server().debug("backtrace[{}] {}", i, bt_syms[i]);
              }
              free(bt_syms);
            }
          } catch (...) {
            dbfs_log::server().error(
                "failed while attempting to log exception/backtrace");
          }

          nlohmann::json error;
          error["ok"] = false;
          error["sqlState"] = "58000";
          error["errorMessage"] = e.what();
          try {
            writeFrame(client_fd, error.dump());
          } catch (...) {
            break;
          }
        }
      }

      ::close(client_fd);
    }).detach();
  }
}

std::string Server::readFrame(int client_fd) {
  // read 4 byte length prefix
  uint32_t network_length = 0;
  recvAll(client_fd, &network_length, sizeof(network_length));
  uint32_t length = ntohl(network_length);

  // read payload
  std::string payload(length, '\0');
  if (length > 0) {
    recvAll(client_fd, payload.data(), length);
  }
  return payload;
}

void Server::writeFrame(int client_fd, const std::string& payload) {
  uint32_t network_length = htonl(static_cast<uint32_t>(payload.size()));
  std::string frame(sizeof(network_length) + payload.size(), '\0');
  std::memcpy(frame.data(), &network_length, sizeof(network_length));
  std::memcpy(frame.data() + sizeof(network_length), payload.data(),
              payload.size());
  sendAll(client_fd, frame.data(), frame.size());
}

std::string Server::handleRequest(const std::string& request) {
  dbfs_log::server().debug("received request: {}", request);
  nlohmann::json req = nlohmann::json::parse(request);

  const std::string operation = req.value("operation", "");
  nlohmann::json res;
  res["ok"] = true;

  if (operation == "resetBufferPool") {
    const BufferPoolResetResult result = pool_->resetForBenchmark();
    storage_->flush();
    res["residentFramesCleared"] = result.resident_frames_cleared;
    res["dirtyFramesFlushed"] = result.dirty_frames_flushed;
    res["generation"] = result.generation;
    return res.dump();
  }

  const std::string sql_template = req.at("sql").get<std::string>();
  const nlohmann::json parameter_values =
      req.value("parameters", nlohmann::json::array());
  const QueryParameters parameters = queryParametersFromJson(parameter_values);
  const std::string keyword = leadingKeyword(sql_template);

  dbfs_log::server().debug("SQL template: {}", sql_template);

  if (operation == "batchUpdate") {
    const nlohmann::json parameter_sets =
        req.value("parameterSets", nlohmann::json::array());
    if (!parameter_sets.is_array()) {
      res["ok"] = false;
      res["sqlState"] = "22023";
      res["errorMessage"] = "batchUpdate requires parameterSets array";
    } else if (parameter_sets.empty()) {
      res["updateCounts"] = nlohmann::json::array();
      res["updateCount"] = 0;
    } else {
      if (keyword != "INSERT") {
        res["ok"] = false;
        res["sqlState"] = "0A000";
        res["errorMessage"] =
            "batchUpdate currently supports INSERT statements only";
      } else {
        const InsertParser& parser = parser_cache_->insert.get(sql_template);
        nlohmann::json update_counts = nlohmann::json::array();

        // Minimal TPC-C-oriented batch path. This reduces JDBC/server round
        // trips and metadata lookup, but does not provide transactional
        // rollback for partial batch failures yet.
        for (const nlohmann::json& parameter_set : parameter_sets) {
          if (!parameter_set.is_array()) {
            throw std::runtime_error(
                "batchUpdate parameterSets must contain arrays");
          }
          executor::insert(*pool_, *storage_, *index_accessor_,
                           *statement_cache_, parser,
                           queryParametersFromJson(parameter_set));
          update_counts.push_back(1);
        }

        res["updateCounts"] = std::move(update_counts);
        res["updateCount"] = parameter_sets.size();
      }
    }
  } else if (operation == "query") {
    const SelectParser& parser = parser_cache_->select.get(sql_template);
    const std::vector<TypedRow> rows =
        executor::read(*pool_, *storage_, *index_accessor_, *statement_cache_,
                       parser, parameters);
    res["columns"] = selectColumnNames(parser);
    res["rows"] = nlohmann::json::array();
    for (const TypedRow& row : rows) {
      nlohmann::json json_row = nlohmann::json::array();
      for (const FieldValue& value : row.values) {
        json_row.push_back(fieldValueToJson(value));
      }
      res["rows"].push_back(std::move(json_row));
    }
  } else if (keyword == "CREATE") {
    if (sql_template.find("CREATE INDEX") == 0 ||
        sql_template.find("create index") == 0 ||
        sql_template.find("CREATE UNIQUE INDEX") == 0 ||
        sql_template.find("create unique index") == 0) {
      executor::create_index(*storage_, *statement_cache_,
                             parser_cache_->create_index.get(sql_template));
    } else {
      executor::create_table(*storage_, *statement_cache_,
                             parser_cache_->create_table.get(sql_template));
    }
    res["updateCount"] = 0;
  } else if (keyword == "DROP") {
    executor::drop_table(*storage_, *statement_cache_,
                         parser_cache_->drop_table.get(sql_template));
    res["updateCount"] = 0;
  } else if (keyword == "INSERT") {
    const InsertParser& parser = parser_cache_->insert.get(sql_template);
    executor::insert(*pool_, *storage_, *index_accessor_, *statement_cache_,
                     parser, parameters);
    res["updateCount"] = 1;
  } else if (keyword == "UPDATE") {
    const UpdateParser& parser = parser_cache_->update.get(sql_template);
    executor::update(*pool_, *storage_, *index_accessor_, *statement_cache_,
                     parser, parameters);
    res["updateCount"] = 1;
  } else if (keyword == "DELETE") {
    const DeleteParser& parser = parser_cache_->remove.get(sql_template);
    executor::remove(*pool_, *storage_, *index_accessor_, *statement_cache_,
                     parser, parameters);
    res["updateCount"] = 1;
  } else {
    res["ok"] = false;
    res["sqlState"] = "0A000";
    res["errorMessage"] = "unsupported SQL: " + sql_template;
  }

  std::string response = res.dump();
  const std::size_t max_log_len = 2000;
  if (response.size() > max_log_len) {
    dbfs_log::server().debug("response (truncated to {} bytes): {}...",
                             max_log_len, response.substr(0, max_log_len));
  } else {
    dbfs_log::server().debug("response: {}", response);
  }
  return response;
}
