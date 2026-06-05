#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <execinfo.h>
#include <cstdlib>
#include <iostream>
#include "logging.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "catalog/table.h"
#include "execution/executor.h"
#include "execution/parsers/create_index_parser.h"
#include "execution/parsers/create_table_parser.h"
#include "execution/parsers/delete_parser.h"
#include "execution/parsers/drop_table_parser.h"
#include "execution/parsers/insert_parser.h"
#include "execution/parsers/select_parser.h"
#include "execution/parsers/update_parser.h"
#include "execution/select_item.h"
#include "storage/buffer/bufferpool.h"
#include "storage/wal/wal.h"

namespace {

std::string quoteSqlString(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (char ch : value) {
    escaped.push_back(ch);
    if (ch == '\'') {
      escaped.push_back('\'');
    }
  }
  return "'" + escaped + "'";
}

std::string renderJsonLiteral(const nlohmann::json& value) {
  if (value.is_null()) {
    return "NULL";
  }
  if (value.is_string()) {
    return quoteSqlString(value.get<std::string>());
  }
  if (value.is_boolean()) {
    return value.get<bool>() ? "TRUE" : "FALSE";
  }
  if (value.is_number_integer() || value.is_number_unsigned() || value.is_number_float()) {
    return value.dump();
  }
  throw std::runtime_error("Unsupported JSON parameter type.");
}

std::string renderSqlWithParameters(const std::string& sql,
                                    const nlohmann::json& parameters) {
  if (!parameters.is_array() || parameters.empty()) {
    return sql;
  }

  std::string rendered;
  rendered.reserve(sql.size() + parameters.size() * 8);
  std::size_t parameter_index = 0;
  bool in_single_quoted_string = false;

  for (std::size_t index = 0; index < sql.size(); ++index) {
    const char current = sql[index];
    if (current == '\'') {
      rendered.push_back(current);
      if (in_single_quoted_string && index + 1 < sql.size() && sql[index + 1] == '\'') {
        rendered.push_back(sql[index + 1]);
        ++index;
        continue;
      }
      in_single_quoted_string = !in_single_quoted_string;
      continue;
    }

    if (current == '?' && !in_single_quoted_string && parameter_index < parameters.size()) {
      rendered += renderJsonLiteral(parameters[parameter_index]);
      ++parameter_index;
      continue;
    }

    rendered.push_back(current);
  }

  return rendered;
}

std::string leadingKeyword(const std::string& sql) {
  const std::size_t first = sql.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }

  const std::size_t last = sql.find_first_of(" \t\r\n", first);
  std::string keyword = sql.substr(first, last == std::string::npos ? std::string::npos : last - first);
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
    tables.push_back(Table::getTable(table_name));
  }

  const Schema joined_schema = joinedSchemaForTables(tables);
  std::vector<std::string> columns;
  const std::vector<UnboundSelectItem> select_items = parser.extractSelectItems();
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
    std::string name = aggregate.function == AggregateFunction::Count ? "count" : "sum";
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
      throw std::runtime_error(std::string("recv failed: ") + std::strerror(errno));
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
      throw std::runtime_error(std::string("send failed: ") + std::strerror(errno));
    }
    sent += static_cast<size_t>(byte_sent);
  }
}

}  // namespace

Server::Server(int port)
    : port_(port) {
  std::filesystem::create_directories("data");
  const std::string wal_path = "data/server.wal";
  wal_ = std::filesystem::exists(wal_path)
             ? WAL::openExisting(wal_path)
             : WAL::initializeNew(wal_path);
  pool_ = std::make_unique<BufferPool>(*wal_);
}

Server::~Server() = default;

void Server::start() {
  int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    throw std::runtime_error(std::string("socket failed: ") + std::strerror(errno));
  }

  int opt = 1;
  if (::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("setsockopt failed: ") + std::strerror(errno));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port_));
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("bind failed: ") + std::strerror(errno));
  }

  if (::listen(server_fd, 16) < 0) {
    ::close(server_fd);
    throw std::runtime_error(std::string("listen failed: ") + std::strerror(errno));
  }

  for (;;) {
    int client_fd = ::accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }

    std::string request;
    try {
      request = readFrame(client_fd);
      std::string response = handleRequest(request);
      writeFrame(client_fd, response);
    } catch (const std::exception& e) {
      // Log request and exception (backtrace) through the logging system
      try {
        if (!request.empty()) {
          const std::size_t maxlen = 2000;
          if (request.size() > maxlen) {
            dbfs_log::server().error("caught exception processing request (truncated): {}", request.substr(0, maxlen));
          } else {
            dbfs_log::server().error("caught exception processing request: {}", request);
          }
        } else {
          dbfs_log::server().error("caught exception processing request: <empty request>");
        }

        std::string what = e.what();
        if (what.find("client closed connection") != std::string::npos) {
          dbfs_log::server().debug("client closed connection while processing request");
        } else {
          dbfs_log::server().error("exception: {}", what);
        }

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
        dbfs_log::server().error("failed while attempting to log exception/backtrace");
      }

      nlohmann::json error;
      error["ok"] = false;
      error["sqlState"] = "58000";
      error["errorMessage"] = e.what();
      try {
        writeFrame(client_fd, error.dump());
      } catch (...) {
      }
    }

    ::close(client_fd);
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
  sendAll(client_fd, &network_length, sizeof(network_length));
  if (!payload.empty()) {
    sendAll(client_fd, payload.data(), payload.size());
  }
}

std::string Server::handleRequest(const std::string& request) {
  dbfs_log::server().info("received request: {}", request);
  nlohmann::json req = nlohmann::json::parse(request);

  const std::string operation = req.value("operation", "");
  const std::string sql = renderSqlWithParameters(
      req.at("sql").get<std::string>(),
      req.value("parameters", nlohmann::json::array()));
  const nlohmann::json parameters = req.value("parameters", nlohmann::json::array());

  dbfs_log::server().debug("parsed SQL: {}", sql);
  nlohmann::json res;
  res["ok"] = true;

  if (operation == "query" || leadingKeyword(sql) == "SELECT") {
    SelectParser parser(sql);
    const std::vector<TypedRow> rows = executor::read(*pool_, parser);
    res["columns"] = selectColumnNames(parser);
    res["rows"] = nlohmann::json::array();
    for (const TypedRow& row : rows) {
      nlohmann::json json_row = nlohmann::json::array();
      for (const FieldValue& value : row.values) {
        json_row.push_back(fieldValueToJson(value));
      }
      res["rows"].push_back(std::move(json_row));
    }
  } else if (leadingKeyword(sql) == "CREATE") {
    if (sql.find("CREATE INDEX") == 0 || sql.find("create index") == 0) {
      executor::create_index(CreateIndexParser(sql));
    } else {
      executor::create_table(CreateTableParser(sql));
    }
    res["updateCount"] = 0;
  } else if (leadingKeyword(sql) == "DROP") {
    executor::drop_table(DropTableParser(sql));
    res["updateCount"] = 0;
  } else if (leadingKeyword(sql) == "INSERT") {
    InsertParser parser(sql);
    Table table = Table::getTable(parser.extractTableName());
    executor::insert(*pool_, table, parser, *wal_);
    res["updateCount"] = 1;
  } else if (leadingKeyword(sql) == "UPDATE") {
    UpdateParser parser(sql);
    Table table = Table::getTable(parser.extractTableName());
    executor::update(*pool_, table, parser, *wal_);
    res["updateCount"] = 1;
  } else if (leadingKeyword(sql) == "DELETE") {
    DeleteParser parser(sql);
    Table table = Table::getTable(parser.extractTableName());
    executor::remove(*pool_, table, parser, *wal_);
    res["updateCount"] = 1;
  } else {
    res["ok"] = false;
    res["sqlState"] = "0A000";
    res["errorMessage"] = "unsupported SQL: " + sql;
  }

  std::string response = res.dump();
  const std::size_t max_log_len = 2000;
  if (response.size() > max_log_len) {
    dbfs_log::server().info("response (truncated to {} bytes): {}...", max_log_len,
             response.substr(0, max_log_len));
  } else {
    dbfs_log::server().info("response: {}", response);
  }
  return response;
}
