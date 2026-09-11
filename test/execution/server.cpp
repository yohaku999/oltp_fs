#include "server/server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

#include "../test_database.h"
#include "catalog/table.h"
#include "execution/executor.h"
#include "parser/create_table_parser.h"
#include "parser/insert_parser.h"
#include "storage/buffer/bufferpool.h"
#include "storage/index/index_accessor.h"
#include "storage/local_storage_manager.h"
#include "storage/wal/wal.h"

namespace {

using namespace std::chrono_literals;

constexpr int kWarehouseId = 1;
constexpr int kDistrictId = 8;
constexpr int kStockItems = 3000;
constexpr int kOrderCount = 16;
constexpr int kLinesPerOrder = 16;
constexpr int kStockLevelTimeoutMs = 50;
constexpr int kPaymentTimeoutMs = 50;
constexpr int kConcurrentStockLevelTimeoutMs = 500;

class SocketReadTimeout : public std::runtime_error {
 public:
  explicit SocketReadTimeout(const std::string& message)
      : std::runtime_error(message) {}
};

class ScopedCurrentDirectory {
 public:
  explicit ScopedCurrentDirectory(const std::filesystem::path& new_path)
      : old_path_(std::filesystem::current_path()) {
    std::filesystem::current_path(new_path);
  }

  ~ScopedCurrentDirectory() { std::filesystem::current_path(old_path_); }

 private:
  std::filesystem::path old_path_;
};

int pickUnusedPort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "socket failed");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(), "bind failed");
  }

  socklen_t length = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &length) < 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(),
                            "getsockname failed");
  }

  ::close(fd);
  return ntohs(addr.sin_port);
}

void recvAllWithTimeout(int fd, void* buffer, std::size_t length) {
  char* bytes = static_cast<char*>(buffer);
  std::size_t received = 0;
  while (received < length) {
    ssize_t byte_read = ::recv(fd, bytes + received, length - received, 0);
    if (byte_read == 0) {
      throw std::runtime_error("socket closed unexpectedly");
    }
    if (byte_read < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        throw SocketReadTimeout("socket read timed out");
      }
      throw std::system_error(errno, std::generic_category(), "recv failed");
    }
    received += static_cast<std::size_t>(byte_read);
  }
}

nlohmann::json sendJsonRequest(int port, const nlohmann::json& request,
                               int read_timeout_ms) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "socket failed");
  }

  timeval timeout{};
  timeout.tv_sec = read_timeout_ms / 1000;
  timeout.tv_usec = (read_timeout_ms % 1000) * 1000;
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) <
      0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(),
                            "setsockopt failed");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(), "connect failed");
  }

  const std::string payload = request.dump();
  const std::uint32_t payload_size =
      htonl(static_cast<uint32_t>(payload.size()));

  if (::send(fd, &payload_size, sizeof(payload_size), 0) < 0 ||
      ::send(fd, payload.data(), payload.size(), 0) < 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(), "send failed");
  }

  std::uint32_t response_size = 0;
  recvAllWithTimeout(fd, &response_size, sizeof(response_size));
  response_size = ntohl(response_size);

  std::string response(response_size, '\0');
  if (response_size > 0) {
    recvAllWithTimeout(fd, response.data(), response.size());
  }
  ::close(fd);
  return nlohmann::json::parse(response);
}

nlohmann::json sendRequest(int port, const std::string& operation,
                           const std::string& sql,
                           const nlohmann::json& parameters,
                           int read_timeout_ms) {
  nlohmann::json request = {
      {"operation", operation},
      {"database", "benchbase"},
      {"sql", sql},
      {"parameters", parameters},
  };
  return sendJsonRequest(port, request, read_timeout_ms);
}

nlohmann::json sendBatchRequest(int port, const std::string& sql,
                                const nlohmann::json& parameter_sets,
                                int read_timeout_ms) {
  nlohmann::json request = {
      {"operation", "batchUpdate"},
      {"database", "benchbase"},
      {"sql", sql},
      {"parameterSets", parameter_sets},
  };
  return sendJsonRequest(port, request, read_timeout_ms);
}

void waitForServerReady(int port) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(static_cast<uint16_t>(port));
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) ==
          0) {
        ::close(fd);
        return;
      }
      ::close(fd);
    }
    std::this_thread::sleep_for(20ms);
  }

  throw std::runtime_error("server did not become ready");
}

void forceDataPagesToDisk(BufferPool& pool, LocalStorageManager& storage,
                          IndexAccessor& index_accessor, WAL& wal) {
  executor::create_table(
      storage, CreateTableParser(
                   "CREATE TABLE evictor (id int NOT NULL, note varchar)"));
  Table evictor = test_db::getTable("evictor");
  const std::string payload(256, 'x');
  wal.flush();
  for (int row_id = 0; row_id < 400; ++row_id) {
    executor::insert(
        pool, storage, index_accessor, evictor,
        InsertParser("INSERT INTO evictor VALUES (" + std::to_string(row_id) +
                     ", '" + payload + "')"));
  }
  wal.flush();
}

void prepareTpccLikeDataset(const std::filesystem::path& temp_dir) {
  ScopedCurrentDirectory cwd(temp_dir);
  std::filesystem::create_directories("data");
  std::remove("server_setup.wal");

  auto wal = WAL::initializeNew("server_setup.wal");
  auto pool = std::make_unique<BufferPool>(*wal);
  auto storage = std::make_unique<LocalStorageManager>();
  auto index_accessor = std::make_unique<IndexAccessor>(*pool, *storage);

  executor::create_table(*storage,
                         CreateTableParser("CREATE TABLE warehouse ("
                                           "w_id int NOT NULL, "
                                           "w_ytd decimal(12, 2) NOT NULL, "
                                           "PRIMARY KEY (w_id))"));
  executor::create_table(*storage,
                         CreateTableParser("CREATE TABLE stock ("
                                           "s_i_id int NOT NULL, "
                                           "s_w_id int NOT NULL, "
                                           "s_quantity int NOT NULL, "
                                           "PRIMARY KEY (s_i_id, s_w_id))"));
  executor::create_table(
      *storage,
      CreateTableParser("CREATE TABLE order_line ("
                        "ol_o_id int NOT NULL, "
                        "ol_d_id int NOT NULL, "
                        "ol_w_id int NOT NULL, "
                        "ol_number int NOT NULL, "
                        "ol_i_id int NOT NULL, "
                        "PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number))"));

  Table warehouse = test_db::getTable("warehouse");
  Table stock = test_db::getTable("stock");
  Table order_line = test_db::getTable("order_line");

  executor::insert(*pool, *storage, *index_accessor, warehouse,
                   InsertParser("INSERT INTO warehouse VALUES (1, 300000.0)"));

  for (int item_id = 1; item_id <= kStockItems; ++item_id) {
    const int quantity = (item_id % 20) + 1;
    executor::insert(
        *pool, *storage, *index_accessor, stock,
        InsertParser("INSERT INTO stock VALUES (" + std::to_string(item_id) +
                     ", 1, " + std::to_string(quantity) + ")"));
  }

  const int first_order_id = 3001 - kOrderCount;
  for (int order_id = first_order_id; order_id < 3001; ++order_id) {
    for (int line_number = 1; line_number <= kLinesPerOrder; ++line_number) {
      const int item_id =
          ((order_id - first_order_id) * kLinesPerOrder + line_number) %
              kStockItems +
          1;
      executor::insert(*pool, *storage, *index_accessor, order_line,
                       InsertParser("INSERT INTO order_line VALUES (" +
                                    std::to_string(order_id) + ", " +
                                    std::to_string(kDistrictId) + ", 1, " +
                                    std::to_string(line_number) + ", " +
                                    std::to_string(item_id) + ")"));
    }
  }

  forceDataPagesToDisk(*pool, *storage, *index_accessor, *wal);
  index_accessor.reset();
  storage.reset();
  pool.reset();
  wal.reset();
}

pid_t startServerProcess(const std::filesystem::path& temp_dir, int port) {
  pid_t child_pid = ::fork();
  if (child_pid < 0) {
    throw std::system_error(errno, std::generic_category(), "fork failed");
  }
  if (child_pid == 0) {
    if (::chdir(temp_dir.c_str()) != 0) {
      _Exit(2);
    }
    try {
      Server server(port);
      server.start();
    } catch (...) {
      _Exit(3);
    }
    _Exit(0);
  }
  return child_pid;
}

void stopServerProcess(pid_t child_pid) {
  if (child_pid <= 0) {
    return;
  }
  ::kill(child_pid, SIGKILL);
  int status = 0;
  ::waitpid(child_pid, &status, 0);
}

class ServerTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    temp_dir_ = std::filesystem::temp_directory_path() /
                ("dbfs_server_test_" + std::to_string(getpid()));
    std::filesystem::remove_all(temp_dir_);
    std::filesystem::create_directories(temp_dir_);
    prepareTpccLikeDataset(temp_dir_);
  }

  static void TearDownTestSuite() { std::filesystem::remove_all(temp_dir_); }

  void SetUp() override {
    port_ = pickUnusedPort();
    child_pid_ = startServerProcess(temp_dir_, port_);
    waitForServerReady(port_);
  }

  void TearDown() override {
    stopServerProcess(child_pid_);
    child_pid_ = -1;
  }

  static std::filesystem::path temp_dir_;
  int port_ = -1;
  pid_t child_pid_ = -1;
};

std::filesystem::path ServerTest::temp_dir_;

}  // namespace

TEST_F(ServerTest, BatchUpdateInsertsPreparedStatementRows) {
  nlohmann::json create_response =
      sendRequest(port_, "update",
                  "CREATE TABLE batch_probe (id int NOT NULL, value varchar, "
                  "quantity int, "
                  "PRIMARY KEY (id))",
                  nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(create_response.value("ok", false));

  nlohmann::json batch_response = sendBatchRequest(
      port_, "INSERT INTO batch_probe VALUES (?, ?, ?)",
      nlohmann::json::array({nlohmann::json::array({1, "a", 10}),
                             nlohmann::json::array({2, "b", 20}),
                             nlohmann::json::array({3, "c", 30})}),
      kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(batch_response.value("ok", false));
  EXPECT_EQ(3, batch_response.at("updateCount").get<int>());
  ASSERT_TRUE(batch_response.at("updateCounts").is_array());
  EXPECT_EQ(3, batch_response.at("updateCounts").size());
  EXPECT_EQ(1, batch_response.at("updateCounts").at(0).get<int>());
  EXPECT_EQ(1, batch_response.at("updateCounts").at(1).get<int>());
  EXPECT_EQ(1, batch_response.at("updateCounts").at(2).get<int>());

  nlohmann::json query_response =
      sendRequest(port_, "query", "SELECT value FROM batch_probe WHERE id = ?",
                  nlohmann::json::array({2}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(query_response.value("ok", false));
  ASSERT_EQ(1, query_response.at("rows").size());
  ASSERT_EQ(1, query_response.at("rows").at(0).size());
  EXPECT_EQ("b", query_response.at("rows").at(0).at(0).get<std::string>());

  query_response =
      sendRequest(port_, "query", "SELECT value FROM batch_probe WHERE id = ?",
                  nlohmann::json::array({3}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(query_response.value("ok", false));
  ASSERT_EQ(1, query_response.at("rows").size());
  EXPECT_EQ("c", query_response.at("rows").at(0).at(0).get<std::string>());

  nlohmann::json update_response = sendRequest(
      port_, "update",
      "UPDATE batch_probe SET value = ?, quantity = quantity + ? WHERE id = ?",
      nlohmann::json::array({"updated", 5, 2}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(update_response.value("ok", false));

  query_response = sendRequest(
      port_, "query", "SELECT value, quantity FROM batch_probe WHERE id = ?",
      nlohmann::json::array({2}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(query_response.value("ok", false));
  ASSERT_EQ(1, query_response.at("rows").size());
  EXPECT_EQ("updated",
            query_response.at("rows").at(0).at(0).get<std::string>());
  EXPECT_EQ(25, query_response.at("rows").at(0).at(1).get<int>());

  nlohmann::json delete_response =
      sendRequest(port_, "update", "DELETE FROM batch_probe WHERE id = ?",
                  nlohmann::json::array({1}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(delete_response.value("ok", false));

  query_response = sendRequest(
      port_, "query", "SELECT id FROM batch_probe ORDER BY id LIMIT ?",
      nlohmann::json::array({1}), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(query_response.value("ok", false));
  ASSERT_EQ(1, query_response.at("rows").size());
  EXPECT_EQ(2, query_response.at("rows").at(0).at(0).get<int>());
}

TEST_F(ServerTest, CreatesUniqueSecondaryIndex) {
  nlohmann::json create_response = sendRequest(
      port_, "update",
      "CREATE TABLE unique_index_probe (id int PRIMARY KEY, email varchar)",
      nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(create_response.value("ok", false));

  nlohmann::json index_response =
      sendRequest(port_, "update",
                  "CREATE UNIQUE INDEX idx_unique_index_probe_email ON "
                  "unique_index_probe (email)",
                  nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(index_response.value("ok", false));

  nlohmann::json first_insert = sendRequest(
      port_, "update",
      "INSERT INTO unique_index_probe VALUES (1, 'one@example.com')",
      nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(first_insert.value("ok", false));

  nlohmann::json duplicate_insert = sendRequest(
      port_, "update",
      "INSERT INTO unique_index_probe VALUES (2, 'one@example.com')",
      nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  EXPECT_FALSE(duplicate_insert.value("ok", false));
}

TEST_F(ServerTest, ResetBufferPoolFlushesDataWithoutSqlPayload) {
  nlohmann::json create_response =
      sendRequest(port_, "update",
                  "CREATE TABLE reset_probe (id int NOT NULL, value varchar, "
                  "PRIMARY KEY (id))",
                  nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(create_response.value("ok", false));

  nlohmann::json insert_response = sendRequest(
      port_, "update", "INSERT INTO reset_probe VALUES (1, 'persisted')",
      nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(insert_response.value("ok", false));

  nlohmann::json reset_response =
      sendJsonRequest(port_, {{"operation", "resetBufferPool"}},
                      kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(reset_response.value("ok", false));
  EXPECT_GT(reset_response.at("residentFramesCleared").get<int>(), 0);
  EXPECT_GT(reset_response.at("dirtyFramesFlushed").get<int>(), 0);
  EXPECT_EQ(1, reset_response.at("generation").get<int>());

  nlohmann::json query_response =
      sendRequest(port_, "query", "SELECT value FROM reset_probe WHERE id = 1",
                  nlohmann::json::array(), kConcurrentStockLevelTimeoutMs);
  ASSERT_TRUE(query_response.value("ok", false));
  ASSERT_EQ(1, query_response.at("rows").size());
  EXPECT_EQ("persisted",
            query_response.at("rows").at(0).at(0).get<std::string>());
}
