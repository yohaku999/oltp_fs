#include <arpa/inet.h>
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
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "catalog/table.h"
#include "execution/executor.h"
#include "execution/parsers/create_table_parser.h"
#include "execution/parsers/insert_parser.h"
#include "server/server.h"
#include "storage/runtime/bufferpool.h"
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
    throw std::system_error(error, std::generic_category(), "getsockname failed");
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

nlohmann::json sendRequest(int port, const std::string& operation,
                           const std::string& sql,
                           const nlohmann::json& parameters,
                           int read_timeout_ms) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "socket failed");
  }

  timeval timeout{};
  timeout.tv_sec = read_timeout_ms / 1000;
  timeout.tv_usec = (read_timeout_ms % 1000) * 1000;
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category(), "setsockopt failed");
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

  nlohmann::json request = {
      {"operation", operation},
      {"database", "benchbase"},
      {"sql", sql},
      {"parameters", parameters},
  };
  const std::string payload = request.dump();
  const std::uint32_t payload_size = htonl(static_cast<uint32_t>(payload.size()));

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

void waitForServerReady(int port) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(static_cast<uint16_t>(port));
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        ::close(fd);
        return;
      }
      ::close(fd);
    }
    std::this_thread::sleep_for(20ms);
  }

  throw std::runtime_error("server did not become ready");
}

void forceDataPagesToDisk(BufferPool& pool, WAL& wal) {
  executor::create_table(CreateTableParser(
      "CREATE TABLE evictor (id int NOT NULL, note varchar)"));
  Table evictor = Table::getTable("evictor");
  const std::string payload(256, 'x');
  wal.flush();
  for (int row_id = 0; row_id < 400; ++row_id) {
    executor::insert(
        pool, evictor,
        InsertParser("INSERT INTO evictor VALUES (" + std::to_string(row_id) +
                     ", '" + payload + "')"),
        wal);
  }
  wal.flush();
}

void prepareTpccLikeDataset(const std::filesystem::path& temp_dir) {
  ScopedCurrentDirectory cwd(temp_dir);
  std::filesystem::create_directories("data");
  std::remove("server_setup.wal");

  auto wal = WAL::initializeNew("server_setup.wal");
  auto pool = std::make_unique<BufferPool>(*wal);

  executor::create_table(CreateTableParser(
      "CREATE TABLE warehouse ("
      "w_id int NOT NULL, "
      "w_ytd decimal(12, 2) NOT NULL, "
      "PRIMARY KEY (w_id))"));
  executor::create_table(CreateTableParser(
      "CREATE TABLE stock ("
      "s_i_id int NOT NULL, "
      "s_w_id int NOT NULL, "
      "s_quantity int NOT NULL, "
      "PRIMARY KEY (s_i_id, s_w_id))"));
  executor::create_table(CreateTableParser(
      "CREATE TABLE order_line ("
      "ol_o_id int NOT NULL, "
      "ol_d_id int NOT NULL, "
      "ol_w_id int NOT NULL, "
      "ol_number int NOT NULL, "
      "ol_i_id int NOT NULL, "
      "PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number))"));

  Table warehouse = Table::getTable("warehouse");
  Table stock = Table::getTable("stock");
  Table order_line = Table::getTable("order_line");

  executor::insert(*pool, warehouse,
                   InsertParser("INSERT INTO warehouse VALUES (1, 300000.0)"),
                   *wal);

  for (int item_id = 1; item_id <= kStockItems; ++item_id) {
    const int quantity = (item_id % 20) + 1;
    executor::insert(
        *pool, stock,
        InsertParser("INSERT INTO stock VALUES (" + std::to_string(item_id) +
                     ", 1, " + std::to_string(quantity) + ")"),
        *wal);
  }

  const int first_order_id = 3001 - kOrderCount;
  for (int order_id = first_order_id; order_id < 3001; ++order_id) {
    for (int line_number = 1; line_number <= kLinesPerOrder; ++line_number) {
      const int item_id = ((order_id - first_order_id) * kLinesPerOrder +
                           line_number) % kStockItems + 1;
      executor::insert(
          *pool, order_line,
          InsertParser("INSERT INTO order_line VALUES (" +
                       std::to_string(order_id) + ", " +
                       std::to_string(kDistrictId) + ", 1, " +
                       std::to_string(line_number) + ", " +
                       std::to_string(item_id) + ")"),
          *wal);
    }
  }

  forceDataPagesToDisk(*pool, *wal);
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

  static void TearDownTestSuite() {
    std::filesystem::remove_all(temp_dir_);
  }

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

TEST_F(ServerTest, StockLevelQueryHitsReadTimeoutOnBenchmarkLikeData) {
  EXPECT_THROW(
      (void)sendRequest(
          port_, "query",
          "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM order_line, stock "
          "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? "
          "AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?",
          nlohmann::json::array({kWarehouseId, kDistrictId, 3001,
                                 3001 - kOrderCount, kWarehouseId, 11}),
          kStockLevelTimeoutMs),
      SocketReadTimeout);
}

TEST_F(ServerTest, PaymentUpdateHitsReadTimeoutWhileStockLevelBlocksServer) {
  std::thread stock_level_request([&]() {
    EXPECT_THROW(
        (void)sendRequest(
            port_, "query",
            "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM order_line, stock "
            "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? "
            "AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?",
            nlohmann::json::array({kWarehouseId, kDistrictId, 3001,
                                   3001 - kOrderCount, kWarehouseId, 11}),
            kConcurrentStockLevelTimeoutMs),
        SocketReadTimeout);
  });

  std::this_thread::sleep_for(20ms);

  EXPECT_THROW(
      (void)sendRequest(
          port_, "update",
          "UPDATE warehouse SET W_YTD = W_YTD + ? WHERE W_ID = ?",
          nlohmann::json::array({4619.22998046875, kWarehouseId}),
          kPaymentTimeoutMs),
      SocketReadTimeout);

  stock_level_request.join();
}

}  // namespace