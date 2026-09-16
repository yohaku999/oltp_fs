#pragma once

#include <memory>
#include <mutex>
#include <string>

class BufferPool;
class IndexAccessor;
class LocalStorageManager;
class WAL;

namespace executor {
class PreparedStatementCache;
}

class Server {
 public:
  explicit Server(int port = 25432);
  ~Server();
  void start();

 private:
  struct ParserCache;

  int port_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<LocalStorageManager> storage_;
  std::unique_ptr<IndexAccessor> index_accessor_;
  std::unique_ptr<ParserCache> parser_cache_;
  std::unique_ptr<executor::PreparedStatementCache> statement_cache_;
  std::mutex request_mutex_;

  std::string readFrame(int client_fd);
  void writeFrame(int client_fd, const std::string& response);
  std::string handleRequest(const std::string& request);
};
