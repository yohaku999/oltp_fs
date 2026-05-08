#pragma once

#include <memory>
#include <string>

class BufferPool;
class WAL;

class Server {
 public:
  explicit Server(int port = 25432);
  ~Server();
  void start();

 private:
  int port_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<BufferPool> pool_;

  std::string readFrame(int client_fd);
  void writeFrame(int client_fd, const std::string& response);
  std::string handleRequest(const std::string& request);
};