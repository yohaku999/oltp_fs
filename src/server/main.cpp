#include <cstdlib>
#include <string>

#include "server/server.h"

int main() {
  const char* port_text = std::getenv("DBFS_PORT");
  Server server(port_text == nullptr ? 25432 : std::stoi(port_text));
  server.start();
  return 0;
}