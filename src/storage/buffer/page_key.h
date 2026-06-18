#pragma once

#include <functional>
#include <string>

struct PageKey {
  std::string file_identifier;
  int page_id;

  bool operator==(const PageKey& other) const {
    return file_identifier == other.file_identifier &&
           page_id == other.page_id;
  }
};

struct PageKeyHash {
  size_t operator()(const PageKey& key) const {
    const size_t file_hash =
        std::hash<std::string>{}(key.file_identifier);
    const size_t page_hash = std::hash<int>{}(key.page_id);
    return file_hash ^ (page_hash << 1);
  }
};