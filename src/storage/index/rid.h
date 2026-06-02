#pragma once

#include <cstdint>
#include <string>

struct RID {
  uint16_t heap_page_id;
  uint16_t slot_id;
};

struct IndexEntry {
  std::string key;
  RID rid;
};
