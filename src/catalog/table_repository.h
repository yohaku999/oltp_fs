#pragma once

#include <string>

#include "catalog/table.h"

class TableRepository {
 public:
  Table get(const std::string& table_name) const;
  void insert(const Table& table);
  void update(const Table& table);
  void erase(const std::string& table_name);
};
