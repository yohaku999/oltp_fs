#pragma once

#include <optional>
#include <string>

#include "catalog/table.h"

class TableRepository {
 public:
  std::optional<Table> find(const std::string& table_name) const;
  Table get(const std::string& table_name) const;
  void insert(const Table& table);
  void update(const Table& table);
  void erase(const std::string& table_name);
};
