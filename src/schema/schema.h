#pragma once
#include <stdexcept>
#include <string>
#include <vector>

#include "column.h"

class Schema {
 public:
  Schema(std::string name, std::vector<Column> columns)
      : name_(std::move(name)), columns_(std::move(columns)) {}
  std::vector<Column> columns_;

  const std::string& name() const { return name_; }

  const std::vector<Column>& columns() const { return columns_; }

  int getFixedColumnIndex(const std::string& column_name) const {
    int fixed_index = 0;
    for (const auto& column : columns_) {
      if (column.isFixedLength()) {
        if (column.getName() == column_name) {
          return fixed_index;
        }
        ++fixed_index;
      }
    }
    return -1;
  }

  int getVariableColumnIndex(const std::string& column_name) const {
    int var_index = 0;
    for (const auto& column : columns_) {
      if (!column.isFixedLength()) {
        if (column.getName() == column_name) {
          return var_index;
        }
        ++var_index;
      }
    }
    return -1;
  }

  int getVariableColumnCount() const {
    int count = 0;
    for (const auto& column : columns_) {
      if (!column.isFixedLength()) {
        ++count;
      }
    }
    return count;
  }

  // Sum of sizes of fixed-length columns from index 0 up to (but excluding)
  // fixed_index.
  std::size_t getFixedColumnsPrefixSize(int fixed_index) const {
    if (fixed_index <= 0) {
      return 0;
    }

    std::size_t total_size = 0;
    int current_fixed_index = 0;
    for (const auto& column : columns_) {
      if (!column.isFixedLength()) {
        continue;
      }
      total_size += column.size();
      ++current_fixed_index;
      if (current_fixed_index >= fixed_index) {
        break;
      }
    }
    return total_size;
  }

 private:
  std::string name_;
  const Column& getColumnByName(const std::string& column_name) const {
    for (const auto& column : columns_) {
      if (column.getName() == column_name) {
        return column;
      }
    }
    throw std::runtime_error("Column not found: " + column_name);
  }
};
