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

  std::size_t getFixedColumnsPrefixSize(int target_fixed_column_index) const {
    if (target_fixed_column_index <= 0) {
      return 0;
    }

    std::size_t prefix_size = 0;
    int seen_fixed_column_count = 0;
    for (const auto& column : columns_) {
      if (!column.isFixedLength()) {
        continue;
      }
      prefix_size += column.size();
      ++seen_fixed_column_count;
      if (seen_fixed_column_count >= target_fixed_column_index) {
        break;
      }
    }
    return prefix_size;
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
