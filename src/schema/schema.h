#pragma once

#include <stdexcept>
#include <vector>

#include "column.h"

class Schema {
 public:
  explicit Schema(std::vector<Column> columns)
    : columns_(std::move(columns)) {}

  const std::vector<Column>& columns() const { return columns_; }

  int getColumnIndex(const std::string& column_name) const {
    for (std::size_t index = 0; index < columns_.size(); ++index) {
      if (columns_[index].getName() == column_name) {
        return static_cast<int>(index);
      }
    }
    return -1;
  }

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

  std::vector<Column> columns_;
};
