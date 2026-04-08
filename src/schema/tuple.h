#pragma once

#include <stdexcept>
#include <string>

#include "../storage/record_cell.h"
#include "column.h"
#include "schema.h"

class Tuple {
 public:
  static std::string getValuesAsString(const char* cell_start,
                                       const Schema& schema,
                                       const std::string& column_name) {
    RecordCellView view(cell_start);
    TypedRow row = view.getTypedRow(schema);

    // get column from schema.
    for (std::size_t index = 0; index < schema.columns_.size(); ++index) {
      const auto& column = schema.columns_[index];
      if (column.getName() == column_name) {
        const auto& value = row.values[index];
        if (std::holds_alternative<std::monostate>(value)) {
          return "NULL";
        }
        if (std::holds_alternative<Column::IntegerType>(value)) {
          return std::to_string(std::get<Column::IntegerType>(value));
        }
        if (std::holds_alternative<Column::VarcharType>(value)) {
          return std::get<Column::VarcharType>(value);
        }
        throw std::runtime_error("Unsupported FieldValue in Tuple.");
      }
    }

    throw std::runtime_error("Column not found: " + column_name);
  }
};