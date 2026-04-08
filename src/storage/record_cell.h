#pragma once
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../schema/schema.h"
#include "../schema/typed_row.h"
#include "cell.h"

/**
 * Heap record binary layout.
 *
 * Serialization:
 *  - byte[0]: per-cell flags (bit0 = invalid)
 *  - byte[1..2]: variable-length payload begin offset(y)
 *  - byte[3..6]: NULL bit map
 *  - byte[7..y-1]: fixed-length payload bytes
 *  - byte[y..]: variable-length payload bytes
 *    - variable-column end offset table (uint16_t * var_count)
 *    - variable-length payload bytes
 */

/**
 * Read/write responsibilities are intentionally split:
 * - RecordCellView is a non-owning view over heap-record bytes already stored
 * in a page
 * - RecordSerializer owns temporary bytes while serializing a record for
 * insertion
 * - column-level interpretation belongs to Tuple/Schema, not this file
 */

struct RecordCellLayout {
  static constexpr uint16_t HEADER_SIZE_BYTE =
      Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) + sizeof(uint32_t);
};

class RecordCellView {
 public:
  explicit RecordCellView(const char* cell_start) : cell_start_(cell_start) {}

  const char* cellStart() const { return cell_start_; }

  bool isNull(int x) const {
    uint32_t null_bitmap = readValue<uint32_t>(
        cell_start_ + Cell::FLAG_FIELD_SIZE + sizeof(uint16_t));
    return (null_bitmap & (1u << x)) != 0;
  }

  const char* getFixedPayloadBegin() const {
    return cell_start_ + RecordCellLayout::HEADER_SIZE_BYTE;
  }

  const char* getVariableLengthPayloadBegin() const {
    return cell_start_ +
           readValue<uint16_t>(cell_start_ + Cell::FLAG_FIELD_SIZE);
  }

  std::pair<const char*, uint16_t> getXthVariableColumnbegin(
      int x, int variable_column_count) const {
    const char* variable_payload_begin = getVariableLengthPayloadBegin();
    const char* value_bytes_begin =
        variable_payload_begin + sizeof(uint16_t) * variable_column_count;

    uint16_t column_begin_offset = 0;
    if (x > 0) {
      const char* previous_end_offset_entry =
          variable_payload_begin + sizeof(uint16_t) * (x - 1);
      column_begin_offset = readValue<uint16_t>(previous_end_offset_entry);
    }

    const char* offset_entry = variable_payload_begin + sizeof(uint16_t) * x;
    uint16_t column_end_offset = readValue<uint16_t>(offset_entry);

    return {value_bytes_begin + column_begin_offset,
            static_cast<uint16_t>(column_end_offset - column_begin_offset)};
  }

  TypedRow getTypedRow(const Schema& schema) const {
    TypedRow row;
    row.values.reserve(schema.columns_.size());

    const char* fixed_payload_ptr = getFixedPayloadBegin();
    int variable_column_index = 0;

    for (std::size_t column_index = 0; column_index < schema.columns_.size();
         ++column_index) {
      const auto& column = schema.columns_[column_index];
      if (isNull(static_cast<int>(column_index))) {
        row.values.emplace_back(std::monostate{});
        if (column.isFixedLength()) {
          fixed_payload_ptr += column.size();
        } else {
          ++variable_column_index;
        }
        continue;
      }

      if (column.isFixedLength()) {
        switch (column.getType()) {
          case Column::Type::Integer: {
            row.values.emplace_back(
                readValue<Column::IntegerType>(fixed_payload_ptr));
            fixed_payload_ptr += column.size();
            break;
          }
          case Column::Type::Varchar:
            throw std::runtime_error(
                "Unsupported fixed-length column type in RecordCellView.");
        }
      } else {
        const auto [value_ptr, value_size] = getXthVariableColumnbegin(
            variable_column_index, schema.getVariableColumnCount());
        switch (column.getType()) {
          case Column::Type::Varchar:
            row.values.emplace_back(std::string(value_ptr, value_size));
            ++variable_column_index;
            break;
          case Column::Type::Integer:
            throw std::runtime_error(
                "Unsupported variable-length column type in RecordCellView.");
        }
      }
    }

    return row;
  }

 private:
  const char* cell_start_;
};