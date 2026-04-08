#include "record_builder.h"

#include <cstring>
#include <stdexcept>

namespace {

uint32_t buildNullBitmap(const Schema& schema, const TypedRow& row) {
  uint32_t null_bitmap = 0;
  for (std::size_t index = 0; index < row.values.size(); ++index) {
    if (isNullFieldValue(row.values[index])) {
      null_bitmap |= (1u << index);
    }
  }
  return null_bitmap;
}

std::size_t computeFixedPayloadSize(const Schema& schema) {
  std::size_t total_size = 0;
  for (const auto& column : schema.columns_) {
    if (column.isFixedLength()) {
      total_size += column.size();
    }
  }
  return total_size;
}

std::size_t computeVariablePayloadDataSize(const Schema& schema,
                                           const TypedRow& row) {
  std::size_t total_size = 0;
  for (std::size_t index = 0; index < schema.columns_.size(); ++index) {
    const auto& column = schema.columns_[index];
    if (column.isFixedLength() || isNullFieldValue(row.values[index])) {
      continue;
    }
    total_size += std::get<Column::VarcharType>(row.values[index]).size();
  }
  return total_size;
}

}  // namespace

RecordBuilder::RecordBuilder(const Schema& schema, const TypedRow& row)
    : serialized_bytes_() {
  if (schema.columns_.size() != row.values.size()) {
    throw std::runtime_error(
        "Schema column count and TypedRow value count must match.");
  }

  // calcluate header and payload size.
  const int variable_column_count = schema.getVariableColumnCount();
  const std::size_t fixed_payload_size = computeFixedPayloadSize(schema);
  const std::size_t variable_table_size =
      sizeof(uint16_t) * variable_column_count;
  const std::size_t variable_payload_data_size =
      computeVariablePayloadDataSize(schema, row);
  const uint16_t variable_length_payload_begin_offset = static_cast<uint16_t>(
      RecordCellLayout::HEADER_SIZE_BYTE + fixed_payload_size);

  // start writing to serialized_bytes_ buffer.
  serialized_bytes_.resize(RecordCellLayout::HEADER_SIZE_BYTE +
                           fixed_payload_size + variable_table_size +
                           variable_payload_data_size);

  char* dst = reinterpret_cast<char*>(serialized_bytes_.data());

  // flag
  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;

  // variable-length payload begin offset
  std::memcpy(dst, &variable_length_payload_begin_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  // null bitmap
  const uint32_t null_bitmap = buildNullBitmap(schema, row);
  std::memcpy(dst, &null_bitmap, sizeof(uint32_t));
  dst += sizeof(uint32_t);

  // maintain two pointers for variable-length payload and variable-length
  // payload and write in column order.
  char* fixed_payload_dst = dst;
  char* variable_table_dst = reinterpret_cast<char*>(serialized_bytes_.data()) +
                             variable_length_payload_begin_offset;
  char* variable_data_dst = variable_table_dst + variable_table_size;

  uint16_t variable_end_offset = 0;
  int variable_column_index = 0;

  for (std::size_t index = 0; index < schema.columns_.size(); ++index) {
    const auto& column = schema.columns_[index];
    const auto& value = row.values[index];
    const bool is_fixed_length = column.isFixedLength();
    const bool is_null = isNullFieldValue(value);

    if (is_fixed_length) {
      if (is_null) {
        std::memset(fixed_payload_dst, 0, column.size());
      } else {
        if (!std::holds_alternative<Column::IntegerType>(value)) {
          throw std::runtime_error("Expected Integer field value.");
        }
        const auto fixed_value = std::get<Column::IntegerType>(value);
        std::memcpy(fixed_payload_dst, &fixed_value, sizeof(fixed_value));
      }
      fixed_payload_dst += column.size();
    } else {
      if (is_null) {
        // NULL variable-length columns contribute no bytes.
      } else {
        if (!std::holds_alternative<Column::VarcharType>(value)) {
          throw std::runtime_error("Expected Varchar field value.");
        }
        const auto& variable_value = std::get<Column::VarcharType>(value);
        std::memcpy(variable_data_dst, variable_value.data(),
                    variable_value.size());
        variable_data_dst += variable_value.size();
        variable_end_offset += static_cast<uint16_t>(variable_value.size());
      }

      char* offset_entry =
          variable_table_dst + sizeof(uint16_t) * variable_column_index;
      std::memcpy(offset_entry, &variable_end_offset, sizeof(uint16_t));
      ++variable_column_index;
    }
  }
}

// fix builder later.
RecordBuilder::RecordBuilder(char* value, size_t value_size)
    : serialized_bytes_(RecordCellLayout::HEADER_SIZE_BYTE + sizeof(uint16_t) +
                        value_size) {
  char* dst = reinterpret_cast<char*>(serialized_bytes_.data());
  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;

  const uint16_t variable_length_payload_begin_offset =
      static_cast<uint16_t>(RecordCellLayout::HEADER_SIZE_BYTE);
  std::memcpy(dst, &variable_length_payload_begin_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  const uint32_t null_bitmap = 0;
  std::memcpy(dst, &null_bitmap, sizeof(uint32_t));
  dst += sizeof(uint32_t);

  const uint16_t first_column_end_offset = static_cast<uint16_t>(value_size);
  std::memcpy(dst, &first_column_end_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  std::memcpy(dst, value, value_size);
}

std::vector<std::byte> RecordBuilder::serialize() const {
  return serialized_bytes_;
}