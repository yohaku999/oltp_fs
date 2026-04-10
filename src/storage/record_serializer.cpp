#include "record_serializer.h"

#include <cstring>
#include <stdexcept>

namespace {

template <typename T>
const T& getFieldValueOrThrow(const FieldValue& value,
                              const char* error_message) {
  if (!std::holds_alternative<T>(value)) {
    throw std::runtime_error(error_message);
  }
  return std::get<T>(value);
}

std::size_t getNonNullFieldPayloadSize(const Column& column,
                                       const FieldValue& value) {
  switch (column.getType()) {
    case Column::Type::Integer:
      getFieldValueOrThrow<Column::IntegerType>(
          value, "Expected Integer field value.");
      return sizeof(Column::IntegerType);
    case Column::Type::Varchar:
      return getFieldValueOrThrow<Column::VarcharType>(
                 value, "Expected Varchar field value.")
          .size();
  }
  throw std::runtime_error("Unsupported column type.");
}

void writeNonNullFieldValue(const Column& column, const FieldValue& value,
                            char* dst) {
  switch (column.getType()) {
    case Column::Type::Integer: {
      const auto fixed_value = getFieldValueOrThrow<Column::IntegerType>(
          value, "Expected Integer field value.");
      std::memcpy(dst, &fixed_value, sizeof(fixed_value));
      return;
    }
    case Column::Type::Varchar: {
      const auto& variable_value = getFieldValueOrThrow<Column::VarcharType>(
          value, "Expected Varchar field value.");
      std::memcpy(dst, variable_value.data(), variable_value.size());
      return;
    }
  }
  throw std::runtime_error("Unsupported column type.");
}

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
    const auto& value = row.values[index];
    if (column.isFixedLength() || isNullFieldValue(value)) {
      continue;
    }

    total_size += getNonNullFieldPayloadSize(column, value);
  }
  return total_size;
}

void writeFixedLengthValue(const Column& column, const FieldValue& value,
                           char* dst) {
  if (isNullFieldValue(value)) {
    std::memset(dst, 0, column.size());
    return;
  }

  writeNonNullFieldValue(column, value, dst);
}

uint16_t writeVariableLengthValue(const Column& column, const FieldValue& value,
                                  char*& dst) {
  if (isNullFieldValue(value)) {
    return 0;
  }

  const auto field_payload_size = getNonNullFieldPayloadSize(column, value);
  writeNonNullFieldValue(column, value, dst);
  dst += field_payload_size;
  return static_cast<uint16_t>(field_payload_size);
}

}

RecordSerializer::RecordSerializer(const Schema& schema, const TypedRow& row)
    : serialized_bytes_() {
  if (schema.columns_.size() != row.values.size()) {
    throw std::runtime_error(
        "Schema column count and TypedRow value count must match.");
  }

  const int variable_column_count = schema.getVariableColumnCount();
  const std::size_t fixed_payload_size = computeFixedPayloadSize(schema);
  const std::size_t variable_table_size =
      sizeof(uint16_t) * variable_column_count;
  const std::size_t variable_payload_data_size =
      computeVariablePayloadDataSize(schema, row);
  const uint16_t variable_length_payload_begin_offset = static_cast<uint16_t>(
      RecordCellLayout::HEADER_SIZE_BYTE + fixed_payload_size);

  serialized_bytes_.resize(RecordCellLayout::HEADER_SIZE_BYTE +
                           fixed_payload_size + variable_table_size +
                           variable_payload_data_size);

  char* dst = reinterpret_cast<char*>(serialized_bytes_.data());

  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;

  std::memcpy(dst, &variable_length_payload_begin_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  const uint32_t null_bitmap = buildNullBitmap(schema, row);
  std::memcpy(dst, &null_bitmap, sizeof(uint32_t));
  dst += sizeof(uint32_t);

  char* fixed_payload_dst = dst;
  char* variable_table_dst = reinterpret_cast<char*>(serialized_bytes_.data()) +
                             variable_length_payload_begin_offset;
  char* variable_data_dst = variable_table_dst + variable_table_size;

  uint16_t variable_end_offset = 0;
  int variable_column_index = 0;

  for (std::size_t index = 0; index < schema.columns_.size(); ++index) {
    const auto& column = schema.columns_[index];
    const auto& value = row.values[index];
    if (column.isFixedLength()) {
      writeFixedLengthValue(column, value, fixed_payload_dst);
      fixed_payload_dst += column.size();
    } else {
      variable_end_offset +=
          writeVariableLengthValue(column, value, variable_data_dst);

      char* offset_entry =
          variable_table_dst + sizeof(uint16_t) * variable_column_index;
      std::memcpy(offset_entry, &variable_end_offset, sizeof(uint16_t));
      ++variable_column_index;
    }
  }
}
