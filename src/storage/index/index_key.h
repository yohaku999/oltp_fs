#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "schema/schema.h"
#include "tuple/typed_row.h"

namespace index_key {

// Index keys are encoded so storage can compare raw bytes directly without
// decoding typed values on every B-tree traversal. The contract is that the
// lexicographic order of the encoded bytes must match the logical order of the
// original non-NULL values, and that concatenating encoded fields must remain
// unambiguous for composite keys.

inline void appendUint32(std::string& buffer, std::uint32_t value) {
  // Emit the sortable unsigned representation in big-endian byte order so
  // lexicographic byte comparison matches numeric order.
  for (int shift = 24; shift >= 0; shift -= 8) {
    buffer.push_back(static_cast<char>((value >> shift) & 0xFF));
  }
}

inline void appendUint64(std::string& buffer, std::uint64_t value) {
  // Emit the sortable unsigned representation in big-endian byte order so
  // lexicographic byte comparison matches numeric order.
  for (int shift = 56; shift >= 0; shift -= 8) {
    buffer.push_back(static_cast<char>((value >> shift) & 0xFF));
  }
}

inline std::string encodeInteger(int value) {
  std::string encoded;
  encoded.reserve(sizeof(std::uint32_t));
  // Flip the sign bit to map signed integer order into unsigned order before
  // serializing it as bytes.
  const std::uint32_t sortable =
      static_cast<std::uint32_t>(value) ^ 0x80000000u;
  appendUint32(encoded, sortable);
  return encoded;
}

inline std::string encodeDouble(double value) {
  std::uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));

  // Map IEEE754 bits into a sortable unsigned space before serializing them as
  // bytes.
  const std::uint64_t sortable =
      (bits & (1ull << 63)) != 0 ? ~bits : (bits ^ (1ull << 63));

  std::string encoded;
  encoded.reserve(sizeof(std::uint64_t));
  appendUint64(encoded, sortable);
  return encoded;
}

inline std::string encodeVarchar(const std::string& value) {
  std::string encoded;
  encoded.reserve(value.size() + 2);
  for (const unsigned char byte : value) {
    if (byte == '\0') {
      encoded.push_back('\0');
      encoded.push_back(static_cast<char>(0xFF));
      continue;
    }
    encoded.push_back(static_cast<char>(byte));
  }
  encoded.push_back('\0');
  encoded.push_back('\0');
  return encoded;
}

inline std::string encodeFieldValue(const FieldValue& value, Column::Type type) {
  if (isNullFieldValue(value)) {
    throw std::runtime_error("Index keys do not support NULL values.");
  }

  switch (type) {
    case Column::Type::Integer: {
      std::string encoded;
      encoded.push_back('I');
      encoded += encodeInteger(std::get<Column::IntegerType>(value));
      return encoded;
    }
    case Column::Type::Double: {
      std::string encoded;
      encoded.push_back('D');
      encoded += encodeDouble(std::get<Column::DoubleType>(value));
      return encoded;
    }
    case Column::Type::Varchar: {
      std::string encoded;
      encoded.push_back('S');
      encoded += encodeVarchar(std::get<Column::VarcharType>(value));
      return encoded;
    }
  }

  throw std::runtime_error("Unsupported index key column type.");
}

inline std::string encodeRow(const Schema& schema, const TypedRow& row,
                             const std::vector<std::size_t>& column_indexes) {
  std::string encoded;
  for (const std::size_t column_index : column_indexes) {
    if (column_index >= row.values.size() ||
        column_index >= schema.columns().size()) {
      throw std::runtime_error("Indexed column is out of row bounds.");
    }
    encoded += encodeFieldValue(row.values[column_index],
                                schema.columns()[column_index].getType());
  }
  return encoded;
}

inline int compare(std::string_view lhs, std::string_view rhs) {
  const int result = lhs.compare(rhs);
  if (result < 0) {
    return -1;
  }
  if (result > 0) {
    return 1;
  }
  return 0;
}

inline std::string formatForDebug(std::string_view key) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string formatted;
  formatted.reserve(key.size() * 2);
  for (const unsigned char byte : key) {
    formatted.push_back(kHex[(byte >> 4) & 0x0F]);
    formatted.push_back(kHex[byte & 0x0F]);
  }
  return formatted;
}

}  // namespace index_key