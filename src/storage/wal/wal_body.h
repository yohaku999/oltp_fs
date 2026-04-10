#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <vector>

struct InsertRedoBody {
  uint16_t offset;
  std::vector<std::byte> tuple;

  InsertRedoBody() = default;
  InsertRedoBody(uint16_t offset_, std::vector<std::byte> tuple_)
      : offset(offset_), tuple(std::move(tuple_)) {}

  std::vector<std::byte> encode() const;
  static InsertRedoBody decode(const std::vector<std::byte>& buffer);
};

struct UpdateRedoBody {
  uint16_t offset;
  std::vector<std::byte> before;
  std::vector<std::byte> after;

  std::vector<std::byte> encode() const;
  static UpdateRedoBody decode(const std::vector<std::byte>& buffer);
};

struct DeleteRedoBody {
  uint16_t offset;
  std::vector<std::byte> before;

  DeleteRedoBody() = default;
  DeleteRedoBody(uint16_t offset_, std::vector<std::byte> before_ = {})
      : offset(offset_), before(std::move(before_)) {}

  std::vector<std::byte> encode() const;
  static DeleteRedoBody decode(const std::vector<std::byte>& buffer);
};

using WALBody = std::variant<InsertRedoBody, UpdateRedoBody, DeleteRedoBody>;
