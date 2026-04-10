#include "wal_body.h"

#include <cstring>

namespace {

template <class T>
void append_pod(std::vector<std::byte>& out, const T& value) {
  const auto* p = reinterpret_cast<const std::byte*>(&value);
  out.insert(out.end(), p, p + sizeof(T));
}

void append_bytes(std::vector<std::byte>& out, const std::byte* data,
                  std::size_t size) {
  out.insert(out.end(), data, data + size);
}

}  // namespace

std::vector<std::byte> InsertRedoBody::encode() const {
  std::vector<std::byte> out;
  const auto size = static_cast<uint32_t>(tuple.size());
  out.reserve(sizeof(offset) + sizeof(uint32_t) + size);

  append_pod(out, offset);
  append_pod(out, size);
  append_bytes(out, tuple.data(), size);
  return out;
}

InsertRedoBody InsertRedoBody::decode(const std::vector<std::byte>& buffer) {
  InsertRedoBody body{};

  const std::byte* p = buffer.data();
  body.offset = *reinterpret_cast<const uint16_t*>(p);
  p += sizeof(uint16_t);
  const auto data_size = *reinterpret_cast<const uint32_t*>(p);
  p += sizeof(uint32_t);

  const std::size_t remaining =
      static_cast<std::size_t>(buffer.data() + buffer.size() - p);

  body.tuple.resize(data_size);
  std::memcpy(body.tuple.data(), p, data_size);
  return body;
}

std::vector<std::byte> UpdateRedoBody::encode() const {
  std::vector<std::byte> out;
  const auto before_size = static_cast<uint32_t>(before.size());
  const auto after_size = static_cast<uint32_t>(after.size());
  out.reserve(sizeof(offset) + sizeof(uint32_t) * 2 + before_size + after_size);

  append_pod(out, offset);
  append_pod(out, before_size);
  append_bytes(out, before.data(), before_size);
  append_pod(out, after_size);
  append_bytes(out, after.data(), after_size);
  return out;
}

UpdateRedoBody UpdateRedoBody::decode(const std::vector<std::byte>& buffer) {
  UpdateRedoBody body{};

  const std::byte* p = buffer.data();
  body.offset = *reinterpret_cast<const uint16_t*>(p);
  p += sizeof(uint16_t);
  const auto before_size = *reinterpret_cast<const uint32_t*>(p);
  p += sizeof(uint32_t);

  const auto total_size = buffer.size();
  const auto min_after_header =
      sizeof(uint16_t) + sizeof(uint32_t) * 2 + before_size;

  body.before.resize(before_size);
  std::memcpy(body.before.data(), p, before_size);
  p += before_size;

  const auto after_size = *reinterpret_cast<const uint32_t*>(p);
  p += sizeof(uint32_t);

  const auto consumed = static_cast<std::size_t>(p - buffer.data());

  body.after.resize(after_size);
  std::memcpy(body.after.data(), p, after_size);
  return body;
}

std::vector<std::byte> DeleteRedoBody::encode() const {
  std::vector<std::byte> out;
  const auto before_size = static_cast<uint32_t>(before.size());
  out.reserve(sizeof(offset) + sizeof(uint32_t) + before_size);

  append_pod(out, offset);
  append_pod(out, before_size);
  append_bytes(out, before.data(), before_size);
  return out;
}

DeleteRedoBody DeleteRedoBody::decode(const std::vector<std::byte>& buffer) {
  DeleteRedoBody body{};

  const std::byte* p = buffer.data();
  body.offset = *reinterpret_cast<const uint16_t*>(p);
  p += sizeof(uint16_t);
  const auto before_size = *reinterpret_cast<const uint32_t*>(p);
  p += sizeof(uint32_t);

  const auto total_size = buffer.size();
  const auto consumed = static_cast<std::size_t>(p - buffer.data());

  body.before.resize(before_size);
  std::memcpy(body.before.data(), p, before_size);
  return body;
}
