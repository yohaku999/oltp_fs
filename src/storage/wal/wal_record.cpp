#include "wal_record.h"

#include <stdexcept>

std::vector<std::byte> WALRecord::serialize() const {
  std::vector<std::byte> serialized_data;
  serialized_data.reserve(sizeof(lsn_) + sizeof(type_) + sizeof(page_id_) +
                          sizeof(body_size_) + record_body_.size());

  // Header
  const auto* p_lsn = reinterpret_cast<const std::byte*>(&lsn_);
  serialized_data.insert(serialized_data.end(), p_lsn, p_lsn + sizeof(lsn_));

  const auto* p_type = reinterpret_cast<const std::byte*>(&type_);
  serialized_data.insert(serialized_data.end(), p_type, p_type + sizeof(type_));

  const auto* p_page = reinterpret_cast<const std::byte*>(&page_id_);
  serialized_data.insert(serialized_data.end(), p_page,
                         p_page + sizeof(page_id_));

  const auto* p_size = reinterpret_cast<const std::byte*>(&body_size_);
  serialized_data.insert(serialized_data.end(), p_size,
                         p_size + sizeof(body_size_));

  // Body
  serialized_data.insert(serialized_data.end(), record_body_.begin(),
                         record_body_.end());
  return serialized_data;
}

WALRecord WALRecord::deserialize(const std::vector<std::byte>& buffer) {
  if (buffer.size() < header_size_bytes()) {
    throw std::runtime_error("WALRecord::deserialize: buffer too small");
  }

  const std::byte* p = buffer.data();

  uint64_t lsn_value = *reinterpret_cast<const uint64_t*>(p);
  p += sizeof(uint64_t);

  auto type_value = *reinterpret_cast<const RecordType*>(p);
  p += sizeof(RecordType);

  uint16_t page_id_value = *reinterpret_cast<const uint16_t*>(p);
  p += sizeof(uint16_t);

  uint32_t body_size_value = *reinterpret_cast<const uint32_t*>(p);
  p += sizeof(uint32_t);

  const std::size_t remaining =
      static_cast<std::size_t>(buffer.data() + buffer.size() - p);
  if (remaining < body_size_value) {
    throw std::runtime_error("WALRecord::deserialize: buffer body too small");
  }

  std::vector<std::byte> body(body_size_value);
  std::memcpy(body.data(), p, body_size_value);

  return WALRecord(lsn_value, type_value, page_id_value, body);
}

WALBody decode_body(const WALRecord& record) {
  const auto& buf = record.get_body();

  switch (record.get_type()) {
    case WALRecord::RecordType::INSERT:
      return InsertRedoBody::decode(buf);
    case WALRecord::RecordType::UPDATE:
      return UpdateRedoBody::decode(buf);
    case WALRecord::RecordType::DELETE:
      return DeleteRedoBody::decode(buf);
  }

  throw std::logic_error("Unknown WALRecord::RecordType in decode_body");
}
