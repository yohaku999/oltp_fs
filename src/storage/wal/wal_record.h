#pragma once
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

#include "wal_body.h"

class WALRecord {
 public:
  enum class RecordType : uint8_t { INSERT, UPDATE, DELETE };

 private:
  uint64_t lsn_;
  RecordType type_;
  uint16_t page_id_;
  uint32_t body_size_;
  std::vector<std::byte> record_body_;

  static constexpr std::size_t header_size_bytes() {
    return sizeof(uint64_t) + sizeof(RecordType) + sizeof(uint16_t) +
           sizeof(uint32_t);
  }

 public:
  WALRecord(uint64_t lsn, RecordType type, uint16_t page_id,
            const std::vector<std::byte>& record_body)
      : lsn_(lsn),
        type_(type),
        page_id_(page_id),
        body_size_(static_cast<uint32_t>(record_body.size())),
        record_body_(record_body) {}

  uint64_t get_lsn() const { return lsn_; }
  RecordType get_type() const { return type_; }
  uint16_t get_page_id() const { return page_id_; }
  const std::vector<std::byte>& get_body() const { return record_body_; }

  static std::size_t size_bytes(const std::vector<std::byte>& body) {
    return header_size_bytes() + body.size();
  }

  std::vector<std::byte> serialize() const;

  static WALRecord deserialize(const std::vector<std::byte>& buffer);
};

WALBody decode_body(const WALRecord& record);