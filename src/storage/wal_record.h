#pragma once
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

#include "lsn_allocator.h"
#include "wal_body.h"

class WALRecord {
 public:
  enum class RecordType : uint8_t { INSERT, UPDATE, DELETE };

 private:
  uint64_t lsn;
  RecordType type;
  uint16_t pageID;
  uint32_t body_size;
  std::vector<std::byte> record_body;

  static constexpr std::size_t header_size_bytes() {
    return sizeof(uint64_t) + sizeof(RecordType) + sizeof(uint16_t) +
           sizeof(uint32_t);
  }

 public:
  WALRecord(uint64_t lsn, RecordType type, uint16_t PageID,
            const std::vector<std::byte>& record_body)
      : lsn(lsn),
        type(type),
        pageID(PageID),
        body_size(static_cast<uint32_t>(record_body.size())),
        record_body(record_body) {}

  uint64_t get_lsn() const { return lsn; }
  RecordType get_type() const { return type; }
  uint16_t get_page_id() const { return pageID; }
  const std::vector<std::byte>& get_body() const { return record_body; }

  static std::size_t size_bytes(const std::vector<std::byte>& body) {
    return header_size_bytes() + body.size();
  }

  std::vector<std::byte> serialize() const;

  static WALRecord deserialize(const std::vector<std::byte>& buffer);
};

// Helper that uses an external LSNAllocator to assign the next LSN
// based on the serialized size of the record, and then constructs
// a WALRecord with that LSN.
inline WALRecord make_wal_record(LSNAllocator& allocator,
                                 WALRecord::RecordType type, uint16_t page_id,
                                 const std::vector<std::byte>& body) {
  const auto size = WALRecord::size_bytes(body);
  const auto lsn = allocator.allocate(size);
  return WALRecord(lsn, type, page_id, body);
}

WALBody decode_body(const WALRecord& record);