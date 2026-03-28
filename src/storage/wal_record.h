#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <variant>

#include "wal_body.h"

class WALRecord
{
public:
    enum class RecordType : uint8_t {
        INSERT,
        UPDATE,
        DELETE
    };

private:
    uint64_t lsn;
    RecordType type;
    uint16_t pageID;
    uint32_t body_size;
    std::vector<std::byte> record_body;
public:
    WALRecord(uint64_t lsn, RecordType type, uint16_t PageID,  const std::vector<std::byte>& record_body)
        : lsn(lsn), type(type), pageID(PageID), body_size(static_cast<uint32_t>(record_body.size())), record_body(record_body) {}

    uint64_t get_lsn() const { return lsn; }
    RecordType get_type() const { return type; }
    uint16_t get_page_id() const { return pageID; }
    const std::vector<std::byte>& get_body() const { return record_body; }

    std::vector<std::byte> serialize() const;

    static WALRecord deserialize(const std::vector<std::byte>& buffer);
};

WALBody decode_body(const WALRecord& record);