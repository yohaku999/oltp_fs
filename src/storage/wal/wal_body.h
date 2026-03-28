#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <variant>

// Logical bodies for different kinds of WAL records.
struct InsertRedoBody {
    uint16_t offset;
    std::vector<std::byte> tuple;

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

    std::vector<std::byte> encode() const;
    static DeleteRedoBody decode(const std::vector<std::byte>& buffer);
};

using WALBody = std::variant<InsertRedoBody, UpdateRedoBody, DeleteRedoBody>;
