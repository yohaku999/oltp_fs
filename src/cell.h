#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>
enum class CellKind
{
    Leaf,
    Intermediate,
    Record
};

/**
 * Base class for page-resident cells.
 *
 * Serialization:
 *  - byte[0]: per-cell flags (bit0 = invalid)
 *  - byte[1..]: type-specific payload
 *
 * Note: Cell does not own memory; it only provides helpers for (de)serialization and
 * flag manipulation.
 */
class Cell
{
public:
    // NOTE: byte[0] currently packs an invalid flag; extend this byte (or move to a bitmap) if more per-cell flags are required.
    static constexpr size_t FLAG_FIELD_SIZE = sizeof(uint8_t);
    static constexpr uint8_t FLAG_INVALID_MASK = 0x1;

    static void markInvalid(char *cell_start)
    {
        cell_start[0] |= FLAG_INVALID_MASK;
    }

    static bool isValid(const char *cell_start)
    {
        return (static_cast<uint8_t>(cell_start[0]) & FLAG_INVALID_MASK) == 0;
    }

    virtual ~Cell() = default;
    virtual int key() const = 0;
    virtual size_t payloadSize() const = 0;
    virtual std::vector<std::byte> serialize() const = 0;
    virtual CellKind kind() const = 0;
};

// Little-endian is used for storing data.
template <typename T>
inline T readValue(const char *ptr)
{
    T value;
    std::memcpy(&value, ptr, sizeof(T));
    return value;
}
