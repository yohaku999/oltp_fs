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

class Cell
{
public:
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
