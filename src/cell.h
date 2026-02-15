#pragma once
#include <cstdint>
#include <cstring>

// Little-endian is used for storing data.
template <typename T>
inline T readValue(const char *ptr)
{
    T value;
    std::memcpy(&value, ptr, sizeof(T));
    return value;
}
