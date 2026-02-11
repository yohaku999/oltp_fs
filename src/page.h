#pragma once
#include <span>

class Page
{
public:
    static constexpr size_t PAGE_SIZE_BYTE = 4096;
    Page(char *start_p);
};