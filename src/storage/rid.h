#pragma once

#include <cstdint>

struct RID {
    uint16_t heap_page_id;
    uint16_t slot_id;
};
