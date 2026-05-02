#pragma once

#include <vector>

#include "storage/index/rid.h"

class BufferPool;
class File;

namespace heap_file_access {

std::vector<RID> collectRids(BufferPool& pool, File& heap_file);

}  // namespace heap_file_access