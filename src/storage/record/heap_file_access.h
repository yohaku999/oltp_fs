#pragma once

#include <cstddef>
#include <vector>

#include "storage/index/rid.h"

class BufferPool;
class File;

namespace heap_file_access {

std::vector<RID> collectRids(BufferPool& pool, File& heap_file);
RID appendCell(BufferPool& pool, File& heap_file,
			   const std::vector<std::byte>& serialized_cell);

}  // namespace heap_file_access