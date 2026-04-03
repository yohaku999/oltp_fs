#pragma once

#include <cstddef>

class BufferPool;
class File;

namespace executor
{

char *read(BufferPool &pool, File &indexFile, File &heapFile, int key);

void insert(BufferPool &pool, File &indexFile, File &heapFile,
			int key, char *value, std::size_t value_size);

void remove(BufferPool &pool, File &indexFile, File &heapFile, int key);

void update(BufferPool &pool, File &indexFile, File &heapFile,
			int key, char *value, std::size_t value_size);

}

