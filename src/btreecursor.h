#pragma once
#include <string>
#include <utility>
#include "bufferpool.h"

/**
 * BTreeCursor is an arbitration layer that coordinates BufferPool and File to execute
 * B-Tree index traversal and insertion of the actual record into the heap file as a
 * single transactional sequence.
 * 
 * A path-generation helper centralizes file naming under ./data/<table>.{index,db}.
 * File is responsible for page-ID allocation and persistence (e.g., maintaining the
 * high-water mark), while BufferPool provides page caching.
 */
class BTreeCursor
{
private:
    static std::pair<uint16_t, uint16_t> findRecordLocation(BufferPool& pool, File& indexFile, int key, bool do_invalidate=false);
    static int findLeafPageID(BufferPool& pool, File& indexFile, int key);
public:
    static char* read(BufferPool& pool, File& indexFile, File& heapFile, int key);
    static void insert(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size);
    static void remove(BufferPool& pool, File& indexFile, File& heapFile, int key);
};