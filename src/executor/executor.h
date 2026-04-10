#pragma once

#include "../schema/typed_row.h"

class BufferPool;
class LSNAllocator;
class Table;
class WAL;

namespace executor {

TypedRow read(BufferPool& pool, Table& table, int key);

void insert(BufferPool& pool, Table& table, int key, const TypedRow& row);
void insert(BufferPool& pool, Table& table, int key, const TypedRow& row,
            LSNAllocator& allocator, WAL& wal);

void remove(BufferPool& pool, Table& table, int key);
void remove(BufferPool& pool, Table& table, int key, LSNAllocator& allocator,
            WAL& wal);

void update(BufferPool& pool, Table& table, int key, const TypedRow& row);
void update(BufferPool& pool, Table& table, int key, const TypedRow& row,
            LSNAllocator& allocator, WAL& wal);

}
