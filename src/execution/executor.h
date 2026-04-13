#pragma once

#include "tuple/typed_row.h"

class BufferPool;
class Table;
class WAL;

namespace executor {

TypedRow read(BufferPool& pool, Table& table, int key);

void insert(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal);

void remove(BufferPool& pool, Table& table, int key, WAL& wal);

void update(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal);

}  // namespace executor
