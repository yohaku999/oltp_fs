#pragma once

#include <vector>

#include "tuple/typed_row.h"

class BufferPool;
class SelectParser;
class Table;
class WAL;

namespace executor {

TypedRow read(BufferPool& pool, Table& table, int key);
std::vector<TypedRow> read(BufferPool& pool, const SelectParser& parser);

void insert(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal);

void remove(BufferPool& pool, Table& table, int key, WAL& wal);

void update(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal);

}  // namespace executor
