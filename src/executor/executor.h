#pragma once

#include "../schema/typed_row.h"

class BufferPool;
class Table;

namespace executor {

TypedRow read(BufferPool& pool, Table& table, int key);

void insert(BufferPool& pool, Table& table, int key, const TypedRow& row);

void remove(BufferPool& pool, Table& table, int key);

void update(BufferPool& pool, Table& table, int key, const TypedRow& row);

}  // namespace executor
