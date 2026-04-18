#pragma once

#include <vector>

#include "tuple/typed_row.h"

class BufferPool;
class CreateIndexParser;
class CreateTableParser;
class DropTableParser;
class SelectParser;
class InsertParser;
class UpdateParser;
class Table;
class WAL;

namespace executor {

TypedRow read(BufferPool& pool, Table& table, int key);
std::vector<TypedRow> read(BufferPool& pool, const SelectParser& parser);

void insert(BufferPool& pool, Table& table, const InsertParser& parser, WAL& wal);

void remove(BufferPool& pool, Table& table, int key, WAL& wal);

void update(BufferPool& pool, Table& table, const UpdateParser& parser, WAL& wal);

void create_index(const CreateIndexParser& parser);

void create_table(const CreateTableParser& parser);

void drop_table(const DropTableParser& parser);

}
