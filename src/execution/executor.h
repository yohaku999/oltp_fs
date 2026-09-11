#pragma once

#include <vector>

#include "parser/query_parameters.h"
#include "tuple/typed_row.h"

class BufferPool;
class IndexAccessor;
class LocalStorageManager;
class CreateIndexParser;
class CreateTableParser;
class DeleteParser;
class DropTableParser;
class SelectParser;
class InsertParser;
class UpdateParser;
class Table;

namespace executor {

std::vector<TypedRow> read(BufferPool& pool, LocalStorageManager& storage,
                           IndexAccessor& index_accessor,
                           const SelectParser& parser,
                           const QueryParameters& parameters = {});

void insert(BufferPool& pool, LocalStorageManager& storage,
            IndexAccessor& index_accessor, Table& table,
            const InsertParser& parser, const QueryParameters& parameters = {});

int remove(BufferPool& pool, LocalStorageManager& storage,
           IndexAccessor& index_accessor, Table& table,
           const DeleteParser& parser, const QueryParameters& parameters = {});

void update(BufferPool& pool, LocalStorageManager& storage,
            IndexAccessor& index_accessor, Table& table,
            const UpdateParser& parser, const QueryParameters& parameters = {});

void create_index(LocalStorageManager& storage,
                  const CreateIndexParser& parser);

void create_table(LocalStorageManager& storage,
                  const CreateTableParser& parser);

void drop_table(LocalStorageManager& storage, const DropTableParser& parser);

}  // namespace executor
