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

namespace executor {

class PreparedStatementCache;

std::vector<TypedRow> read(BufferPool& pool, LocalStorageManager& storage,
                           IndexAccessor& index_accessor,
                           PreparedStatementCache& cache,
                           const SelectParser& parser,
                           const QueryParameters& parameters = {});

void insert(BufferPool& pool, LocalStorageManager& storage,
            IndexAccessor& index_accessor, PreparedStatementCache& cache,
            const InsertParser& parser, const QueryParameters& parameters = {});

int remove(BufferPool& pool, LocalStorageManager& storage,
           IndexAccessor& index_accessor, PreparedStatementCache& cache,
           const DeleteParser& parser, const QueryParameters& parameters = {});

void update(BufferPool& pool, LocalStorageManager& storage,
            IndexAccessor& index_accessor, PreparedStatementCache& cache,
            const UpdateParser& parser, const QueryParameters& parameters = {});

void create_index(LocalStorageManager& storage, PreparedStatementCache& cache,
                  const CreateIndexParser& parser);

void create_table(LocalStorageManager& storage, PreparedStatementCache& cache,
                  const CreateTableParser& parser);

void drop_table(LocalStorageManager& storage, PreparedStatementCache& cache,
                const DropTableParser& parser);

}  // namespace executor
