#include "execution/prepared_statement_cache.h"

#include <utility>

#include "parser/delete_parser.h"
#include "parser/insert_parser.h"
#include "parser/select_parser.h"
#include "parser/update_parser.h"
#include "storage/table_metadata_store.h"

namespace executor {

const binder::PreparedSelectQuery& PreparedStatementCache::select(
    const SelectParser& parser) {
  const std::string& sql_template = parser.sqlTemplate();
  const auto cached = selects_.find(sql_template);
  if (cached != selects_.end()) {
    return cached->second;
  }
  return selects_.emplace(sql_template, binder::prepareSelectQuery(parser))
      .first->second;
}

const PreparedInsertStatement& PreparedStatementCache::insert(
    const InsertParser& parser) {
  const std::string& sql_template = parser.sqlTemplate();
  const auto cached = inserts_.find(sql_template);
  if (cached != inserts_.end()) {
    return cached->second;
  }

  Table table = TableMetadataStore::read(parser.extractTableName()).value();
  PreparedInsertStatement statement{
      table, parser.extractPreparedValues(table.schema())};
  return inserts_.emplace(sql_template, std::move(statement)).first->second;
}

const PreparedUpdateStatement& PreparedStatementCache::update(
    const UpdateParser& parser) {
  const std::string& sql_template = parser.sqlTemplate();
  const auto cached = updates_.find(sql_template);
  if (cached != updates_.end()) {
    return cached->second;
  }

  Table table = TableMetadataStore::read(parser.extractTableName()).value();
  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractPreparedComparisonPredicates(table.schema());
  PreparedUpdateStatement statement{
      table, binder::preparePredicates(predicates, {table}),
      binder::prepareUpdateAssignments(
          parser.extractPreparedAssignments(table.schema()), table)};
  return updates_.emplace(sql_template, std::move(statement)).first->second;
}

const PreparedDeleteStatement& PreparedStatementCache::remove(
    const DeleteParser& parser) {
  const std::string& sql_template = parser.sqlTemplate();
  const auto cached = deletes_.find(sql_template);
  if (cached != deletes_.end()) {
    return cached->second;
  }

  Table table = TableMetadataStore::read(parser.extractTableName()).value();
  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractPreparedComparisonPredicates(table.schema());
  PreparedDeleteStatement statement{
      table, binder::preparePredicates(predicates, {table})};
  return deletes_.emplace(sql_template, std::move(statement)).first->second;
}

void PreparedStatementCache::clear() {
  selects_.clear();
  inserts_.clear();
  updates_.clear();
  deletes_.clear();
}

}  // namespace executor
