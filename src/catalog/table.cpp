#include "catalog/table.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

Table::Table(std::string name, Schema schema, std::string heap_file_name,
             std::vector<IndexMetadata> indexes)
    : name_(std::move(name)),
      schema_(std::move(schema)),
      heap_file_name_(std::move(heap_file_name)),
      indexes_(std::move(indexes)) {
  if (name_.empty()) {
    throw std::invalid_argument("Table name must not be empty.");
  }
  if (heap_file_name_.empty()) {
    throw std::invalid_argument("Heap file name must not be empty.");
  }
  for (const IndexMetadata& index : indexes_) {
    validateIndex(index);
  }
}

bool Table::hasIndexForColumn(const std::string& column_name) const {
  return std::any_of(
      indexes_.begin(), indexes_.end(), [&](const IndexMetadata& index) {
        return std::find(index.column_names.begin(), index.column_names.end(),
                         column_name) != index.column_names.end();
      });
}

const IndexMetadata& Table::requireIndex() const {
  if (!hasIndex()) {
    throw std::runtime_error("Table has no index: " + name_);
  }
  return indexes_.front();
}

const std::vector<std::string>& Table::indexedColumnNames() const {
  static const std::vector<std::string> no_columns;
  return hasIndex() ? requireIndex().column_names : no_columns;
}


/**
 * returns the indexes of the columns that are indexed in the table's schema.
 */
std::vector<std::size_t> Table::indexedColumnIndexes() const {
  if (!hasIndex()) {
    return {};
  }
  return indexedColumnIndexes(requireIndex());
}

std::vector<std::size_t> Table::indexedColumnIndexes(
    const IndexMetadata& index) const {
  std::vector<std::size_t> column_indexes;
  column_indexes.reserve(index.column_names.size());
  for (const std::string& column_name : index.column_names) {
    const int column_index = schema_.getColumnIndex(column_name);
    if (column_index < 0) {
      throw std::logic_error("Index references unknown column: " +
                             column_name);
    }
    column_indexes.push_back(static_cast<std::size_t>(column_index));
  }
  return column_indexes;
}

void Table::addIndex(IndexMetadata index) {
  validateIndex(index);
  indexes_.push_back(std::move(index));
}

void Table::validateIndex(const IndexMetadata& index) const {
  if (index.file_name.empty()) {
    throw std::invalid_argument("Index file name must not be empty.");
  }
  if (index.column_names.empty()) {
    throw std::invalid_argument("Index requires at least one column.");
  }
  for (const std::string& column_name : index.column_names) {
    if (schema_.getColumnIndex(column_name) < 0) {
      throw std::invalid_argument("Index references unknown column: " +
                                  column_name);
    }
  }
}
