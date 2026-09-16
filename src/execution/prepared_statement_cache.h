#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/table.h"
#include "parser/query_parameters.h"
#include "planning/binder.h"
class DeleteParser;
class InsertParser;
class SelectParser;
class UpdateParser;

namespace executor {

struct PreparedInsertStatement {
  Table table;
  std::vector<PreparedFieldValue> values;
};

struct PreparedDeleteStatement {
  Table table;
  std::vector<binder::PreparedComparisonPredicate> predicates;
};

struct PreparedUpdateStatement {
  Table table;
  std::vector<binder::PreparedComparisonPredicate> predicates;
  std::vector<binder::PreparedUpdateAssignment> assignments;
};

class PreparedStatementCache {
 public:
  const binder::PreparedSelectQuery& select(const SelectParser& parser);
  const PreparedInsertStatement& insert(const InsertParser& parser);
  const PreparedUpdateStatement& update(const UpdateParser& parser);
  const PreparedDeleteStatement& remove(const DeleteParser& parser);
  void clear();

 private:
  std::unordered_map<std::string, binder::PreparedSelectQuery> selects_;
  std::unordered_map<std::string, PreparedInsertStatement> inserts_;
  std::unordered_map<std::string, PreparedUpdateStatement> updates_;
  std::unordered_map<std::string, PreparedDeleteStatement> deletes_;
};

}  // namespace executor
