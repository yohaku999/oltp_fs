#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "execution/operator.h"

class LoopJoinOperator : public Operator {
 public:
  explicit LoopJoinOperator(std::vector<std::unique_ptr<Operator>> children);

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
    std::vector<TypedRow> materializeSourceRows(Operator& child) const;
    TypedRow buildJoinedRow() const;
    void advanceSourceRowCursors();

  std::vector<std::unique_ptr<Operator>> children_;
    std::vector<std::vector<TypedRow>> materialized_source_rows_;
    std::vector<std::size_t> source_row_cursors_;
  bool exhausted_ = true;
};