#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "execution/operator.h"

class ProjectionOperator : public Operator {
 public:
  ProjectionOperator(std::unique_ptr<Operator> child,
                     std::vector<std::size_t> projection_indices)
      : child_(std::move(child)),
        projection_indices_(std::move(projection_indices)) {}

  void open() override { child_->open(); }

  std::optional<TypedRow> next() {
    std::optional<TypedRow> row = child_->next();
    if (!row.has_value()) {
      return std::nullopt;
    }

    TypedRow projected_row;
    projected_row.values.reserve(projection_indices_.size());

    for (std::size_t row_index : projection_indices_) {
      projected_row.values.push_back(row->values[row_index]);
    }

    return projected_row;
  }

  void close() override { child_->close(); }

 private:
  std::unique_ptr<Operator> child_;
  std::vector<std::size_t> projection_indices_;
};