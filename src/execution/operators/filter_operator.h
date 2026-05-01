#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"

class FilterOperator : public Operator {
 public:
  FilterOperator(std::unique_ptr<Operator> child,
                 std::vector<BoundComparisonPredicate> predicates)
      : child_(std::move(child)),
        predicates_(std::move(predicates)) {}

  void open() override { child_->open(); }

  std::optional<TypedRow> next() override {
    while (std::optional<TypedRow> row = child_->next()) {
      bool matches = true;
      for (const auto& predicate : predicates_) {

        // すべてのpredicateについてのvalueとrowのvalueを比較しているが、predicateのvalueもrowから取得する必要がある。
        // predicateのvalueにカラム名を持てるようにするのもあり？
        // テーブルをイメージして実装してきたけど、実際はtuple相手なので、indexは改めて取る必要がある。
        const FieldValue left = resolveBoundOperand(predicate.left, row.value());
        const FieldValue right = resolveBoundOperand(predicate.right, row.value());
        switch (predicate.op) {
          case Op::Eq:
            if (left != right) {
              matches = false;
            }
            break;
          case Op::Gt:
            if (left <= right) {
              matches = false;
            }
            break;
          case Op::Ge:
            if (left < right) {
              matches = false;
            }
            break;
          case Op::Lt:
            if (left >= right) {
              matches = false;
            }
            break;
          case Op::Le:
            if (left > right) {
              matches = false;
            }
            break;
        }

        if (!matches) {
          break;
        }
      }

      if (matches) {
        return row;
      }
    }

    return std::nullopt;
  }

  void close() override { child_->close(); }

 private:
  std::unique_ptr<Operator> child_;
  std::vector<BoundComparisonPredicate> predicates_;
};