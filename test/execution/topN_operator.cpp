#include "execution/operators/topN_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "stub_row_operator.h"

TEST(TopNOperatorTest, KeepsSmallestRowsInAscendingOrder) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(4, "dave", 40));
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(3, "carol", 30));
  rows.push_back(makeStubRow(2, "bob", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  TopNOperator top_n(std::move(child), {{0, OrderByDirection::Asc}}, 2);

  top_n.open();

  std::optional<TypedRow> first = top_n.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = top_n.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  EXPECT_FALSE(top_n.next().has_value());
  top_n.close();
}

TEST(TopNOperatorTest, KeepsLargestRowsInDescendingOrder) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(4, "dave", 40));
  rows.push_back(makeStubRow(2, "bob", 20));
  rows.push_back(makeStubRow(3, "carol", 30));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  TopNOperator top_n(std::move(child), {{0, OrderByDirection::Desc}}, 2);

  top_n.open();

  std::optional<TypedRow> first = top_n.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 4);

  std::optional<TypedRow> second = top_n.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 3);

  EXPECT_FALSE(top_n.next().has_value());
  top_n.close();
}

TEST(TopNOperatorTest, UsesLaterColumnsAsTieBreaker) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(3, "carol", 20));
  rows.push_back(makeStubRow(2, "bob", 10));
  rows.push_back(makeStubRow(1, "alice", 10));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  TopNOperator top_n(std::move(child),
                     {{2, OrderByDirection::Asc}, {0, OrderByDirection::Asc}},
                     2);

  top_n.open();

  std::optional<TypedRow> first = top_n.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = top_n.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  EXPECT_FALSE(top_n.next().has_value());
  top_n.close();
}