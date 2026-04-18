#include "execution/operators/orderby_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "stub_row_operator.h"

TEST(OrderByOperatorTest, SortsRowsBySingleColumnAscending) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(3, "carol", 30));
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  OrderByOperator order_by(
      std::move(child), {{0, OrderByDirection::Asc}});

  order_by.open();

  std::optional<TypedRow> first = order_by.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = order_by.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  std::optional<TypedRow> third = order_by.next();
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(third->values[0]), 3);

  EXPECT_FALSE(order_by.next().has_value());
  order_by.close();
}

TEST(OrderByOperatorTest, UsesLaterColumnsAsTieBreaker) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(2, "bob", 10));
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(3, "carol", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  OrderByOperator order_by(
      std::move(child),
      {{2, OrderByDirection::Asc}, {0, OrderByDirection::Asc}});

  order_by.open();

  std::optional<TypedRow> first = order_by.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = order_by.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  std::optional<TypedRow> third = order_by.next();
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(third->values[0]), 3);

  EXPECT_FALSE(order_by.next().has_value());
  order_by.close();
}

TEST(OrderByOperatorTest, SortsRowsBySingleColumnDescending) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(3, "carol", 30));
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  OrderByOperator order_by(
      std::move(child), {{0, OrderByDirection::Desc}});

  order_by.open();

  std::optional<TypedRow> first = order_by.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 3);

  std::optional<TypedRow> second = order_by.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  std::optional<TypedRow> third = order_by.next();
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(third->values[0]), 1);

  EXPECT_FALSE(order_by.next().has_value());
  order_by.close();
}