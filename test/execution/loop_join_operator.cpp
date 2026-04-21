#include "execution/operators/loop_join_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "stub_row_operator.h"

namespace {

TypedRow makeJoinRow(std::initializer_list<FieldValue> values) {
  return TypedRow{std::vector<FieldValue>(values)};
}

void expectInteger(const FieldValue& value, Column::IntegerType expected) {
  ASSERT_TRUE(std::holds_alternative<Column::IntegerType>(value));
  EXPECT_EQ(std::get<Column::IntegerType>(value), expected);
}

void expectString(const FieldValue& value, const std::string& expected) {
  ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(value));
  EXPECT_EQ(std::get<Column::VarcharType>(value), expected);
}

}  // namespace

TEST(LoopJoinOperatorTest, ReturnsCartesianProductForTwoSources) {
  std::vector<TypedRow> left_rows;
  left_rows.push_back(makeJoinRow({Column::IntegerType(1)}));
  left_rows.push_back(makeJoinRow({Column::IntegerType(2)}));

  std::vector<TypedRow> right_rows;
  right_rows.push_back(makeJoinRow({Column::VarcharType("a")}));
  right_rows.push_back(makeJoinRow({Column::VarcharType("b")}));

  std::vector<std::unique_ptr<Operator>> children;
  children.push_back(std::make_unique<StubRowOperator>(std::move(left_rows)));
  children.push_back(std::make_unique<StubRowOperator>(std::move(right_rows)));

  LoopJoinOperator join(std::move(children));

  join.open();

  std::optional<TypedRow> first = join.next();
  ASSERT_TRUE(first.has_value());
  expectInteger(first->values[0], 1);
  expectString(first->values[1], "a");

  std::optional<TypedRow> second = join.next();
  ASSERT_TRUE(second.has_value());
  expectInteger(second->values[0], 1);
  expectString(second->values[1], "b");

  std::optional<TypedRow> third = join.next();
  ASSERT_TRUE(third.has_value());
  expectInteger(third->values[0], 2);
  expectString(third->values[1], "a");

  std::optional<TypedRow> fourth = join.next();
  ASSERT_TRUE(fourth.has_value());
  expectInteger(fourth->values[0], 2);
  expectString(fourth->values[1], "b");

  EXPECT_FALSE(join.next().has_value());
  join.close();
}

TEST(LoopJoinOperatorTest, SupportsMultipleSources) {
  std::vector<TypedRow> source0_rows;
  source0_rows.push_back(makeJoinRow({Column::IntegerType(1)}));
  source0_rows.push_back(makeJoinRow({Column::IntegerType(2)}));

  std::vector<TypedRow> source1_rows;
  source1_rows.push_back(makeJoinRow({Column::VarcharType("x")}));

  std::vector<TypedRow> source2_rows;
  source2_rows.push_back(makeJoinRow({Column::IntegerType(10)}));
  source2_rows.push_back(makeJoinRow({Column::IntegerType(20)}));

  std::vector<std::unique_ptr<Operator>> children;
  children.push_back(std::make_unique<StubRowOperator>(std::move(source0_rows)));
  children.push_back(std::make_unique<StubRowOperator>(std::move(source1_rows)));
  children.push_back(std::make_unique<StubRowOperator>(std::move(source2_rows)));

  LoopJoinOperator join(std::move(children));

  join.open();

  std::optional<TypedRow> first = join.next();
  ASSERT_TRUE(first.has_value());
  expectInteger(first->values[0], 1);
  expectString(first->values[1], "x");
  expectInteger(first->values[2], 10);

  std::optional<TypedRow> second = join.next();
  ASSERT_TRUE(second.has_value());
  expectInteger(second->values[0], 1);
  expectString(second->values[1], "x");
  expectInteger(second->values[2], 20);

  std::optional<TypedRow> third = join.next();
  ASSERT_TRUE(third.has_value());
  expectInteger(third->values[0], 2);
  expectString(third->values[1], "x");
  expectInteger(third->values[2], 10);

  std::optional<TypedRow> fourth = join.next();
  ASSERT_TRUE(fourth.has_value());
  expectInteger(fourth->values[0], 2);
  expectString(fourth->values[1], "x");
  expectInteger(fourth->values[2], 20);

  EXPECT_FALSE(join.next().has_value());
  join.close();
}

TEST(LoopJoinOperatorTest, ReturnsNoRowsWhenAnySourceIsEmpty) {
  std::vector<TypedRow> left_rows;
  left_rows.push_back(makeJoinRow({Column::IntegerType(1)}));

  std::vector<TypedRow> empty_rows;

  std::vector<std::unique_ptr<Operator>> children;
  children.push_back(std::make_unique<StubRowOperator>(std::move(left_rows)));
  children.push_back(std::make_unique<StubRowOperator>(std::move(empty_rows)));

  LoopJoinOperator join(std::move(children));

  join.open();
  EXPECT_FALSE(join.next().has_value());
  join.close();
}