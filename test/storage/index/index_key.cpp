#include "storage/index/index_key.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "storage/index/btreecursor.h"

namespace {

TEST(IndexKeyTest, IntegerEncodingPreservesNumericOrder) {
  const std::vector<int> values = {-100, -1, 0, 1, 42, 1000};

  for (std::size_t index = 1; index < values.size(); ++index) {
    const std::string previous = index_key::encodeInteger(values[index - 1]);
    const std::string current = index_key::encodeInteger(values[index]);
    EXPECT_LT(index_key::compare(previous, current), 0)
        << values[index - 1] << " should sort before " << values[index];
  }
}

TEST(IndexKeyTest, DoubleEncodingPreservesNumericOrder) {
  const std::vector<double> values = {-100.5, -0.25, 0.0, 0.25, 3.5, 100.125};

  for (std::size_t index = 1; index < values.size(); ++index) {
    const std::string previous = index_key::encodeDouble(values[index - 1]);
    const std::string current = index_key::encodeDouble(values[index]);
    EXPECT_LT(index_key::compare(previous, current), 0)
        << values[index - 1] << " should sort before " << values[index];
  }
}

TEST(IndexKeyTest, VarcharEncodingPreservesLexicographicOrder) {
  const std::vector<std::string> values = {"alpha", "alphabet", "beta"};

  for (std::size_t index = 1; index < values.size(); ++index) {
    const std::string previous = index_key::encodeVarchar(values[index - 1]);
    const std::string current = index_key::encodeVarchar(values[index]);
    EXPECT_LT(index_key::compare(previous, current), 0)
        << values[index - 1] << " should sort before " << values[index];
  }
}

TEST(IndexKeyTest, VarcharEncodingEscapesEmbeddedNullBytes) {
  const std::string prefix = "ab";
  const std::string with_null = prefix + std::string("\0", 1);
  const std::string with_suffix = prefix + "c";

  EXPECT_LT(index_key::compare(index_key::encodeVarchar(prefix),
                               index_key::encodeVarchar(with_null)),
            0);
  EXPECT_LT(index_key::compare(index_key::encodeVarchar(with_null),
                               index_key::encodeVarchar(with_suffix)),
            0);
}

TEST(IndexKeyTest, CompositeRowEncodingFollowsColumnOrder) {
  const Schema schema(std::vector<Column>{
      Column("warehouse_id", Column::Type::Integer),
      Column("district_id", Column::Type::Integer),
      Column("name", Column::Type::Varchar),
  });
  const std::vector<std::size_t> column_indexes = {0, 1, 2};

  const TypedRow lhs{{1, 2, std::string("alpha")}};
  const TypedRow rhs{{1, 3, std::string("alpha")}};
  const TypedRow third{{1, 3, std::string("beta")}};

  const std::string lhs_key = index_key::encodeRow(schema, lhs, column_indexes);
  const std::string rhs_key = index_key::encodeRow(schema, rhs, column_indexes);
  const std::string third_key =
      index_key::encodeRow(schema, third, column_indexes);

  EXPECT_LT(index_key::compare(lhs_key, rhs_key), 0);
  EXPECT_LT(index_key::compare(rhs_key, third_key), 0);
}

TEST(IndexKeyTest, PrefixMatchesSupportRangeOperators) {
  const std::string prefix = std::string("I") + index_key::encodeInteger(1) +
                             std::string("I") + index_key::encodeInteger(7);
  const std::string matching =
      prefix + std::string("I") + index_key::encodeInteger(3001);
  const std::string previous = std::string("I") + index_key::encodeInteger(1) +
                               std::string("I") + index_key::encodeInteger(6) +
                               std::string("I") +
                               index_key::encodeInteger(9999);
  const std::string next = std::string("I") + index_key::encodeInteger(1) +
                           std::string("I") + index_key::encodeInteger(8);

  const BTreeCursor::Boundary inclusive{prefix, true};
  const BTreeCursor::Boundary exclusive{prefix, false};

  EXPECT_TRUE(BTreeCursor::isInsideBoundary(matching, inclusive, true));
  EXPECT_TRUE(BTreeCursor::isInsideBoundary(matching, inclusive, false));
  EXPECT_FALSE(BTreeCursor::isInsideBoundary(matching, exclusive, true));
  EXPECT_FALSE(BTreeCursor::isInsideBoundary(matching, exclusive, false));
  EXPECT_TRUE(BTreeCursor::isInsideBoundary(previous, exclusive, false));
  EXPECT_TRUE(BTreeCursor::isInsideBoundary(next, exclusive, true));
}

TEST(IndexKeyTest, FieldEncodingRejectsNullValues) {
  EXPECT_THROW(index_key::encodeFieldValue(FieldValue(std::monostate{}),
                                           Column::Type::Integer),
               std::runtime_error);
}

}  // namespace
