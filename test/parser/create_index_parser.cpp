#include <gtest/gtest.h>

#include "parser/create_index_parser.h"

TEST(CreateIndexParserTest, ExtractsNonUniqueIndex) {
  const CreateIndexParser parser("CREATE INDEX idx_a ON records (a)");

  EXPECT_FALSE(parser.isUnique());
}

TEST(CreateIndexParserTest, ExtractsUniqueIndex) {
  const CreateIndexParser parser("CREATE UNIQUE INDEX idx_a ON records (a)");

  EXPECT_TRUE(parser.isUnique());
}
