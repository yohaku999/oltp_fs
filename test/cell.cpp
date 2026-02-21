#include "../src/intermediate_cell.h"
#include "../src/leaf_cell.h"
#include <vector>
#include <gtest/gtest.h>

TEST(CellTest, IntermediateCellRoundTrip)
{
    IntermediateCell original(500, 77777);
    std::vector<std::byte> serialized = original.serialize();

    IntermediateCell decoded = IntermediateCell::decodeCell(reinterpret_cast<char *>(serialized.data()));

    EXPECT_EQ(decoded.key_size(), original.key_size());
    EXPECT_EQ(decoded.page_id(), original.page_id());
    EXPECT_EQ(decoded.key(), original.key());
}

TEST(CellTest, LeafCellRoundTrip)
{
    LeafCell original(11111, 999, 15);
    std::vector<std::byte> serialized = original.serialize();
    LeafCell decoded = LeafCell::decodeCell(reinterpret_cast<char *>(serialized.data()));

    EXPECT_EQ(decoded.heap_page_id(), original.heap_page_id());
    EXPECT_EQ(decoded.slot_id(), original.slot_id());
    EXPECT_EQ(decoded.key(), original.key());
}
