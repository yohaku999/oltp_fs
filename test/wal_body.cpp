#include "../src/storage/wal/wal_body.h"

#include <gtest/gtest.h>
#include <vector>
#include <cstddef>
#include <cstdint>

TEST(WALBodyTest, InsertRedoBodyRoundTrip)
{
    InsertRedoBody original;
    original.offset = 123;
    original.tuple = {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};

    std::vector<std::byte> encoded = original.encode();
    InsertRedoBody decoded = InsertRedoBody::decode(encoded);

    EXPECT_EQ(original.offset, decoded.offset);
    ASSERT_EQ(original.tuple.size(), decoded.tuple.size());
    for (std::size_t i = 0; i < original.tuple.size(); ++i)
    {
        EXPECT_EQ(original.tuple[i], decoded.tuple[i]);
    }
}

TEST(WALBodyTest, UpdateRedoBodyRoundTrip)
{
    UpdateRedoBody original;
    original.offset = 42;
    original.before = {std::byte{0x01}, std::byte{0x02}};
    original.after  = {std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};

    std::vector<std::byte> encoded = original.encode();
    UpdateRedoBody decoded = UpdateRedoBody::decode(encoded);

    EXPECT_EQ(original.offset, decoded.offset);
    ASSERT_EQ(original.before.size(), decoded.before.size());
    for (std::size_t i = 0; i < original.before.size(); ++i)
    {
        EXPECT_EQ(original.before[i], decoded.before[i]);
    }
    ASSERT_EQ(original.after.size(), decoded.after.size());
    for (std::size_t i = 0; i < original.after.size(); ++i)
    {
        EXPECT_EQ(original.after[i], decoded.after[i]);
    }
}

TEST(WALBodyTest, DeleteRedoBodyRoundTrip)
{
    DeleteRedoBody original;
    original.offset = 7;
    original.before = {std::byte{0xFF}, std::byte{0x00}, std::byte{0x11}};

    std::vector<std::byte> encoded = original.encode();
    DeleteRedoBody decoded = DeleteRedoBody::decode(encoded);

    EXPECT_EQ(original.offset, decoded.offset);
    ASSERT_EQ(original.before.size(), decoded.before.size());
    for (std::size_t i = 0; i < original.before.size(); ++i)
    {
        EXPECT_EQ(original.before[i], decoded.before[i]);
    }
}
