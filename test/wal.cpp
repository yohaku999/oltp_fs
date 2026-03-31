#include "../src/storage/wal/wal.h"
#include "../src/storage/wal_record.h"

#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <thread>
#include <vector>

class WALTest : public ::testing::Test
{
protected:
    const char* wal_path = "testwal.log";

    void SetUp() override
    {
        std::remove(wal_path);
    }

    void TearDown() override
    {
        std::remove(wal_path);
    }
};

TEST_F(WALTest, FlushedLSNFollowsLastRecordLSN)
{
    LSNAllocator allocator(0);
    WAL wal(wal_path);

    std::vector<std::byte> body1 = {std::byte{0x01}, std::byte{0x02}};
    std::vector<std::byte> body2 = {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};

    WALRecord rec1 = make_wal_record(allocator, WALRecord::RecordType::INSERT, 1, body1);
    WALRecord rec2 = make_wal_record(allocator, WALRecord::RecordType::DELETE, 2, body2);

    wal.write(rec1);
    wal.write(rec2);

    // make sure it is flushed.
    wal.flush();

    auto flushed = wal.getFlushedLSN();
    EXPECT_EQ(flushed, rec2.get_lsn());
}