#include "../src/storage/lsn_allocator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <thread>
#include <vector>

TEST(LSNAllocatorTest, SingleThreadSequential)
{
    LSNAllocator alloc(0);

    auto a = alloc.allocate(100);
    auto b = alloc.allocate(200);

    EXPECT_EQ(a, 0);
    EXPECT_EQ(b, 100);
    EXPECT_EQ(alloc.current(), 300);
}

TEST(LSNAllocatorTest, MultiThreadAllocationsAreUniqueAndCoverRange)
{
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kPerThread = 1000;
    constexpr std::size_t kTotal = kThreads * kPerThread;

    LSNAllocator alloc(0);

    struct Allocation {
        LSNAllocator::value_type start;
        std::size_t size;
    };

    std::vector<Allocation> results(kTotal);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t)
    {
        threads.emplace_back([t, &alloc, &results]() {
            const std::size_t base = t * kPerThread;
            for (std::size_t i = 0; i < kPerThread; ++i)
            {
                const std::size_t index = base + i;
                // Vary record_size_bytes so that we also test that
                // allocate() advances by the requested size.
                const std::size_t size = (index % 4) + 1;
                const auto start = alloc.allocate(size);
                results[index] = Allocation{start, size};
            }
        });
    }

    for (auto &th : threads)
    {
        th.join();
    }

    // All allocated regions [start, start+size) should be disjoint and cover
    // a contiguous range [0, alloc.current()).
    std::sort(results.begin(), results.end(), [](const Allocation& a, const Allocation& b) {
        return a.start < b.start;
    });

    LSNAllocator::value_type offset = 0;
    for (const auto& r : results)
    {
        EXPECT_EQ(r.start, offset);
        offset += static_cast<LSNAllocator::value_type>(r.size);
    }

    EXPECT_EQ(offset, alloc.current());
}
