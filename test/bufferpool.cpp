#include "../src/bufferpool.h"
#include <cassert>
#include <iostream>

class BufferPoolTest
{
public:
    static void run()
    {
        BufferPool pool("testfile.db");

        // get first page
        Page *page1 = pool.getPage(1);
        assert(page1 != nullptr);

        // get the same page again, should be cached
        Page *page1_again = pool.getPage(1);
        assert(page1_again == page1);

        // get different page
        Page *page2 = pool.getPage(2);
        assert(page2 != nullptr);
        assert(page2 != page1);

        // fill up all frames
        for (int i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
        {
            Page *p = pool.getPage(i);
            assert(p != nullptr);
        }

        // check no free frame exception
        try
        {
            pool.getPage(BufferPool::MAX_FRAME_COUNT);
            assert(false);
        }
        catch (const std::runtime_error &e)
        {
        }
    }
};

int main()
{
    BufferPoolTest::run();
    return 0;
}
