#include "../src/bufferpool.h"
#include <cassert>
#include <iostream>
#include <cstring>
#include <cstdio>

class BufferPoolTest
{
private:
    static constexpr const char *TEST_FILE = "testfile.db";
    BufferPool *pool = nullptr;

    void setUp()
    {
        std::remove(TEST_FILE);
        pool = new BufferPool(TEST_FILE);
    }

    void tearDown()
    {
        delete pool;
        pool = nullptr;
        std::remove(TEST_FILE);
    }

public:
    void runAll()
    {
        // getPage tests
        getPage_NewPage_ReturnsNonNull();
        getPage_SamePage_ReturnsCachedPage();
        getPage_DifferentPages_ReturnsDifferentObjects();
        getPage_AllFramesFilled_Success();
        getPage_NoFreeFrame_ThrowsException();

        std::cout << "All tests passed." << std::endl;
    }

    // ========== getPage tests ==========

    void getPage_NewPage_ReturnsNonNull()
    {
        setUp();
        Page *page = pool->getPage(1);
        assert(page != nullptr);
        tearDown();
        std::cout << "getPage_NewPage_ReturnsNonNull: OK" << std::endl;
    }

    void getPage_SamePage_ReturnsCachedPage()
    {
        setUp();
        Page *page1 = pool->getPage(1);
        Page *page1_again = pool->getPage(1);
        assert(page1 == page1_again);
        tearDown();
        std::cout << "getPage_SamePage_ReturnsCachedPage: OK" << std::endl;
    }

    void getPage_DifferentPages_ReturnsDifferentObjects()
    {
        setUp();
        Page *page1 = pool->getPage(1);
        Page *page2 = pool->getPage(2);
        assert(page1 != page2);
        tearDown();
        std::cout << "getPage_DifferentPages_ReturnsDifferentObjects: OK" << std::endl;
    }

    void getPage_AllFramesFilled_Success()
    {
        setUp();
        for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
        {
            Page *p = pool->getPage(i);
            assert(p != nullptr);
        }
        tearDown();
        std::cout << "getPage_AllFramesFilled_Success: OK" << std::endl;
    }

    void getPage_NoFreeFrame_ThrowsException()
    {
        setUp();
        for (size_t i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
        {
            pool->getPage(i);
        }
        try
        {
            pool->getPage(BufferPool::MAX_FRAME_COUNT);
            assert(false);
        }
        catch (const std::runtime_error &e)
        {
            // Expected
        }
        tearDown();
        std::cout << "getPage_NoFreeFrame_ThrowsException: OK" << std::endl;
    }
};

int main()
{
    BufferPoolTest test;
    test.runAll();
    return 0;
}
