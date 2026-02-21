#include "../src/file.h"
#include "../src/page.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <vector>

class FileTest
{
public:

    void runAll()
    {
        setUp();
        testAllocateAndUsage();

        setUp();
        testWriteAndLoadPage();

        setUp();
        testLoadMaxPageIdFromHeader();

        tearDown();
        std::cout << "All File tests passed!" << std::endl;
    }

private:
    const std::string test_file_path_ = "file_test_temp.db";
    std::unique_ptr<File> file_p;

    void setUp()
    {
        // delete test file if exists.
        std::remove(test_file_path_.c_str());
        std::ofstream ofs(test_file_path_, std::ios::binary | std::ios::trunc);
        ofs.close();
        // initialize File object.
        file_p = std::make_unique<File>(test_file_path_);
    }

    void tearDown()
    {
        file_p.reset();
        std::remove(test_file_path_.c_str());
    }

    void testAllocateAndUsage()
    {
        assert(file_p->isPageIDUsed(0));
        uint16_t next = file_p->allocateNextPageId();
        assert(next == 1);
        assert(file_p->isPageIDUsed(1));
        assert(!file_p->isPageIDUsed(2));
        std::cout << "testAllocateAndUsage passed" << std::endl;
    }

    void testWriteAndLoadPage()
    {
        std::vector<char> write_buffer(Page::PAGE_SIZE_BYTE);
        for (size_t i = 0; i < write_buffer.size(); ++i)
        {
            write_buffer[i] = static_cast<char>(i % 256);
        }

        file_p->writePageOnFile(1, write_buffer.data());

        std::vector<char> read_buffer(Page::PAGE_SIZE_BYTE);
        file_p->loadPageOnFrame(1, read_buffer.data());

        assert(std::memcmp(write_buffer.data(), read_buffer.data(), Page::PAGE_SIZE_BYTE) == 0);
        std::cout << "testWriteAndLoadPage passed" << std::endl;
    }

    void testLoadMaxPageIdFromHeader()
    {
        constexpr uint16_t persisted_max = 42;

        // modify test file with persisted max_page_id in header.
        std::vector<char> header(File::HEADDER_SIZE_BYTE, 0);
        std::memcpy(header.data(), &persisted_max, sizeof(persisted_max));
        std::ofstream ofs(test_file_path_, std::ios::binary | std::ios::trunc);
        ofs.write(header.data(), header.size());
        ofs.close();

        // close file to reload cache on file object.
        file_p.reset();
        // read the file and check.
        file_p = std::make_unique<File>(test_file_path_);
        assert(file_p->isPageIDUsed(persisted_max));
        assert(!file_p->isPageIDUsed(persisted_max + 1));
        uint16_t next = file_p->allocateNextPageId();
        std::cout << next << std::endl;
        std::cout << persisted_max+1 << std::endl;
        assert(next == persisted_max + 1);
        assert(file_p->isPageIDUsed(next));
        std::cout << "testLoadMaxPageIdFromHeader passed" << std::endl;
    }
};

int main()
{
    FileTest test;
    test.runAll();
    return 0;
}
