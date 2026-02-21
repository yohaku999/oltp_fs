#include "../src/frame_directory.h"
#include "../src/page.h"
#include <array>
#include <gtest/gtest.h>

class FrameDirectoryTest : public ::testing::Test
{
protected:
    FrameDirectory directory;
    std::array<char, 4096> page_buffer1;
    std::array<char, 4096> page_buffer2;
    std::array<char, 4096> page_buffer3;
    Page* page1;
    Page* page2;
    Page* page3;

    void SetUp() override
    {
        page1 = Page::initializePage(page_buffer1.data(), true, 0);
        page2 = Page::initializePage(page_buffer2.data(), true, 0);
        page3 = Page::initializePage(page_buffer3.data(), true, 0);
    }
};

TEST_F(FrameDirectoryTest, TestFindFreeFrame)
{
    // use all frames
    for (int i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
        auto frame_opt = directory.findFreeFrame();
        ASSERT_TRUE(frame_opt.has_value());
        EXPECT_GE(frame_opt.value(), 0);
        EXPECT_LT(frame_opt.value(), FrameDirectory::MAX_FRAME_COUNT);
    }
    
    // check api returns nullopt when no free frames
    auto no_frame = directory.findFreeFrame();
    EXPECT_FALSE(no_frame.has_value());
}

TEST_F(FrameDirectoryTest, RegisterAndFindPageByID)
{
    auto frame_opt = directory.findFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    directory.registerPage(frame_id, 100, "test.db", page1);
    
    // search frame by pageID
    auto found_frame = directory.findFrameByPage(100, "test.db");
    ASSERT_TRUE(found_frame.has_value());
    EXPECT_EQ(frame_id, found_frame.value());
    
    // checkinside frame details
    const auto& frame = directory.getFrame(frame_id);
    EXPECT_EQ(page1, frame.page);
    EXPECT_EQ(100, frame.page_id);
    EXPECT_EQ("test.db", frame.file_path);
    EXPECT_EQ(0, frame.pin_count);
}

TEST_F(FrameDirectoryTest, FindNonExistentPageReturnsNullopt)
{
    auto result = directory.findFrameByPage(999, "nonexistent.db");
    EXPECT_FALSE(result.has_value());
}

TEST_F(FrameDirectoryTest, RegisterMultiplePagesInDifferentFrames)
{
    auto frame1 = directory.findFreeFrame();
    auto frame2 = directory.findFreeFrame();
    auto frame3 = directory.findFreeFrame();
    
    ASSERT_TRUE(frame1.has_value());
    ASSERT_TRUE(frame2.has_value());
    ASSERT_TRUE(frame3.has_value());
    
    directory.registerPage(frame1.value(), 10, "file1.db", page1);
    directory.registerPage(frame2.value(), 20, "file2.db", page2);
    directory.registerPage(frame3.value(), 30, "file1.db", page3);
    
    // each page can be found by its pageID and filePath
    auto found1 = directory.findFrameByPage(10, "file1.db");
    auto found2 = directory.findFrameByPage(20, "file2.db");
    auto found3 = directory.findFrameByPage(30, "file1.db");
    
    // ensure reading from the correct frame
    EXPECT_EQ(frame1.value(), found1.value());
    EXPECT_EQ(frame2.value(), found2.value());
    EXPECT_EQ(frame3.value(), found3.value());
}

TEST_F(FrameDirectoryTest, UnregisterPageFreesFrame)
{
    auto frame_opt = directory.findFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    directory.registerPage(frame_id, 100, "test.db", page1);
    
    // check registerd
    auto found = directory.findFrameByPage(100, "test.db");
    ASSERT_TRUE(found.has_value());
    
    // api to test
    directory.unregisterPage(frame_id);
    
    // page not found
    found = directory.findFrameByPage(100, "test.db");
    EXPECT_FALSE(found.has_value());
    
    // frame is free again
    const auto& frame = directory.getFrame(frame_id);
    EXPECT_EQ(nullptr, frame.page);
    EXPECT_EQ(-1, frame.page_id);
}

TEST_F(FrameDirectoryTest, UnpinBelowZeroDoesNothing)
{
    auto frame_opt = directory.findFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    directory.registerPage(frame_id, 100, "test.db", page1);
    
    // 既にpin_count=0の状態でunpinしてもマイナスにならない
    directory.unpin(frame_id);
    EXPECT_EQ(0, directory.getFrame(frame_id).pin_count);
    
    directory.unpin(frame_id);
    EXPECT_EQ(0, directory.getFrame(frame_id).pin_count);
}

TEST_F(FrameDirectoryTest, FindVictimFrameReturnsUnpinnedFrame)
{
    auto frame1 = directory.findFreeFrame();
    auto frame2 = directory.findFreeFrame();
    
    ASSERT_TRUE(frame1.has_value());
    ASSERT_TRUE(frame2.has_value());
    
    directory.registerPage(frame1.value(), 100, "test1.db", page1);
    directory.registerPage(frame2.value(), 200, "test2.db", page2);
    
    // frame1:pinned, frame2:unpinned
    directory.pin(frame1.value());
    
    // victim should be frame2
    auto victim = directory.findVictimFrame();
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(frame2.value(), victim.value());
}

TEST_F(FrameDirectoryTest, FindVictimFrameReturnsNulloptWhenAllPinned)
{
    auto frame1 = directory.findFreeFrame();
    auto frame2 = directory.findFreeFrame();
    
    ASSERT_TRUE(frame1.has_value());
    ASSERT_TRUE(frame2.has_value());
    
    directory.registerPage(frame1.value(), 100, "test1.db", page1);
    directory.registerPage(frame2.value(), 200, "test2.db", page2);
    
    // pin all frames
    directory.pin(frame1.value());
    directory.pin(frame2.value());
    
    // victim cannot be found
    auto victim = directory.findVictimFrame();
    EXPECT_FALSE(victim.has_value());
}

TEST_F(FrameDirectoryTest, CheckOccupiedStatusBeforeAndAfterRegistration)
{
    auto frame_opt = directory.findFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    // before registration: free
    EXPECT_FALSE(directory.getFrame(frame_id).isOccupied());
    
    directory.registerPage(frame_id, 100, "test.db", page1);
    
    // after registration: occupied
    EXPECT_TRUE(directory.getFrame(frame_id).isOccupied());
    
    directory.unregisterPage(frame_id);
    
    // after unregistration: free again
    EXPECT_FALSE(directory.getFrame(frame_id).isOccupied());
}
