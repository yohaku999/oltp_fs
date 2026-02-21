cmake_minimum_required(VERSION 3.10)
project(dbfs CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(spdlog REQUIRED)

include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_disable_pthreads ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_library(dbfs_src
    src/bufferpool.cpp
    src/file.cpp
    src/page.cpp
    src/intermediate_cell.cpp
    src/leaf_cell.cpp
    src/record_cell.cpp
    src/btreecursor.cpp
)

target_include_directories(dbfs_src PUBLIC src)
target_link_libraries(dbfs_src PUBLIC spdlog::spdlog)

add_executable(bufferpool_test test/bufferpool.cpp)
target_link_libraries(bufferpool_test dbfs_src GTest::gtest_main)

add_executable(cell_test test/cell.cpp)
target_link_libraries(cell_test dbfs_src GTest::gtest_main)

add_executable(page_test test/page.cpp)
target_link_libraries(page_test dbfs_src GTest::gtest_main)

add_executable(file_test test/file.cpp)
target_link_libraries(file_test dbfs_src GTest::gtest_main)

add_executable(btreecursor_test test/btreecursor.cpp)
target_link_libraries(btreecursor_test dbfs_src GTest::gtest_main)

enable_testing()
add_test(NAME BufferPoolTest COMMAND bufferpool_test)
add_test(NAME CellTest COMMAND cell_test)
add_test(NAME PageTest COMMAND page_test)
add_test(NAME FileTest COMMAND file_test)
add_test(NAME BTreeCursorTest COMMAND btreecursor_test)