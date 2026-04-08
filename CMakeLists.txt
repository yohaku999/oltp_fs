cmake_minimum_required(VERSION 3.10)
project(dbfs CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(spdlog REQUIRED)

include(FetchContent)
include(ExternalProject)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_disable_pthreads ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# External libpg_query (PostgreSQL parser as a C library)
ExternalProject_Add(libpg_query_proj
    GIT_REPOSITORY https://github.com/pganalyze/libpg_query.git
    GIT_TAG v4.2.0
    PREFIX ${CMAKE_BINARY_DIR}/libpg_query
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE 1
    UPDATE_DISCONNECTED 1
)

set(LIBPG_QUERY_INCLUDE_DIR ${CMAKE_BINARY_DIR}/libpg_query/src/libpg_query_proj)
set(LIBPG_QUERY_LIBRARY ${CMAKE_BINARY_DIR}/libpg_query/src/libpg_query_proj/libpg_query.a)

add_library(pg_query STATIC IMPORTED GLOBAL)
set_target_properties(pg_query PROPERTIES
    IMPORTED_LOCATION ${LIBPG_QUERY_LIBRARY}
    INTERFACE_INCLUDE_DIRECTORIES ${LIBPG_QUERY_INCLUDE_DIR}
)

add_dependencies(pg_query libpg_query_proj)

add_library(dbfs_src
    src/storage/bufferpool.cpp
    src/storage/file.cpp
    src/storage/page.cpp
    src/storage/index_page.cpp
    src/storage/intermediate_cell.cpp
    src/storage/leaf_cell.cpp
    src/storage/record_builder.cpp
    src/storage/btreecursor.cpp
    src/storage/frame_directory.cpp
    src/executor/heap_fetch.cpp
    src/executor/executor.cpp
    src/executor/index_scan.cpp
    src/logging.cpp
)

target_include_directories(dbfs_src PUBLIC src src/storage)
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

add_executable(executor_test test/executor.cpp)
target_link_libraries(executor_test dbfs_src GTest::gtest_main)

add_executable(frame_directory_test test/frame_directory.cpp)
target_link_libraries(frame_directory_test dbfs_src GTest::gtest_main)

add_executable(e2e_test test/e2e.cpp)
target_link_libraries(e2e_test dbfs_src GTest::gtest_main pg_query)

enable_testing()
add_test(NAME BufferPoolTest COMMAND bufferpool_test)
add_test(NAME CellTest COMMAND cell_test)
add_test(NAME PageTest COMMAND page_test)
add_test(NAME FileTest COMMAND file_test)
add_test(NAME BTreeCursorTest COMMAND btreecursor_test)
add_test(NAME FrameDirectoryTest COMMAND frame_directory_test)
add_test(NAME ExecutorTest COMMAND executor_test)
add_test(NAME E2ESelectTest COMMAND e2e_test)