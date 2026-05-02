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
FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
set(gtest_disable_pthreads ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
FetchContent_MakeAvailable(nlohmann_json)

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
    src/storage/runtime/bufferpool.cpp
    src/storage/runtime/file.cpp
    src/storage/page/page.cpp
    src/storage/index/index_page.cpp
    src/storage/index/intermediate_cell.cpp
    src/storage/index/leaf_cell.cpp
    src/storage/record/heap_file_access.cpp
    src/storage/record/record_serializer.cpp
    src/storage/index/btreecursor.cpp
    src/storage/runtime/frame_directory.cpp
    src/storage/wal/lsn_allocator.cpp
    src/storage/wal/wal_record.cpp
    src/storage/wal/wal_body.cpp
    src/storage/wal/wal.cpp
    src/execution/binder.cpp
    src/execution/comparison_predicate.cpp
    src/execution/select_item.cpp
    src/execution/parsers/parser_ast_helpers.cpp
    src/execution/parsers/pg_query_json_parser.cpp
    src/execution/parsers/create_index_parser.cpp
    src/execution/parsers/create_table_parser.cpp
    src/execution/parsers/delete_parser.cpp
    src/execution/parsers/drop_table_parser.cpp
    src/execution/parsers/insert_parser.cpp
    src/execution/parsers/update_parser.cpp
    src/execution/operators/heap_fetch.cpp
    src/execution/executor.cpp
    src/execution/operators/index_scan.cpp
    src/execution/operators/limit_operator.cpp
    src/execution/operators/loop_join_operator.cpp
    src/execution/operators/orderby_operator.cpp
    src/execution/operators/seq_scan_operator.cpp
    src/execution/parsers/select_parser.cpp
    src/catalog/table_metadata.cpp
    src/catalog/table.cpp
    src/logging.cpp
)

target_include_directories(dbfs_src PUBLIC src)
target_link_libraries(dbfs_src PUBLIC spdlog::spdlog nlohmann_json::nlohmann_json pg_query)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(dbfs_src PUBLIC
        -Wall
        -Wextra
        -Wpedantic
    )
elseif (MSVC)
    target_compile_options(dbfs_src PUBLIC /W4)
endif()

add_executable(bufferpool_test test/storage/runtime/bufferpool.cpp)
target_link_libraries(bufferpool_test dbfs_src GTest::gtest_main)

add_executable(cell_test test/storage/page/cell.cpp)
target_link_libraries(cell_test dbfs_src GTest::gtest_main)

add_executable(page_test test/storage/page/page.cpp)
target_link_libraries(page_test dbfs_src GTest::gtest_main)

add_executable(file_test test/storage/runtime/file.cpp)
target_link_libraries(file_test dbfs_src GTest::gtest_main)

add_executable(btreecursor_test test/storage/index/btreecursor.cpp)
target_link_libraries(btreecursor_test dbfs_src GTest::gtest_main)

add_executable(executor_test test/execution/executor.cpp)
target_link_libraries(executor_test dbfs_src GTest::gtest_main)

add_executable(projection_operator_test test/execution/projection_operator.cpp)
target_link_libraries(projection_operator_test dbfs_src GTest::gtest_main)

add_executable(filter_operator_test test/execution/filter_operator.cpp)
target_link_libraries(filter_operator_test dbfs_src GTest::gtest_main)

add_executable(loop_join_operator_test test/execution/loop_join_operator.cpp)
target_link_libraries(loop_join_operator_test dbfs_src GTest::gtest_main)

add_executable(limit_operator_test test/execution/limit_operator.cpp)
target_link_libraries(limit_operator_test dbfs_src GTest::gtest_main)

add_executable(orderby_operator_test test/execution/orderby_operator.cpp)
target_link_libraries(orderby_operator_test dbfs_src GTest::gtest_main)

add_executable(table_test test/catalog/table.cpp)
target_link_libraries(table_test dbfs_src GTest::gtest_main)

add_executable(frame_directory_test test/storage/runtime/frame_directory.cpp)
target_link_libraries(frame_directory_test dbfs_src GTest::gtest_main)

add_executable(wal_body_test test/storage/wal/wal_body.cpp)
target_link_libraries(wal_body_test dbfs_src GTest::gtest_main)

add_executable(wal_record_test test/storage/wal/wal_record.cpp)
target_link_libraries(wal_record_test dbfs_src GTest::gtest_main)

add_executable(lsn_allocator_test test/storage/wal/lsn_allocator.cpp)
target_link_libraries(lsn_allocator_test dbfs_src GTest::gtest_main)
add_executable(wal_test test/storage/wal/wal.cpp)
target_link_libraries(wal_test dbfs_src GTest::gtest_main)

enable_testing()
add_test(NAME BufferPoolTest COMMAND bufferpool_test)
add_test(NAME CellTest COMMAND cell_test)
add_test(NAME PageTest COMMAND page_test)
add_test(NAME FileTest COMMAND file_test)
add_test(NAME BTreeCursorTest COMMAND btreecursor_test)
add_test(NAME FrameDirectoryTest COMMAND frame_directory_test)
add_test(NAME WALBodyTest COMMAND wal_body_test)
add_test(NAME WALRecordTest COMMAND wal_record_test)
add_test(NAME LSNAllocatorTest COMMAND lsn_allocator_test)
add_test(NAME WALTest COMMAND wal_test)
add_test(NAME ExecutorTest COMMAND executor_test)
add_test(NAME ProjectionOperatorTest COMMAND projection_operator_test)
add_test(NAME FilterOperatorTest COMMAND filter_operator_test)
add_test(NAME LimitOperatorTest COMMAND limit_operator_test)
add_test(NAME LoopJoinOperatorTest COMMAND loop_join_operator_test)
add_test(NAME OrderByOperatorTest COMMAND orderby_operator_test)
add_test(NAME TableTest COMMAND table_test)