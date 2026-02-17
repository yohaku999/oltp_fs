cmake_minimum_required(VERSION 3.10)
project(dbfs CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(spdlog REQUIRED)

add_library(dbfs_src
    src/bufferpool.cpp
    src/page.cpp
    src/intermediate_cell.cpp
    src/leaf_cell.cpp
    src/record_cell.cpp
)

target_include_directories(dbfs_src PUBLIC src)
target_link_libraries(dbfs_src PUBLIC spdlog::spdlog)

add_executable(bufferpool_test test/bufferpool.cpp)
target_link_libraries(bufferpool_test dbfs_src)

add_executable(cell_test test/cell.cpp)
target_link_libraries(cell_test dbfs_src)

add_executable(page_test test/page.cpp)
target_link_libraries(page_test dbfs_src)

enable_testing()
add_test(NAME BufferPoolTest COMMAND bufferpool_test)
add_test(NAME CellTest COMMAND cell_test)
add_test(NAME PageTest COMMAND page_test)