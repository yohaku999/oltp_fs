cmake_minimum_required(VERSION 3.10)
project(dbfs CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(spdlog REQUIRED)

add_library(dbfs_src
    src/bufferpool.cpp
    src/page.cpp
    src/intermediate_cell.cpp
    src/leaf_cell.cpp
)

target_include_directories(dbfs_src PUBLIC src)
target_link_libraries(dbfs_src PUBLIC spdlog::spdlog)

add_executable(bufferpool_test test/bufferpool.cpp)
target_link_libraries(bufferpool_test dbfs_src)

enable_testing()
add_test(NAME BufferPoolTest COMMAND bufferpool_test)