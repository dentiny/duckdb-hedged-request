cmake_minimum_required(VERSION 3.5)

project(hedged_filesystem_test)

# Include DuckDB headers
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)

add_executable(
  test_hedged_filesystem test_hedged_filesystem.cpp
                         ${CMAKE_SOURCE_DIR}/src/hedged_file_system.cpp)

# Link against DuckDB library
target_link_libraries(test_hedged_filesystem duckdb_static pthread)
