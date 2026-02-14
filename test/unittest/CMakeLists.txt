include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(
  unittest_hedged_file_system
  main.cpp
  ${CATCHFS_UNITTEST_OBJECTS}
  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/mock_file_system.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/hedged_file_system.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/hedged_request_fs_entry.cpp)

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_hedged_file_system test_helpers duckdb)
else()
  target_link_libraries(unittest_hedged_file_system test_helpers duckdb_static)
endif()
