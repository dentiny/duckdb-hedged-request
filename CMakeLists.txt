cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME hedged_request_fs)

set(CMAKE_CXX_STANDARD 14)

if(CMAKE_CXX_COMPILER_ID_LOWER MATCHES "clang" OR CMAKE_CXX_COMPILER_LOWER
                                                  MATCHES "clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=thread-safety")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/hedged_request_fs_extension.cpp src/hedged_file_system.cpp
    src/hedged_request_fs_entry.cpp src/mock_file_system.cpp
    src/hedged_fs_functions.cpp src/hedged_fs_settings.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
