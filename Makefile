PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hedged_request_fs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all: format
	find test/unittest -iname '*.hpp' -o -iname '*.cpp' | xargs clang-format-19 --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

PHONY: format-all
