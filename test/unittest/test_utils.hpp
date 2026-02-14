#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Util function to create a test file with the given content
void CreateTestFile(const string &path, const string &content);

} // namespace duckdb
