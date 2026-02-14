#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Table function: hedged_fs_list_filesystems()
// Lists all registered filesystems in the virtual file system.
// Columns: name VARCHAR
TableFunction GetHedgedFsListFilesystemsFunction();

// Scalar function: hedged_fs_wrap(filesystem_name VARCHAR) -> BOOLEAN
// Wrap the requested filesystem with the hedged filesystem.
// Throws an error if the requested filesystem does not exist.
ScalarFunction GetHedgedFsWrapFunction();

} // namespace duckdb
