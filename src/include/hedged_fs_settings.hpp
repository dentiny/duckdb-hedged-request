#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

// Register all hedged filesystem settings
void RegisterHedgedFsSettings(DatabaseInstance &db);

} // namespace duckdb
