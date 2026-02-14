#pragma once

namespace duckdb {

// Forward declaration
class DatabaseInstance;

// Register all hedged filesystem settings
void RegisterHedgedFsSettings(DatabaseInstance &db);

} // namespace duckdb
