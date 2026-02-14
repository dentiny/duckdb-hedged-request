#pragma once

#include <chrono>

namespace duckdb {

// Configuration for hedged request thresholds (when to start a hedged request)
// These values represent the delay before triggering a hedged request, not operation timeouts
struct HedgedRequestConfig {
	// Delay before starting hedged request for OpenFile operation (default: 3 seconds)
	std::chrono::milliseconds open_file_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for Glob operation (default: 5 seconds)
	std::chrono::milliseconds glob_hedging_delay_ms = std::chrono::milliseconds(5000);

	// Delay before starting hedged request for FileExists operation (default: 3 seconds)
	std::chrono::milliseconds file_exists_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for DirectoryExists operation (default: 3 seconds)
	std::chrono::milliseconds directory_exists_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for GetFileSize operation (default: 3 seconds)
	std::chrono::milliseconds get_file_size_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for GetLastModifiedTime operation (default: 3 seconds)
	std::chrono::milliseconds get_last_modified_time_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for GetFileType operation (default: 3 seconds)
	std::chrono::milliseconds get_file_type_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for GetVersionTag operation (default: 3 seconds)
	std::chrono::milliseconds get_version_tag_hedging_delay_ms = std::chrono::milliseconds(3000);

	// Delay before starting hedged request for ListFiles operation (default: 5 seconds)
	std::chrono::milliseconds list_files_hedging_delay_ms = std::chrono::milliseconds(5000);
};

} // namespace duckdb
