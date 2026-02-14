#pragma once

#include "duckdb/common/array.hpp"

#include <chrono>

namespace duckdb {

enum class HedgedRequestOperation : size_t {
	OPEN_FILE = 0,
	GLOB = 1,
	FILE_EXISTS = 2,
	DIRECTORY_EXISTS = 3,
	GET_FILE_SIZE = 4,
	GET_LAST_MODIFIED_TIME = 5,
	GET_FILE_TYPE = 6,
	GET_VERSION_TAG = 7,
	LIST_FILES = 8,
	COUNT
};

// Default hedging delays in milliseconds for each operation
constexpr duckdb::array<int64_t, static_cast<size_t>(HedgedRequestOperation::COUNT)> DEFAULT_HEDGING_DELAYS_MS = {
    3000, // OPEN_FILE
    5000, // GLOB
    3000, // FILE_EXISTS
    3000, // DIRECTORY_EXISTS
    3000, // GET_FILE_SIZE
    3000, // GET_LAST_MODIFIED_TIME
    3000, // GET_FILE_TYPE
    3000, // GET_VERSION_TAG
    5000  // LIST_FILES
};

// Configuration for hedged request thresholds (when to start a hedged request)
// These values represent the delay before triggering a hedged request, not operation timeouts
struct HedgedRequestConfig {
	// Delay before starting hedged request for each operation (in milliseconds)
	duckdb::array<std::chrono::milliseconds, static_cast<size_t>(HedgedRequestOperation::COUNT)> delays_ms;

	HedgedRequestConfig() {
		for (size_t idx = 0; idx < static_cast<size_t>(HedgedRequestOperation::COUNT); ++idx) {
			delays_ms[idx] = std::chrono::milliseconds(DEFAULT_HEDGING_DELAYS_MS[idx]);
		}
	}
};

} // namespace duckdb
