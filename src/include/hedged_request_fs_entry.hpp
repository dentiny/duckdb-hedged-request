#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "hedged_request_config.hpp"
#include "thread_pool.hpp"

#include <future>
#include <string_view>

namespace duckdb {

// Cache to manage pending requests that didn't win the hedged race
class HedgedRequestFsEntry : public ObjectCacheEntry {
public:
	static constexpr std::string_view OBJECT_TYPE = "hedged_request_fs_entry";

	HedgedRequestFsEntry();
	~HedgedRequestFsEntry() override;

	optional_idx GetEstimatedCacheMemory() const override;

	string GetObjectType() override {
		return string(OBJECT_TYPE);
	}

	static string ObjectType() {
		return string(OBJECT_TYPE);
	}

	// Queue pending operations; functor represents a type-eraused operation.
	void AddPendingRequest(std::function<void()> functor);

	// Block wait for all pending requests to complete.
	void WaitAll();

	// Get the hedged request configuration
	HedgedRequestConfig GetConfig() const;

	// Update a specific operation's delay threshold directly
	void UpdateConfig(HedgedRequestOperation operation, std::chrono::milliseconds delay_ms);

	// Update the maximum number of hedged requests to spawn
	void UpdateMaxHedgedRequestCount(size_t max_count);

	// Per-database thread pool.
	ThreadPool &GetThreadPool() {
		return thread_pool;
	}

private:
	// Try to clean up completed requests in a non-blocking style.
	void CleanupCompleted() DUCKDB_REQUIRES(cache_mutex);

	mutable annotated_mutex cache_mutex;
	vector<std::future<void>> pending_requests DUCKDB_GUARDED_BY(cache_mutex);
	HedgedRequestConfig config DUCKDB_GUARDED_BY(cache_mutex);
	ThreadPool thread_pool;
};

} // namespace duckdb
