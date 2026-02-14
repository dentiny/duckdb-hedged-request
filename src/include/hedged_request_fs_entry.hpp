#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "future_utils.hpp"
#include "hedged_request_config.hpp"
#include "thread_annotation.hpp"

namespace duckdb {

// Cache to manage pending requests that didn't win the hedged race
class HedgedRequestFsEntry : public ObjectCacheEntry {
public:
	HedgedRequestFsEntry();
	~HedgedRequestFsEntry() override;

	string GetObjectType() override {
		return "hedged_request_fs_entry";
	}

	static string ObjectType() {
		return "hedged_request_fs_entry";
	}

	// Queue pending operations; functor represents a type-eraused operation.
	void AddPendingRequest(std::function<void()> functor);

	// Block wait for all pending requests to complete.
	void WaitAll();

	// Get the hedged request configuration
	HedgedRequestConfig GetConfig() const;

	// Update the hedged request configuration
	void SetConfig(const HedgedRequestConfig &config);

	// Update a specific operation's delay threshold directly
	void UpdateConfig(HedgedRequestOperation operation, std::chrono::milliseconds delay_ms);

private:
	// Try to clean up completed requests in a non-blocking style.
	void CleanupCompleted() DUCKDB_REQUIRES(cache_mutex);

	mutable mutex cache_mutex;
	vector<FutureWrapper<void>> pending_requests DUCKDB_GUARDED_BY(cache_mutex);
	HedgedRequestConfig config DUCKDB_GUARDED_BY(cache_mutex);
};

} // namespace duckdb
