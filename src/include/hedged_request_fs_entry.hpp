#pragma once

#include "future_utils.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"

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

	void AddPendingRequest(FutureWrapper<void> future);

private:
	// Try to clean up completed requests in a non-blocking style.
	void CleanupCompleted();

	// Block wait for all pending requests to complete.
	void WaitAll();

	mutex cache_mutex;
	vector<FutureWrapper<void>> pending_requests;
};

} // namespace duckdb
