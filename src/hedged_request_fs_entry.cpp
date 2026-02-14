#include "hedged_request_fs_entry.hpp"

#include <algorithm>

namespace duckdb {

HedgedRequestFsEntry::HedgedRequestFsEntry() {
}

HedgedRequestFsEntry::~HedgedRequestFsEntry() {
	WaitAll();
}

void HedgedRequestFsEntry::AddPendingRequest(FutureWrapper<void> future) {
	const lock_guard<mutex> lock(cache_mutex);
	pending_requests.push_back(std::move(future));
	CleanupCompleted();
}

void HedgedRequestFsEntry::CleanupCompleted() {
	pending_requests.erase(std::remove_if(pending_requests.begin(), pending_requests.end(),
	                                      [](FutureWrapper<void> &f) { return f.IsReady(); }),
	                       pending_requests.end());
}

void HedgedRequestFsEntry::WaitAll() {
	const lock_guard<mutex> lock(cache_mutex);
	for (auto &future : pending_requests) {
		future.Wait();
	}
	pending_requests.clear();
}

} // namespace duckdb
