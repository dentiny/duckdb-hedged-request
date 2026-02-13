#include "hedged_request_fs_entry.hpp"

#include <algorithm>

namespace duckdb {

HedgedRequestFsEntry::HedgedRequestFsEntry() {
}

HedgedRequestFsEntry::~HedgedRequestFsEntry() {
	WaitAll();
}

void HedgedRequestFsEntry::AddPendingRequest(std::future<void> future) {
	const lock_guard<mutex> lock(cache_mutex);
	pending_requests.emplace_back(std::move(future));
	CleanupCompleted();
}

void HedgedRequestFsEntry::CleanupCompleted() {
	pending_requests.erase(
	    std::remove_if(pending_requests.begin(), pending_requests.end(),
	                   [](std::future<void> &f) {
		                   return f.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
	                   }),
	    pending_requests.end());
}

void HedgedRequestFsEntry::WaitAll() {
	const lock_guard<mutex> lock(cache_mutex);
	for (auto &future : pending_requests) {
		if (future.valid()) {
			future.wait();
		}
	}
	pending_requests.clear();
}

} // namespace duckdb

