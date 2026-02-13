#include "hedged_file_handle.hpp"
#include "hedged_file_system.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

HedgedFileHandle::HedgedFileHandle(HedgedFileSystem &fs, unique_ptr<FileHandle> wrapped_handle, const string &path)
    : FileHandle(fs, path, wrapped_handle->GetFlags()), hedged_fs(fs), wrapped_handle(std::move(wrapped_handle)) {
}

HedgedFileHandle::~HedgedFileHandle() {
}

void HedgedFileHandle::Close() {
	lock_guard<mutex> lock(handle_mutex);
	if (wrapped_handle) {
		wrapped_handle->Close();
	}
}

int64_t HedgedFileHandle::HedgedRead(void *buffer, int64_t nr_bytes) {
	lock_guard<mutex> lock(handle_mutex);

	auto timeout = hedged_fs.timeout;
	auto current_position = hedged_fs.wrapped_fs->SeekPosition(*wrapped_handle);

	// Start the primary read
	auto primary_task = std::async(std::launch::async, [&, current_pos = current_position]() {
		try {
			auto bytes = hedged_fs.wrapped_fs->Read(*wrapped_handle, buffer, nr_bytes);
			return bytes;
		} catch (...) {
			throw;
		}
	});

	// Wait for timeout or completion
	auto status = primary_task.wait_for(timeout);

	if (status == std::future_status::ready) {
		// Primary read completed in time - return immediately
		try {
			return primary_task.get();
		} catch (...) {
			throw;
		}
	}

	// Primary read timed out, start hedged read
	// Allocate a separate buffer for the hedged read
	auto hedged_buffer = make_shared_ptr<vector<char>>(nr_bytes);

	auto hedged_task = std::async(std::launch::async, [&, hedged_buffer, current_position]() -> int64_t {
		try {
			// Open a new file handle for the hedged read
			auto hedged_handle = hedged_fs.wrapped_fs->OpenFile(path, wrapped_handle->GetFlags(), nullptr);

			// Seek to the same position
			hedged_fs.wrapped_fs->Seek(*hedged_handle, current_position);

			// Perform the read
			auto bytes = hedged_fs.wrapped_fs->Read(*hedged_handle, hedged_buffer->data(), nr_bytes);

			// Copy the data to the output buffer
			memcpy(buffer, hedged_buffer->data(), bytes);
			return bytes;
		} catch (...) {
			throw;
		}
	});

	// Race between primary and hedged read
	// Check both futures in a loop until one completes
	while (true) {
		// Check primary
		status = primary_task.wait_for(std::chrono::milliseconds(10));
		if (status == std::future_status::ready) {
			// Primary won - add hedged to cache and return
			hedged_fs.cache->AddPendingRequest(
			    std::async(std::launch::async, [task = std::move(hedged_task)]() mutable {
				    try {
					    task.wait();
				    } catch (...) {
					    // Ignore errors from losing read
				    }
			    }));
			try {
				return primary_task.get();
			} catch (...) {
				throw;
			}
		}

		// Check hedged
		status = hedged_task.wait_for(std::chrono::milliseconds(10));
		if (status == std::future_status::ready) {
			// Hedged won - add primary to cache and return
			hedged_fs.cache->AddPendingRequest(
			    std::async(std::launch::async, [task = std::move(primary_task)]() mutable {
				    try {
					    task.wait();
				    } catch (...) {
					    // Ignore errors from losing read
				    }
			    }));
			try {
				return hedged_task.get();
			} catch (...) {
				throw;
			}
		}
	}
}

void HedgedFileHandle::HedgedRead(void *buffer, int64_t nr_bytes, idx_t location) {
	lock_guard<mutex> lock(handle_mutex);

	auto timeout = hedged_fs.timeout;

	// Start the primary read
	auto primary_task = std::async(std::launch::async, [&, location]() {
		hedged_fs.wrapped_fs->Read(*wrapped_handle, buffer, nr_bytes, location);
	});
	auto status = primary_task.wait_for(timeout);

	if (status == std::future_status::ready) {
		// Primary read completed in time - return immediately
		try {
			primary_task.get(); // May throw if read failed
			return;
		} catch (...) {
			throw;
		}
	}

	// Primary read timed out, start hedged read
	// Allocate a separate buffer for the hedged read
	auto hedged_buffer = make_shared_ptr<vector<char>>(nr_bytes);

	auto hedged_task = std::async(std::launch::async, [&, hedged_buffer, location]() {
		// Open a new file handle for the hedged read
		auto hedged_handle = hedged_fs.wrapped_fs->OpenFile(path, wrapped_handle->GetFlags(), nullptr);

		// Perform the read at the specified location
		hedged_fs.wrapped_fs->Read(*hedged_handle, hedged_buffer->data(), nr_bytes, location);

		// Copy the data to the output buffer
		memcpy(buffer, hedged_buffer->data(), nr_bytes);
	});

	// Race between primary and hedged read
	// Check both futures in a loop until one completes
	while (true) {
		// Check primary
		status = primary_task.wait_for(std::chrono::milliseconds(10));
		if (status == std::future_status::ready) {
			// Primary won - add hedged to cache and return
			hedged_fs.cache->AddPendingRequest(
			    std::async(std::launch::async, [task = std::move(hedged_task)]() mutable {
				    try {
					    task.wait();
				    } catch (...) {
					    // Ignore errors from losing read
				    }
			    }));
			try {
				primary_task.get();
				return;
			} catch (...) {
				throw;
			}
		}

		// Check hedged
		status = hedged_task.wait_for(std::chrono::milliseconds(10));
		if (status == std::future_status::ready) {
			// Hedged won - add primary to cache and return
			hedged_fs.cache->AddPendingRequest(
			    std::async(std::launch::async, [task = std::move(primary_task)]() mutable {
				    try {
					    task.wait();
				    } catch (...) {
					    // Ignore errors from losing read
				    }
			    }));
			try {
				hedged_task.get();
				return;
			} catch (...) {
				throw;
			}
		}
	}
}

} // namespace duckdb

