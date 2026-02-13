#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class HedgedFileSystem;

// HedgedFileHandle wraps a file handle and provides hedged read operations
class HedgedFileHandle : public FileHandle {
public:
	// TODO(hjiang): take a hedged request config.
	HedgedFileHandle(HedgedFileSystem &fs, unique_ptr<FileHandle> wrapped_handle, const string &path);
	~HedgedFileHandle() override;

	void Close() override;

	int64_t HedgedRead(void *buffer, int64_t nr_bytes);
	void HedgedRead(void *buffer, int64_t nr_bytes, idx_t location);

	FileHandle &GetWrappedHandle() {
		return *wrapped_handle;
	}

private:
	HedgedFileSystem &hedged_fs;
	unique_ptr<FileHandle> wrapped_handle;
	mutex handle_mutex;
};

} // namespace duckdb

