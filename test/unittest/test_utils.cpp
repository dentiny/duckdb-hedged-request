#include "test_utils.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

void CreateTestFile(const string &path, const string &content) {
	auto fs = FileSystem::CreateLocal();
	auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	if (!handle) {
		throw InternalException("Failed to create test file: %s", path);
	}
	// Write content to file
	auto content_size = NumericCast<int64_t>(content.size());
	fs->Write(*handle, const_cast<char *>(content.c_str()), content_size);
	handle->Close();
}

} // namespace duckdb

