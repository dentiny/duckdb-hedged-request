#pragma once

#include "duckdb/common/local_file_system.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

#include <chrono>

namespace duckdb {

// A mock filesystem that inherits LocalFileSystem and adds configurable IO delay
// to simulate slow operations for testing hedged requests.
class MockFileSystem : public LocalFileSystem {
public:
	MockFileSystem();

	void SetDelay(std::chrono::milliseconds delay_p);

	// After simulated delay, throw IOException.
	void SetSimulateIoFailure(bool failure);

	void SetSkipSimulatedIoFailureCalls(int count);

	string GetName() const override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override;

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override;

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override;

private:
	void SimulateDelay();

	concurrency::mutex delay_mutex;
	std::chrono::milliseconds delay DUCKDB_GUARDED_BY(delay_mutex);
	bool simulate_io_failure DUCKDB_GUARDED_BY(delay_mutex) = false;
	int skip_simulated_io_failure_calls DUCKDB_GUARDED_BY(delay_mutex) = 0;
};

} // namespace duckdb
