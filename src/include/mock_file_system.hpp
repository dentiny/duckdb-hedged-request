#pragma once

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "thread_annotation.hpp"

#include <atomic>
#include <chrono>

namespace duckdb {

// A mock filesystem that inherits LocalFileSystem and adds configurable IO delay
// to simulate slow operations for testing hedged requests.
class MockFileSystem : public LocalFileSystem {
public:
	MockFileSystem();

	void SetDelay(std::chrono::milliseconds delay_p);

	idx_t GetOpenFileCount() const;
	void ResetOpenFileCount();

	idx_t GetFileExistsCount() const;
	void ResetFileExistsCount();

	idx_t GetDirectoryExistsCount() const;
	void ResetDirectoryExistsCount();

	idx_t GetListFilesCount() const;
	void ResetListFilesCount();

	idx_t GetGlobCount() const;
	void ResetGlobCount();

	idx_t GetFileSizeCount() const;
	void ResetFileSizeCount();

	idx_t GetLastModifiedTimeCount() const;
	void ResetLastModifiedTimeCount();

	idx_t GetFileTypeCount() const;
	void ResetFileTypeCount();

	string GetName() const override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

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

	mutex delay_mutex;
	std::chrono::milliseconds delay DUCKDB_GUARDED_BY(delay_mutex);
	std::atomic<idx_t> open_file_count;
	std::atomic<idx_t> file_exists_count;
	std::atomic<idx_t> directory_exists_count;
	std::atomic<idx_t> list_files_count;
	std::atomic<idx_t> glob_count;
	std::atomic<idx_t> get_file_size_count;
	std::atomic<idx_t> get_last_modified_time_count;
	std::atomic<idx_t> get_file_type_count;
};

} // namespace duckdb
