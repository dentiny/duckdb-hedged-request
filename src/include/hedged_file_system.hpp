#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "hedged_request_fs_entry.hpp"

namespace duckdb {

class HedgedFileHandle;
class HedgedRequestFsEntry;

// HedgedFileSystem is a wrapper filesystem that performs hedged requests on slow IO operations.
class HedgedFileSystem : public FileSystem {
public:
	explicit HedgedFileSystem(unique_ptr<FileSystem> wrapped_fs,
							  // TODO(hjiang): provide hedged request config.
	                          std::chrono::milliseconds timeout_p = std::chrono::milliseconds(3000),
	                          shared_ptr<HedgedRequestFsEntry> entry_p);
	~HedgedFileSystem() override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	// All other methods delegate to the wrapped filesystem
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void FileSync(FileHandle &handle) override;

	string GetHomeDirectory() override;
	string ExpandPath(const string &path) override;
	string PathSeparator(const string &path) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	string GetName() const override;

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	void UnregisterSubSystem(const string &name) override;
	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;
	vector<string> ListSubSystems() override;

	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override;
	bool OnDiskFile(FileHandle &handle) override;

private:
	unique_ptr<FileSystem> wrapped_fs;
	std::chrono::milliseconds timeout;
	shared_ptr<HedgedRequestFsEntry> cache;
};

// HedgedFileHandle wraps a file handle and delegates to HedgedFileSystem for hedged reads
class HedgedFileHandle : public FileHandle {
public:
	HedgedFileHandle(HedgedFileSystem &fs, unique_ptr<FileHandle> wrapped_handle, const string &path);
	~HedgedFileHandle() override;

	void Close() override;

	FileHandle &GetWrappedHandle() {
		return *wrapped_handle;
	}

private:
	HedgedFileSystem &hedged_fs;
	unique_ptr<FileHandle> wrapped_handle;
};

} // namespace duckdb

