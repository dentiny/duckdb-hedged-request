#include "mock_file_system.hpp"

#include <thread>

namespace duckdb {

MockFileSystem::MockFileSystem()
    : delay(std::chrono::milliseconds(0)), open_file_count(0), file_exists_count(0), directory_exists_count(0),
      list_files_count(0), glob_count(0), get_file_size_count(0), get_last_modified_time_count(0),
      get_file_type_count(0) {
}

void MockFileSystem::SetDelay(std::chrono::milliseconds delay_p) {
	const lock_guard<mutex> lock(delay_mutex);
	delay = delay_p;
}

idx_t MockFileSystem::GetOpenFileCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return open_file_count;
}

void MockFileSystem::ResetOpenFileCount() {
	const lock_guard<mutex> lock(count_mutex);
	open_file_count = 0;
}

idx_t MockFileSystem::GetFileExistsCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return file_exists_count;
}

void MockFileSystem::ResetFileExistsCount() {
	const lock_guard<mutex> lock(count_mutex);
	file_exists_count = 0;
}

idx_t MockFileSystem::GetDirectoryExistsCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return directory_exists_count;
}

void MockFileSystem::ResetDirectoryExistsCount() {
	const lock_guard<mutex> lock(count_mutex);
	directory_exists_count = 0;
}

idx_t MockFileSystem::GetListFilesCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return list_files_count;
}

void MockFileSystem::ResetListFilesCount() {
	const lock_guard<mutex> lock(count_mutex);
	list_files_count = 0;
}

idx_t MockFileSystem::GetGlobCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return glob_count;
}

void MockFileSystem::ResetGlobCount() {
	const lock_guard<mutex> lock(count_mutex);
	glob_count = 0;
}

idx_t MockFileSystem::GetFileSizeCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return get_file_size_count;
}

void MockFileSystem::ResetFileSizeCount() {
	const lock_guard<mutex> lock(count_mutex);
	get_file_size_count = 0;
}

idx_t MockFileSystem::GetLastModifiedTimeCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return get_last_modified_time_count;
}

void MockFileSystem::ResetLastModifiedTimeCount() {
	const lock_guard<mutex> lock(count_mutex);
	get_last_modified_time_count = 0;
}

idx_t MockFileSystem::GetFileTypeCount() const {
	const lock_guard<mutex> lock(count_mutex);
	return get_file_type_count;
}

void MockFileSystem::ResetFileTypeCount() {
	const lock_guard<mutex> lock(count_mutex);
	get_file_type_count = 0;
}

string MockFileSystem::GetName() const {
	return "MockFileSystem";
}

unique_ptr<FileHandle> MockFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		open_file_count++;
	}
	SimulateDelay();
	return LocalFileSystem::OpenFile(path, flags, opener);
}

int64_t MockFileSystem::GetFileSize(FileHandle &handle) {
	{
		const lock_guard<mutex> lock(count_mutex);
		get_file_size_count++;
	}
	SimulateDelay();
	return LocalFileSystem::GetFileSize(handle);
}

timestamp_t MockFileSystem::GetLastModifiedTime(FileHandle &handle) {
	{
		const lock_guard<mutex> lock(count_mutex);
		get_last_modified_time_count++;
	}
	SimulateDelay();
	return LocalFileSystem::GetLastModifiedTime(handle);
}

string MockFileSystem::GetVersionTag(FileHandle &handle) {
	SimulateDelay();
	return LocalFileSystem::GetVersionTag(handle);
}

FileType MockFileSystem::GetFileType(FileHandle &handle) {
	{
		const lock_guard<mutex> lock(count_mutex);
		get_file_type_count++;
	}
	SimulateDelay();
	return LocalFileSystem::GetFileType(handle);
}

bool MockFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		directory_exists_count++;
	}
	SimulateDelay();
	return LocalFileSystem::DirectoryExists(directory, opener);
}

bool MockFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		file_exists_count++;
	}
	SimulateDelay();
	return LocalFileSystem::FileExists(filename, opener);
}

vector<OpenFileInfo> MockFileSystem::Glob(const string &path, FileOpener *opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		glob_count++;
	}
	SimulateDelay();
	return LocalFileSystem::Glob(path, opener);
}

bool MockFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		list_files_count++;
	}
	SimulateDelay();
	return LocalFileSystem::ListFiles(directory, callback, opener);
}

unique_ptr<FileHandle> MockFileSystem::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	{
		const lock_guard<mutex> lock(count_mutex);
		open_file_count++;
	}
	SimulateDelay();
	return LocalFileSystem::OpenFile(info.path, flags, opener);
}

bool MockFileSystem::SupportsOpenFileExtended() const {
	return true;
}

bool MockFileSystem::ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                                       optional_ptr<FileOpener> opener) {
	SimulateDelay();
	return LocalFileSystem::ListFilesExtended(directory, callback, opener);
}

bool MockFileSystem::SupportsListFilesExtended() const {
	return true;
}

void MockFileSystem::SimulateDelay() {
	std::chrono::milliseconds current_delay;
	{
		const lock_guard<mutex> lock(delay_mutex);
		current_delay = delay;
	}
	if (current_delay.count() > 0) {
		std::this_thread::sleep_for(current_delay);
	}
}

} // namespace duckdb
