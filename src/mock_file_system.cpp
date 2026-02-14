#include "mock_file_system.hpp"

#include <thread>

namespace duckdb {

MockFileSystem::MockFileSystem() : delay(std::chrono::milliseconds(0)) {
}

void MockFileSystem::SetDelay(std::chrono::milliseconds delay_p) {
	const lock_guard<mutex> lock(delay_mutex);
	delay = delay_p;
}

string MockFileSystem::GetName() const {
	return "MockFileSystem";
}

unique_ptr<FileHandle> MockFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	SimulateDelay();
	return LocalFileSystem::OpenFile(path, flags, opener);
}

int64_t MockFileSystem::GetFileSize(FileHandle &handle) {
	SimulateDelay();
	return LocalFileSystem::GetFileSize(handle);
}

timestamp_t MockFileSystem::GetLastModifiedTime(FileHandle &handle) {
	SimulateDelay();
	return LocalFileSystem::GetLastModifiedTime(handle);
}

string MockFileSystem::GetVersionTag(FileHandle &handle) {
	SimulateDelay();
	return LocalFileSystem::GetVersionTag(handle);
}

FileType MockFileSystem::GetFileType(FileHandle &handle) {
	SimulateDelay();
	return LocalFileSystem::GetFileType(handle);
}

bool MockFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	SimulateDelay();
	return LocalFileSystem::DirectoryExists(directory, opener);
}

bool MockFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	SimulateDelay();
	return LocalFileSystem::FileExists(filename, opener);
}

vector<OpenFileInfo> MockFileSystem::Glob(const string &path, FileOpener *opener) {
	SimulateDelay();
	return LocalFileSystem::Glob(path, opener);
}

bool MockFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	SimulateDelay();
	return LocalFileSystem::ListFiles(directory, callback, opener);
}

unique_ptr<FileHandle> MockFileSystem::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
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
