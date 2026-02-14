#include "hedged_file_system.hpp"
#include "future_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// HedgedFileSystem
//===--------------------------------------------------------------------===//

HedgedFileSystem::HedgedFileSystem(unique_ptr<FileSystem> wrapped_fs_p, std::chrono::milliseconds timeout_p,
                                   shared_ptr<HedgedRequestFsEntry> entry_p)
    : wrapped_fs(std::move(wrapped_fs_p)), timeout(timeout_p), cache(std::move(entry_p)) {
	if (!this->wrapped_fs) {
		throw InternalException("HedgedFileSystem: wrapped_fs cannot be null");
	}
	if (!this->cache) {
		throw InternalException("HedgedFileSystem: cache entry cannot be null");
	}
}

HedgedFileSystem::~HedgedFileSystem() {
}

unique_ptr<FileHandle> HedgedFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	auto token = make_shared_ptr<Token>();
	using FutureType = FutureWrapper<unique_ptr<FileHandle>>;

	auto *fs_ptr = wrapped_fs.get();
	auto open_fn = std::function<unique_ptr<FileHandle>()>(
	    [fs_ptr, path, flags, opener]() { return fs_ptr->OpenFile(path, flags, opener); });
	auto primary = CreateFuture<unique_ptr<FileHandle>>(open_fn, token);

	{
		unique_lock<mutex> lock(token->mu);
		token->cv.wait_for(lock, timeout, [&] { return token->completed; });
		token->completed = false;
	}

	if (primary.IsReady()) {
		return make_uniq<HedgedFileHandle>(*this, primary.Get(), path);
	}

	// Start the hedged open
	auto hedged = CreateFuture<unique_ptr<FileHandle>>(open_fn, token);
	vector<FutureType> futs;
	futs.reserve(2);
	futs.push_back(std::move(primary));
	futs.push_back(std::move(hedged));
	auto unready = WaitForAny(futs, token);
	auto wrapped_handle = futs[0].Get();

	// Add unready futures to cache for background cleanup
	for (auto &fut : unready) {
		auto pending_token = make_shared_ptr<Token>();
		auto pending_fut = make_shared_ptr<FutureType>(std::move(fut));
		cache->AddPendingRequest(
		    CreateFuture<void>(std::function<void()>([pending_fut]() { pending_fut->Wait(); }), pending_token));
	}

	return make_uniq<HedgedFileHandle>(*this, std::move(wrapped_handle), path);
}

int64_t HedgedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->Read(hedged_handle.GetWrappedHandle(), buffer, nr_bytes);
}

void HedgedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Read(hedged_handle.GetWrappedHandle(), buffer, nr_bytes, location);
}

void HedgedFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Write(hedged_handle.GetWrappedHandle(), buffer, nr_bytes, location);
}

int64_t HedgedFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->Write(hedged_handle.GetWrappedHandle(), buffer, nr_bytes);
}

bool HedgedFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->Trim(hedged_handle.GetWrappedHandle(), offset_bytes, length_bytes);
}

int64_t HedgedFileSystem::GetFileSize(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->GetFileSize(hedged_handle.GetWrappedHandle());
}

timestamp_t HedgedFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->GetLastModifiedTime(hedged_handle.GetWrappedHandle());
}

string HedgedFileSystem::GetVersionTag(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->GetVersionTag(hedged_handle.GetWrappedHandle());
}

FileType HedgedFileSystem::GetFileType(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->GetFileType(hedged_handle.GetWrappedHandle());
}

void HedgedFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Truncate(hedged_handle.GetWrappedHandle(), new_size);
}

bool HedgedFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return wrapped_fs->DirectoryExists(directory, opener);
}

void HedgedFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->CreateDirectory(directory, opener);
}

void HedgedFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->RemoveDirectory(directory, opener);
}

bool HedgedFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                  FileOpener *opener) {
	return wrapped_fs->ListFiles(directory, callback, opener);
}

void HedgedFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	wrapped_fs->MoveFile(source, target, opener);
}

bool HedgedFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return wrapped_fs->FileExists(filename, opener);
}

bool HedgedFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return wrapped_fs->IsPipe(filename, opener);
}

void HedgedFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	wrapped_fs->RemoveFile(filename, opener);
}

bool HedgedFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return wrapped_fs->TryRemoveFile(filename, opener);
}

void HedgedFileSystem::FileSync(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->FileSync(hedged_handle.GetWrappedHandle());
}

string HedgedFileSystem::GetHomeDirectory() {
	return wrapped_fs->GetHomeDirectory();
}

string HedgedFileSystem::ExpandPath(const string &path) {
	return wrapped_fs->ExpandPath(path);
}

string HedgedFileSystem::PathSeparator(const string &path) {
	return wrapped_fs->PathSeparator(path);
}

vector<OpenFileInfo> HedgedFileSystem::Glob(const string &path, FileOpener *opener) {
	return wrapped_fs->Glob(path, opener);
}

string HedgedFileSystem::GetName() const {
	return StringUtil::Format("HedgedFileSystem(%s)", wrapped_fs->GetName());
}

void HedgedFileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	wrapped_fs->RegisterSubSystem(std::move(sub_fs));
}

void HedgedFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	wrapped_fs->RegisterSubSystem(compression_type, std::move(fs));
}

void HedgedFileSystem::UnregisterSubSystem(const string &name) {
	wrapped_fs->UnregisterSubSystem(name);
}

void HedgedFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	wrapped_fs->SetDisabledFileSystems(names);
}

bool HedgedFileSystem::SubSystemIsDisabled(const string &name) {
	return wrapped_fs->SubSystemIsDisabled(name);
}

vector<string> HedgedFileSystem::ListSubSystems() {
	return wrapped_fs->ListSubSystems();
}

void HedgedFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Seek(hedged_handle.GetWrappedHandle(), location);
}

void HedgedFileSystem::Reset(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Reset(hedged_handle.GetWrappedHandle());
}

idx_t HedgedFileSystem::SeekPosition(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->SeekPosition(hedged_handle.GetWrappedHandle());
}

bool HedgedFileSystem::CanSeek() {
	return wrapped_fs->CanSeek();
}

bool HedgedFileSystem::OnDiskFile(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	return wrapped_fs->OnDiskFile(hedged_handle.GetWrappedHandle());
}

//===--------------------------------------------------------------------===//
// HedgedFileHandle
//===--------------------------------------------------------------------===//

HedgedFileHandle::HedgedFileHandle(HedgedFileSystem &fs, unique_ptr<FileHandle> wrapped_handle, const string &path)
    : FileHandle(fs, path, wrapped_handle->GetFlags()), hedged_fs(fs), wrapped_handle(std::move(wrapped_handle)) {
}

HedgedFileHandle::~HedgedFileHandle() {
}

void HedgedFileHandle::Close() {
	if (wrapped_handle) {
		wrapped_handle->Close();
	}
}

} // namespace duckdb
