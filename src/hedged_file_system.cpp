#include "hedged_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "future_utils.hpp"
#include "hedged_request_config.hpp"

namespace duckdb {

namespace {
shared_ptr<FileOpener> CopyFileOpener(optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		return nullptr;
	}

	// Possibility-1: client context file opener
	auto *client_context_opener = dynamic_cast<ClientContextFileOpener *>(opener.get());
	if (client_context_opener != nullptr) {
		return make_shared_ptr<ClientContextFileOpener>(*client_context_opener->TryGetClientContext());
	}

	// Possibility-2: database file opener
	auto *database_file_opener = dynamic_cast<DatabaseFileOpener *>(opener.get());
	D_ASSERT(database_file_opener != nullptr);
	return make_shared_ptr<DatabaseFileOpener>(*database_file_opener->TryGetDatabase());
}

template <typename T>
T HedgedRequest(std::function<T()> fn, std::chrono::milliseconds timeout, shared_ptr<HedgedRequestFsEntry> entry) {
	auto token = make_shared_ptr<Token>();
	using FutureType = FutureWrapper<T>;

	FutureType primary {fn, token};

	{
		unique_lock<mutex> lock(token->mu);
		token->cv.wait_for(lock, timeout, [&] { return token->completed; });
		token->completed = false;
	}
	if (primary.IsReady()) {
		return primary.Get();
	}

	// Start the hedged request
	FutureType hedged(fn, token);
	vector<FutureType> futs;
	futs.reserve(2);
	futs.emplace_back(std::move(primary));
	futs.emplace_back(std::move(hedged));

	auto wait_result = WaitForAny(std::move(futs), token);
	for (auto &fut : wait_result.pending_futures) {
		auto pending_fut = make_shared_ptr<FutureType>(std::move(fut));
		entry->AddPendingRequest([pending_fut]() { pending_fut->Wait(); });
	}

	return std::move(wait_result.result);
}
} // namespace

//===--------------------------------------------------------------------===//
// HedgedFileSystem
//===--------------------------------------------------------------------===//

HedgedFileSystem::HedgedFileSystem(unique_ptr<FileSystem> wrapped_fs_p, std::chrono::milliseconds timeout_p,
                                   shared_ptr<HedgedRequestFsEntry> entry_p)
    : wrapped_fs(std::move(wrapped_fs_p)), timeout(timeout_p), entry(std::move(entry_p)) {
	if (!this->wrapped_fs) {
		throw InternalException("HedgedFileSystem: wrapped_fs cannot be null");
	}
	if (!this->entry) {
		throw InternalException("HedgedFileSystem: entry cannot be null");
	}
}

HedgedFileSystem::~HedgedFileSystem() {
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

void HedgedFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	wrapped_fs->Truncate(hedged_handle.GetWrappedHandle(), new_size);
}

void HedgedFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->CreateDirectory(directory, opener);
}

void HedgedFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->RemoveDirectory(directory, opener);
}

void HedgedFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	wrapped_fs->MoveFile(source, target, opener);
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

string HedgedFileSystem::GetName() const {
	return StringUtil::Format("HedgedFileSystem - %s", wrapped_fs->GetName());
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
// Hedged Request Operations
//===--------------------------------------------------------------------===//

unique_ptr<FileHandle> HedgedFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                  optional_ptr<FileOpener> opener) {
	auto *fs_ptr = wrapped_fs.get();
	auto opener_copy = CopyFileOpener(opener);
	auto config = entry->GetConfig();
	auto result = HedgedRequest<unique_ptr<FileHandle>>(
	    std::function<unique_ptr<FileHandle>()>([fs_ptr, path_copy = path, flags, opener_copy]() {
		    return fs_ptr->OpenFile(path_copy, flags, opener_copy.get());
	    }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::OPEN_FILE)], entry);
	return make_uniq<HedgedFileHandle>(*this, std::move(result), path);
}

bool HedgedFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	auto *fs_ptr = wrapped_fs.get();
	auto opener_copy = CopyFileOpener(opener);
	auto config = entry->GetConfig();
	return HedgedRequest<bool>(std::function<bool()>([fs_ptr, directory_copy = directory, opener_copy]() {
		                           return fs_ptr->DirectoryExists(directory_copy, opener_copy.get());
	                           }),
	                           config.delays_ms[static_cast<size_t>(HedgedRequestOperation::DIRECTORY_EXISTS)], entry);
}

bool HedgedFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                 FileOpener *opener) {
	auto results = make_shared_ptr<vector<std::pair<string, bool>>>();
	auto results_mutex = make_shared_ptr<mutex>();

	auto *fs_ptr = wrapped_fs.get();
	auto opener_copy = CopyFileOpener(opener);
	auto config = entry->GetConfig();
	bool success = HedgedRequest<bool>(
	    std::function<bool()>([fs_ptr, directory_copy = directory, results, results_mutex, opener_copy]() {
		    return fs_ptr->ListFiles(
		        directory_copy,
		        [results, results_mutex](const string &name, bool is_dir) {
			        const lock_guard<mutex> lock(*results_mutex);
			        results->emplace_back(name, is_dir);
		        },
		        opener_copy.get());
	    }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::LIST_FILES)], entry);

	if (success) {
		for (auto &result : *results) {
			callback(result.first, result.second);
		}
	}
	return success;
}

bool HedgedFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto *fs_ptr = wrapped_fs.get();
	auto opener_copy = CopyFileOpener(opener);
	auto config = entry->GetConfig();
	return HedgedRequest<bool>(std::function<bool()>([fs_ptr, filename_copy = filename, opener_copy]() {
		                           return fs_ptr->FileExists(filename_copy, opener_copy.get());
	                           }),
	                           config.delays_ms[static_cast<size_t>(HedgedRequestOperation::FILE_EXISTS)], entry);
}

vector<OpenFileInfo> HedgedFileSystem::Glob(const string &path, FileOpener *opener) {
	auto *fs_ptr = wrapped_fs.get();
	auto opener_copy = CopyFileOpener(opener);
	auto config = entry->GetConfig();
	return HedgedRequest<vector<OpenFileInfo>>(
	    std::function<vector<OpenFileInfo>()>(
	        [fs_ptr, path_copy = path, opener_copy]() { return fs_ptr->Glob(path_copy, opener_copy.get()); }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::GLOB)], entry);
}

int64_t HedgedFileSystem::GetFileSize(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	auto *fs_ptr = wrapped_fs.get();
	auto *wrapped_handle_ptr = &hedged_handle.GetWrappedHandle();
	auto config = entry->GetConfig();
	return HedgedRequest<int64_t>(
	    std::function<int64_t()>([fs_ptr, wrapped_handle_ptr]() { return fs_ptr->GetFileSize(*wrapped_handle_ptr); }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::GET_FILE_SIZE)], entry);
}

timestamp_t HedgedFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	auto *fs_ptr = wrapped_fs.get();
	auto *wrapped_handle_ptr = &hedged_handle.GetWrappedHandle();
	auto config = entry->GetConfig();
	return HedgedRequest<timestamp_t>(
	    std::function<timestamp_t()>(
	        [fs_ptr, wrapped_handle_ptr]() { return fs_ptr->GetLastModifiedTime(*wrapped_handle_ptr); }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::GET_LAST_MODIFIED_TIME)], entry);
}

string HedgedFileSystem::GetVersionTag(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	auto *fs_ptr = wrapped_fs.get();
	auto *wrapped_handle_ptr = &hedged_handle.GetWrappedHandle();
	auto config = entry->GetConfig();
	return HedgedRequest<string>(
	    std::function<string()>([fs_ptr, wrapped_handle_ptr]() { return fs_ptr->GetVersionTag(*wrapped_handle_ptr); }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::GET_VERSION_TAG)], entry);
}

FileType HedgedFileSystem::GetFileType(FileHandle &handle) {
	auto &hedged_handle = handle.Cast<HedgedFileHandle>();
	auto *fs_ptr = wrapped_fs.get();
	auto *wrapped_handle_ptr = &hedged_handle.GetWrappedHandle();
	auto config = entry->GetConfig();
	return HedgedRequest<FileType>(
	    std::function<FileType()>([fs_ptr, wrapped_handle_ptr]() { return fs_ptr->GetFileType(*wrapped_handle_ptr); }),
	    config.delays_ms[static_cast<size_t>(HedgedRequestOperation::GET_FILE_TYPE)], entry);
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
