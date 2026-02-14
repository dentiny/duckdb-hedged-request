#include "catch/catch.hpp"

#include "duckdb/common/array.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "hedged_file_system.hpp"
#include "hedged_request_fs_entry.hpp"
#include "mock_file_system.hpp"
#include "test_helpers.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {
constexpr const char *TEST_CONTENT =
    "Hello, HedgedFileSystem! This is a test file for hedged reads.\nThe quick brown fox jumps over the lazy dog.\n";
constexpr const char *FAST_TEST_CONTENT = "Fast test file\n";
constexpr const char *DB_TEST_CONTENT = "Database test file\n";
} // namespace

TEST_CASE("HedgedFileSystem with slow open", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	REQUIRE(mock_fs_ptr->GetOpenFileCount() == 2);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(bytes_read == NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(string(buffer.data(), bytes_read) == TEST_CONTENT);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem with fast open (no hedging)", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_fast.txt");
	CreateTestFile(test_file, FAST_TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	REQUIRE(mock_fs_ptr->GetOpenFileCount() == 1);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(strlen(FAST_TEST_CONTENT)));
	REQUIRE(bytes_read == NumericCast<int64_t>(strlen(FAST_TEST_CONTENT)));
	REQUIRE(string(buffer.data(), bytes_read) == FAST_TEST_CONTENT);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem with ClientContextFileOpener", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_opener.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto *opener = ClientData::Get(context).file_opener.get();

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, opener);
	REQUIRE(file_handle != nullptr);
	REQUIRE(mock_fs_ptr->GetOpenFileCount() == 2);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(bytes_read == NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(string(buffer.data(), bytes_read) == TEST_CONTENT);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem FileExists with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_exists.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetFileExistsCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	bool exists = hedged_fs->FileExists(test_file, nullptr);
	REQUIRE(exists == true);
	REQUIRE(mock_fs_ptr->GetFileExistsCount() == 2);
}

TEST_CASE("HedgedFileSystem FileExists with fast operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_exists_fast.txt");
	CreateTestFile(test_file, FAST_TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));
	mock_fs->ResetFileExistsCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	bool exists = hedged_fs->FileExists(test_file, nullptr);
	REQUIRE(exists == true);
	REQUIRE(mock_fs_ptr->GetFileExistsCount() == 1);
}

TEST_CASE("HedgedFileSystem DirectoryExists with slow operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_dir");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetDirectoryExistsCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	bool exists = hedged_fs->DirectoryExists(test_dir, nullptr);
	REQUIRE(exists == true);
	REQUIRE(mock_fs_ptr->GetDirectoryExistsCount() == 2);
}

TEST_CASE("HedgedFileSystem DirectoryExists with fast operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_dir_fast");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));
	mock_fs->ResetDirectoryExistsCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	bool exists = hedged_fs->DirectoryExists(test_dir, nullptr);
	REQUIRE(exists == true);
	REQUIRE(mock_fs_ptr->GetDirectoryExistsCount() == 1);
}

TEST_CASE("HedgedFileSystem ListFiles with slow operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_list_dir");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);
	CreateTestFile(TestCreatePath("hedged_test_list_dir/file1.txt"), "file1 content");
	CreateTestFile(TestCreatePath("hedged_test_list_dir/file2.txt"), "file2 content");

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetListFilesCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto files = make_shared_ptr<vector<string>>();
	auto files_mutex = make_shared_ptr<mutex>();
	bool success = hedged_fs->ListFiles(test_dir, [files, files_mutex](const string &name, bool is_dir) {
		if (!is_dir) {
			const lock_guard<mutex> lock(*files_mutex);
			files->push_back(name);
		}
	}, nullptr);
	REQUIRE(success == true);
	REQUIRE(files->size() == 2);
	REQUIRE(mock_fs_ptr->GetListFilesCount() == 2);
}

TEST_CASE("HedgedFileSystem Glob with slow operation", "[hedged_file_system]") {
	string test_file1 = TestCreatePath("hedged_glob_test1.txt");
	string test_file2 = TestCreatePath("hedged_glob_test2.txt");
	CreateTestFile(test_file1, "content1");
	CreateTestFile(test_file2, "content2");

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetGlobCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	string glob_pattern = TestCreatePath("hedged_glob_test*.txt");
	vector<OpenFileInfo> results = hedged_fs->Glob(glob_pattern, nullptr);
	REQUIRE(results.size() >= 2);
	REQUIRE(mock_fs_ptr->GetGlobCount() == 2);
}

TEST_CASE("HedgedFileSystem GetFileSize with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_size.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	mock_fs_ptr->ResetFileSizeCount();

	int64_t file_size = hedged_fs->GetFileSize(*file_handle);
	REQUIRE(file_size == NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(mock_fs_ptr->GetFileSizeCount() == 2);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem GetLastModifiedTime with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_mtime.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	mock_fs_ptr->ResetLastModifiedTimeCount();

	timestamp_t mtime = hedged_fs->GetLastModifiedTime(*file_handle);
	REQUIRE(mtime.value != 0);
	REQUIRE(mock_fs_ptr->GetLastModifiedTimeCount() == 2);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem GetFileType with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_type.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(50), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	mock_fs_ptr->ResetFileTypeCount();

	FileType file_type = hedged_fs->GetFileType(*file_handle);
	REQUIRE(file_type == FileType::FILE_TYPE_REGULAR);
	REQUIRE(mock_fs_ptr->GetFileTypeCount() == 2);

	file_handle->Close();
}
