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
const string TEST_CONTENT =
    "Hello, HedgedFileSystem! This is a test file for hedged reads.\nThe quick brown fox jumps over the lazy dog.\n";
const string FAST_TEST_CONTENT = "Fast test file\n";
const string DB_TEST_CONTENT = "Database test file\n";
} // namespace

TEST_CASE("HedgedFileSystem with slow open", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(bytes_read == NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(string(buffer.data(), bytes_read) == TEST_CONTENT);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem with fast open (no hedging)", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_fast.txt");
	CreateTestFile(test_file, FAST_TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(FAST_TEST_CONTENT.size()));
	REQUIRE(bytes_read == NumericCast<int64_t>(FAST_TEST_CONTENT.size()));
	REQUIRE(string(buffer.data(), bytes_read) == FAST_TEST_CONTENT);

	file_handle->Close();
	entry->WaitAll();
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

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, opener);
	REQUIRE(file_handle != nullptr);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(bytes_read == NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(string(buffer.data(), bytes_read) == TEST_CONTENT);

	file_handle->Close();
}

TEST_CASE("HedgedFileSystem FileExists with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_exists.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	bool exists = hedged_fs->FileExists(test_file, nullptr);
	REQUIRE(exists == true);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem FileExists with fast operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_exists_fast.txt");
	CreateTestFile(test_file, FAST_TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	bool exists = hedged_fs->FileExists(test_file, nullptr);
	REQUIRE(exists == true);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem DirectoryExists with slow operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_dir");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	bool exists = hedged_fs->DirectoryExists(test_dir, nullptr);
	REQUIRE(exists == true);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem DirectoryExists with fast operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_dir_fast");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(0));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	bool exists = hedged_fs->DirectoryExists(test_dir, nullptr);
	REQUIRE(exists == true);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem ListFiles with slow operation", "[hedged_file_system]") {
	string test_dir = TestCreatePath("hedged_test_list_dir");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);
	CreateTestFile(TestCreatePath("hedged_test_list_dir/file1.txt"), "file1 content");
	CreateTestFile(TestCreatePath("hedged_test_list_dir/file2.txt"), "file2 content");

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	vector<string> files;
	bool success = hedged_fs->ListFiles(
	    test_dir,
	    [&files](const string &name, bool is_dir) {
		    if (!is_dir) {
			    files.push_back(name);
		    }
	    },
	    nullptr);
	REQUIRE(success == true);
	REQUIRE(files.size() == 2);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem Glob with slow operation", "[hedged_file_system]") {
	string test_file1 = TestCreatePath("hedged_glob_test1.txt");
	string test_file2 = TestCreatePath("hedged_glob_test2.txt");
	CreateTestFile(test_file1, "content1");
	CreateTestFile(test_file2, "content2");

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	string glob_pattern = TestCreatePath("hedged_glob_test*.txt");
	vector<OpenFileInfo> results = hedged_fs->Glob(glob_pattern, nullptr);
	REQUIRE(results.size() >= 2);
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem GetFileSize with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_size.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	int64_t file_size = hedged_fs->GetFileSize(*file_handle);
	REQUIRE(file_size == NumericCast<int64_t>(TEST_CONTENT.size()));

	file_handle->Close();
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem GetLastModifiedTime with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_mtime.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	timestamp_t mtime = hedged_fs->GetLastModifiedTime(*file_handle);
	REQUIRE(mtime.value != 0);

	file_handle->Close();
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem GetFileType with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_type.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	FileType file_type = hedged_fs->GetFileType(*file_handle);
	REQUIRE(file_type == FileType::FILE_TYPE_REGULAR);

	file_handle->Close();
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem Stats with slow operation", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_stats.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);

	FileMetadata stats = hedged_fs->Stats(*file_handle);
	REQUIRE(stats.file_size == NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(stats.file_type == FileType::FILE_TYPE_REGULAR);

	file_handle->Close();
	entry->WaitAll();
}

TEST_CASE("HedgedFileSystem GetFileSize with early file handle destruction", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_size_destruct.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	int64_t file_size = 0;
	{
		// Destroy file handle before hedged requests complete
		auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
		REQUIRE(file_handle != nullptr);

		file_size = hedged_fs->GetFileSize(*file_handle);
		REQUIRE(file_size == NumericCast<int64_t>(TEST_CONTENT.size()));
	}

	// Wait for all pending hedged requests to complete
	entry->WaitAll();
	REQUIRE(file_size == NumericCast<int64_t>(TEST_CONTENT.size()));
}

TEST_CASE("HedgedFileSystem GetLastModifiedTime with early file handle destruction", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_mtime_destruct.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	timestamp_t mtime;
	{
		// Destroy file handle before hedged requests complete
		auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
		REQUIRE(file_handle != nullptr);

		mtime = hedged_fs->GetLastModifiedTime(*file_handle);
		REQUIRE(mtime.value != 0);
	}

	// Wait for all pending hedged requests to complete
	entry->WaitAll();
	REQUIRE(mtime.value != 0);
}

TEST_CASE("HedgedFileSystem GetVersionTag with early file handle destruction", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_vtag_destruct.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	string version_tag;
	{
		// Destroy file handle before hedged requests complete
		auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
		REQUIRE(file_handle != nullptr);
		version_tag = hedged_fs->GetVersionTag(*file_handle);
	}

	// Wait for all pending hedged requests to complete
	entry->WaitAll();
	REQUIRE(!version_tag.empty());
}

TEST_CASE("HedgedFileSystem Stats with early file handle destruction", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_stats_destruct.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	FileMetadata stats;
	{
		// Destroy file handle before hedged requests complete
		auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
		REQUIRE(file_handle != nullptr);
		stats = hedged_fs->Stats(*file_handle);
		REQUIRE(stats.file_size == NumericCast<int64_t>(TEST_CONTENT.size()));
		REQUIRE(stats.file_type == FileType::FILE_TYPE_REGULAR);
	}

	// Wait for all pending hedged requests to complete
	entry->WaitAll();
	REQUIRE(stats.file_size == NumericCast<int64_t>(TEST_CONTENT.size()));
	REQUIRE(stats.file_type == FileType::FILE_TYPE_REGULAR);
}

TEST_CASE("HedgedFileSystem GetFileType with early file handle destruction", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_type_destruct.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(100));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	FileType file_type;
	{
		// Destroy file handle before hedged requests complete
		auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
		REQUIRE(file_handle != nullptr);

		file_type = hedged_fs->GetFileType(*file_handle);
		REQUIRE(file_type == FileType::FILE_TYPE_REGULAR);
	}

	// Wait for all pending hedged requests to complete
	entry->WaitAll();
	REQUIRE(file_type == FileType::FILE_TYPE_REGULAR);
}

// Testing scenario: if operation timeout is far shorter than the delay, multiple hedged requests should be spawned.
TEST_CASE("HedgedFileSystem multiple hedged requests at threshold intervals", "[hedged_file_system]") {
	string test_file = TestCreatePath("hedged_test_multiple.txt");
	CreateTestFile(test_file, TEST_CONTENT);

	auto mock_fs = make_uniq<MockFileSystem>();
	mock_fs->SetDelay(std::chrono::milliseconds(200));

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), entry);

	// Set threshold to 50ms, which is much less than the 200ms delay
	// This should trigger multiple hedged requests (at 50ms, 100ms, 150ms, 200ms)
	entry->UpdateConfig(HedgedRequestOperation::GET_FILE_SIZE, std::chrono::milliseconds(50));

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, nullptr);
	REQUIRE(file_handle != nullptr);
	int64_t file_size = hedged_fs->GetFileSize(*file_handle);
	REQUIRE(file_size == NumericCast<int64_t>(TEST_CONTENT.size()));

	file_handle->Close();
	entry->WaitAll();
}
