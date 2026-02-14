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

#include <cstring>

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
	mock_fs->SetDelay(std::chrono::milliseconds(5000));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(1000), entry);

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
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(1000), entry);

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
	mock_fs->SetDelay(std::chrono::milliseconds(5000));
	mock_fs->ResetOpenFileCount();
	auto *mock_fs_ptr = mock_fs.get();

	auto entry = make_shared_ptr<HedgedRequestFsEntry>();
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(mock_fs), std::chrono::milliseconds(1000), entry);

	auto file_handle = hedged_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_READ, opener);
	REQUIRE(file_handle != nullptr);
	REQUIRE(mock_fs_ptr->GetOpenFileCount() == 2);

	array<char, 256> buffer {};
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer.data(), NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(bytes_read == NumericCast<int64_t>(strlen(TEST_CONTENT)));
	REQUIRE(string(buffer.data(), bytes_read) == TEST_CONTENT);

	file_handle->Close();
}
