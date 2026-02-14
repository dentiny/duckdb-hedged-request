#include "hedged_file_system.hpp"
#include "hedged_request_fs_entry.hpp"
#include "duckdb/common/file_system.hpp"

#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>

using namespace duckdb;

int main() {
	std::cout << "Testing HedgedFileSystem..." << std::endl;

	// Create a test file
	std::string test_file = "/tmp/hedged_test.txt";
	std::ofstream out(test_file);
	out << "Hello, HedgedFileSystem! This is a test file for hedged reads." << std::endl;
	out << "The quick brown fox jumps over the lazy dog." << std::endl;
	out << "Testing hedged requests on slow IO operations." << std::endl;
	out.close();

	// Create a local filesystem
	auto local_fs = FileSystem::CreateLocal();

	// Create a HedgedRequestFsEntry
	auto entry = make_shared<HedgedRequestFsEntry>();

	// Wrap it with HedgedFileSystem (3 second timeout)
	auto hedged_fs = make_uniq<HedgedFileSystem>(std::move(local_fs), std::chrono::milliseconds(3000), entry);

	std::cout << "Filesystem name: " << hedged_fs->GetName() << std::endl;

	// Test 1: Open and read a file (should complete quickly, no hedging)
	std::cout << "\nTest 1: Normal read (should complete quickly)..." << std::endl;
	auto start = std::chrono::steady_clock::now();

	auto file_handle = hedged_fs->OpenFile(test_file, FileOpenFlags::FILE_FLAGS_READ, nullptr);
	char buffer[256];
	memset(buffer, 0, sizeof(buffer));
	int64_t bytes_read = hedged_fs->Read(*file_handle, buffer, 100);

	auto end = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

	std::cout << "Read " << bytes_read << " bytes in " << duration << " ms" << std::endl;
	std::cout << "Content: " << std::string(buffer, bytes_read) << std::endl;

	// Test 2: Read at a specific location
	std::cout << "\nTest 2: Read at specific location..." << std::endl;
	memset(buffer, 0, sizeof(buffer));
	hedged_fs->Read(*file_handle, buffer, 30, 50);
	std::cout << "Content at offset 50: " << buffer << std::endl;

	// Test 3: File operations
	std::cout << "\nTest 3: File operations..." << std::endl;
	std::cout << "File exists: " << hedged_fs->FileExists(test_file, nullptr) << std::endl;
	std::cout << "File size: " << hedged_fs->GetFileSize(*file_handle) << " bytes" << std::endl;

	file_handle->Close();

	// Cleanup
	hedged_fs->RemoveFile(test_file, nullptr);

	std::cout << "\nAll tests passed!" << std::endl;
	return 0;
}

