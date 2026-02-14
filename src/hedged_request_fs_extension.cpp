#define DUCKDB_EXTENSION_MAIN

#include "hedged_request_fs_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "hedged_fs_functions.hpp"
#include "hedged_fs_settings.hpp"
#include "mock_file_system.hpp"

namespace duckdb {

namespace {
void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();

	// Register settings
	RegisterHedgedFsSettings(db);

	// Register filesystem management functions
	loader.RegisterFunction(GetHedgedFsListFilesystemsFunction());
	loader.RegisterFunction(GetHedgedFsWrapFunction());

	// Register MockFileSystem at extension load for testing purpose
	auto &opener_fs = db.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_fs.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<MockFileSystem>());
}
} // namespace

void HedgedRequestFsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HedgedRequestFsExtension::Name() {
	return "hedged_request_fs";
}

std::string HedgedRequestFsExtension::Version() const {
#ifdef EXT_VERSION_HEDGED_REQUEST_FS
	return EXT_VERSION_HEDGED_REQUEST_FS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hedged_request_fs, loader) {
	duckdb::LoadInternal(loader);
}
}
