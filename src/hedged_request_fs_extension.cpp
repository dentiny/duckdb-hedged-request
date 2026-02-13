#define DUCKDB_EXTENSION_MAIN

#include "hedged_request_fs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void QuackScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto quack_scalar_function = ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
	loader.RegisterFunction(quack_scalar_function);
}

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
