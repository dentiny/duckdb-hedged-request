#include "hedged_fs_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "hedged_file_system.hpp"
#include "hedged_request_fs_entry.hpp"

namespace duckdb {

namespace {

// Util to get the VirtualFileSystem from context
FileSystem &GetVirtualFileSystem(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &opener_fs = db.GetFileSystem().Cast<OpenerFileSystem>();
	return opener_fs.GetFileSystem();
}

// Util to get or create HedgedRequestFsEntry
shared_ptr<HedgedRequestFsEntry> GetOrCreateHedgedRequestFsEntry(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &object_cache = db.GetObjectCache();
	return object_cache.GetOrCreate<HedgedRequestFsEntry>(HedgedRequestFsEntry::ObjectType());
}

//===--------------------------------------------------------------------===//
// hedged_fs_list_filesystems() - Table Function
//===--------------------------------------------------------------------===//

struct ListFilesystemsData : public GlobalTableFunctionState {
	vector<string> filesystems;
	idx_t current_idx;

	ListFilesystemsData() : current_idx(0) {
	}
};

unique_ptr<FunctionData> ListFilesystemsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> ListFilesystemsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ListFilesystemsData>();
	auto &vfs = GetVirtualFileSystem(context).Cast<VirtualFileSystem>();
	result->filesystems = vfs.ListSubSystems();
	std::sort(result->filesystems.begin(), result->filesystems.end());
	return std::move(result);
}

void ListFilesystemsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<ListFilesystemsData>();

	idx_t count = 0;
	while (state.current_idx < state.filesystems.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(state.filesystems[state.current_idx]));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// hedged_fs_wrap(filesystem_name)
//===--------------------------------------------------------------------===//

void HedgedFsWrapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &vfs = GetVirtualFileSystem(context).Cast<VirtualFileSystem>();
	auto entry = GetOrCreateHedgedRequestFsEntry(context);

	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t fs_name) {
		string fs_str = fs_name.GetString();
		auto extracted_fs = vfs.ExtractSubSystem(fs_str);
		if (!extracted_fs) {
			throw InvalidInputException(
			    "Filesystem '%s' not found. Use hedged_fs_list_filesystems() to see available filesystems.", fs_str);
		}

		auto wrapped_fs = make_uniq<HedgedFileSystem>(std::move(extracted_fs), entry);
		string wrapped_name = wrapped_fs->GetName();
		vfs.RegisterSubSystem(std::move(wrapped_fs));
		auto &db = DatabaseInstance::GetDatabase(context);
		DUCKDB_LOG_DEBUG(db, StringUtil::Format("Wrap filesystem %s with hedged filesystem (registered as %s).", fs_str,
		                                        wrapped_name));

		return true;
	});
}

} // namespace

TableFunction GetHedgedFsListFilesystemsFunction() {
	TableFunction func("hedged_fs_list_filesystems", {}, ListFilesystemsFunction, ListFilesystemsBind,
	                   ListFilesystemsInit);
	return func;
}

ScalarFunction GetHedgedFsWrapFunction() {
	return ScalarFunction("hedged_fs_wrap", {/*filesystem_name=*/LogicalType {LogicalTypeId::VARCHAR}},
	                      /*return_type=*/LogicalType {LogicalTypeId::BOOLEAN}, HedgedFsWrapFunction);
}

} // namespace duckdb
