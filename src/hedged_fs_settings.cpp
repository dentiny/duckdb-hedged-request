#include "hedged_fs_settings.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "hedged_request_config.hpp"
#include "hedged_request_fs_entry.hpp"

namespace duckdb {

namespace {

void UpdateConfigDelay(ClientContext &context, SetScope scope, const string &setting_name,
                       HedgedRequestOperation operation, int64_t value_ms) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &object_cache = db.GetObjectCache();
	auto entry = object_cache.GetOrCreate<HedgedRequestFsEntry>(HedgedRequestFsEntry::ObjectType());
	entry->UpdateConfig(operation, std::chrono::milliseconds(value_ms));
}

void SetOpenFileHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_open_file_delay_ms", HedgedRequestOperation::OPEN_FILE, value_ms);
}

void SetGlobHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_glob_delay_ms", HedgedRequestOperation::GLOB, value_ms);
}

void SetFileExistsHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_file_exists_delay_ms", HedgedRequestOperation::FILE_EXISTS, value_ms);
}

void SetDirectoryExistsHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_directory_exists_delay_ms", HedgedRequestOperation::DIRECTORY_EXISTS,
	                  value_ms);
}

void SetGetFileSizeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_file_size_delay_ms", HedgedRequestOperation::GET_FILE_SIZE,
	                  value_ms);
}

void SetGetLastModifiedTimeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_last_modified_time_delay_ms",
	                  HedgedRequestOperation::GET_LAST_MODIFIED_TIME, value_ms);
}

void SetGetFileTypeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_file_type_delay_ms", HedgedRequestOperation::GET_FILE_TYPE,
	                  value_ms);
}

void SetGetVersionTagHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_version_tag_delay_ms", HedgedRequestOperation::GET_VERSION_TAG,
	                  value_ms);
}

void SetListFilesHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<uint64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_list_files_delay_ms", HedgedRequestOperation::LIST_FILES, value_ms);
}

void SetMaxHedgedRequestCount(ClientContext &context, SetScope scope, Value &parameter) {
	auto max_count = parameter.GetValue<uint64_t>();
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &object_cache = db.GetObjectCache();
	auto entry = object_cache.GetOrCreate<HedgedRequestFsEntry>(HedgedRequestFsEntry::ObjectType());
	entry->UpdateMaxHedgedRequestCount(NumericCast<size_t>(max_count));
}

} // namespace

void RegisterHedgedFsSettings(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "hedged_fs_open_file_delay_ms", "Delay in milliseconds before starting hedged request for OpenFile operation",
	    LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::OPEN_FILE)]),
	    SetOpenFileHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_glob_delay_ms", "Delay in milliseconds before starting hedged request for Glob operation",
	    LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::GLOB)]),
	    SetGlobHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_file_exists_delay_ms",
	    "Delay in milliseconds before starting hedged request for FileExists operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::FILE_EXISTS)]),
	    SetFileExistsHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_directory_exists_delay_ms",
	    "Delay in milliseconds before starting hedged request for DirectoryExists operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::DIRECTORY_EXISTS)]),
	    SetDirectoryExistsHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_get_file_size_delay_ms",
	    "Delay in milliseconds before starting hedged request for GetFileSize operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::GET_FILE_SIZE)]),
	    SetGetFileSizeHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_get_last_modified_time_delay_ms",
	    "Delay in milliseconds before starting hedged request for GetLastModifiedTime operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::GET_LAST_MODIFIED_TIME)]),
	    SetGetLastModifiedTimeHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_get_file_type_delay_ms",
	    "Delay in milliseconds before starting hedged request for GetFileType operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::GET_FILE_TYPE)]),
	    SetGetFileTypeHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_get_version_tag_delay_ms",
	    "Delay in milliseconds before starting hedged request for GetVersionTag operation", LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::GET_VERSION_TAG)]),
	    SetGetVersionTagHedgingDelay);

	config.AddExtensionOption(
	    "hedged_fs_list_files_delay_ms", "Delay in milliseconds before starting hedged request for ListFiles operation",
	    LogicalType::UBIGINT,
	    Value::UBIGINT(DEFAULT_HEDGING_DELAYS_MS[NumericCast<size_t>(HedgedRequestOperation::LIST_FILES)]),
	    SetListFilesHedgingDelay);

	config.AddExtensionOption("hedged_fs_max_hedged_request_count",
	                          "Maximum number of hedged requests to spawn for each operation", LogicalType::UBIGINT,
	                          Value::UBIGINT(DEFAULT_MAX_HEDGED_REQUEST_COUNT), SetMaxHedgedRequestCount);
}

} // namespace duckdb
