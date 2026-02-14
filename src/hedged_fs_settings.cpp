#include "hedged_fs_settings.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "hedged_request_fs_entry.hpp"

namespace duckdb {

namespace {

void UpdateConfigDelay(ClientContext &context, SetScope scope, const string &setting_name,
                       std::chrono::milliseconds HedgedRequestConfig::*field, int64_t value_ms) {
	if (value_ms < 0) {
		throw InvalidInputException("%s must be non-negative, got %lld", setting_name, value_ms);
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &object_cache = db.GetObjectCache();
	auto entry = object_cache.GetOrCreate<HedgedRequestFsEntry>(HedgedRequestFsEntry::ObjectType());
	auto config = entry->GetConfig();
	config.*field = std::chrono::milliseconds(value_ms);
	entry->SetConfig(config);
}

void SetOpenFileHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_open_file_delay_ms", &HedgedRequestConfig::open_file_hedging_delay_ms,
	                  value_ms);
}

void SetGlobHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_glob_delay_ms", &HedgedRequestConfig::glob_hedging_delay_ms, value_ms);
}

void SetFileExistsHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_file_exists_delay_ms",
	                  &HedgedRequestConfig::file_exists_hedging_delay_ms, value_ms);
}

void SetDirectoryExistsHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_directory_exists_delay_ms",
	                  &HedgedRequestConfig::directory_exists_hedging_delay_ms, value_ms);
}

void SetGetFileSizeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_file_size_delay_ms",
	                  &HedgedRequestConfig::get_file_size_hedging_delay_ms, value_ms);
}

void SetGetLastModifiedTimeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_last_modified_time_delay_ms",
	                  &HedgedRequestConfig::get_last_modified_time_hedging_delay_ms, value_ms);
}

void SetGetFileTypeHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_file_type_delay_ms",
	                  &HedgedRequestConfig::get_file_type_hedging_delay_ms, value_ms);
}

void SetGetVersionTagHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_get_version_tag_delay_ms",
	                  &HedgedRequestConfig::get_version_tag_hedging_delay_ms, value_ms);
}

void SetListFilesHedgingDelay(ClientContext &context, SetScope scope, Value &parameter) {
	auto value_ms = parameter.GetValue<int64_t>();
	UpdateConfigDelay(context, scope, "hedged_fs_list_files_delay_ms",
	                  &HedgedRequestConfig::list_files_hedging_delay_ms, value_ms);
}

} // namespace

void RegisterHedgedFsSettings(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("hedged_fs_open_file_delay_ms",
	                          "Delay in milliseconds before starting hedged request for OpenFile operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetOpenFileHedgingDelay);

	config.AddExtensionOption("hedged_fs_glob_delay_ms",
	                          "Delay in milliseconds before starting hedged request for Glob operation",
	                          LogicalType::BIGINT, Value::BIGINT(5000), SetGlobHedgingDelay);

	config.AddExtensionOption("hedged_fs_file_exists_delay_ms",
	                          "Delay in milliseconds before starting hedged request for FileExists operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetFileExistsHedgingDelay);

	config.AddExtensionOption("hedged_fs_directory_exists_delay_ms",
	                          "Delay in milliseconds before starting hedged request for DirectoryExists operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetDirectoryExistsHedgingDelay);

	config.AddExtensionOption("hedged_fs_get_file_size_delay_ms",
	                          "Delay in milliseconds before starting hedged request for GetFileSize operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetGetFileSizeHedgingDelay);

	config.AddExtensionOption("hedged_fs_get_last_modified_time_delay_ms",
	                          "Delay in milliseconds before starting hedged request for GetLastModifiedTime operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetGetLastModifiedTimeHedgingDelay);

	config.AddExtensionOption("hedged_fs_get_file_type_delay_ms",
	                          "Delay in milliseconds before starting hedged request for GetFileType operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetGetFileTypeHedgingDelay);

	config.AddExtensionOption("hedged_fs_get_version_tag_delay_ms",
	                          "Delay in milliseconds before starting hedged request for GetVersionTag operation",
	                          LogicalType::BIGINT, Value::BIGINT(3000), SetGetVersionTagHedgingDelay);

	config.AddExtensionOption("hedged_fs_list_files_delay_ms",
	                          "Delay in milliseconds before starting hedged request for ListFiles operation",
	                          LogicalType::BIGINT, Value::BIGINT(5000), SetListFilesHedgingDelay);
}

} // namespace duckdb
