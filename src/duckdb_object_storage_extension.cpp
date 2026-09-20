#define DUCKDB_EXTENSION_MAIN

#include "duckdb_object_storage_extension.hpp"
#include "slatedb_cache_stats.hpp"
#include "slatedb_config_utils.hpp"
#include "slatedb_file_system.hpp"
#include "slatedb_io_stats.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

#define SLATEDB_FREEZE_GUARD(SETTING_NAME)                                                                             \
	[](ClientContext &context, SetScope, Value &value) {                                                               \
		CheckFrozenSlateDBSetting(context, SETTING_NAME, value);                                                       \
	}

namespace {

string AttachMappedDatabase(ClientContext &context, const FunctionParameters &parameters) {
	auto &database_manager = DatabaseManager::Get(context);
	auto &default_database_name = DatabaseManager::GetDefaultDatabase(context);
	auto default_database = database_manager.GetDatabase(context, default_database_name);
	if (!default_database) {
		throw InternalException("Cannot resolve the current database for SlateDB path mapping");
	}

	auto source_path = default_database->StoredPath();
	if (source_path.empty()) {
		source_path = ":memory:";
	}

	MD5Context md5;
	md5.Add(source_path);
	auto mapped_path = StringUtil::Format("duckdb_objfs://mapped_%s.db", md5.FinishHex());
	auto catalog_name = parameters.values[0].GetValue<string>();

	return StringUtil::Format("ATTACH IF NOT EXISTS %s AS %s%s", KeywordHelper::WriteQuoted(mapped_path, '\''),
	                          KeywordHelper::WriteOptionallyQuoted(catalog_name),
	                          default_database->IsReadOnly() ? " (READ_ONLY)" : "");
}

} // namespace

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	// Storage backend configuration.
	config.AddExtensionOption("duckdb_objfs_backend", "Storage backend: s3, local, or memory", LogicalType::VARCHAR,
	                          Value("local"), SLATEDB_FREEZE_GUARD("duckdb_objfs_backend"));
	config.AddExtensionOption("duckdb_objfs_bucket", "S3 bucket used by the DuckDB object filesystem",
	                          LogicalType::VARCHAR, Value(), SLATEDB_FREEZE_GUARD("duckdb_objfs_bucket"));
	config.AddExtensionOption("duckdb_objfs_root", "Local directory or S3 object prefix used by the filesystem",
	                          LogicalType::VARCHAR, Value(), SLATEDB_FREEZE_GUARD("duckdb_objfs_root"));

	// Cache configuration.
	config.AddExtensionOption("duckdb_objfs_memory_cache_size", "Foyer data-block cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(512ULL * 1024 * 1024),
	                          SLATEDB_FREEZE_GUARD("duckdb_objfs_memory_cache_size"));
	config.AddExtensionOption("duckdb_objfs_metadata_cache_size", "Foyer SST metadata cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(128ULL * 1024 * 1024),
	                          SLATEDB_FREEZE_GUARD("duckdb_objfs_metadata_cache_size"));
	config.AddExtensionOption("duckdb_objfs_cache_shards", "In-memory cache shard count; zero selects the CPU count",
	                          LogicalType::UBIGINT, Value::UBIGINT(0),
	                          SLATEDB_FREEZE_GUARD("duckdb_objfs_cache_shards"));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_path",
	                          "Local path for the persistent SST cache; empty disables it", LogicalType::VARCHAR,
	                          Value(""), SLATEDB_FREEZE_GUARD("duckdb_objfs_persistent_cache_path"));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_size", "Persistent SST cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(16ULL * 1024 * 1024 * 1024),
	                          SLATEDB_FREEZE_GUARD("duckdb_objfs_persistent_cache_size"));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_part_size", "Persistent SST cache part size in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(4ULL * 1024 * 1024),
	                          SLATEDB_FREEZE_GUARD("duckdb_objfs_persistent_cache_part_size"));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_on_flush",
	                          "Populate the persistent cache from memtable flush output", LogicalType::BOOLEAN,
	                          Value(false), SLATEDB_FREEZE_GUARD("duckdb_objfs_persistent_cache_on_flush"));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_on_compaction",
	                          "Populate the persistent cache from compaction output", LogicalType::BOOLEAN,
	                          Value(false), SLATEDB_FREEZE_GUARD("duckdb_objfs_persistent_cache_on_compaction"));

	auto file_system = make_uniq<SlateDBFileSystem>();
	loader.RegisterFunction(GetSlateDBCacheStatsFunction(*file_system));
	loader.RegisterFunction(GetSlateDBIoStatsFunction(*file_system));
	loader.RegisterFunction(PragmaFunction::PragmaCall("duckdb_objfs_attach_mapped_database", AttachMappedDatabase,
	                                                   {LogicalType::VARCHAR}));
	instance.GetFileSystem().RegisterSubSystem(std::move(file_system));
}

void DuckdbObjectStorageExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckdbObjectStorageExtension::Name() {
	return "duckdb_object_storage";
}

std::string DuckdbObjectStorageExtension::Version() const {
#ifdef EXT_VERSION_DUCKDB_OBJECT_STORAGE
	return EXT_VERSION_DUCKDB_OBJECT_STORAGE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckdb_object_storage, loader) {
	duckdb::LoadInternal(loader);
}
}
