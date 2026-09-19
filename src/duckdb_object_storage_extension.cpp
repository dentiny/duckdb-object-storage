#define DUCKDB_EXTENSION_MAIN

#include "duckdb_object_storage_extension.hpp"
#include "slatedb_file_system.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("duckdb_objfs_backend", "Storage backend: s3, local, or memory", LogicalType::VARCHAR,
	                          Value("local"));
	config.AddExtensionOption("duckdb_objfs_bucket", "S3 bucket used by the DuckDB object filesystem",
	                          LogicalType::VARCHAR);
	config.AddExtensionOption("duckdb_objfs_root", "Local directory or S3 object prefix used by the filesystem",
	                          LogicalType::VARCHAR);
	config.AddExtensionOption("duckdb_objfs_memory_cache_size", "Foyer data-block cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(512ULL * 1024 * 1024));
	config.AddExtensionOption("duckdb_objfs_metadata_cache_size", "Foyer SST metadata cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(128ULL * 1024 * 1024));
	config.AddExtensionOption("duckdb_objfs_cache_shards", "In-memory cache shard count; zero selects the CPU count",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_path",
	                          "Local path for the persistent SST cache; empty disables it", LogicalType::VARCHAR,
	                          Value(""));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_size", "Persistent SST cache capacity in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(16ULL * 1024 * 1024 * 1024));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_part_size", "Persistent SST cache part size in bytes",
	                          LogicalType::UBIGINT, Value::UBIGINT(4ULL * 1024 * 1024));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_on_flush",
	                          "Populate the persistent cache from memtable flush output", LogicalType::BOOLEAN,
	                          Value(false));
	config.AddExtensionOption("duckdb_objfs_persistent_cache_on_compaction",
	                          "Populate the persistent cache from compaction output", LogicalType::BOOLEAN,
	                          Value(false));
	instance.GetFileSystem().RegisterSubSystem(make_uniq<SlateDBFileSystem>());
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
