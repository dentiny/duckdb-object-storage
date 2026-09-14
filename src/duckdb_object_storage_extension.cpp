#define DUCKDB_EXTENSION_MAIN

#include "duckdb_object_storage_extension.hpp"
#include "slatedb_file_system.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("duckdb_objfs_backend", "Storage backend: s3, local, or memory", LogicalType::VARCHAR,
	                          Value("s3"));
	config.AddExtensionOption("duckdb_objfs_bucket", "S3 bucket used by the DuckDB object filesystem",
	                          LogicalType::VARCHAR);
	config.AddExtensionOption("duckdb_objfs_root", "Object prefix reserved for the DuckDB object filesystem",
	                          LogicalType::VARCHAR, Value("duckdb_objfs"));
	config.AddExtensionOption("duckdb_objfs_local_path", "Local directory used by the DuckDB object filesystem",
	                          LogicalType::VARCHAR);
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
