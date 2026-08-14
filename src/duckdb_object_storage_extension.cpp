#define DUCKDB_EXTENSION_MAIN

#include "duckdb_object_storage_extension.hpp"
#include "slatedb_file_system.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DuckdbObjectStorageScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckdbObjectStorage " + name.GetString() + " 🐥");
	});
}

inline void DuckdbObjectStorageOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckdbObjectStorage " + name.GetString() +
		                                           ", my linked OpenSSL version is " + OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	instance.GetFileSystem().RegisterSubSystem(make_uniq<SlateDBFileSystem>());

	// Register a scalar function
	auto duckdb_object_storage_scalar_function = ScalarFunction("duckdb_object_storage", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, DuckdbObjectStorageScalarFun);
	loader.RegisterFunction(duckdb_object_storage_scalar_function);

	// Register another scalar function
	auto duckdb_object_storage_openssl_version_scalar_function =
	    ScalarFunction("duckdb_object_storage_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   DuckdbObjectStorageOpenSSLVersionScalarFun);
	loader.RegisterFunction(duckdb_object_storage_openssl_version_scalar_function);
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
