#include "slatedb_config_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

string GetRequiredSetting(optional_ptr<FileOpener> opener, const string &name) {
	Value value;
	if (!FileOpener::TryGetCurrentSetting(opener, name, value) || value.IsNull()) {
		throw InvalidConfigurationException("Required setting '%s' is not configured", name);
	}
	auto result = value.GetValue<string>();
	if (result.empty()) {
		throw InvalidConfigurationException("Required setting '%s' must not be empty", name);
	}
	return result;
}

string GetOptionalSetting(optional_ptr<FileOpener> opener, const string &name) {
	Value value;
	if (!FileOpener::TryGetCurrentSetting(opener, name, value) || value.IsNull()) {
		return "";
	}
	return value.GetValue<string>();
}

template <class T>
T GetSettingOrDefault(optional_ptr<FileOpener> opener, const string &name, T default_value) {
	Value value;
	if (!FileOpener::TryGetCurrentSetting(opener, name, value) || value.IsNull()) {
		return default_value;
	}
	return value.GetValue<T>();
}

S3InitializationConfig ReadS3InitializationConfig(optional_ptr<FileOpener> opener) {
	if (!opener) {
		throw InvalidConfigurationException("Cannot initialize object storage without a FileOpener");
	}

	S3InitializationConfig result;
	result.bucket = GetRequiredSetting(opener, "duckdb_objfs_bucket");
	result.root = GetOptionalSetting(opener, "duckdb_objfs_root");
	if (result.root.empty()) {
		result.root = "duckdb_objfs";
	}

	auto secret_path = StringUtil::Format("s3://%s", result.bucket);
	if (!result.root.empty()) {
		auto scope_root = result.root;
		while (!scope_root.empty() && scope_root[0] == '/') {
			scope_root.erase(0, 1);
		}
		secret_path = StringUtil::Format("%s/%s", secret_path, scope_root);
	}
	FileOpenerInfo info {secret_path};
	KeyValueSecretReader secret_reader(*opener, &info, "s3");
	secret_reader.TryGetSecretKey("key_id", result.key_id);
	secret_reader.TryGetSecretKey("secret", result.secret);
	secret_reader.TryGetSecretKey("session_token", result.session_token);
	secret_reader.TryGetSecretKey("endpoint", result.endpoint);
	secret_reader.TryGetSecretKey("region", result.region);
	secret_reader.TryGetSecretKey("use_ssl", result.use_ssl);

	string url_style;
	secret_reader.TryGetSecretKey("url_style", url_style);
	if (!url_style.empty() && url_style != "path" && url_style != "vhost") {
		throw InvalidConfigurationException("S3 secret url_style must be either 'path' or 'vhost'");
	}
	result.virtual_host_style = url_style == "vhost";
	return result;
}

CacheInitializationConfig ReadCacheInitializationConfig(optional_ptr<FileOpener> opener) {
	CacheInitializationConfig result;
	result.block_cache_size_bytes =
	    GetSettingOrDefault<uint64_t>(opener, "duckdb_objfs_memory_cache_size", result.block_cache_size_bytes);
	result.metadata_cache_size_bytes =
	    GetSettingOrDefault<uint64_t>(opener, "duckdb_objfs_metadata_cache_size", result.metadata_cache_size_bytes);
	result.cache_shards = GetSettingOrDefault<uint64_t>(opener, "duckdb_objfs_cache_shards", result.cache_shards);
	result.persistent_cache_path = GetOptionalSetting(opener, "duckdb_objfs_persistent_cache_path");
	result.persistent_cache_size_bytes =
	    GetSettingOrDefault<uint64_t>(opener, "duckdb_objfs_persistent_cache_size", result.persistent_cache_size_bytes);
	result.persistent_cache_part_size_bytes = GetSettingOrDefault<uint64_t>(
	    opener, "duckdb_objfs_persistent_cache_part_size", result.persistent_cache_part_size_bytes);
	result.persistent_cache_on_flush =
	    GetSettingOrDefault<bool>(opener, "duckdb_objfs_persistent_cache_on_flush", result.persistent_cache_on_flush);
	result.persistent_cache_on_compaction = GetSettingOrDefault<bool>(
	    opener, "duckdb_objfs_persistent_cache_on_compaction", result.persistent_cache_on_compaction);
	return result;
}

void CheckFrozenSlateDBSetting(ClientContext &context, const string &name, const Value &new_value) {
	auto frozen = context.db->GetObjectCache().Get<FrozenSlateDBSettings>(FrozenSlateDBSettings::CACHE_KEY);
	if (!frozen) {
		// The filesystem has not been initialized yet; the setting still takes effect.
		return;
	}
	auto entry = frozen->values.find(name);
	if (entry == frozen->values.end()) {
		// Not an initialization setting.
		return;
	}

	auto raw_string = new_value.IsNull() ? "" : new_value.GetValue<string>();
	string effective;
	if (name == "duckdb_objfs_backend") {
		effective = StringUtil::Lower(raw_string);
		if (effective.empty()) {
			effective = "local";
		}
	} else if (name == "duckdb_objfs_root") {
		effective = raw_string;
		auto &backend = frozen->values.at("duckdb_objfs_backend");
		if (backend != "memory" && effective.empty()) {
			effective = backend == "s3" ? "duckdb_objfs" : ".duckdb_objfs";
		}
	} else {
		effective = new_value.IsNull() ? "" : new_value.ToString();
	}

	// slatedb and foyer doesn't allow runtime configuration changes, so directly throw an error.
	if (effective != entry->second) {
		throw InvalidConfigurationException(
		    "Cannot change setting '%s' from '%s' to '%s': the SlateDB filesystem is already initialized. "
		    "Restart the database to use a different value.",
		    name, entry->second, effective);
	}
}

} // namespace duckdb
