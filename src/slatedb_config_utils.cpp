#include "slatedb_config_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
namespace slatedb_config {

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

	auto secret_path = "s3://" + result.bucket;
	if (!result.root.empty()) {
		auto scope_root = result.root;
		while (!scope_root.empty() && scope_root[0] == '/') {
			scope_root.erase(0, 1);
		}
		secret_path += "/" + scope_root;
	}
	FileOpenerInfo info {secret_path};
	KeyValueSecretReader secret_reader(*opener, &info, "s3");
	result.key_id = secret_reader.GetSecretKey("key_id").GetValue<string>();
	result.secret = secret_reader.GetSecretKey("secret").GetValue<string>();
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

} // namespace slatedb_config
} // namespace duckdb
