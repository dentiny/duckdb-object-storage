#pragma once

#include "duckdb/common/file_opener.hpp"

namespace duckdb {

struct S3InitializationConfig {
	string bucket;
	string root;
	string endpoint;
	string region;
	string key_id;
	string secret;
	string session_token;
	bool use_ssl = true;
	bool virtual_host_style = false;
};

string GetRequiredSetting(optional_ptr<FileOpener> opener, const string &name);
string GetOptionalSetting(optional_ptr<FileOpener> opener, const string &name);
S3InitializationConfig ReadS3InitializationConfig(optional_ptr<FileOpener> opener);

} // namespace duckdb
