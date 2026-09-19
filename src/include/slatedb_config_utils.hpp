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

struct CacheInitializationConfig {
	uint64_t block_cache_size_bytes = 512ULL * 1024 * 1024;
	uint64_t metadata_cache_size_bytes = 128ULL * 1024 * 1024;
	uint64_t foyer_shards = 0;
	string persistent_cache_path;
	uint64_t persistent_cache_size_bytes = 16ULL * 1024 * 1024 * 1024;
	uint64_t persistent_cache_part_size_bytes = 4ULL * 1024 * 1024;
	bool persistent_cache_on_flush = false;
	bool persistent_cache_on_compaction = false;
	string persistent_cache_preload = "none";
};

string GetRequiredSetting(optional_ptr<FileOpener> opener, const string &name);
string GetOptionalSetting(optional_ptr<FileOpener> opener, const string &name);
S3InitializationConfig ReadS3InitializationConfig(optional_ptr<FileOpener> opener);
CacheInitializationConfig ReadCacheInitializationConfig(optional_ptr<FileOpener> opener);

} // namespace duckdb
