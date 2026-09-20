#pragma once

#include "duckdb/common/file_opener.hpp"

namespace duckdb {

struct S3InitializationConfig {
	//! S3 bucket that stores SlateDB objects.
	string bucket;
	//! Prefix within the bucket reserved for this filesystem.
	string root;
	//! Optional S3-compatible service endpoint.
	string endpoint;
	//! AWS region used to sign S3 requests.
	string region;
	//! Access-key identifier resolved from DuckDB's secret manager.
	string key_id;
	//! Secret access key resolved from DuckDB's secret manager.
	string secret;
	//! Optional temporary-credential session token.
	string session_token;
	//! Whether the endpoint should use HTTPS.
	bool use_ssl = true;
	//! Whether S3 requests should use virtual-host-style addressing.
	bool virtual_host_style = false;
};

struct CacheInitializationConfig {
	//! Maximum bytes retained in the in-memory data-block cache; zero disables it.
	uint64_t block_cache_size_bytes = 512ULL * 1024 * 1024;
	//! Maximum bytes retained in the in-memory SST metadata cache; zero disables it.
	uint64_t metadata_cache_size_bytes = 128ULL * 1024 * 1024;
	//! Number of in-memory cache shards; zero selects an implementation default.
	uint64_t cache_shards = 0;
	//! Local directory for persistent cached SST parts; empty disables persistence.
	string persistent_cache_path;
	//! Maximum total size of the persistent cache.
	uint64_t persistent_cache_size_bytes = 16ULL * 1024 * 1024 * 1024;
	//! Size of each persistent cache part; must be a multiple of 1024 bytes.
	uint64_t persistent_cache_part_size_bytes = 4ULL * 1024 * 1024;
	//! Whether memtable flush output should be inserted into the persistent cache.
	bool persistent_cache_on_flush = false;
	//! Whether compaction output should be inserted into the persistent cache.
	bool persistent_cache_on_compaction = false;
};

struct DatabaseInitializationConfig {
	CacheInitializationConfig cache;
	bool read_only = false;
};

string GetRequiredSetting(optional_ptr<FileOpener> opener, const string &name);
string GetOptionalSetting(optional_ptr<FileOpener> opener, const string &name);
S3InitializationConfig ReadS3InitializationConfig(optional_ptr<FileOpener> opener);
CacheInitializationConfig ReadCacheInitializationConfig(optional_ptr<FileOpener> opener);

} // namespace duckdb
