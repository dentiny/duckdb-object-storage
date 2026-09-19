#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct slatedb_fs slatedb_fs;
typedef struct slatedb_file_handle slatedb_file_handle;

typedef struct slatedb_fs_open_options {
	// Open the file for reads.
	int32_t read;
	// Open the file for writes.
	int32_t write;
	// Create the file when it does not exist.
	int32_t create;
	// Position sequential writes at the end of the file.
	int32_t append;
	// Clear an existing file when it is opened.
	int32_t truncate_existing;
} slatedb_fs_open_options;

typedef struct slatedb_s3_config {
	// S3 bucket that stores SlateDB objects.
	const char *bucket;
	// Prefix within the bucket reserved for this filesystem.
	const char *root;
	// Optional S3-compatible service endpoint.
	const char *endpoint;
	// AWS region used to sign S3 requests.
	const char *region;
	// Optional access-key identifier.
	const char *key_id;
	// Optional secret access key.
	const char *secret;
	// Optional temporary-credential session token.
	const char *session_token;
	// Whether the endpoint should use HTTPS.
	int32_t use_ssl;
	// Whether S3 requests should use virtual-host-style addressing.
	int32_t virtual_host_style;
} slatedb_s3_config;

typedef struct slatedb_cache_config {
	// Maximum bytes retained in the in-memory data-block cache; zero disables it.
	uint64_t block_cache_size_bytes;
	// Maximum bytes retained in the in-memory SST metadata cache; zero disables it.
	uint64_t metadata_cache_size_bytes;
	// Number of cache shards; zero selects an implementation default.
	uint64_t cache_shards;
	// Local directory for persistent cached SST parts; empty disables persistence.
	const char *persistent_cache_path;
	// Maximum total size of the persistent cache.
	uint64_t persistent_cache_size_bytes;
	// Size of each persistent cache part; must be a multiple of 1024 bytes.
	uint64_t persistent_cache_part_size_bytes;
	// Whether memtable flush output should be inserted into the persistent cache.
	int32_t persistent_cache_on_flush;
	// Whether compaction output should be inserted into the persistent cache.
	int32_t persistent_cache_on_compaction;
} slatedb_cache_config;

typedef struct slatedb_cache_stats {
	// Successful in-memory data-block cache lookups.
	uint64_t block_cache_hits;
	// Unsuccessful in-memory data-block cache lookups.
	uint64_t block_cache_misses;
	// Successful in-memory metadata cache lookups.
	uint64_t metadata_cache_hits;
	// Unsuccessful in-memory metadata cache lookups.
	uint64_t metadata_cache_misses;
	// Successful persistent cache part lookups.
	uint64_t persistent_cache_hits;
	// Unsuccessful persistent cache part lookups.
	uint64_t persistent_cache_misses;
	// Current number of persistent cache entries.
	uint64_t persistent_cache_entries;
	// Current persistent cache size.
	uint64_t persistent_cache_size_bytes;
	// Number of persistent cache entries evicted.
	uint64_t persistent_cache_evictions;
	// Number of persistent cache bytes evicted.
	uint64_t persistent_cache_evicted_bytes;
} slatedb_cache_stats;

typedef struct slatedb_io_stats {
	// Number of OpenDAL read requests.
	uint64_t read_request_count;
	// Average OpenDAL read latency in seconds.
	double read_average_latency_seconds;
	// Population standard deviation of OpenDAL read latency in seconds.
	double read_stddev_latency_seconds;
	// Number of OpenDAL write requests.
	uint64_t write_request_count;
	// Average OpenDAL write latency in seconds.
	double write_average_latency_seconds;
	// Population standard deviation of OpenDAL write latency in seconds.
	double write_stddev_latency_seconds;
} slatedb_io_stats;

typedef enum slatedb_fs_error_code {
	SLATEDB_FS_ERROR_NONE = 0,
	SLATEDB_FS_ERROR_METADATA_DECODE = 1,
	SLATEDB_FS_ERROR_FILE_NOT_FOUND = 2,
	SLATEDB_FS_ERROR_FILE_ALREADY_EXISTS = 3,
	SLATEDB_FS_ERROR_READ_ONLY_VIOLATION = 4,
	SLATEDB_FS_ERROR_SLATE_DB = 5,
	SLATEDB_FS_ERROR_INVALID_ARGUMENT = 6,
	SLATEDB_FS_ERROR_IO = 7,
} slatedb_fs_error_code;

int32_t slatedb_fs_create_memory(const slatedb_cache_config *cache_config, slatedb_fs **output);
int32_t slatedb_fs_create_local(const char *root, const slatedb_cache_config *cache_config, slatedb_fs **output);
int32_t slatedb_fs_create_s3(const slatedb_s3_config *config, const slatedb_cache_config *cache_config,
                             slatedb_fs **output);
void slatedb_fs_destroy(slatedb_fs *fs);
int32_t slatedb_fs_get_cache_stats(const slatedb_fs *fs, slatedb_cache_stats *output);
int32_t slatedb_fs_get_io_stats(const slatedb_fs *fs, slatedb_io_stats *output);

// Operations return zero on success and a stable error code on failure.
// The error message remains valid on the calling thread until its next failure.
int32_t slatedb_fs_open_file(const slatedb_fs *fs, const char *path, const slatedb_fs_open_options *options,
                             slatedb_file_handle **output);
int32_t slatedb_fs_file_exists(const slatedb_fs *fs, const char *path, int32_t *output);
int32_t slatedb_fs_remove_file(const slatedb_fs *fs, const char *path);
int32_t slatedb_fs_move_file(const slatedb_fs *fs, const char *source, const char *target);
int32_t slatedb_file_read(const slatedb_file_handle *handle, uint8_t *buffer, size_t len, size_t *bytes_read);
int32_t slatedb_file_pread(const slatedb_file_handle *handle, uint8_t *buffer, size_t len, uint64_t offset,
                           size_t *bytes_read);
int32_t slatedb_file_write(const slatedb_file_handle *handle, const uint8_t *buffer, size_t len,
                           size_t *bytes_written);
int32_t slatedb_file_pwrite(const slatedb_file_handle *handle, const uint8_t *buffer, size_t len, uint64_t offset);
int32_t slatedb_file_sync(const slatedb_file_handle *handle);
int32_t slatedb_file_truncate(const slatedb_file_handle *handle, uint64_t new_size);
int32_t slatedb_file_seek(const slatedb_file_handle *handle, uint64_t position);
int32_t slatedb_file_get_position(const slatedb_file_handle *handle, uint64_t *output);
int32_t slatedb_file_get_size(const slatedb_file_handle *handle, uint64_t *output);
int32_t slatedb_file_close(slatedb_file_handle *handle);
void slatedb_file_destroy(slatedb_file_handle *handle);

const char *slatedb_fs_last_error_message(void);

int slatedb_fs_can_handle(const slatedb_fs *fs, const char *path);
const char *slatedb_fs_name(void);

#ifdef __cplusplus
}
#endif
