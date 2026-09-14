#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct slatedb_fs slatedb_fs;
typedef struct slatedb_file_handle slatedb_file_handle;

typedef struct slatedb_fs_open_options {
	int32_t read;
	int32_t write;
	int32_t create;
	int32_t append;
	int32_t truncate_existing;
	int32_t exclusive_create;
} slatedb_fs_open_options;

typedef struct slatedb_s3_config {
	const char *bucket;
	const char *root;
	const char *endpoint;
	const char *region;
	const char *key_id;
	const char *secret;
	const char *session_token;
	int32_t use_ssl;
	int32_t virtual_host_style;
} slatedb_s3_config;

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

int32_t slatedb_fs_create_memory(slatedb_fs **output);
int32_t slatedb_fs_create_local(const char *root, slatedb_fs **output);
int32_t slatedb_fs_create_s3(const slatedb_s3_config *config, slatedb_fs **output);
void slatedb_fs_destroy(slatedb_fs *fs);

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
