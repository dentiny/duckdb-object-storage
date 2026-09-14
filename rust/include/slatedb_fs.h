#pragma once

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
} slatedb_fs_open_options;

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

slatedb_fs *slatedb_fs_create(void);
void slatedb_fs_destroy(slatedb_fs *fs);

// Operations return zero on success and a stable error code on failure.
// The error message remains valid on the calling thread until its next failure.
int32_t slatedb_fs_open_file(const slatedb_fs *fs, const char *path, const slatedb_fs_open_options *options,
                             slatedb_file_handle **output);
int32_t slatedb_file_close(slatedb_file_handle *handle);
void slatedb_file_destroy(slatedb_file_handle *handle);

const char *slatedb_fs_last_error_message(void);

int slatedb_fs_can_handle(const slatedb_fs *fs, const char *path);
const char *slatedb_fs_name(void);
const char *slatedb_fs_dummy_error(void);

#ifdef __cplusplus
}
#endif
