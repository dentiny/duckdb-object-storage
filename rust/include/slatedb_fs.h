#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct slatedb_fs slatedb_fs;

typedef enum slatedb_fs_error_code {
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

int slatedb_fs_can_handle(const slatedb_fs *fs, const char *path);
const char *slatedb_fs_name(void);
const char *slatedb_fs_dummy_error(void);

#ifdef __cplusplus
}
#endif
