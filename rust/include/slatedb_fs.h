#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct slatedb_fs slatedb_fs;

slatedb_fs *slatedb_fs_create(void);
void slatedb_fs_destroy(slatedb_fs *fs);

int slatedb_fs_can_handle(const slatedb_fs *fs, const char *path);
const char *slatedb_fs_name(void);
const char *slatedb_fs_dummy_error(void);

#ifdef __cplusplus
}
#endif
