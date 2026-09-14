#include "slatedb_ffi_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "slatedb_fs.h"

namespace duckdb {

void ThrowSlateDBError(int32_t code, const string &operation) {
	if (code == SLATEDB_FS_ERROR_NONE) {
		return;
	}
	auto message = slatedb_fs_last_error_message();
	auto detail = message ? string(message) : string("unknown SlateDB filesystem error");
	if (code == SLATEDB_FS_ERROR_INVALID_ARGUMENT) {
		throw InvalidInputException("%s: %s", operation, detail);
	}
	throw IOException("%s: %s", operation, detail);
}

} // namespace duckdb
