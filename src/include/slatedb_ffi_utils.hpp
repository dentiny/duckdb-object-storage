#pragma once

#include "duckdb/common/string.hpp"

#include <cstdint>

namespace duckdb::slatedb_ffi {

void ThrowIfError(int32_t code, const string &operation);

} // namespace duckdb::slatedb_ffi
