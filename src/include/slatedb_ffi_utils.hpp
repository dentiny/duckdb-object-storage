#pragma once

#include "duckdb/common/string.hpp"

#include <cstdint>

namespace duckdb {

void ThrowSlateDBError(int32_t code, const string &operation);

} // namespace duckdb
