#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class SlateDBFileSystem;

TableFunction GetSlateDBCacheStatsFunction(SlateDBFileSystem &file_system);

} // namespace duckdb
