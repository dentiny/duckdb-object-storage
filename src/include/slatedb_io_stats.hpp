#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class SlateDBFileSystem;

TableFunction GetSlateDBIoStatsFunction(SlateDBFileSystem &file_system);

} // namespace duckdb
