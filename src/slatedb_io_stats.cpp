#include "slatedb_io_stats.hpp"
#include "slatedb_file_system.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

struct IoStatsFunctionInfo : TableFunctionInfo {
	explicit IoStatsFunctionInfo(SlateDBFileSystem &file_system_p) : file_system(file_system_p) {
	}

	SlateDBFileSystem &file_system;
};

struct IoStatsBindData : TableFunctionData {
	explicit IoStatsBindData(SlateDBFileSystem &file_system_p) : file_system(file_system_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IoStatsBindData>(file_system);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IoStatsBindData>();
		return &file_system == &other.file_system;
	}

	SlateDBFileSystem &file_system;
};

struct IoStatsGlobalState : GlobalTableFunctionState {
	slatedb_io_stats stats {};
	idx_t offset = 0;
	bool initialized = false;
};

unique_ptr<FunctionData> IoStatsBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                     vector<string> &names) {
	names.emplace_back("operation");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("request_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("average_latency_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("stddev_latency_ms");
	return_types.emplace_back(LogicalType::DOUBLE);

	if (!input.info) {
		throw InternalException("duckdb_objfs_io_stats is missing function information");
	}
	auto &info = input.info->Cast<IoStatsFunctionInfo>();
	return make_uniq<IoStatsBindData>(info.file_system);
}

unique_ptr<GlobalTableFunctionState> IoStatsInit(ClientContext &, TableFunctionInitInput &input) {
	auto result = make_uniq<IoStatsGlobalState>();
	auto &bind_data = input.bind_data->Cast<IoStatsBindData>();
	result->initialized = bind_data.file_system.TryGetIoStats(result->stats);
	return std::move(result);
}

void IoStatsFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<IoStatsGlobalState>();
	if (!state.initialized || state.offset >= 2) {
		return;
	}

	idx_t count = 0;
	while (state.offset < 2 && count < STANDARD_VECTOR_SIZE) {
		const bool is_read = state.offset == 0;
		const auto request_count = is_read ? state.stats.read_request_count : state.stats.write_request_count;
		const auto average_ms =
		    is_read ? state.stats.read_average_latency_ms : state.stats.write_average_latency_ms;
		const auto stddev_ms = is_read ? state.stats.read_stddev_latency_ms : state.stats.write_stddev_latency_ms;

		output.SetValue(0, count, Value(is_read ? "read" : "write"));
		output.SetValue(1, count, Value::UBIGINT(request_count));
		output.SetValue(2, count, Value::DOUBLE(average_ms));
		output.SetValue(3, count, Value::DOUBLE(stddev_ms));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetSlateDBIoStatsFunction(SlateDBFileSystem &file_system) {
	TableFunction function("duckdb_objfs_io_stats", {}, IoStatsFunction, IoStatsBind, IoStatsInit);
	function.function_info = make_shared_ptr<IoStatsFunctionInfo>(file_system);
	return function;
}

} // namespace duckdb
