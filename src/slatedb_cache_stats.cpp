#include "slatedb_cache_stats.hpp"
#include "slatedb_file_system.hpp"

#include "duckdb/common/exception.hpp"

#include <string_view>

namespace duckdb {

namespace {

constexpr std::string_view FUNCTION_NAME = "duckdb_objfs_cache_stats";
constexpr std::string_view MEMORY_DATA = "memory_data";
constexpr std::string_view MEMORY_METADATA = "memory_metadata";
constexpr std::string_view PERSISTENT = "persistent";

struct CacheStatsFunctionInfo : TableFunctionInfo {
	explicit CacheStatsFunctionInfo(SlateDBFileSystem &file_system_p) : file_system(file_system_p) {
	}

	SlateDBFileSystem &file_system;
};

struct CacheStatsBindData : TableFunctionData {
	explicit CacheStatsBindData(SlateDBFileSystem &file_system_p) : file_system(file_system_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CacheStatsBindData>(file_system);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CacheStatsBindData>();
		return &file_system == &other.file_system;
	}

	SlateDBFileSystem &file_system;
};

struct CacheStatsGlobalState : GlobalTableFunctionState {
	slatedb_cache_stats stats {};
	idx_t offset = 0;
	bool initialized = false;
};

unique_ptr<FunctionData> CacheStatsBind(ClientContext &, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("cache");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("hit_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("miss_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("hit_rate");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("entry_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("size_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("eviction_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("evicted_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	if (!input.info) {
		throw InternalException("duckdb_objfs_cache_stats is missing function information");
	}
	auto &info = input.info->Cast<CacheStatsFunctionInfo>();
	return make_uniq<CacheStatsBindData>(info.file_system);
}

unique_ptr<GlobalTableFunctionState> CacheStatsInit(ClientContext &, TableFunctionInitInput &input) {
	auto result = make_uniq<CacheStatsGlobalState>();
	auto &bind_data = input.bind_data->Cast<CacheStatsBindData>();
	result->initialized = bind_data.file_system.TryGetCacheStats(result->stats);
	return result;
}

Value HitRate(uint64_t hits, uint64_t misses) {
	auto accesses = hits + misses;
	if (accesses == 0) {
		return Value();
	}
	return Value::DOUBLE(static_cast<double>(hits) / static_cast<double>(accesses));
}

void AppendCommonValues(DataChunk &output, std::string_view cache, uint64_t hits, uint64_t misses) {
	output.data[0].Append(Value(cache));
	output.data[1].Append(Value::UBIGINT(hits));
	output.data[2].Append(Value::UBIGINT(misses));
	output.data[3].Append(HitRate(hits, misses));
}

void CacheStatsFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<CacheStatsGlobalState>();
	if (!state.initialized || state.offset >= 3) {
		return;
	}

	idx_t count = 0;
	while (state.offset < 3 && count < STANDARD_VECTOR_SIZE) {
		if (state.offset == 0) {
			AppendCommonValues(output, MEMORY_DATA, state.stats.block_cache_hits, state.stats.block_cache_misses);
			output.data[4].Append(Value());
			output.data[5].Append(Value());
			output.data[6].Append(Value());
			output.data[7].Append(Value());
		} else if (state.offset == 1) {
			AppendCommonValues(output, MEMORY_METADATA, state.stats.metadata_cache_hits,
			                   state.stats.metadata_cache_misses);
			output.data[4].Append(Value());
			output.data[5].Append(Value());
			output.data[6].Append(Value());
			output.data[7].Append(Value());
		} else {
			AppendCommonValues(output, PERSISTENT, state.stats.persistent_cache_hits,
			                   state.stats.persistent_cache_misses);
			output.data[4].Append(Value::UBIGINT(state.stats.persistent_cache_entries));
			output.data[5].Append(Value::UBIGINT(state.stats.persistent_cache_size_bytes));
			output.data[6].Append(Value::UBIGINT(state.stats.persistent_cache_evictions));
			output.data[7].Append(Value::UBIGINT(state.stats.persistent_cache_evicted_bytes));
		}
		state.offset++;
		count++;
	}
	output.CheckCardinality(count);
}

} // namespace

TableFunction GetSlateDBCacheStatsFunction(SlateDBFileSystem &file_system) {
	TableFunction function(Identifier(FUNCTION_NAME), {}, CacheStatsFunction, CacheStatsBind, CacheStatsInit);
	function.function_info = make_shared_ptr<CacheStatsFunctionInfo>(file_system);
	return function;
}

} // namespace duckdb
