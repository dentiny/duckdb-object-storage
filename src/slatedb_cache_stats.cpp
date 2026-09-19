#include "slatedb_cache_stats.hpp"
#include "slatedb_file_system.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

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
                                        vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("cache");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("enabled");
	return_types.emplace_back(LogicalType::BOOLEAN);
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
	names.emplace_back("capacity_bytes");
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
	return std::move(result);
}

Value HitRate(uint64_t hits, uint64_t misses) {
	auto accesses = hits + misses;
	if (accesses == 0) {
		return Value();
	}
	return Value::DOUBLE(static_cast<double>(hits) / static_cast<double>(accesses));
}

void SetCommonValues(DataChunk &output, idx_t row, const char *cache, bool enabled, uint64_t hits, uint64_t misses,
                     uint64_t capacity) {
	output.SetValue(0, row, Value(cache));
	output.SetValue(1, row, Value::BOOLEAN(enabled));
	output.SetValue(2, row, Value::UBIGINT(hits));
	output.SetValue(3, row, Value::UBIGINT(misses));
	output.SetValue(4, row, HitRate(hits, misses));
	output.SetValue(7, row, Value::UBIGINT(capacity));
}

void CacheStatsFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<CacheStatsGlobalState>();
	if (!state.initialized || state.offset >= 3) {
		return;
	}

	idx_t count = 0;
	while (state.offset < 3 && count < STANDARD_VECTOR_SIZE) {
		if (state.offset == 0) {
			SetCommonValues(output, count, "memory_data", state.stats.block_cache_enabled != 0,
			                state.stats.block_cache_hits, state.stats.block_cache_misses,
			                state.stats.block_cache_capacity_bytes);
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
			output.SetValue(8, count, Value());
			output.SetValue(9, count, Value());
		} else if (state.offset == 1) {
			SetCommonValues(output, count, "memory_metadata", state.stats.metadata_cache_enabled != 0,
			                state.stats.metadata_cache_hits, state.stats.metadata_cache_misses,
			                state.stats.metadata_cache_capacity_bytes);
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
			output.SetValue(8, count, Value());
			output.SetValue(9, count, Value());
		} else {
			SetCommonValues(output, count, "persistent", state.stats.persistent_cache_enabled != 0,
			                state.stats.persistent_cache_hits, state.stats.persistent_cache_misses,
			                state.stats.persistent_cache_capacity_bytes);
			output.SetValue(5, count, Value::UBIGINT(state.stats.persistent_cache_entries));
			output.SetValue(6, count, Value::UBIGINT(state.stats.persistent_cache_size_bytes));
			output.SetValue(8, count, Value::UBIGINT(state.stats.persistent_cache_evictions));
			output.SetValue(9, count, Value::UBIGINT(state.stats.persistent_cache_evicted_bytes));
		}
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetSlateDBCacheStatsFunction(SlateDBFileSystem &file_system) {
	TableFunction function("duckdb_objfs_cache_stats", {}, CacheStatsFunction, CacheStatsBind, CacheStatsInit);
	function.function_info = make_shared_ptr<CacheStatsFunctionInfo>(file_system);
	return function;
}

} // namespace duckdb
