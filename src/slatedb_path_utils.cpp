#include "slatedb_path_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string GetLogicalPath(const string &path) {
	const string prefix = "duckdb_objfs:";
	if (!StringUtil::StartsWith(path, prefix)) {
		throw InvalidInputException("Object filesystem path must start with '%s'", prefix);
	}
	auto logical_path = path.substr(prefix.size());
	while (!logical_path.empty() && logical_path[0] == '/') {
		logical_path.erase(0, 1);
	}
	if (logical_path.empty()) {
		throw InvalidInputException("Object filesystem path must name a file");
	}
	return logical_path;
}

} // namespace duckdb
