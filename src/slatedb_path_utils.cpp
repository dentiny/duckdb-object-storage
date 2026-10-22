#include "slatedb_path_utils.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>

namespace duckdb {

string GetLogicalPath(const string &path) {
	std::string_view logical_path(path);
	if (logical_path.substr(0, SLATEDB_URI_PREFIX.size()) != SLATEDB_URI_PREFIX) {
		throw InvalidInputException("Object filesystem path must start with '%s'", string(SLATEDB_URI_PREFIX));
	}

	logical_path.remove_prefix(SLATEDB_URI_PREFIX.size());
	auto file_start = logical_path.find_first_not_of('/');
	if (file_start == std::string_view::npos) {
		throw InvalidInputException("Object filesystem path must name a file");
	}
	return string(logical_path.substr(file_start));
}

string GetDefaultTemporaryDirectory() {
#ifdef _WIN32
	const char *temporary_directory = std::getenv("TEMP");
	if (!temporary_directory || !temporary_directory[0]) {
		temporary_directory = std::getenv("TMP");
	}
	return temporary_directory && temporary_directory[0] ? temporary_directory : ".";
#else
	const char *temporary_directory = std::getenv("TMPDIR");
	return temporary_directory && temporary_directory[0] ? temporary_directory : "/tmp";
#endif
}

} // namespace duckdb
