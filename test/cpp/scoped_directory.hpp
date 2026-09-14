#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ScopedDirectory {
public:
	explicit ScopedDirectory(string path);
	~ScopedDirectory();

	ScopedDirectory(const ScopedDirectory &) = delete;
	ScopedDirectory &operator=(const ScopedDirectory &) = delete;

	const string &GetPath() const {
		return path;
	}

private:
	string path;
};

} // namespace duckdb
