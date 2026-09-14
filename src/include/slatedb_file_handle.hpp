#pragma once

#include "duckdb/common/file_system.hpp"
#include "slatedb_fs.h"

#include <memory>

namespace duckdb {

namespace slatedb_ffi {

void ThrowIfError(int32_t code, const string &operation);

} // namespace slatedb_ffi

struct SlateDBFileHandleDeleter {
	void operator()(slatedb_file_handle *ptr) const;
};

class SlateDBFileHandle : public FileHandle {
public:
	SlateDBFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, slatedb_file_handle *handle);
	~SlateDBFileHandle() override;

	void Close() override;

private:
	friend class SlateDBFileSystem;

	slatedb_file_handle *GetHandle() const;

	unique_ptr<slatedb_file_handle, SlateDBFileHandleDeleter> impl;
};

} // namespace duckdb
