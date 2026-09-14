#pragma once

#include "duckdb/common/file_system.hpp"
#include "slatedb_fs.h"

#include <memory>

namespace duckdb {

struct SlateDBFileHandleDeleter {
	void operator()(slatedb_file_handle *ptr) const;
};

class SlateDBFileHandle : public FileHandle {
public:
	// `handle`'s ownership is transferred to the current handle.
	SlateDBFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, slatedb_file_handle *handle);
	~SlateDBFileHandle() override;

	void Close() override;

private:
	friend class SlateDBFileSystem;

	slatedb_file_handle *GetHandle() const;

	unique_ptr<slatedb_file_handle, SlateDBFileHandleDeleter> impl;
};

} // namespace duckdb
