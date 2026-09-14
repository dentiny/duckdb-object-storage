#include "slatedb_file_handle.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace slatedb_ffi {

void ThrowIfError(int32_t code, const string &operation) {
	if (code == SLATEDB_FS_ERROR_NONE) {
		return;
	}
	auto message = slatedb_fs_last_error_message();
	auto detail = message ? string(message) : string("unknown SlateDB filesystem error");
	if (code == SLATEDB_FS_ERROR_INVALID_ARGUMENT) {
		throw InvalidInputException("%s: %s", operation, detail);
	}
	throw IOException("%s: %s", operation, detail);
}

} // namespace slatedb_ffi

void SlateDBFileHandleDeleter::operator()(slatedb_file_handle *ptr) const {
	if (ptr) {
		slatedb_file_destroy(ptr);
	}
}

SlateDBFileHandle::SlateDBFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                                     slatedb_file_handle *handle)
    : FileHandle(file_system, std::move(path), flags), impl(handle) {
}

SlateDBFileHandle::~SlateDBFileHandle() = default;

void SlateDBFileHandle::Close() {
	if (!impl) {
		return;
	}
	slatedb_ffi::ThrowIfError(slatedb_file_close(impl.get()), "close file");
	impl.reset();
}

slatedb_file_handle *SlateDBFileHandle::GetHandle() const {
	return impl.get();
}

} // namespace duckdb
