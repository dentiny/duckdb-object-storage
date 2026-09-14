#include "slatedb_file_handle.hpp"
#include "slatedb_ffi_utils.hpp"

namespace duckdb {

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
