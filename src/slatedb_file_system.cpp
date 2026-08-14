#include "slatedb_file_system.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SlateDBFileSystem::SlateDBFileSystem() {
	auto *ptr = slatedb_fs_create();
	if (!ptr) {
		throw IOException("Failed to initialize SlateDB filesystem");
	}
	impl.reset(ptr);
}

unique_ptr<FileHandle> SlateDBFileSystem::OpenFile(const string &, FileOpenFlags, optional_ptr<FileOpener>) {
	ThrowDummy();
}

vector<OpenFileInfo> SlateDBFileSystem::Glob(const string &, FileOpener *) {
	ThrowDummy();
}

bool SlateDBFileSystem::FileExists(const string &, optional_ptr<FileOpener>) {
	ThrowDummy();
}

bool SlateDBFileSystem::DirectoryExists(const string &, optional_ptr<FileOpener>) {
	ThrowDummy();
}

bool SlateDBFileSystem::ListFiles(const string &, const std::function<void(const string &, bool)> &, FileOpener *) {
	ThrowDummy();
}

bool SlateDBFileSystem::CanHandleFile(const string &fpath) {
	return slatedb_fs_can_handle(impl.get(), fpath.c_str()) != 0;
}

string SlateDBFileSystem::PathSeparator(const string &) {
	return "/";
}

std::string SlateDBFileSystem::GetName() const {
	return slatedb_fs_name();
}

[[noreturn]] void SlateDBFileSystem::ThrowDummy() const {
	throw NotImplementedException("%s", slatedb_fs_dummy_error());
}

} // namespace duckdb
