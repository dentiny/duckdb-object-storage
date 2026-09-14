#include "scoped_directory.hpp"

#include "duckdb/common/file_system.hpp"

namespace duckdb {

ScopedDirectory::ScopedDirectory(string path) : path(std::move(path)) {
	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(this->path)) {
		fs->CreateDirectory(this->path);
	}
}

ScopedDirectory::~ScopedDirectory() {
	if (path.empty()) {
		return;
	}
	auto fs = FileSystem::CreateLocal();
	if (fs->DirectoryExists(path)) {
		fs->RemoveDirectory(path);
	}
}

} // namespace duckdb
