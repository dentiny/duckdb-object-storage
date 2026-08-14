#pragma once

#include "duckdb/common/file_system.hpp"
#include "slatedb_fs.h"

#include <memory>

namespace duckdb {

struct SlateDBFsDeleter {
	void operator()(slatedb_fs *ptr) const {
		if (ptr) {
			slatedb_fs_destroy(ptr);
		}
	}
};

//! DuckDB filesystem adapter over the Rust SlateDB crate. File I/O is dummy.
class SlateDBFileSystem : public FileSystem {
public:
	SlateDBFileSystem();

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	bool CanHandleFile(const string &fpath) override;
	string PathSeparator(const string &path) override;
	std::string GetName() const override;

private:
	[[noreturn]] void ThrowDummy() const;

	unique_ptr<slatedb_fs, SlateDBFsDeleter> impl;
};

} // namespace duckdb
