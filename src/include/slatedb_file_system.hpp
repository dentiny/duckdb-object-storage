#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "slatedb_fs.h"

#include <memory>

namespace duckdb {

struct SlateDBFsDeleter {
	void operator()(slatedb_fs *ptr) const;
};

//! DuckDB filesystem adapter over the Rust SlateDB crate.
class SlateDBFileSystem : public FileSystem {
public:
	SlateDBFileSystem();
	static unique_ptr<SlateDBFileSystem> CreateInMemory();
	static unique_ptr<SlateDBFileSystem> CreateLocal(const string &root);

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override;
	bool OnDiskFile(FileHandle &handle) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	bool CanHandleFile(const string &fpath) override;
	string PathSeparator(const string &path) override;
	string CanonicalizePath(const string &path, optional_ptr<FileOpener> opener = nullptr) override;
	std::string GetName() const override;

private:
	void EnsureTemporaryFilesStayLocal(optional_ptr<FileOpener> opener);
	void InitializeMemory();
	void InitializeLocal(const string &root);
	void InitializeS3(optional_ptr<FileOpener> opener);
	slatedb_fs *GetOrCreateFileSystem(optional_ptr<FileOpener> opener);

	mutex initialization_lock;
	unique_ptr<slatedb_fs, SlateDBFsDeleter> impl;
};

} // namespace duckdb
