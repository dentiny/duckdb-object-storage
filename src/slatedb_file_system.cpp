#include "slatedb_file_system.hpp"
#include "slatedb_file_handle.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

namespace {

using slatedb_ffi::ThrowIfError;

slatedb_fs_open_options ConvertOpenFlags(FileOpenFlags flags) {
	slatedb_fs_open_options options;
	options.read = flags.OpenForReading();
	options.write = flags.OpenForWriting();
	options.create = flags.CreateFileIfNotExists() || flags.OverwriteExistingFile();
	options.append = flags.OpenForAppending();
	options.truncate_existing = flags.OverwriteExistingFile();
	return options;
}

SlateDBFileHandle &GetSlateDBFileHandle(FileHandle &handle) {
	return handle.Cast<SlateDBFileHandle>();
}

size_t CheckedSize(int64_t size, const string &operation) {
	if (size < 0) {
		throw InvalidInputException("%s: byte count must not be negative", operation);
	}
	return NumericCast<size_t>(size);
}

} // namespace

void SlateDBFsDeleter::operator()(slatedb_fs *ptr) const {
	if (ptr) {
		slatedb_fs_destroy(ptr);
	}
}

SlateDBFileSystem::SlateDBFileSystem() {
	auto *ptr = slatedb_fs_create();
	if (!ptr) {
		throw IOException("Failed to initialize SlateDB filesystem");
	}
	impl.reset(ptr);
}

unique_ptr<FileHandle> SlateDBFileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener>) {
	flags.Verify();
	auto options = ConvertOpenFlags(flags);
	slatedb_file_handle *handle = nullptr;
	auto code = slatedb_fs_open_file(impl.get(), path.c_str(), &options, &handle);
	if ((code == SLATEDB_FS_ERROR_FILE_NOT_FOUND && flags.ReturnNullIfNotExists()) ||
	    (code == SLATEDB_FS_ERROR_FILE_ALREADY_EXISTS && flags.ReturnNullIfExists())) {
		return nullptr;
	}
	ThrowIfError(code, "open file");
	return make_uniq<SlateDBFileHandle>(*this, path, flags, handle);
}

void SlateDBFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_read = 0;
	ThrowIfError(slatedb_file_pread(slatedb_handle.GetHandle(), reinterpret_cast<uint8_t *>(buffer),
	                                CheckedSize(nr_bytes, "read file"), location, &bytes_read),
	             "read file");
	if (bytes_read != NumericCast<size_t>(nr_bytes)) {
		throw IOException("read file: expected %llu bytes but read %llu", NumericCast<uint64_t>(nr_bytes),
		                  NumericCast<uint64_t>(bytes_read));
	}
}

void SlateDBFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowIfError(slatedb_file_pwrite(slatedb_handle.GetHandle(), reinterpret_cast<const uint8_t *>(buffer),
	                                 CheckedSize(nr_bytes, "write file"), location),
	             "write file");
}

int64_t SlateDBFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_read = 0;
	ThrowIfError(slatedb_file_read(slatedb_handle.GetHandle(), reinterpret_cast<uint8_t *>(buffer),
	                               CheckedSize(nr_bytes, "read file"), &bytes_read),
	             "read file");
	return NumericCast<int64_t>(bytes_read);
}

int64_t SlateDBFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_written = 0;
	ThrowIfError(slatedb_file_write(slatedb_handle.GetHandle(), reinterpret_cast<const uint8_t *>(buffer),
	                                CheckedSize(nr_bytes, "write file"), &bytes_written),
	             "write file");
	return NumericCast<int64_t>(bytes_written);
}

int64_t SlateDBFileSystem::GetFileSize(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	uint64_t size = 0;
	ThrowIfError(slatedb_file_get_size(slatedb_handle.GetHandle(), &size), "get file size");
	return NumericCast<int64_t>(size);
}

void SlateDBFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowIfError(slatedb_file_truncate(slatedb_handle.GetHandle(), CheckedSize(new_size, "truncate file")),
	             "truncate file");
}

void SlateDBFileSystem::FileSync(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowIfError(slatedb_file_sync(slatedb_handle.GetHandle()), "sync file");
}

void SlateDBFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowIfError(slatedb_file_seek(slatedb_handle.GetHandle(), location), "seek file");
}

idx_t SlateDBFileSystem::SeekPosition(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	uint64_t position = 0;
	ThrowIfError(slatedb_file_get_position(slatedb_handle.GetHandle(), &position), "get file position");
	return NumericCast<idx_t>(position);
}

bool SlateDBFileSystem::CanSeek() {
	return true;
}

bool SlateDBFileSystem::OnDiskFile(FileHandle &) {
	return false;
}

void SlateDBFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener>) {
	ThrowIfError(slatedb_fs_move_file(impl.get(), source.c_str(), target.c_str()), "move file");
}

void SlateDBFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener>) {
	ThrowIfError(slatedb_fs_remove_file(impl.get(), filename.c_str()), "remove file");
}

vector<OpenFileInfo> SlateDBFileSystem::Glob(const string &, FileOpener *) {
	ThrowDummy();
}

bool SlateDBFileSystem::FileExists(const string &filename, optional_ptr<FileOpener>) {
	int32_t exists = 0;
	ThrowIfError(slatedb_fs_file_exists(impl.get(), filename.c_str(), &exists), "check if file exists");
	return exists != 0;
}

bool SlateDBFileSystem::DirectoryExists(const string &, optional_ptr<FileOpener>) {
	ThrowDummy();
}

bool SlateDBFileSystem::ListFiles(const string &, const std::function<void(const string &, bool)> &, FileOpener *) {
	ThrowDummy();
}

bool SlateDBFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("slatedb:", 0) == 0;
}

string SlateDBFileSystem::PathSeparator(const string &) {
	return "/";
}

string SlateDBFileSystem::CanonicalizePath(const string &path, optional_ptr<FileOpener>) {
	return path;
}

std::string SlateDBFileSystem::GetName() const {
	return slatedb_fs_name();
}

[[noreturn]] void SlateDBFileSystem::ThrowDummy() const {
	throw NotImplementedException("%s", slatedb_fs_dummy_error());
}

} // namespace duckdb
