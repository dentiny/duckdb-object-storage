#include "slatedb_config_utils.hpp"
#include "slatedb_file_system.hpp"
#include "slatedb_ffi_utils.hpp"
#include "slatedb_file_handle.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

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

string LogicalPath(const string &path) {
	const string prefix = "duckdb_objfs:";
	if (!StringUtil::StartsWith(path, prefix)) {
		throw InvalidInputException("Object filesystem path must start with '%s'", prefix);
	}
	auto logical_path = path.substr(prefix.size());
	while (!logical_path.empty() && logical_path[0] == '/') {
		logical_path.erase(0, 1);
	}
	if (logical_path.empty()) {
		throw InvalidInputException("Object filesystem path must name a file");
	}
	return logical_path;
}

} // namespace

void SlateDBFsDeleter::operator()(slatedb_fs *ptr) const {
	if (ptr) {
		slatedb_fs_destroy(ptr);
	}
}

SlateDBFileSystem::SlateDBFileSystem() {
}

unique_ptr<SlateDBFileSystem> SlateDBFileSystem::CreateInMemory() {
	auto result = make_uniq<SlateDBFileSystem>();
	slatedb_fs *ptr = nullptr;
	ThrowSlateDBError(slatedb_fs_create_memory(&ptr), "initialize in-memory SlateDB filesystem");
	result->impl.reset(ptr);
	return result;
}

unique_ptr<SlateDBFileSystem> SlateDBFileSystem::CreateLocal(const string &root) {
	auto result = make_uniq<SlateDBFileSystem>();
	slatedb_fs *ptr = nullptr;
	ThrowSlateDBError(slatedb_fs_create_local(root.c_str(), &ptr), "initialize local SlateDB filesystem");
	result->impl.reset(ptr);
	return result;
}

slatedb_fs *SlateDBFileSystem::GetOrCreateFileSystem(optional_ptr<FileOpener> opener) {
	lock_guard<mutex> guard(initialization_lock);
	if (impl) {
		return impl.get();
	}

	auto backend = slatedb_config::GetOptionalSetting(opener, "duckdb_objfs_backend");
	if (backend.empty()) {
		backend = "local";
	}
	if (backend == "memory") {
		slatedb_fs *ptr = nullptr;
		ThrowSlateDBError(slatedb_fs_create_memory(&ptr), "initialize in-memory SlateDB filesystem");
		impl.reset(ptr);
		return impl.get();
	}
	if (backend == "local") {
		auto local_path = slatedb_config::GetOptionalSetting(opener, "duckdb_objfs_root");
		if (local_path.empty()) {
			local_path = ".duckdb_objfs";
		}
		slatedb_fs *ptr = nullptr;
		ThrowSlateDBError(slatedb_fs_create_local(local_path.c_str(), &ptr), "initialize local SlateDB filesystem");
		impl.reset(ptr);
		return impl.get();
	}
	if (backend != "s3") {
		throw InvalidConfigurationException("Unsupported duckdb_objfs backend '%s'; expected s3, local, or memory",
		                                    backend);
	}

	auto config = slatedb_config::ReadS3InitializationConfig(opener);
	slatedb_s3_config ffi_config {
	    config.bucket.c_str(),        config.root.c_str(),   config.endpoint.c_str(),
	    config.region.c_str(),        config.key_id.c_str(), config.secret.c_str(),
	    config.session_token.c_str(), config.use_ssl,        config.virtual_host_style,
	};
	slatedb_fs *ptr = nullptr;
	ThrowSlateDBError(slatedb_fs_create_s3(&ffi_config, &ptr), "initialize S3-backed SlateDB filesystem");
	impl.reset(ptr);
	return impl.get();
}

unique_ptr<FileHandle> SlateDBFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	flags.Verify();
	auto options = ConvertOpenFlags(flags);
	slatedb_file_handle *handle = nullptr;
	auto logical_path = LogicalPath(path);
	auto code = slatedb_fs_open_file(GetOrCreateFileSystem(opener), logical_path.c_str(), &options, &handle);
	if ((code == SLATEDB_FS_ERROR_FILE_NOT_FOUND && flags.ReturnNullIfNotExists()) ||
	    (code == SLATEDB_FS_ERROR_FILE_ALREADY_EXISTS && flags.ReturnNullIfExists())) {
		return nullptr;
	}
	ThrowSlateDBError(code, "open file");
	return make_uniq<SlateDBFileHandle>(*this, path, flags, handle);
}

void SlateDBFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_read = 0;
	ThrowSlateDBError(slatedb_file_pread(slatedb_handle.GetHandle(), reinterpret_cast<uint8_t *>(buffer),
	                                     CheckedSize(nr_bytes, "read file"), location, &bytes_read),
	                  "read file");
	if (bytes_read != NumericCast<size_t>(nr_bytes)) {
		throw IOException("read file: expected %llu bytes but read %llu", NumericCast<uint64_t>(nr_bytes),
		                  NumericCast<uint64_t>(bytes_read));
	}
}

void SlateDBFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowSlateDBError(slatedb_file_pwrite(slatedb_handle.GetHandle(), reinterpret_cast<const uint8_t *>(buffer),
	                                      CheckedSize(nr_bytes, "write file"), location),
	                  "write file");
}

int64_t SlateDBFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_read = 0;
	ThrowSlateDBError(slatedb_file_read(slatedb_handle.GetHandle(), reinterpret_cast<uint8_t *>(buffer),
	                                    CheckedSize(nr_bytes, "read file"), &bytes_read),
	                  "read file");
	return NumericCast<int64_t>(bytes_read);
}

int64_t SlateDBFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	size_t bytes_written = 0;
	ThrowSlateDBError(slatedb_file_write(slatedb_handle.GetHandle(), reinterpret_cast<const uint8_t *>(buffer),
	                                     CheckedSize(nr_bytes, "write file"), &bytes_written),
	                  "write file");
	return NumericCast<int64_t>(bytes_written);
}

int64_t SlateDBFileSystem::GetFileSize(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	uint64_t size = 0;
	ThrowSlateDBError(slatedb_file_get_size(slatedb_handle.GetHandle(), &size), "get file size");
	return NumericCast<int64_t>(size);
}

void SlateDBFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowSlateDBError(slatedb_file_truncate(slatedb_handle.GetHandle(), CheckedSize(new_size, "truncate file")),
	                  "truncate file");
}

void SlateDBFileSystem::FileSync(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowSlateDBError(slatedb_file_sync(slatedb_handle.GetHandle()), "sync file");
}

void SlateDBFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	ThrowSlateDBError(slatedb_file_seek(slatedb_handle.GetHandle(), location), "seek file");
}

idx_t SlateDBFileSystem::SeekPosition(FileHandle &handle) {
	auto &slatedb_handle = GetSlateDBFileHandle(handle);
	uint64_t position = 0;
	ThrowSlateDBError(slatedb_file_get_position(slatedb_handle.GetHandle(), &position), "get file position");
	return NumericCast<idx_t>(position);
}

bool SlateDBFileSystem::CanSeek() {
	return true;
}

bool SlateDBFileSystem::OnDiskFile(FileHandle &) {
	return false;
}

void SlateDBFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	auto logical_source = LogicalPath(source);
	auto logical_target = LogicalPath(target);
	ThrowSlateDBError(
	    slatedb_fs_move_file(GetOrCreateFileSystem(opener), logical_source.c_str(), logical_target.c_str()),
	    "move file");
}

void SlateDBFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto logical_path = LogicalPath(filename);
	ThrowSlateDBError(slatedb_fs_remove_file(GetOrCreateFileSystem(opener), logical_path.c_str()), "remove file");
}

vector<OpenFileInfo> SlateDBFileSystem::Glob(const string &, FileOpener *) {
	throw NotImplementedException("SlateDBFileSystem::Glob is not implemented");
}

bool SlateDBFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	int32_t exists = 0;
	auto logical_path = LogicalPath(filename);
	ThrowSlateDBError(slatedb_fs_file_exists(GetOrCreateFileSystem(opener), logical_path.c_str(), &exists),
	                  "check if file exists");
	return exists != 0;
}

bool SlateDBFileSystem::DirectoryExists(const string &, optional_ptr<FileOpener>) {
	throw NotImplementedException("SlateDBFileSystem::DirectoryExists is not implemented");
}

bool SlateDBFileSystem::ListFiles(const string &, const std::function<void(const string &, bool)> &, FileOpener *) {
	throw NotImplementedException("SlateDBFileSystem::ListFiles is not implemented");
}

bool SlateDBFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("duckdb_objfs:", 0) == 0;
}

string SlateDBFileSystem::PathSeparator(const string &) {
	return "/";
}

string SlateDBFileSystem::CanonicalizePath(const string &path, optional_ptr<FileOpener>) {
	return "duckdb_objfs://" + LogicalPath(path);
}

std::string SlateDBFileSystem::GetName() const {
	return slatedb_fs_name();
}

} // namespace duckdb
