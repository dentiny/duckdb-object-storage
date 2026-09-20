#include "slatedb_config_utils.hpp"
#include "slatedb_file_system.hpp"
#include "slatedb_ffi_utils.hpp"
#include "slatedb_file_handle.hpp"
#include "slatedb_path_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

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

slatedb_cache_config ConvertCacheConfig(const CacheInitializationConfig &config) {
	return {config.block_cache_size_bytes,
	        config.metadata_cache_size_bytes,
	        config.cache_shards,
	        config.persistent_cache_path.c_str(),
	        config.persistent_cache_size_bytes,
	        config.persistent_cache_part_size_bytes,
	        config.persistent_cache_on_flush,
	        config.persistent_cache_on_compaction};
}

slatedb_db_config ConvertDatabaseConfig(bool read_only) {
	return {read_only};
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
	result->InitializeMemory(CacheInitializationConfig(), false);
	return result;
}

unique_ptr<SlateDBFileSystem> SlateDBFileSystem::CreateLocal(const string &root) {
	auto result = make_uniq<SlateDBFileSystem>();
	result->InitializeLocal(root, CacheInitializationConfig(), false);
	return result;
}

void SlateDBFileSystem::InitializeMemory(const CacheInitializationConfig &cache, bool read_only) {
	slatedb_fs *ptr = nullptr;
	auto ffi_cache = ConvertCacheConfig(cache);
	auto ffi_database = ConvertDatabaseConfig(read_only);
	auto code = slatedb_fs_create_memory(&ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize in-memory SlateDB filesystem");
	(read_only ? read_only_impl : read_write_impl).reset(ptr);
}

void SlateDBFileSystem::InitializeLocal(const string &root, const CacheInitializationConfig &cache, bool read_only) {
	slatedb_fs *ptr = nullptr;
	auto ffi_cache = ConvertCacheConfig(cache);
	auto ffi_database = ConvertDatabaseConfig(read_only);
	auto code = slatedb_fs_create_local(root.c_str(), &ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize local SlateDB filesystem");
	(read_only ? read_only_impl : read_write_impl).reset(ptr);
}

void SlateDBFileSystem::InitializeS3(const S3InitializationConfig &config, const CacheInitializationConfig &cache,
                                     bool read_only) {
	slatedb_s3_config ffi_config {
	    config.bucket.c_str(),        config.root.c_str(),   config.endpoint.c_str(),
	    config.region.c_str(),        config.key_id.c_str(), config.secret.c_str(),
	    config.session_token.c_str(), config.use_ssl,        config.virtual_host_style,
	};
	auto ffi_cache = ConvertCacheConfig(cache);
	auto ffi_database = ConvertDatabaseConfig(read_only);
	slatedb_fs *ptr = nullptr;
	auto code = slatedb_fs_create_s3(&ffi_config, &ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize S3-backed SlateDB filesystem");
	(read_only ? read_only_impl : read_write_impl).reset(ptr);
}

SlateDBFileSystem::InitializationConfig SlateDBFileSystem::ReadInitializationConfig(optional_ptr<FileOpener> opener) {
	InitializationConfig result;
	result.backend = GetOptionalSetting(opener, "duckdb_objfs_backend");
	if (result.backend.empty()) {
		result.backend = "local";
	}
	result.backend = StringUtil::Lower(result.backend);
	result.cache = ReadCacheInitializationConfig(opener);
	if (result.backend == "local") {
		result.local_root = GetOptionalSetting(opener, "duckdb_objfs_root");
		if (result.local_root.empty()) {
			result.local_root = ".duckdb_objfs";
		}
	} else if (result.backend == "s3") {
		result.s3 = ReadS3InitializationConfig(opener);
	} else if (result.backend != "memory") {
		throw InvalidConfigurationException("Unsupported duckdb_objfs backend '%s'; expected s3, local, or memory",
		                                    result.backend);
	}
	return result;
}

void SlateDBFileSystem::EnsureTemporaryFilesStayLocal(optional_ptr<FileOpener> opener) {
	auto database = FileOpener::TryGetDatabase(opener);
	if (!database) {
		return;
	}

	auto &buffer_manager = database->GetBufferManager();
	if (!CanHandleFile(buffer_manager.GetTemporaryDirectory())) {
		return;
	}

	auto &local_fs = FileSystem::GetLocal(*database);
	auto local_directory =
	    local_fs.JoinPath(GetDefaultTemporaryDirectory(),
	                      StringUtil::Format("duckdb_objfs-%s.tmp", UUID::ToString(UUID::GenerateRandomUUID())));
	buffer_manager.SetTemporaryDirectory(local_directory);
}

slatedb_fs *SlateDBFileSystem::GetOrCreateFileSystem(optional_ptr<FileOpener> opener, bool read_only) {
	lock_guard<mutex> guard(initialization_lock);
	EnsureTemporaryFilesStayLocal(opener);
	if (read_only && read_write_impl) {
		return read_write_impl.get();
	}
	auto &selected_impl = read_only ? read_only_impl : read_write_impl;
	if (selected_impl) {
		return selected_impl.get();
	}

	if (!initialization_config) {
		initialization_config = make_uniq<InitializationConfig>(ReadInitializationConfig(opener));
	}
	auto &config = *initialization_config;
	if (config.backend == "memory") {
		InitializeMemory(config.cache, read_only);
	} else if (config.backend == "local") {
		InitializeLocal(config.local_root, config.cache, read_only);
	} else {
		D_ASSERT(config.backend == "s3");
		InitializeS3(config.s3, config.cache, read_only);
	}
	return selected_impl.get();
}

bool SlateDBFileSystem::TryGetCacheStats(slatedb_cache_stats &stats) {
	lock_guard<mutex> guard(initialization_lock);
	auto impl = read_write_impl ? read_write_impl.get() : read_only_impl.get();
	if (!impl) {
		return false;
	}
	ThrowSlateDBError(slatedb_fs_get_cache_stats(impl, &stats), "get SlateDB cache statistics");
	return true;
}

bool SlateDBFileSystem::TryGetIoStats(slatedb_io_stats &stats) {
	lock_guard<mutex> guard(initialization_lock);
	auto impl = read_write_impl ? read_write_impl.get() : read_only_impl.get();
	if (!impl) {
		return false;
	}
	ThrowSlateDBError(slatedb_fs_get_io_stats(impl, &stats), "get SlateDB I/O statistics");
	return true;
}

unique_ptr<FileHandle> SlateDBFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	flags.Verify();
	auto options = ConvertOpenFlags(flags);
	slatedb_file_handle *handle = nullptr;
	auto logical_path = GetLogicalPath(path);
	auto read_only = !flags.OpenForWriting();
	auto code = slatedb_fs_open_file(GetOrCreateFileSystem(opener, read_only), logical_path.c_str(), &options, &handle);
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
	auto logical_source = GetLogicalPath(source);
	auto logical_target = GetLogicalPath(target);
	ThrowSlateDBError(
	    slatedb_fs_move_file(GetOrCreateFileSystem(opener, false), logical_source.c_str(), logical_target.c_str()),
	    "move file");
}

void SlateDBFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto logical_path = GetLogicalPath(filename);
	ThrowSlateDBError(slatedb_fs_remove_file(GetOrCreateFileSystem(opener, false), logical_path.c_str()),
	                  "remove file");
}

vector<OpenFileInfo> SlateDBFileSystem::Glob(const string &, FileOpener *) {
	throw NotImplementedException("SlateDBFileSystem::Glob is not implemented");
}

bool SlateDBFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	int32_t exists = 0;
	auto logical_path = GetLogicalPath(filename);
	ThrowSlateDBError(slatedb_fs_file_exists(GetOrCreateFileSystem(opener, false), logical_path.c_str(), &exists),
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
	return StringUtil::Format("duckdb_objfs://%s", GetLogicalPath(path));
}

std::string SlateDBFileSystem::GetName() const {
	return slatedb_fs_name();
}

} // namespace duckdb
