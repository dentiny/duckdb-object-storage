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

slatedb_db_config ConvertDatabaseConfig(const DatabaseInitializationConfig &config) {
	return {config.read_only};
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
	result->InitializeMemory(DatabaseInitializationConfig());
	return result;
}

unique_ptr<SlateDBFileSystem> SlateDBFileSystem::CreateLocal(const string &root) {
	auto result = make_uniq<SlateDBFileSystem>();
	result->InitializeLocal(root, DatabaseInitializationConfig());
	return result;
}

void SlateDBFileSystem::InitializeMemory(const DatabaseInitializationConfig &config) {
	slatedb_fs *ptr = nullptr;
	auto ffi_cache = ConvertCacheConfig(config.cache);
	auto ffi_database = ConvertDatabaseConfig(config);
	auto code = slatedb_fs_create_memory(&ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize in-memory SlateDB filesystem");
	(config.read_only ? read_only_impl : read_write_impl).reset(ptr);
}

void SlateDBFileSystem::InitializeLocal(const string &root, const DatabaseInitializationConfig &config) {
	slatedb_fs *ptr = nullptr;
	auto ffi_cache = ConvertCacheConfig(config.cache);
	auto ffi_database = ConvertDatabaseConfig(config);
	auto code = slatedb_fs_create_local(root.c_str(), &ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize local SlateDB filesystem");
	(config.read_only ? read_only_impl : read_write_impl).reset(ptr);
}

void SlateDBFileSystem::InitializeS3(const S3InitializationConfig &s3_config,
                                     const DatabaseInitializationConfig &config) {
	slatedb_s3_config ffi_config {
	    s3_config.bucket.c_str(),        s3_config.root.c_str(),   s3_config.endpoint.c_str(),
	    s3_config.region.c_str(),        s3_config.key_id.c_str(), s3_config.secret.c_str(),
	    s3_config.session_token.c_str(), s3_config.use_ssl,        s3_config.virtual_host_style,
	};
	auto ffi_cache = ConvertCacheConfig(config.cache);
	auto ffi_database = ConvertDatabaseConfig(config);
	slatedb_fs *ptr = nullptr;
	auto code = slatedb_fs_create_s3(&ffi_config, &ffi_cache, &ffi_database, &ptr);
	ThrowSlateDBError(code, "initialize S3-backed SlateDB filesystem");
	(config.read_only ? read_only_impl : read_write_impl).reset(ptr);
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

void SlateDBFileSystem::FreezeSettingsSnapshot(optional_ptr<FileOpener> opener, const InitializationConfig &config) {
	auto database = FileOpener::TryGetDatabase(opener);
	if (!database) {
		return;
	}
	auto frozen = make_shared_ptr<FrozenSlateDBSettings>();
	frozen->backend = config.backend;
	if (config.backend == "local") {
		frozen->root = config.local_root;
	} else if (config.backend == "s3") {
		frozen->root = config.s3.root;
		frozen->bucket = config.s3.bucket;
	} else {
		// The memory backend never reads the root; freeze the raw value so
		// post-initialization changes are still rejected.
		frozen->root = GetOptionalSetting(opener, "duckdb_objfs_root");
	}
	frozen->cache = config.cache;
	database->GetObjectCache().Put(FrozenSlateDBSettings::CACHE_KEY, std::move(frozen));
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
		FreezeSettingsSnapshot(opener, *initialization_config);
	}
	auto &config = *initialization_config;
	DatabaseInitializationConfig database_config;
	database_config.cache = config.cache;
	database_config.read_only = read_only;
	if (config.backend == "memory") {
		InitializeMemory(database_config);
	} else if (config.backend == "local") {
		InitializeLocal(config.local_root, database_config);
	} else {
		D_ASSERT(config.backend == "s3");
		InitializeS3(config.s3, database_config);
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
