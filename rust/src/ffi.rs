use std::any::Any;
use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

use crate::cache::CacheConfig;
use crate::error::{Error, Result};
use crate::file_handle::FileHandle;
use crate::flags::FileOpenFlags;
use crate::fs::{S3StorageConfig, SlateDbAccessMode, SlateDbFileSystem};

const DATABASE_PATH: &str = "duckdb-object-storage";

#[repr(C)]
pub struct FfiOpenOptions {
    /// Open the file for reads.
    read: i32,
    /// Open the file for writes.
    write: i32,
    /// Create the file when it does not exist.
    create: i32,
    /// Position sequential writes at the end of the file.
    append: i32,
    /// Clear an existing file when it is opened.
    truncate_existing: i32,
}

#[repr(C)]
pub struct FfiS3Config {
    /// S3 bucket that stores SlateDB objects.
    bucket: *const c_char,
    /// Prefix within the bucket reserved for this filesystem.
    root: *const c_char,
    /// Optional S3-compatible service endpoint.
    endpoint: *const c_char,
    /// AWS region used to sign S3 requests.
    region: *const c_char,
    /// Optional access-key identifier.
    key_id: *const c_char,
    /// Optional secret access key.
    secret: *const c_char,
    /// Optional temporary-credential session token.
    session_token: *const c_char,
    /// Whether the endpoint should use HTTPS.
    use_ssl: i32,
    /// Whether S3 requests should use virtual-host-style addressing.
    virtual_host_style: i32,
}

#[repr(C)]
pub struct FfiCacheConfig {
    /// Maximum bytes retained in the in-memory data-block cache; zero disables it.
    block_cache_size_bytes: u64,
    /// Maximum bytes retained in the in-memory SST metadata cache; zero disables it.
    metadata_cache_size_bytes: u64,
    /// Number of cache shards; zero selects an implementation default.
    cache_shards: u64,
    /// Local directory for persistent cached SST parts; empty disables persistence.
    persistent_cache_path: *const c_char,
    /// Maximum total size of the persistent cache.
    persistent_cache_size_bytes: u64,
    /// Size of each persistent cache part; must be a multiple of 1024 bytes.
    persistent_cache_part_size_bytes: u64,
    /// Whether memtable flush output should be inserted into the persistent cache.
    persistent_cache_on_flush: i32,
    /// Whether compaction output should be inserted into the persistent cache.
    persistent_cache_on_compaction: i32,
}

#[repr(C)]
pub struct FfiDatabaseConfig {
    /// Open the SlateDB database without acquiring a writer epoch.
    read_only: i32,
}

#[repr(C)]
#[derive(Default)]
pub struct FfiCacheStats {
    /// Successful in-memory data-block cache lookups.
    block_cache_hits: u64,
    /// Unsuccessful in-memory data-block cache lookups.
    block_cache_misses: u64,
    /// Successful in-memory metadata cache lookups.
    metadata_cache_hits: u64,
    /// Unsuccessful in-memory metadata cache lookups.
    metadata_cache_misses: u64,
    /// Successful persistent cache part lookups.
    persistent_cache_hits: u64,
    /// Unsuccessful persistent cache part lookups.
    persistent_cache_misses: u64,
    /// Current number of persistent cache entries.
    persistent_cache_entries: u64,
    /// Current persistent cache size.
    persistent_cache_size_bytes: u64,
    /// Number of persistent cache entries evicted.
    persistent_cache_evictions: u64,
    /// Number of persistent cache bytes evicted.
    persistent_cache_evicted_bytes: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct FfiIoStats {
    /// Number of OpenDAL read requests.
    read_request_count: u64,
    /// Average OpenDAL read latency in milliseconds.
    read_average_latency_ms: f64,
    /// Population standard deviation of OpenDAL read latency in milliseconds.
    read_stddev_latency_ms: f64,
    /// Number of OpenDAL write requests.
    write_request_count: u64,
    /// Average OpenDAL write latency in milliseconds.
    write_average_latency_ms: f64,
    /// Population standard deviation of OpenDAL write latency in milliseconds.
    write_stddev_latency_ms: f64,
    /// Number of OpenDAL stat requests.
    stat_request_count: u64,
    /// Average OpenDAL stat latency in milliseconds.
    stat_average_latency_ms: f64,
    /// Population standard deviation of OpenDAL stat latency in milliseconds.
    stat_stddev_latency_ms: f64,
    /// Number of OpenDAL delete requests.
    delete_request_count: u64,
    /// Average OpenDAL delete latency in milliseconds.
    delete_average_latency_ms: f64,
    /// Population standard deviation of OpenDAL delete latency in milliseconds.
    delete_stddev_latency_ms: f64,
    /// Number of OpenDAL list requests.
    list_request_count: u64,
    /// Average OpenDAL list latency in milliseconds.
    list_average_latency_ms: f64,
    /// Population standard deviation of OpenDAL list latency in milliseconds.
    list_stddev_latency_ms: f64,
}

/// Synchronous FFI context owning the one async runtime used by this
/// filesystem instance.
pub struct FfiFileSystem {
    runtime: Arc<Runtime>,
    fs: SlateDbFileSystem,
}

pub struct FfiFileHandle {
    runtime: Arc<Runtime>,
    handle: Mutex<Option<Box<dyn FileHandle + Send>>>,
}

thread_local! {
    static LAST_ERROR_MESSAGE: RefCell<CString> =
        RefCell::new(CString::new("").expect("empty string has no NUL"));
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown Rust panic".to_string()
    }
}

fn store_error(error: Error) -> i32 {
    let code = error.code() as i32;
    let message = CString::new(error.to_string().replace('\0', "\\0"))
        .expect("escaped error message cannot contain NUL");
    LAST_ERROR_MESSAGE.with(|slot| *slot.borrow_mut() = message);
    code
}

fn ffi_result(operation: impl FnOnce() -> Result<()>) -> i32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => store_error(error),
        Err(payload) => store_error(Error::invalid_argument(format!(
            "Rust panic while handling FFI call: {}",
            panic_message(payload)
        ))),
    }
}

unsafe fn require_fs<'a>(fs: *const FfiFileSystem) -> Result<&'a FfiFileSystem> {
    unsafe { fs.as_ref() }
        .ok_or_else(|| Error::invalid_argument("filesystem pointer must not be null"))
}

unsafe fn require_path<'a>(path: *const c_char, name: &str) -> Result<&'a str> {
    if path.is_null() {
        return Err(Error::invalid_argument(format!("{name} must not be null")));
    }
    unsafe { CStr::from_ptr(path) }
        .to_str()
        .map_err(|_| Error::invalid_argument(format!("{name} must be valid UTF-8")))
}

unsafe fn optional_string(value: *const c_char, name: &str) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = unsafe { CStr::from_ptr(value) }
        .to_str()
        .map_err(|_| Error::invalid_argument(format!("{name} must be valid UTF-8")))?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

unsafe fn require_s3_config(config: *const FfiS3Config) -> Result<S3StorageConfig> {
    let config = unsafe { config.as_ref() }
        .ok_or_else(|| Error::invalid_argument("S3 config must not be null"))?;
    let bucket = unsafe { require_path(config.bucket, "S3 bucket")? }.to_string();
    Ok(S3StorageConfig {
        bucket,
        root: unsafe { optional_string(config.root, "S3 root")? },
        endpoint: unsafe { optional_string(config.endpoint, "S3 endpoint")? },
        region: unsafe { optional_string(config.region, "S3 region")? },
        key_id: unsafe { optional_string(config.key_id, "S3 key ID")? },
        secret: unsafe { optional_string(config.secret, "S3 secret")? },
        session_token: unsafe { optional_string(config.session_token, "S3 session token")? },
        use_ssl: config.use_ssl != 0,
        virtual_host_style: config.virtual_host_style != 0,
    })
}

unsafe fn parse_cache_config(config: *const FfiCacheConfig) -> Result<CacheConfig> {
    let Some(config) = (unsafe { config.as_ref() }) else {
        return Ok(CacheConfig::default());
    };
    let persistent_cache_path =
        unsafe { optional_string(config.persistent_cache_path, "persistent cache path")? }
            .map(Into::into);
    let persistent_cache_size_bytes = usize::try_from(config.persistent_cache_size_bytes)
        .map_err(|_| Error::invalid_argument("persistent cache size does not fit this platform"))?;
    let persistent_cache_part_size_bytes = usize::try_from(config.persistent_cache_part_size_bytes)
        .map_err(|_| {
            Error::invalid_argument("persistent cache part size does not fit this platform")
        })?;
    if persistent_cache_path.is_some() {
        if persistent_cache_size_bytes == 0 {
            return Err(Error::invalid_argument(
                "persistent cache size must be greater than zero",
            ));
        }
        if persistent_cache_part_size_bytes == 0 || persistent_cache_part_size_bytes % 1024 != 0 {
            return Err(Error::invalid_argument(
                "persistent cache part size must be a non-zero multiple of 1024 bytes",
            ));
        }
    }
    let cache_shards = match config.cache_shards {
        0 => None,
        value => Some(usize::try_from(value).map_err(|_| {
            Error::invalid_argument("cache shard count does not fit this platform")
        })?),
    };
    Ok(CacheConfig {
        block_cache_size_bytes: config.block_cache_size_bytes,
        metadata_cache_size_bytes: config.metadata_cache_size_bytes,
        cache_shards,
        persistent_cache_path,
        persistent_cache_size_bytes,
        persistent_cache_part_size_bytes,
        persistent_cache_on_flush: config.persistent_cache_on_flush != 0,
        persistent_cache_on_compaction: config.persistent_cache_on_compaction != 0,
    })
}

unsafe fn require_options(options: *const FfiOpenOptions) -> Result<FileOpenFlags> {
    let options = unsafe { options.as_ref() }
        .ok_or_else(|| Error::invalid_argument("open options must not be null"))?;
    Ok(FileOpenFlags {
        read: options.read != 0,
        write: options.write != 0,
        create: options.create != 0,
        append: options.append != 0,
        truncate_existing: options.truncate_existing != 0,
    })
}

unsafe fn require_database_config(config: *const FfiDatabaseConfig) -> Result<SlateDbAccessMode> {
    let config = unsafe { config.as_ref() }
        .ok_or_else(|| Error::invalid_argument("database config must not be null"))?;
    match config.read_only {
        0 => Ok(SlateDbAccessMode::ReadWrite),
        1 => Ok(SlateDbAccessMode::ReadOnly),
        _ => Err(Error::invalid_argument(
            "database config read_only must be 0 or 1",
        )),
    }
}

unsafe fn require_output<'a, T>(output: *mut T, name: &str) -> Result<&'a mut T> {
    unsafe { output.as_mut() }
        .ok_or_else(|| Error::invalid_argument(format!("{name} must not be null")))
}

fn duration_as_milliseconds(duration: std::time::Duration) -> f64 {
    duration.as_secs() as f64 * 1000.0 + f64::from(duration.subsec_nanos()) / 1_000_000.0
}

unsafe fn with_file_handle<T>(
    handle: *const FfiFileHandle,
    operation: impl FnOnce(&Runtime, &mut (dyn FileHandle + Send)) -> Result<T>,
) -> Result<T> {
    let handle = unsafe { handle.as_ref() }
        .ok_or_else(|| Error::invalid_argument("file handle pointer must not be null"))?;
    let mut guard = handle
        .handle
        .lock()
        .map_err(|_| Error::invalid_argument("file handle lock is poisoned"))?;
    let file = guard
        .as_mut()
        .ok_or_else(|| Error::invalid_argument("file handle is closed"))?;
    operation(&handle.runtime, file.as_mut())
}

unsafe fn read_buffer<'a>(buffer: *mut u8, len: usize) -> Result<&'a mut [u8]> {
    if buffer.is_null() {
        return if len == 0 {
            Ok(&mut [])
        } else {
            Err(Error::invalid_argument("read buffer must not be null"))
        };
    }
    Ok(unsafe { slice::from_raw_parts_mut(buffer, len) })
}

unsafe fn write_buffer<'a>(buffer: *const u8, len: usize) -> Result<&'a [u8]> {
    if buffer.is_null() {
        return if len == 0 {
            Ok(&[])
        } else {
            Err(Error::invalid_argument("write buffer must not be null"))
        };
    }
    Ok(unsafe { slice::from_raw_parts(buffer, len) })
}

fn create_runtime() -> Result<Arc<Runtime>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|source| Error::io_with_source("failed to initialize Tokio runtime", source))
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_create_memory(
    cache_config: *const FfiCacheConfig,
    database_config: *const FfiDatabaseConfig,
    output: *mut *mut FfiFileSystem,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "filesystem output")? };
        *output = ptr::null_mut();
        let access_mode = unsafe { require_database_config(database_config)? };
        let cache_config = unsafe { parse_cache_config(cache_config)? };
        let runtime = create_runtime()?;
        let fs = runtime.block_on(SlateDbFileSystem::open_in_memory_with_cache_config(
            DATABASE_PATH,
            cache_config,
            access_mode,
        ))?;
        *output = Box::into_raw(Box::new(FfiFileSystem { runtime, fs }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_create_local(
    root: *const c_char,
    cache_config: *const FfiCacheConfig,
    database_config: *const FfiDatabaseConfig,
    output: *mut *mut FfiFileSystem,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "filesystem output")? };
        *output = ptr::null_mut();
        let access_mode = unsafe { require_database_config(database_config)? };
        let root = unsafe { require_path(root, "local root")? };
        if root.is_empty() {
            return Err(Error::invalid_argument("local root must not be empty"));
        }
        let cache_config = unsafe { parse_cache_config(cache_config)? };
        let runtime = create_runtime()?;
        let fs = runtime.block_on(SlateDbFileSystem::open_local_with_cache_config(
            DATABASE_PATH,
            root,
            cache_config,
            access_mode,
        ))?;
        *output = Box::into_raw(Box::new(FfiFileSystem { runtime, fs }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_create_s3(
    config: *const FfiS3Config,
    cache_config: *const FfiCacheConfig,
    database_config: *const FfiDatabaseConfig,
    output: *mut *mut FfiFileSystem,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "filesystem output")? };
        *output = ptr::null_mut();
        let access_mode = unsafe { require_database_config(database_config)? };
        let config = unsafe { require_s3_config(config)? };
        let cache_config = unsafe { parse_cache_config(cache_config)? };
        let runtime = create_runtime()?;
        let fs = runtime.block_on(SlateDbFileSystem::open_s3_with_cache_config(
            DATABASE_PATH,
            config,
            cache_config,
            access_mode,
        ))?;
        *output = Box::into_raw(Box::new(FfiFileSystem { runtime, fs }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_destroy(fs: *mut FfiFileSystem) {
    if fs.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let mut fs = unsafe { Box::from_raw(fs) };
        let _ = fs.runtime.block_on(fs.fs.close());
    }));
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_get_cache_stats(
    fs: *const FfiFileSystem,
    output: *mut FfiCacheStats,
) -> i32 {
    ffi_result(|| {
        let fs = unsafe { require_fs(fs)? };
        let output = unsafe { require_output(output, "cache stats output")? };
        let stats = fs.fs.cache_stats();
        *output = FfiCacheStats {
            block_cache_hits: stats.block_cache_hits,
            block_cache_misses: stats.block_cache_misses,
            metadata_cache_hits: stats.metadata_cache_hits,
            metadata_cache_misses: stats.metadata_cache_misses,
            persistent_cache_hits: stats.persistent_cache_hits,
            persistent_cache_misses: stats.persistent_cache_misses,
            persistent_cache_entries: stats.persistent_cache_entries,
            persistent_cache_size_bytes: stats.persistent_cache_size_bytes,
            persistent_cache_evictions: stats.persistent_cache_evictions,
            persistent_cache_evicted_bytes: stats.persistent_cache_evicted_bytes,
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_get_io_stats(
    fs: *const FfiFileSystem,
    output: *mut FfiIoStats,
) -> i32 {
    ffi_result(|| {
        let fs = unsafe { require_fs(fs)? };
        let output = unsafe { require_output(output, "I/O stats output")? };
        let stats = fs.fs.io_stats();
        *output = FfiIoStats {
            read_request_count: stats.read.request_count,
            read_average_latency_ms: duration_as_milliseconds(stats.read.average_latency),
            read_stddev_latency_ms: duration_as_milliseconds(stats.read.stddev_latency),
            write_request_count: stats.write.request_count,
            write_average_latency_ms: duration_as_milliseconds(stats.write.average_latency),
            write_stddev_latency_ms: duration_as_milliseconds(stats.write.stddev_latency),
            stat_request_count: stats.stat.request_count,
            stat_average_latency_ms: duration_as_milliseconds(stats.stat.average_latency),
            stat_stddev_latency_ms: duration_as_milliseconds(stats.stat.stddev_latency),
            delete_request_count: stats.delete.request_count,
            delete_average_latency_ms: duration_as_milliseconds(stats.delete.average_latency),
            delete_stddev_latency_ms: duration_as_milliseconds(stats.delete.stddev_latency),
            list_request_count: stats.list.request_count,
            list_average_latency_ms: duration_as_milliseconds(stats.list.average_latency),
            list_stddev_latency_ms: duration_as_milliseconds(stats.list.stddev_latency),
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_open_file(
    fs: *const FfiFileSystem,
    path: *const c_char,
    options: *const FfiOpenOptions,
    output: *mut *mut FfiFileHandle,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "file handle output")? };
        *output = ptr::null_mut();
        let fs = unsafe { require_fs(fs)? };
        let path = unsafe { require_path(path, "file path")? };
        let flags = unsafe { require_options(options)? };
        let handle = fs.runtime.block_on(fs.fs.open_file(path, flags))?;
        *output = Box::into_raw(Box::new(FfiFileHandle {
            runtime: Arc::clone(&fs.runtime),
            handle: Mutex::new(Some(handle)),
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_file_exists(
    fs: *const FfiFileSystem,
    path: *const c_char,
    output: *mut i32,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "file exists output")? };
        *output = 0;
        let fs = unsafe { require_fs(fs)? };
        let path = unsafe { require_path(path, "file path")? };
        let exists = fs.runtime.block_on(fs.fs.file_exists(path))?;
        *output = i32::from(exists);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_remove_file(
    fs: *const FfiFileSystem,
    path: *const c_char,
) -> i32 {
    ffi_result(|| {
        let fs = unsafe { require_fs(fs)? };
        let path = unsafe { require_path(path, "file path")? };
        fs.runtime.block_on(fs.fs.remove_file(path))
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_move_file(
    fs: *const FfiFileSystem,
    source: *const c_char,
    target: *const c_char,
) -> i32 {
    ffi_result(|| {
        let fs = unsafe { require_fs(fs)? };
        let source = unsafe { require_path(source, "source path")? };
        let target = unsafe { require_path(target, "target path")? };
        fs.runtime.block_on(fs.fs.move_file(source, target))
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_read(
    handle: *const FfiFileHandle,
    buffer: *mut u8,
    len: usize,
    bytes_read: *mut usize,
) -> i32 {
    ffi_result(|| {
        let bytes_read = unsafe { bytes_read.as_mut() }
            .ok_or_else(|| Error::invalid_argument("bytes read output must not be null"))?;
        *bytes_read = 0;
        let buffer = unsafe { read_buffer(buffer, len)? };
        *bytes_read = unsafe {
            with_file_handle(handle, |runtime, file| runtime.block_on(file.read(buffer)))?
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_pread(
    handle: *const FfiFileHandle,
    buffer: *mut u8,
    len: usize,
    offset: u64,
    bytes_read: *mut usize,
) -> i32 {
    ffi_result(|| {
        let bytes_read = unsafe { bytes_read.as_mut() }
            .ok_or_else(|| Error::invalid_argument("bytes read output must not be null"))?;
        *bytes_read = 0;
        let buffer = unsafe { read_buffer(buffer, len)? };
        *bytes_read = unsafe {
            with_file_handle(handle, |runtime, file| {
                runtime.block_on(file.pread(buffer, offset))
            })?
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_write(
    handle: *const FfiFileHandle,
    buffer: *const u8,
    len: usize,
    bytes_written: *mut usize,
) -> i32 {
    ffi_result(|| {
        let bytes_written = unsafe { bytes_written.as_mut() }
            .ok_or_else(|| Error::invalid_argument("bytes written output must not be null"))?;
        *bytes_written = 0;
        let buffer = unsafe { write_buffer(buffer, len)? };
        *bytes_written = unsafe {
            with_file_handle(handle, |runtime, file| runtime.block_on(file.write(buffer)))?
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_pwrite(
    handle: *const FfiFileHandle,
    buffer: *const u8,
    len: usize,
    offset: u64,
) -> i32 {
    ffi_result(|| {
        let buffer = unsafe { write_buffer(buffer, len)? };
        unsafe {
            with_file_handle(handle, |runtime, file| {
                runtime.block_on(file.pwrite(buffer, offset))
            })
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_sync(handle: *const FfiFileHandle) -> i32 {
    ffi_result(|| unsafe {
        with_file_handle(handle, |runtime, file| runtime.block_on(file.sync()))
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_truncate(handle: *const FfiFileHandle, new_size: u64) -> i32 {
    ffi_result(|| unsafe {
        with_file_handle(handle, |runtime, file| {
            runtime.block_on(file.truncate(new_size))
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_seek(handle: *const FfiFileHandle, position: u64) -> i32 {
    ffi_result(|| unsafe {
        with_file_handle(handle, |_, file| {
            file.seek(position);
            Ok(())
        })
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_get_position(
    handle: *const FfiFileHandle,
    output: *mut u64,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "file position output")? };
        *output = 0;
        *output = unsafe { with_file_handle(handle, |_, file| Ok(file.seek_position()))? };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_get_size(
    handle: *const FfiFileHandle,
    output: *mut u64,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { require_output(output, "file size output")? };
        *output = 0;
        *output = unsafe { with_file_handle(handle, |_, file| Ok(file.file_size()))? };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_close(handle: *mut FfiFileHandle) -> i32 {
    ffi_result(|| {
        let handle = unsafe { handle.as_mut() }
            .ok_or_else(|| Error::invalid_argument("file handle pointer must not be null"))?;
        let mut guard = handle
            .handle
            .lock()
            .map_err(|_| Error::invalid_argument("file handle lock is poisoned"))?;
        let Some(file) = guard.as_mut() else {
            return Ok(());
        };
        handle.runtime.block_on(file.close())?;
        *guard = None;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_file_destroy(handle: *mut FfiFileHandle) {
    if handle.is_null() {
        return;
    }
    let _ = unsafe { slatedb_file_close(handle) };
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(handle));
    }));
}

#[no_mangle]
pub extern "C" fn slatedb_fs_last_error_message() -> *const c_char {
    LAST_ERROR_MESSAGE.with(|message| message.borrow().as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_can_handle(
    fs: *const FfiFileSystem,
    path: *const c_char,
) -> i32 {
    if fs.is_null() || path.is_null() {
        return 0;
    }
    let path = match CStr::from_ptr(path).to_str() {
        Ok(path) => path,
        Err(_) => return 0,
    };
    i32::from((*fs).fs.can_handle(path))
}

#[no_mangle]
pub extern "C" fn slatedb_fs_name() -> *const c_char {
    static NAME: &[u8] = b"SlateDBFileSystem\0";
    NAME.as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_config(read_only: bool) -> FfiDatabaseConfig {
        FfiDatabaseConfig {
            read_only: i32::from(read_only),
        }
    }

    fn read_write_options() -> FfiOpenOptions {
        FfiOpenOptions {
            read: 1,
            write: 1,
            create: 1,
            append: 0,
            truncate_existing: 0,
        }
    }

    fn read_only_options() -> FfiOpenOptions {
        FfiOpenOptions {
            read: 1,
            write: 0,
            create: 0,
            append: 0,
            truncate_existing: 0,
        }
    }

    unsafe fn expect_ok(code: i32) {
        if code != 0 {
            let message = unsafe { CStr::from_ptr(slatedb_fs_last_error_message()) };
            panic!(
                "FFI call failed with code {code}: {}",
                message.to_string_lossy()
            );
        }
    }

    unsafe fn create_fs() -> *mut FfiFileSystem {
        let mut fs = ptr::null_mut();
        let config = database_config(false);
        expect_ok(slatedb_fs_create_memory(ptr::null(), &config, &mut fs));
        assert!(!fs.is_null());
        fs
    }

    unsafe fn open_file(
        fs: *const FfiFileSystem,
        path: &CStr,
        options: &FfiOpenOptions,
    ) -> *mut FfiFileHandle {
        let mut handle = ptr::null_mut();
        unsafe {
            expect_ok(slatedb_fs_open_file(
                fs,
                path.as_ptr(),
                options,
                &mut handle,
            ));
        }
        assert!(!handle.is_null());
        handle
    }

    unsafe fn file_exists(fs: *const FfiFileSystem, path: &CStr) -> bool {
        let mut exists = 0;
        unsafe {
            expect_ok(slatedb_fs_file_exists(fs, path.as_ptr(), &mut exists));
        }
        exists != 0
    }

    #[test]
    fn ffi_s3_constructor_validates_config() {
        unsafe {
            let bucket = CString::new("").unwrap();
            let config = FfiS3Config {
                bucket: bucket.as_ptr(),
                root: ptr::null(),
                endpoint: ptr::null(),
                region: ptr::null(),
                key_id: ptr::null(),
                secret: ptr::null(),
                session_token: ptr::null(),
                use_ssl: 1,
                virtual_host_style: 0,
            };
            let mut fs = ptr::null_mut();
            let database_config = database_config(false);

            assert_eq!(
                slatedb_fs_create_s3(&config, ptr::null(), &database_config, &mut fs),
                crate::error::ErrorCode::InvalidArgument as i32
            );
            assert!(fs.is_null());
            let message = CStr::from_ptr(slatedb_fs_last_error_message()).to_string_lossy();
            assert!(message.contains("S3 bucket must not be empty"));
        }
    }

    #[test]
    fn ffi_local_constructor_validates_and_opens_root() {
        unsafe {
            let empty = CString::new("").unwrap();
            let mut fs = ptr::null_mut();
            let database_config = database_config(false);
            assert_eq!(
                slatedb_fs_create_local(empty.as_ptr(), ptr::null(), &database_config, &mut fs,),
                crate::error::ErrorCode::InvalidArgument as i32
            );
            assert!(fs.is_null());

            let root = tempfile::tempdir().expect("temporary local root");
            let root = CString::new(root.path().to_str().expect("UTF-8 local root")).unwrap();
            expect_ok(slatedb_fs_create_local(
                root.as_ptr(),
                ptr::null(),
                &database_config,
                &mut fs,
            ));
            assert!(!fs.is_null());
            slatedb_fs_destroy(fs);
        }
    }

    #[test]
    fn ffi_read_only_local_client_reopens_writer_data() {
        unsafe {
            let root = tempfile::tempdir().expect("temporary local root");
            let root = CString::new(root.path().to_str().expect("UTF-8 local root")).unwrap();
            let path = CString::new("database.db").unwrap();

            let mut writer = ptr::null_mut();
            let writer_config = database_config(false);
            expect_ok(slatedb_fs_create_local(
                root.as_ptr(),
                ptr::null(),
                &writer_config,
                &mut writer,
            ));
            let handle = open_file(writer, &path, &read_write_options());
            let mut bytes_written = 0;
            expect_ok(slatedb_file_write(
                handle,
                b"contents".as_ptr(),
                8,
                &mut bytes_written,
            ));
            expect_ok(slatedb_file_sync(handle));
            slatedb_file_destroy(handle);
            slatedb_fs_destroy(writer);

            let mut reader = ptr::null_mut();
            let reader_config = database_config(true);
            expect_ok(slatedb_fs_create_local(
                root.as_ptr(),
                ptr::null(),
                &reader_config,
                &mut reader,
            ));
            assert!(file_exists(reader, &path));
            let handle = open_file(reader, &path, &read_only_options());
            let mut contents = [0; 8];
            let mut bytes_read = 0;
            expect_ok(slatedb_file_read(
                handle,
                contents.as_mut_ptr(),
                contents.len(),
                &mut bytes_read,
            ));
            assert_eq!(&contents, b"contents");
            slatedb_file_destroy(handle);
            slatedb_fs_destroy(reader);
        }
    }

    #[test]
    fn ffi_open_close_lifecycle() {
        unsafe {
            let fs = create_fs();
            let path = CString::new("database.db").unwrap();
            let handle = open_file(fs, &path, &read_write_options());
            expect_ok(slatedb_file_close(handle));
            slatedb_file_destroy(handle);

            let mut missing_handle = ptr::null_mut();
            let missing = CString::new("missing.db").unwrap();
            let error = slatedb_fs_open_file(
                fs,
                missing.as_ptr(),
                &read_only_options(),
                &mut missing_handle,
            );
            assert_eq!(error, crate::ErrorCode::FileNotFound as i32);
            assert!(missing_handle.is_null());
            slatedb_fs_destroy(fs);
        }
    }

    #[test]
    fn ffi_read_write_sync_and_truncate() {
        unsafe {
            let fs = create_fs();
            let path = CString::new("database.db").unwrap();
            let handle = open_file(fs, &path, &read_write_options());

            let mut bytes_written = 0;
            expect_ok(slatedb_file_write(
                handle,
                b"abcdef".as_ptr(),
                6,
                &mut bytes_written,
            ));
            assert_eq!(bytes_written, 6);
            expect_ok(slatedb_file_pwrite(handle, b"XY".as_ptr(), 2, 2));
            expect_ok(slatedb_file_truncate(handle, 4));
            expect_ok(slatedb_file_sync(handle));
            let mut size = 0;
            expect_ok(slatedb_file_get_size(handle, &mut size));
            assert_eq!(size, 4);
            slatedb_file_destroy(handle);

            let handle = open_file(fs, &path, &read_only_options());
            let mut contents = [0; 4];
            let mut bytes_read = 0;
            expect_ok(slatedb_file_read(
                handle,
                contents.as_mut_ptr(),
                contents.len(),
                &mut bytes_read,
            ));
            assert_eq!(bytes_read, contents.len());
            assert_eq!(&contents, b"abXY");
            let mut middle = [0; 2];
            expect_ok(slatedb_file_pread(
                handle,
                middle.as_mut_ptr(),
                middle.len(),
                2,
                &mut bytes_read,
            ));
            assert_eq!(&middle, b"XY");
            slatedb_file_destroy(handle);
            slatedb_fs_destroy(fs);
        }
    }

    #[test]
    fn ffi_seek_and_get_position() {
        unsafe {
            let fs = create_fs();
            let path = CString::new("database.db").unwrap();
            let handle = open_file(fs, &path, &read_write_options());

            let mut bytes_written = 0;
            expect_ok(slatedb_file_write(
                handle,
                b"abc".as_ptr(),
                3,
                &mut bytes_written,
            ));
            expect_ok(slatedb_file_seek(handle, 1));
            let mut position = 0;
            expect_ok(slatedb_file_get_position(handle, &mut position));
            assert_eq!(position, 1);

            let mut byte = [0];
            let mut bytes_read = 0;
            expect_ok(slatedb_file_read(
                handle,
                byte.as_mut_ptr(),
                byte.len(),
                &mut bytes_read,
            ));
            assert_eq!(&byte, b"b");
            expect_ok(slatedb_file_get_position(handle, &mut position));
            assert_eq!(position, 2);

            slatedb_file_destroy(handle);
            slatedb_fs_destroy(fs);
        }
    }

    #[test]
    fn ffi_file_exists_move_and_remove() {
        unsafe {
            let fs = create_fs();
            let source = CString::new("source.db").unwrap();
            let target = CString::new("target.db").unwrap();
            slatedb_file_destroy(open_file(fs, &source, &read_write_options()));

            assert!(file_exists(fs, &source));
            assert!(!file_exists(fs, &target));
            expect_ok(slatedb_fs_move_file(fs, source.as_ptr(), target.as_ptr()));
            assert!(!file_exists(fs, &source));
            assert!(file_exists(fs, &target));
            expect_ok(slatedb_fs_remove_file(fs, target.as_ptr()));
            assert!(!file_exists(fs, &target));

            slatedb_fs_destroy(fs);
        }
    }
}
