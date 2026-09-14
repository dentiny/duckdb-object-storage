use std::any::Any;
use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_handle::{FileHandle, SlateFileHandle};
use crate::flags::FileOpenFlags;
use crate::fs::SlateDbFileSystem;

const DATABASE_PATH: &str = "duckdb-object-storage";

#[repr(C)]
pub struct FfiOpenOptions {
    read: i32,
    write: i32,
    create: i32,
    append: i32,
    truncate_existing: i32,
}

/// Synchronous FFI context owning the one async runtime used by this
/// filesystem instance.
pub struct FfiFileSystem {
    runtime: Arc<Runtime>,
    fs: SlateDbFileSystem,
}

pub struct FfiFileHandle {
    runtime: Arc<Runtime>,
    handle: Mutex<Option<SlateFileHandle>>,
}

thread_local! {
    static LAST_ERROR_MESSAGE: RefCell<CString> =
        RefCell::new(CString::new("").expect("empty string has no NUL"));
}

fn invalid_argument(message: impl Into<String>) -> Error {
    Error::InvalidArgument(ErrorStruct::new(message.into(), ErrorStatus::Permanent))
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
        Err(payload) => store_error(invalid_argument(format!(
            "Rust panic while handling FFI call: {}",
            panic_message(payload)
        ))),
    }
}

unsafe fn require_fs<'a>(fs: *const FfiFileSystem) -> Result<&'a FfiFileSystem> {
    unsafe { fs.as_ref() }.ok_or_else(|| invalid_argument("filesystem pointer must not be null"))
}

unsafe fn require_path<'a>(path: *const c_char, name: &str) -> Result<&'a str> {
    if path.is_null() {
        return Err(invalid_argument(format!("{name} must not be null")));
    }
    unsafe { CStr::from_ptr(path) }
        .to_str()
        .map_err(|_| invalid_argument(format!("{name} must be valid UTF-8")))
}

unsafe fn require_options(options: *const FfiOpenOptions) -> Result<FileOpenFlags> {
    let options = unsafe { options.as_ref() }
        .ok_or_else(|| invalid_argument("open options must not be null"))?;
    Ok(FileOpenFlags {
        read: options.read != 0,
        write: options.write != 0,
        create: options.create != 0,
        append: options.append != 0,
        truncate_existing: options.truncate_existing != 0,
    })
}

unsafe fn with_file_handle<T>(
    handle: *const FfiFileHandle,
    operation: impl FnOnce(&Runtime, &mut SlateFileHandle) -> Result<T>,
) -> Result<T> {
    let handle = unsafe { handle.as_ref() }
        .ok_or_else(|| invalid_argument("file handle pointer must not be null"))?;
    let mut guard = handle
        .handle
        .lock()
        .map_err(|_| invalid_argument("file handle lock is poisoned"))?;
    let file = guard
        .as_mut()
        .ok_or_else(|| invalid_argument("file handle is closed"))?;
    operation(&handle.runtime, file)
}

unsafe fn read_buffer<'a>(buffer: *mut u8, len: usize) -> Result<&'a mut [u8]> {
    if buffer.is_null() {
        return if len == 0 {
            Ok(&mut [])
        } else {
            Err(invalid_argument("read buffer must not be null"))
        };
    }
    Ok(unsafe { slice::from_raw_parts_mut(buffer, len) })
}

unsafe fn write_buffer<'a>(buffer: *const u8, len: usize) -> Result<&'a [u8]> {
    if buffer.is_null() {
        return if len == 0 {
            Ok(&[])
        } else {
            Err(invalid_argument("write buffer must not be null"))
        };
    }
    Ok(unsafe { slice::from_raw_parts(buffer, len) })
}

#[no_mangle]
pub extern "C" fn slatedb_fs_create() -> *mut FfiFileSystem {
    catch_unwind(AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            // TODO(hjiang): Tune the worker count.
            .worker_threads(1)
            .enable_all()
            .build()
            .ok()
            .map(Arc::new)?;
        let fs = runtime
            .block_on(SlateDbFileSystem::open_in_memory(DATABASE_PATH))
            .ok()?;
        Some(Box::into_raw(Box::new(FfiFileSystem { runtime, fs })))
    }))
    .ok()
    .flatten()
    .unwrap_or(ptr::null_mut())
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
pub unsafe extern "C" fn slatedb_fs_open_file(
    fs: *const FfiFileSystem,
    path: *const c_char,
    options: *const FfiOpenOptions,
    output: *mut *mut FfiFileHandle,
) -> i32 {
    ffi_result(|| {
        let output = unsafe { output.as_mut() }
            .ok_or_else(|| invalid_argument("file handle output must not be null"))?;
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
pub unsafe extern "C" fn slatedb_file_read(
    handle: *const FfiFileHandle,
    buffer: *mut u8,
    len: usize,
    bytes_read: *mut usize,
) -> i32 {
    ffi_result(|| {
        let bytes_read = unsafe { bytes_read.as_mut() }
            .ok_or_else(|| invalid_argument("bytes read output must not be null"))?;
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
            .ok_or_else(|| invalid_argument("bytes read output must not be null"))?;
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
            .ok_or_else(|| invalid_argument("bytes written output must not be null"))?;
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
pub unsafe extern "C" fn slatedb_file_close(handle: *mut FfiFileHandle) -> i32 {
    ffi_result(|| {
        let handle = unsafe { handle.as_mut() }
            .ok_or_else(|| invalid_argument("file handle pointer must not be null"))?;
        let mut guard = handle
            .handle
            .lock()
            .map_err(|_| invalid_argument("file handle lock is poisoned"))?;
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

#[no_mangle]
pub extern "C" fn slatedb_fs_dummy_error() -> *const c_char {
    static MSG: &[u8] = b"SlateDBFileSystem is a dummy implementation and cannot open files yet\0";
    MSG.as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn expect_ok(code: i32) {
        if code != 0 {
            let message = unsafe { CStr::from_ptr(slatedb_fs_last_error_message()) };
            panic!(
                "FFI call failed with code {code}: {}",
                message.to_string_lossy()
            );
        }
    }

    #[test]
    fn ffi_file_io_lifecycle() {
        unsafe {
            let fs = slatedb_fs_create();
            assert!(!fs.is_null());
            let path = CString::new("database.db").unwrap();

            let mut options = FfiOpenOptions {
                read: 1,
                write: 1,
                create: 1,
                append: 0,
                truncate_existing: 0,
            };

            let mut handle = ptr::null_mut();
            expect_ok(slatedb_fs_open_file(
                fs,
                path.as_ptr(),
                &options,
                &mut handle,
            ));
            assert!(!handle.is_null());

            let mut bytes_written = 0;
            expect_ok(slatedb_file_write(
                handle,
                b"abcdef".as_ptr(),
                6,
                &mut bytes_written,
            ));
            assert_eq!(bytes_written, 6);
            expect_ok(slatedb_file_pwrite(handle, b"XY".as_ptr(), 2, 2));
            expect_ok(slatedb_file_close(handle));
            slatedb_file_destroy(handle);

            options.read = 1;
            options.write = 0;
            options.create = 0;
            expect_ok(slatedb_fs_open_file(
                fs,
                path.as_ptr(),
                &options,
                &mut handle,
            ));
            let mut contents = [0; 6];
            let mut bytes_read = 0;
            expect_ok(slatedb_file_read(
                handle,
                contents.as_mut_ptr(),
                contents.len(),
                &mut bytes_read,
            ));
            assert_eq!(bytes_read, contents.len());
            assert_eq!(&contents, b"abXYef");
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

            let missing = CString::new("missing.db").unwrap();
            let missing_error = slatedb_fs_open_file(fs, missing.as_ptr(), &options, &mut handle);
            assert_eq!(missing_error, crate::ErrorCode::FileNotFound as i32);
            assert!(handle.is_null());

            slatedb_fs_destroy(fs);
        }
    }
}
