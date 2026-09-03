use std::ffi::{c_char, CStr};
use std::ptr;
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use tokio::runtime::Runtime;

use crate::fs::SlateDbFileSystem;

const DATABASE_PATH: &str = "duckdb-object-storage";

/// Synchronous FFI context owning the one async runtime used by this
/// filesystem instance.
pub struct FfiFileSystem {
    runtime: Arc<Runtime>,
    fs: SlateDbFileSystem,
}

#[no_mangle]
pub extern "C" fn slatedb_fs_create() -> *mut FfiFileSystem {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        // TODO(hjiang): Tune the worker count.
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(runtime) => Arc::new(runtime),
        Err(_) => return ptr::null_mut(),
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fs = match runtime.block_on(SlateDbFileSystem::open(DATABASE_PATH, object_store)) {
        Ok(fs) => fs,
        Err(_) => return ptr::null_mut(),
    };

    Box::into_raw(Box::new(FfiFileSystem { runtime, fs }))
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_destroy(fs: *mut FfiFileSystem) {
    if !fs.is_null() {
        let mut fs = Box::from_raw(fs);
        let _ = fs.runtime.block_on(fs.fs.close());
    }
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
