use std::ffi::{c_char, CStr};
use std::ptr;

use crate::fs::SlateDbFileSystem;

#[no_mangle]
pub extern "C" fn slatedb_fs_create() -> *mut SlateDbFileSystem {
    match SlateDbFileSystem::try_new() {
        Ok(fs) => Box::into_raw(Box::new(fs)),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_destroy(fs: *mut SlateDbFileSystem) {
    if !fs.is_null() {
        drop(Box::from_raw(fs));
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_fs_can_handle(
    fs: *const SlateDbFileSystem,
    path: *const c_char,
) -> i32 {
    if fs.is_null() || path.is_null() {
        return 0;
    }
    let path = match CStr::from_ptr(path).to_str() {
        Ok(path) => path,
        Err(_) => return 0,
    };
    i32::from((*fs).can_handle(path))
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
