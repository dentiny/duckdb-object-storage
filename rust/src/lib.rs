//! Dummy DuckDB filesystem backed by [SlateDB](https://slatedb.io/).
//!
//! The C ABI in [`ffi`] is consumed by the C++ `FileSystem` adapter. File I/O
//! is not implemented yet; this crate exists to prove the SlateDB dependency
//! and VFS registration compile.

mod error;
mod error_struct;
mod ffi;
// Fully exercised once the remaining file handle operations land.
#[allow(dead_code)]
mod file_handle;
mod flags;
mod fs;
#[allow(dead_code)]
mod keys;
#[allow(dead_code)]
mod metadata;
// Grows into the filesystem the DuckDB adapter delegates to.
#[allow(dead_code)]
mod slatefs;
#[cfg(test)]
mod test_utils;
#[allow(dead_code)]
mod util;

pub use error::{Error, Result};
pub use error_struct::{ErrorStatus, ErrorStruct};
pub use file_handle::{FileHandle, SlateFileHandle};
pub use flags::FileOpenFlags;
pub use fs::SlateDbFileSystem;
pub use slatefs::{FileSystem, SlateFs};
