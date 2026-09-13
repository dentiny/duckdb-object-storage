//! DuckDB filesystem backed by [SlateDB](https://slatedb.io/).
//!
//! Handle I/O lives in [`file_handle`]. The C ABI in [`ffi`] is consumed by the
//! C++ `FileSystem` adapter; opening paths through VFS is not wired yet.

mod database_metadata;
mod error;
mod error_struct;
mod ffi;
mod file_handle;
mod file_metadata;
mod flags;
mod fs;
mod keys;
mod util;

pub use error::{Error, ErrorCode, Result};
pub use error_struct::{ErrorStatus, ErrorStruct};
pub use file_handle::{FileHandle, SlateFileHandle};
pub use flags::FileOpenFlags;
pub use fs::SlateDbFileSystem;
