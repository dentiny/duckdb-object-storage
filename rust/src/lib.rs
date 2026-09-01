//! DuckDB filesystem backed by [SlateDB](https://slatedb.io/).
//!
//! [`SlateFs`] is the storage layer: files live in a SlateDB instance on an
//! object store as a path mapping, a metadata record and fixed-size chunks.
//!
//! [`SlateDbFileSystem`] is the DuckDB-facing adapter reached through the C ABI
//! in [`ffi`]. It still refuses every file operation; wiring it to [`SlateFs`]
//! needs configuration plumbing for the object store, which is not here yet.

mod error;
mod error_struct;
mod ffi;
mod file_handle;
mod flags;
mod fs;
#[allow(dead_code)]
mod keys;
#[allow(dead_code)]
mod metadata;
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
