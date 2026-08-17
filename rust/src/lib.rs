//! Dummy DuckDB filesystem backed by [SlateDB](https://slatedb.io/).
//!
//! The C ABI in [`ffi`] is consumed by the C++ `FileSystem` adapter. File I/O
//! is not implemented yet; this crate exists to prove the SlateDB dependency
//! and VFS registration compile.

mod ffi;
mod fs;
#[allow(dead_code)]
mod keys;
#[allow(dead_code)]
mod metadata;

pub use fs::SlateDbFileSystem;
