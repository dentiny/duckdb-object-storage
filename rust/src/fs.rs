use std::sync::Arc;

use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "slatedb://";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

const DEFAULT_DATABASE_PATH: &str = "duckdb-object-storage";

/// Distinctive error so SQL tests can confirm VFS routing.
pub const DUMMY_ERROR: &str =
    "SlateDBFileSystem is a dummy implementation and cannot open files yet";

/// SlateDB-backed filesystem owner.
///
/// One runtime and one live database are kept for the lifetime of the
/// filesystem. File and path operations will share these objects once the VFS
/// adapter is wired.
pub struct SlateDbFileSystem {
    runtime: Arc<Runtime>,
    db: Option<Arc<Db>>,
}

impl SlateDbFileSystem {
    /// Open a SlateDB database over the supplied object store.
    pub fn open(database_path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        if database_path.is_empty() {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "database path must not be empty".to_string(),
                ErrorStatus::Permanent,
            )));
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        );
        let db = runtime.block_on(Db::open(database_path, object_store))?;

        Ok(Self {
            runtime,
            db: Some(Arc::new(db)),
        })
    }

    /// Open an isolated in-memory database.
    pub fn open_in_memory(database_path: &str) -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::open(database_path, object_store)
    }

    /// Open a persistent database rooted at a local directory.
    pub fn open_local(database_path: &str, root: impl AsRef<std::path::Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        let object_store = LocalFileSystem::new_with_prefix(root.as_ref())
            .map_err(|source| {
                Error::Io(
                    ErrorStruct::new(
                        format!(
                            "failed to open local object store at {}",
                            root.as_ref().display()
                        ),
                        ErrorStatus::Permanent,
                    )
                    .with_source(source),
                )
            })?
            .with_fsync(true);
        Self::open(database_path, Arc::new(object_store))
    }

    /// Open the default in-memory database used by the current C ABI.
    pub fn try_new() -> Result<Self> {
        Self::open_in_memory(DEFAULT_DATABASE_PATH)
    }

    /// Flush and close the owned database.
    ///
    /// Calling `close` more than once is harmless. Closing while another owner
    /// still holds the database is rejected so live file handles cannot be
    /// invalidated.
    pub fn close(&mut self) -> Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };

        if Arc::strong_count(db) != 1 {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "cannot close filesystem while database handles are still open".to_string(),
                ErrorStatus::Permanent,
            )));
        }

        let db = self.db.take().expect("database checked above");
        self.runtime.block_on(db.close())?;
        Ok(())
    }

    pub fn name(&self) -> &'static str {
        NAME
    }

    pub fn can_handle(&self, path: &str) -> bool {
        path.starts_with(PREFIX)
    }

    pub fn dummy_error(&self) -> &'static str {
        DUMMY_ERROR
    }
}

impl Drop for SlateDbFileSystem {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn put(fs: &SlateDbFileSystem, key: &[u8], value: &[u8]) {
        let db = fs.db.as_ref().expect("open database");
        fs.runtime
            .block_on(async { db.put(key, value).await })
            .expect("write");
    }

    fn get(fs: &SlateDbFileSystem, key: &[u8]) -> Option<Vec<u8>> {
        let db = fs.db.as_ref().expect("open database");
        fs.runtime
            .block_on(async { db.get(key).await })
            .expect("read")
            .map(|value| value.to_vec())
    }

    #[test]
    fn opens_live_in_memory_database() {
        let mut fs = SlateDbFileSystem::try_new().expect("filesystem");

        put(&fs, b"key", b"value");
        assert_eq!(get(&fs, b"key"), Some(b"value".to_vec()));

        fs.close().expect("close");
    }

    #[test]
    fn local_database_persists_across_reopen() {
        let root = tempdir().expect("temporary object-store root");

        let mut first =
            SlateDbFileSystem::open_local("persistent-db", root.path()).expect("first open");
        put(&first, b"key", b"value");
        first.close().expect("first close");

        let mut second =
            SlateDbFileSystem::open_local("persistent-db", root.path()).expect("second open");
        assert_eq!(get(&second, b"key"), Some(b"value".to_vec()));
        second.close().expect("second close");
    }

    #[test]
    fn close_is_idempotent() {
        let mut fs = SlateDbFileSystem::try_new().expect("filesystem");

        fs.close().expect("first close");
        fs.close().expect("second close");
    }

    #[test]
    fn close_rejects_outstanding_database_owner() {
        let mut fs = SlateDbFileSystem::try_new().expect("filesystem");
        let outstanding = Arc::clone(fs.db.as_ref().expect("open database"));

        assert!(matches!(fs.close(), Err(Error::InvalidArgument(_))));
        put(&fs, b"key", b"value");

        drop(outstanding);
        fs.close().expect("close after releasing owner");
    }

    #[test]
    fn rejects_empty_database_path() {
        let error = match SlateDbFileSystem::open_in_memory("") {
            Ok(_) => panic!("empty path should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn claims_slatedb_urls() {
        let fs = SlateDbFileSystem::try_new().expect("filesystem");
        assert!(fs.can_handle("slatedb://bucket/key"));
        assert!(!fs.can_handle("s3://bucket/key"));
        assert!(!fs.can_handle("/tmp/foo"));
        assert_eq!(fs.name(), NAME);
    }
}
