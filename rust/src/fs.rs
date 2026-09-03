use std::sync::Arc;

use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

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
/// One live database is kept for the lifetime of the filesystem. The FFI layer
/// owns the runtime used to drive these asynchronous operations.
pub struct SlateDbFileSystem {
    db: Option<Arc<Db>>,
}

impl SlateDbFileSystem {
    /// Open a SlateDB database over the supplied object store.
    pub async fn open(database_path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        if database_path.is_empty() {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "database path must not be empty".to_string(),
                ErrorStatus::Permanent,
            )));
        }

        let db = Db::open(database_path, object_store).await?;
        Ok(Self {
            db: Some(Arc::new(db)),
        })
    }

    /// Open an isolated in-memory database.
    pub async fn open_in_memory(database_path: &str) -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::open(database_path, object_store).await
    }

    /// Open a persistent database rooted at a local directory.
    pub async fn open_local(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
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
        Self::open(database_path, Arc::new(object_store)).await
    }

    /// Open the default in-memory database used by the current C ABI.
    pub async fn try_new() -> Result<Self> {
        Self::open_in_memory(DEFAULT_DATABASE_PATH).await
    }

    /// Flush and close the owned database.
    ///
    /// Calling `close` more than once is harmless. Closing while another owner
    /// still holds the database is rejected so live file handles cannot be
    /// invalidated.
    pub async fn close(&mut self) -> Result<()> {
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
        db.close().await?;
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    async fn put(fs: &SlateDbFileSystem, key: &[u8], value: &[u8]) {
        let db = fs.db.as_ref().expect("open database");
        db.put(key, value).await.expect("write");
    }

    async fn get(fs: &SlateDbFileSystem, key: &[u8]) -> Option<Vec<u8>> {
        let db = fs.db.as_ref().expect("open database");
        db.get(key).await.expect("read").map(|value| value.to_vec())
    }

    #[tokio::test]
    async fn opens_live_in_memory_database() {
        let mut fs = SlateDbFileSystem::try_new().await.expect("filesystem");

        put(&fs, b"key", b"value").await;
        assert_eq!(get(&fs, b"key").await, Some(b"value".to_vec()));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn local_database_persists_across_reopen() {
        let root = tempdir().expect("temporary object-store root");

        let mut first = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("first open");
        put(&first, b"key", b"value").await;
        first.close().await.expect("first close");

        let mut second = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("second open");
        assert_eq!(get(&second, b"key").await, Some(b"value".to_vec()));
        second.close().await.expect("second close");
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let mut fs = SlateDbFileSystem::try_new().await.expect("filesystem");

        fs.close().await.expect("first close");
        fs.close().await.expect("second close");
    }

    #[tokio::test]
    async fn close_rejects_outstanding_database_owner() {
        let mut fs = SlateDbFileSystem::try_new().await.expect("filesystem");
        let outstanding = Arc::clone(fs.db.as_ref().expect("open database"));

        assert!(matches!(fs.close().await, Err(Error::InvalidArgument(_))));
        put(&fs, b"key", b"value").await;

        drop(outstanding);
        fs.close().await.expect("close after releasing owner");
    }

    #[tokio::test]
    async fn rejects_empty_database_path() {
        let error = match SlateDbFileSystem::open_in_memory("").await {
            Ok(_) => panic!("empty path should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn claims_slatedb_urls() {
        let mut fs = SlateDbFileSystem::try_new().await.expect("filesystem");
        assert!(fs.can_handle("slatedb://bucket/key"));
        assert!(!fs.can_handle("s3://bucket/key"));
        assert!(!fs.can_handle("/tmp/foo"));
        assert_eq!(fs.name(), NAME);
        fs.close().await.expect("close");
    }
}
