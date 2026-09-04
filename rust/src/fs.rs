use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::services::{Fs, Memory};
use opendal::Operator;
use slatedb::Db;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "slatedb://";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

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
    /// Opens SlateDB on top of an OpenDAL storage operator.
    pub async fn open(database_path: &str, operator: Operator) -> Result<Self> {
        if database_path.is_empty() {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "database path must not be empty".to_string(),
                ErrorStatus::Permanent,
            )));
        }

        let object_store = Arc::new(OpendalStore::new(operator));
        let db = Db::open(database_path, object_store).await?;
        Ok(Self {
            db: Some(Arc::new(db)),
        })
    }

    pub async fn open_in_memory(database_path: &str) -> Result<Self> {
        let operator = Operator::new(Memory::default()).map_err(|source| {
            Error::Io(
                ErrorStruct::new(
                    "failed to initialize OpenDAL memory storage".to_string(),
                    ErrorStatus::Permanent,
                )
                .with_source(source),
            )
        })?;
        Self::open(database_path, operator).await
    }

    pub async fn open_local(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let root_str = root.to_str().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                format!("local OpenDAL root is not valid UTF-8: {}", root.display()),
                ErrorStatus::Permanent,
            ))
        })?;
        tokio::fs::create_dir_all(&root).await?;

        let operator = Operator::new(Fs::default().root(root_str)).map_err(|source| {
            Error::Io(
                ErrorStruct::new(
                    format!("failed to initialize OpenDAL storage at {}", root.display()),
                    ErrorStatus::Permanent,
                )
                .with_source(source),
            )
        })?;
        Self::open(database_path, operator).await
    }

    /// Flush and close the owned database.
    ///
    /// Calling `close` more than once is harmless. SlateDB marks all clones of
    /// the database closed.
    pub async fn close(&mut self) -> Result<()> {
        let Some(db) = self.db.take() else {
            return Ok(());
        };

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
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

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
    async fn claims_slatedb_urls() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        assert!(fs.can_handle("slatedb://bucket/key"));
        assert!(!fs.can_handle("s3://bucket/key"));
        assert!(!fs.can_handle("/tmp/foo"));
        assert_eq!(fs.name(), NAME);
        fs.close().await.expect("close");
    }
}
