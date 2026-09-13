use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::services::{Fs, Memory};
use opendal::Operator;
use slatedb::Db;

use crate::database_metadata::DatabaseMetadata;
use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_handle::SlateFileHandle;
use crate::flags::FileOpenFlags;

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

    /// Open a logical file, creating its persistent catalog entry when allowed.
    ///
    // TODO: Evaluate adding a read-through in-memory path catalog; SlateDB must remain the source of truth.
    pub async fn open_file(&self, path: &str, flags: FileOpenFlags) -> Result<SlateFileHandle> {
        flags.validate()?;

        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        let database_metadata = DatabaseMetadata::new(Arc::clone(&db));
        let (file_id, metadata) = database_metadata
            .get_or_create_file(path, flags.create)
            .await?;

        SlateFileHandle::new(db, file_id, metadata, flags)
    }

    /// Returns whether a logical path is present in the database catalog.
    pub async fn file_exists(&self, path: &str) -> Result<bool> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).file_exists(path).await
    }

    /// Atomically removes a logical file and all of its persisted state.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).remove_file(path).await
    }

    /// Atomically moves a logical file, replacing the destination if present.
    pub async fn move_file(&self, source: &str, target: &str) -> Result<()> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).move_file(source, target).await
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
    use slatedb::WriteBatch;
    use tempfile::tempdir;

    use super::*;
    use crate::file_handle::FileHandle;
    use crate::keys;

    #[tokio::test]
    async fn creates_and_reopens_file_by_path() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let mut created = fs
            .open_file("database.db", FileOpenFlags::create())
            .await
            .expect("create file");
        assert_eq!(created.file_id(), 1);
        created
            .write(b"database contents")
            .await
            .expect("write file");
        created.close().await.expect("close created file");
        drop(created);

        let mut other = fs
            .open_file("other.db", FileOpenFlags::create())
            .await
            .expect("create second file");
        assert_eq!(other.file_id(), 2);
        other.close().await.expect("close second file");
        drop(other);

        let mut reopened = fs
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .expect("reopen file");
        assert_eq!(reopened.file_id(), 1);
        let mut contents = vec![0; "database contents".len()];
        assert_eq!(
            reopened.read(&mut contents).await.expect("read file"),
            contents.len()
        );
        assert_eq!(contents, b"database contents");
        reopened.close().await.expect("close reopened file");
        drop(reopened);

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn local_database_persists_across_reopen() {
        let root = tempdir().expect("temporary object-store root");

        let mut first = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("first open");
        let mut created = first
            .open_file("database.db", FileOpenFlags::create())
            .await
            .expect("create file");
        created.write(b"persisted").await.expect("write file");
        created.close().await.expect("close file");
        drop(created);
        first.close().await.expect("first close");

        let mut second = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("second open");
        let mut reopened = second
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .expect("reopen file");
        let mut contents = vec![0; "persisted".len()];
        assert_eq!(
            reopened.read(&mut contents).await.expect("read file"),
            contents.len()
        );
        assert_eq!(contents, b"persisted");
        reopened.close().await.expect("close file");
        drop(reopened);
        second.close().await.expect("second close");
    }

    #[tokio::test]
    async fn opening_missing_file_without_create_fails() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let error = fs
            .open_file("missing.db", FileOpenFlags::read_only())
            .await
            .expect_err("missing file should fail");
        assert!(matches!(error, Error::FileNotFound(_)));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn file_exists_uses_path_mapping_only() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        assert!(!fs.file_exists("database.db").await.expect("missing lookup"));

        let mut file = fs
            .open_file("database.db", FileOpenFlags::create())
            .await
            .expect("create file");
        let file_id = file.file_id();
        file.close().await.expect("close file");
        drop(file);

        assert!(fs
            .file_exists("database.db")
            .await
            .expect("existing lookup"));

        let mut batch = WriteBatch::new();
        batch.delete(keys::metadata_key(file_id));
        fs.db
            .as_ref()
            .expect("open database")
            .write(batch)
            .await
            .expect("delete metadata");

        assert!(fs.file_exists("database.db").await.expect("path lookup"));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn remove_file_deletes_catalog_metadata_and_chunks() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        let mut file = fs
            .open_file("database.db", FileOpenFlags::create())
            .await
            .expect("create file");
        let file_id = file.file_id();
        file.write(b"contents").await.expect("write file");
        file.close().await.expect("close file");
        drop(file);

        fs.remove_file("database.db").await.expect("remove file");

        assert!(!fs.file_exists("database.db").await.expect("path lookup"));
        let db = fs.db.as_ref().expect("open database");
        assert!(db
            .get(keys::metadata_key(file_id))
            .await
            .expect("read metadata")
            .is_none());
        assert!(db
            .scan_prefix(keys::chunk_prefix(file_id), ..)
            .await
            .expect("scan chunks")
            .next()
            .await
            .expect("read chunk")
            .is_none());
        assert!(matches!(
            fs.remove_file("database.db").await,
            Err(Error::FileNotFound(_))
        ));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn move_file_preserves_source_and_replaces_destination() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let mut source = fs
            .open_file("source.db", FileOpenFlags::create())
            .await
            .expect("create source");
        let source_file_id = source.file_id();
        source.write(b"source").await.expect("write source");
        source.close().await.expect("close source");
        drop(source);

        let mut target = fs
            .open_file("target.db", FileOpenFlags::create())
            .await
            .expect("create target");
        let replaced_file_id = target.file_id();
        target.write(b"target").await.expect("write target");
        target.close().await.expect("close target");
        drop(target);

        fs.move_file("source.db", "target.db")
            .await
            .expect("move file");

        assert!(!fs.file_exists("source.db").await.expect("source lookup"));
        let mut moved = fs
            .open_file("target.db", FileOpenFlags::read_only())
            .await
            .expect("open moved file");
        assert_eq!(moved.file_id(), source_file_id);
        let mut contents = [0; 6];
        assert_eq!(
            moved.read(&mut contents).await.expect("read moved file"),
            contents.len()
        );
        assert_eq!(&contents, b"source");
        moved.close().await.expect("close moved file");
        drop(moved);

        let db = fs.db.as_ref().expect("open database");
        assert!(db
            .get(keys::metadata_key(replaced_file_id))
            .await
            .expect("read replaced metadata")
            .is_none());
        assert!(db
            .scan_prefix(keys::chunk_prefix(replaced_file_id), ..)
            .await
            .expect("scan replaced chunks")
            .next()
            .await
            .expect("read replaced chunk")
            .is_none());

        fs.close().await.expect("close");
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
