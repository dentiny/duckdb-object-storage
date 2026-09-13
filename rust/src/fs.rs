use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::services::{Fs, Memory};
use opendal::Operator;
use slatedb::{Db, IsolationLevel};

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_handle::SlateFileHandle;
use crate::flags::FileOpenFlags;
use crate::keys::{metadata_key, next_file_id_key, path_key};
use crate::metadata::FileMetadata;

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
        if path.is_empty() {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "file path must not be empty".to_string(),
                ErrorStatus::Permanent,
            )));
        }
        flags.validate()?;

        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        let path_key = path_key(path);
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;

        if let Some(file_id_bytes) = transaction.get(&path_key).await? {
            let file_id = decode_file_id(&file_id_bytes, "path mapping")?;
            let metadata = transaction
                .get(metadata_key(file_id))
                .await?
                .ok_or_else(|| {
                    Error::MetadataDecode(ErrorStruct::new(
                        format!("metadata is missing for file_id {file_id}"),
                        ErrorStatus::Permanent,
                    ))
                })
                .and_then(|bytes| FileMetadata::decode_from_bytes(&bytes))?;
            drop(transaction);

            return SlateFileHandle::new(db, file_id, metadata, flags);
        }

        if !flags.create {
            return Err(Error::FileNotFound(ErrorStruct::new(
                format!("file not found: {path}"),
                ErrorStatus::Permanent,
            )));
        }

        let next_file_id_key = next_file_id_key();
        let file_id = match transaction.get(&next_file_id_key).await? {
            Some(bytes) => decode_file_id(&bytes, "next file ID")?,
            None => 1,
        };
        let next_file_id = file_id.checked_add(1).ok_or_else(|| {
            Error::MetadataDecode(ErrorStruct::new(
                "file ID space is exhausted".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;
        let metadata = FileMetadata::new();

        transaction.put(&path_key, file_id.to_be_bytes())?;
        transaction.put(metadata_key(file_id), metadata.encode_to_bytes())?;
        transaction.put(next_file_id_key, next_file_id.to_be_bytes())?;
        transaction.commit().await?;

        SlateFileHandle::new(db, file_id, metadata, flags)
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

fn decode_file_id(bytes: &[u8], record: &str) -> Result<u64> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::MetadataDecode(ErrorStruct::new(
            format!("{record} must contain an 8-byte file ID"),
            ErrorStatus::Permanent,
        ))
    })?;
    let file_id = u64::from_be_bytes(encoded);
    if file_id == 0 {
        return Err(Error::MetadataDecode(ErrorStruct::new(
            format!("{record} contains reserved file ID 0"),
            ErrorStatus::Permanent,
        )));
    }
    Ok(file_id)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::file_handle::FileHandle;

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
