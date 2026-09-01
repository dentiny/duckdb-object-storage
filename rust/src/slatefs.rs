//! Filesystem-level operations over a SlateDB instance.
//!
//! A logical file is three key families: a path mapping `p/<path>` naming a
//! file id, a metadata record `m/<file_id>`, and the chunks `c/<file_id>/...`
//! that hold its bytes. Indirecting through a file id is what lets a rename
//! rebind a path without touching the data.

use std::sync::Arc;

use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel};
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::keys;

/// The filesystem-level operations DuckDB's `FileSystem` needs from SlateFS.
pub trait FileSystem {
    /// Returns whether a file exists at `path`.
    fn file_exists(&self, path: &str) -> Result<bool>;
}

/// A filesystem backed by one SlateDB instance on an object store.
pub struct SlateFs {
    db: Arc<Db>,
    /// Async runtime, so the blocking calls DuckDB makes can drive SlateDB.
    runtime: Arc<Runtime>,
}

impl FileSystem for SlateFs {
    fn file_exists(&self, path: &str) -> Result<bool> {
        validate_path(path, "file")?;

        self.runtime
            .block_on(async { Ok(self.db.get(keys::path_key(path)).await?.is_some()) })
    }
}

impl SlateFs {
    /// Opens the SlateDB instance rooted at `path` on `object_store`.
    pub fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        validate_path(path, "database")?;

        let runtime = Arc::new(Runtime::new().map_err(|source| {
            Error::Io(
                ErrorStruct::new(
                    "failed to create tokio runtime".to_string(),
                    ErrorStatus::Permanent,
                )
                .with_source(source),
            )
        })?);
        let db = runtime.block_on(Db::open(path, object_store))?;

        Ok(Self {
            db: Arc::new(db),
            runtime,
        })
    }

    /// Closes the database.
    ///
    /// Open file handles hold a reference to it, so this fails rather than
    /// closing a database that a handle is still writing through.
    pub fn close(self) -> Result<()> {
        let Self { db, runtime } = self;

        let db = Arc::try_unwrap(db).map_err(|_| {
            Error::InvalidArgument(ErrorStruct::new(
                "cannot close: file handles are still open".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;

        runtime.block_on(async { db.close().await })?;
        Ok(())
    }

    /// Starts a snapshot-isolated transaction, so a multi-key change either
    /// lands whole or not at all.
    async fn begin_txn(&self) -> Result<DbTransaction> {
        Ok(self.db.begin(IsolationLevel::Snapshot).await?)
    }

    /// Resolves `path` to a file id, or `None` if nothing is mapped there.
    async fn lookup_file_id(txn: &DbTransaction, path: &str) -> Result<Option<u64>> {
        match txn.get(keys::path_key(path)).await? {
            Some(bytes) => Ok(Some(decode_file_id(&bytes, "path mapping")?)),
            None => Ok(None),
        }
    }
}

fn validate_path(path: &str, kind: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::InvalidArgument(ErrorStruct::new(
            format!("{kind} path must not be empty"),
            ErrorStatus::Permanent,
        )));
    }
    Ok(())
}

fn decode_file_id(bytes: &[u8], field: &str) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::MetadataDecode(ErrorStruct::new(
            format!(
                "invalid {field} payload: expected 8 bytes, found {}",
                bytes.len()
            ),
            ErrorStatus::Permanent,
        ))
    })?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use uuid::Uuid;

    use super::*;

    fn open_fs() -> SlateFs {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        SlateFs::open(&format!("/slatefs-{}", Uuid::new_v4()), store).expect("filesystem")
    }

    #[test]
    fn empty_paths_are_rejected() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let Err(error) = SlateFs::open("", store) else {
            panic!("an empty database path should be rejected");
        };
        assert!(error
            .to_string()
            .contains("database path must not be empty"));

        let fs = open_fs();
        let error = fs.file_exists("").expect_err("empty file path");
        assert!(error.to_string().contains("file path must not be empty"));

        fs.close().expect("close");
    }

    #[test]
    fn file_exists_follows_the_path_mapping() {
        let fs = open_fs();
        assert!(!fs.file_exists("duck.db").expect("file_exists"));

        fs.runtime
            .block_on(fs.db.put(keys::path_key("duck.db"), 1u64.to_le_bytes()))
            .expect("put");

        assert!(fs.file_exists("duck.db").expect("file_exists"));
        fs.close().expect("close");
    }

    #[test]
    fn file_ids_round_trip_through_their_stored_form() {
        assert_eq!(
            decode_file_id(&7u64.to_le_bytes(), "path mapping").expect("file id"),
            7
        );

        let error = decode_file_id(&[1, 2, 3], "path mapping").expect_err("short payload");
        assert!(matches!(error, Error::MetadataDecode(_)));
        assert!(error.to_string().contains("expected 8 bytes, found 3"));
    }
}
