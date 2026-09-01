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
use crate::file_handle::{FileHandle, SlateFileHandle};
use crate::flags::FileOpenFlags;
use crate::keys;
use crate::metadata::FileMetadata;

/// The filesystem-level operations DuckDB's `FileSystem` needs from SlateFS.
pub trait FileSystem {
    /// Returns whether a file exists at `path`.
    fn file_exists(&self, path: &str) -> Result<bool>;

    /// Opens the file at `path`, creating it when `flags` allow and nothing is
    /// mapped there yet.
    fn open_file(&self, path: &str, flags: FileOpenFlags) -> Result<Box<dyn FileHandle>>;

    /// Deletes the file at `path` along with its metadata and chunks.
    fn remove_file(&self, path: &str) -> Result<()>;

    /// Renames `src` to `dst`, replacing whatever `dst` named. The file keeps
    /// its id, so no data moves.
    fn move_file(&self, src: &str, dst: &str) -> Result<()>;
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

    fn open_file(&self, path: &str, flags: FileOpenFlags) -> Result<Box<dyn FileHandle>> {
        validate_path(path, "file")?;

        // The file is resolved inside the runtime, but the handle is built
        // outside it. A handle drives its own blocking calls on this same
        // runtime, and opening with `truncate_existing` makes one straight
        // away, which would be a `block_on` nested inside this one.
        let (file_id, metadata) = self.runtime.block_on(self.resolve_file(path, flags))?;

        Ok(Box::new(SlateFileHandle::new(
            Arc::clone(&self.db),
            Arc::clone(&self.runtime),
            file_id,
            metadata,
            flags,
        )?))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        validate_path(path, "file")?;
        self.runtime.block_on(self.remove_file_impl(path))
    }

    fn move_file(&self, src: &str, dst: &str) -> Result<()> {
        validate_path(src, "source")?;
        validate_path(dst, "destination")?;

        if src == dst {
            return Ok(());
        }

        self.runtime.block_on(self.move_file_impl(src, dst))
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

    /// Resolves the path to a file and its metadata. When creating, the path
    /// mapping, the metadata record and the bumped id counter are written in
    /// one transaction, so a crash cannot leave a path pointing at nothing.
    async fn resolve_file(&self, path: &str, flags: FileOpenFlags) -> Result<(u64, FileMetadata)> {
        let txn = self.begin_txn().await?;

        let resolved = match Self::lookup_file_id(&txn, path).await? {
            Some(file_id) => {
                let bytes = txn.get(keys::metadata_key(file_id)).await?.ok_or_else(|| {
                    Error::MetadataDecode(ErrorStruct::new(
                        format!("metadata missing for file_id {file_id} at {path}"),
                        ErrorStatus::Permanent,
                    ))
                })?;
                (file_id, FileMetadata::decode_from_bytes(&bytes)?)
            }
            None => {
                if !flags.create {
                    return Err(Error::FileNotFound(ErrorStruct::new(
                        format!("file not found: {path}"),
                        ErrorStatus::Permanent,
                    )));
                }
                self.create_file(&txn, path).await?
            }
        };

        txn.commit().await?;
        Ok(resolved)
    }

    async fn remove_file_impl(&self, path: &str) -> Result<()> {
        let txn = self.begin_txn().await?;

        let file_id = Self::lookup_file_id(&txn, path).await?.ok_or_else(|| {
            Error::FileNotFound(ErrorStruct::new(
                format!("file not found: {path}"),
                ErrorStatus::Permanent,
            ))
        })?;

        Self::delete_file_data(&txn, file_id).await?;
        txn.delete(keys::path_key(path))?;
        txn.commit().await?;
        Ok(())
    }

    async fn move_file_impl(&self, src: &str, dst: &str) -> Result<()> {
        let txn = self.begin_txn().await?;

        let src_file_id = Self::lookup_file_id(&txn, src).await?.ok_or_else(|| {
            Error::FileNotFound(ErrorStruct::new(
                format!("file not found: {src}"),
                ErrorStatus::Permanent,
            ))
        })?;

        // The destination is about to stop naming its current file, so its
        // data goes with it; nothing else refers to that id.
        let replaced = Self::lookup_file_id(&txn, dst)
            .await?
            .filter(|dst_file_id| *dst_file_id != src_file_id);
        if let Some(dst_file_id) = replaced {
            Self::delete_file_data(&txn, dst_file_id).await?;
        }

        txn.put(keys::path_key(dst), src_file_id.to_le_bytes())?;
        txn.delete(keys::path_key(src))?;
        txn.commit().await?;
        Ok(())
    }

    /// Stages the deletion of a file's metadata and every one of its chunks.
    ///
    /// The chunks are scanned rather than derived from the recorded size, so a
    /// sparse file with gaps in its indices leaves nothing behind.
    async fn delete_file_data(txn: &DbTransaction, file_id: u64) -> Result<()> {
        let mut chunks = txn.scan_prefix(keys::chunk_prefix(file_id), ..).await?;
        while let Some(entry) = chunks.next().await? {
            txn.delete(entry.key)?;
        }

        txn.delete(keys::metadata_key(file_id))?;
        Ok(())
    }

    /// Stages the creation of an empty file at `path` in `txn`.
    async fn create_file(&self, txn: &DbTransaction, path: &str) -> Result<(u64, FileMetadata)> {
        let counter_key = keys::next_file_id_key();
        let file_id = match txn.get(&counter_key).await? {
            Some(bytes) => decode_file_id(&bytes, "next file id")?,
            None => 1,
        };
        let next_file_id = file_id.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "file ids are exhausted".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;
        let metadata = FileMetadata::new();

        txn.put(&counter_key, next_file_id.to_le_bytes())?;
        txn.put(keys::path_key(path), file_id.to_le_bytes())?;
        txn.put(keys::metadata_key(file_id), metadata.encode_to_bytes())?;

        Ok((file_id, metadata))
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

    #[test]
    fn opening_a_new_file_allocates_incrementing_ids() {
        let fs = open_fs();

        let first = fs
            .open_file("a.db", FileOpenFlags::create())
            .expect("open a.db");
        let second = fs
            .open_file("b.db", FileOpenFlags::create())
            .expect("open b.db");

        assert_eq!(first.file_id(), 1);
        assert_eq!(second.file_id(), 2);
        assert!(fs.file_exists("a.db").expect("file_exists"));
        assert_eq!(first.file_size(), 0);

        drop((first, second));
        fs.close().expect("close");
    }

    #[test]
    fn reopening_a_file_finds_the_same_id_and_contents() {
        let fs = open_fs();
        let mut created = fs
            .open_file("duck.db", FileOpenFlags::create())
            .expect("create");
        created.write(b"quack").expect("write");
        created.sync().expect("sync");
        let created_id = created.file_id();
        drop(created);

        let mut reopened = fs
            .open_file("duck.db", FileOpenFlags::read_only())
            .expect("reopen");

        assert_eq!(reopened.file_id(), created_id);
        assert_eq!(reopened.file_size(), 5);
        let mut buf = [0u8; 5];
        assert_eq!(reopened.read(&mut buf).expect("read"), 5);
        assert_eq!(&buf, b"quack");

        drop(reopened);
        fs.close().expect("close");
    }

    #[test]
    fn opening_a_missing_file_without_create_fails() {
        let fs = open_fs();

        let error = fs
            .open_file("ghost.db", FileOpenFlags::read_only())
            .expect_err("file should not be found");

        assert!(matches!(error, Error::FileNotFound(_)));
        assert!(!fs.file_exists("ghost.db").expect("file_exists"));
        fs.close().expect("close");
    }

    #[test]
    fn opening_with_truncate_existing_empties_the_file() {
        let fs = open_fs();
        let mut created = fs
            .open_file("duck.db", FileOpenFlags::create())
            .expect("create");
        created.write(b"quack").expect("write");
        created.sync().expect("sync");
        drop(created);

        let mut truncated = fs
            .open_file(
                "duck.db",
                FileOpenFlags {
                    truncate_existing: true,
                    ..FileOpenFlags::read_write()
                },
            )
            .expect("reopen");
        truncated.sync().expect("sync");

        assert_eq!(truncated.file_size(), 0);
        drop(truncated);
        fs.close().expect("close");
    }

    #[test]
    fn a_path_pointing_at_missing_metadata_is_reported_not_recreated() {
        let fs = open_fs();
        fs.runtime
            .block_on(fs.db.put(keys::path_key("duck.db"), 9u64.to_le_bytes()))
            .expect("put");

        let error = fs
            .open_file("duck.db", FileOpenFlags::create())
            .expect_err("metadata is missing");

        assert!(matches!(error, Error::MetadataDecode(_)));
        assert!(error.to_string().contains("metadata missing for file_id 9"));
        fs.close().expect("close");
    }

    #[test]
    fn open_file_rejects_an_empty_path() {
        let fs = open_fs();

        let error = fs
            .open_file("", FileOpenFlags::create())
            .expect_err("empty path");

        assert!(error.to_string().contains("file path must not be empty"));
        fs.close().expect("close");
    }

    /// Every key stored for `file_id`, so a test can assert nothing is left.
    fn stored_keys(fs: &SlateFs, file_id: u64) -> Vec<Vec<u8>> {
        fs.runtime.block_on(async {
            let mut keys = Vec::new();
            for prefix in [keys::chunk_prefix(file_id), keys::metadata_key(file_id)] {
                let mut iter = fs.db.scan_prefix(prefix, ..).await.expect("scan");
                while let Some(entry) = iter.next().await.expect("next") {
                    keys.push(entry.key.to_vec());
                }
            }
            keys
        })
    }

    fn write_file(fs: &SlateFs, path: &str, contents: &[u8]) -> u64 {
        let mut handle = fs.open_file(path, FileOpenFlags::create()).expect("create");
        handle.write(contents).expect("write");
        handle.sync().expect("sync");
        handle.file_id()
    }

    #[test]
    fn removing_a_file_leaves_none_of_it_behind() {
        let fs = open_fs();
        let file_id = write_file(&fs, "duck.db", b"quack");

        fs.remove_file("duck.db").expect("remove");

        assert!(!fs.file_exists("duck.db").expect("file_exists"));
        assert!(stored_keys(&fs, file_id).is_empty());
        fs.close().expect("close");
    }

    #[test]
    fn removing_a_file_frees_its_path_for_a_new_one() {
        let fs = open_fs();
        let first_id = write_file(&fs, "duck.db", b"quack");
        fs.remove_file("duck.db").expect("remove");

        let recreated = fs
            .open_file("duck.db", FileOpenFlags::create())
            .expect("recreate");

        // A fresh id, so the recreated file cannot inherit stale chunks.
        assert_ne!(recreated.file_id(), first_id);
        assert_eq!(recreated.file_size(), 0);
        drop(recreated);
        fs.close().expect("close");
    }

    #[test]
    fn removing_a_missing_file_fails() {
        let fs = open_fs();

        let error = fs
            .remove_file("ghost.db")
            .expect_err("file should not exist");

        assert!(matches!(error, Error::FileNotFound(_)));
        fs.close().expect("close");
    }

    #[test]
    fn moving_a_file_rebinds_the_path_without_moving_data() {
        let fs = open_fs();
        let file_id = write_file(&fs, "src.db", b"quack");

        fs.move_file("src.db", "dst.db").expect("move");

        assert!(!fs.file_exists("src.db").expect("file_exists"));
        let mut moved = fs
            .open_file("dst.db", FileOpenFlags::read_only())
            .expect("open");
        assert_eq!(moved.file_id(), file_id);
        let mut buf = [0u8; 5];
        assert_eq!(moved.read(&mut buf).expect("read"), 5);
        assert_eq!(&buf, b"quack");

        drop(moved);
        fs.close().expect("close");
    }

    #[test]
    fn moving_onto_an_existing_file_replaces_it_and_frees_its_data() {
        let fs = open_fs();
        let src_id = write_file(&fs, "src.db", b"src");
        let dst_id = write_file(&fs, "dst.db", b"destination");

        fs.move_file("src.db", "dst.db").expect("move");

        assert!(stored_keys(&fs, dst_id).is_empty(), "replaced file is gone");
        let moved = fs
            .open_file("dst.db", FileOpenFlags::read_only())
            .expect("open");
        assert_eq!(moved.file_id(), src_id);
        // The destination's longer contents did not survive under the new id.
        assert_eq!(moved.file_size(), 3);

        drop(moved);
        fs.close().expect("close");
    }

    #[test]
    fn moving_a_file_onto_itself_keeps_it() {
        let fs = open_fs();
        let file_id = write_file(&fs, "duck.db", b"quack");

        fs.move_file("duck.db", "duck.db").expect("move");

        assert!(fs.file_exists("duck.db").expect("file_exists"));
        assert!(!stored_keys(&fs, file_id).is_empty());
        fs.close().expect("close");
    }

    #[test]
    fn moving_a_missing_file_fails_and_leaves_the_destination_alone() {
        let fs = open_fs();
        write_file(&fs, "dst.db", b"destination");

        let error = fs
            .move_file("ghost.db", "dst.db")
            .expect_err("source should not exist");

        assert!(matches!(error, Error::FileNotFound(_)));
        assert!(fs.file_exists("dst.db").expect("file_exists"));
        fs.close().expect("close");
    }

    #[test]
    fn move_file_rejects_empty_paths() {
        let fs = open_fs();
        write_file(&fs, "src.db", b"src");

        let error = fs.move_file("src.db", "").expect_err("empty destination");
        assert!(error
            .to_string()
            .contains("destination path must not be empty"));

        let error = fs.move_file("", "dst.db").expect_err("empty source");
        assert!(error.to_string().contains("source path must not be empty"));

        fs.close().expect("close");
    }
}
