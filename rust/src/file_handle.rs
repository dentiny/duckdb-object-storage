//! Handle for one open SlateFS file.
//!
//! A file is stored as a metadata record plus fixed-size chunks, addressed by
//! the keys in [`crate::keys`].

use std::fmt::Debug;
use std::sync::Arc;

use slatedb::Db;
use tokio::runtime::Runtime;

use crate::error::Result;
use crate::flags::FileOpenFlags;
use crate::metadata::FileMetadata;

/// The operations DuckDB's `FileHandle` needs from a SlateFS file.
pub trait FileHandle: Debug {
    /// Sets the file position.
    fn seek(&mut self, position: u64);

    /// Returns the current file position.
    fn seek_position(&self) -> u64;

    /// Resets the file position to 0.
    fn reset(&mut self);

    /// Returns the file size, including writes that have not been flushed.
    fn file_size(&self) -> u64;

    /// Returns the last modification timestamp, in epoch milliseconds.
    fn last_modified_time(&self) -> u64;

    /// Returns the id the file's chunks and metadata are keyed by.
    fn file_id(&self) -> u64;

    /// Returns the flags the file was opened with.
    fn flags(&self) -> FileOpenFlags;
}

/// A file opened against a SlateDB instance.
pub struct SlateFileHandle {
    db: Arc<Db>,
    /// Async runtime, so the blocking calls DuckDB makes can drive SlateDB.
    runtime: Arc<Runtime>,
    file_id: u64,
    position: u64,
    /// File size including buffered writes, which is what readers must see.
    size: u64,
    modified_at_ms: u64,
    chunk_size: usize,
    flags: FileOpenFlags,
}

impl SlateFileHandle {
    pub(crate) fn new(
        db: Arc<Db>,
        runtime: Arc<Runtime>,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        flags.validate(file_id)?;
        let chunk_size = metadata.validated_chunk_size(file_id)?;

        Ok(Self {
            db,
            runtime,
            file_id,
            position: 0,
            size: metadata.size,
            modified_at_ms: metadata.modified_at_ms,
            chunk_size,
            flags,
        })
    }
}

impl Debug for SlateFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateFileHandle")
            .field("file_id", &self.file_id)
            .field("position", &self.position)
            .field("size", &self.size)
            .field("modified_at_ms", &self.modified_at_ms)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl FileHandle for SlateFileHandle {
    fn seek(&mut self, position: u64) {
        self.position = position;
    }

    fn seek_position(&self) -> u64 {
        self.position
    }

    fn reset(&mut self) {
        self.position = 0;
    }

    fn file_size(&self) -> u64 {
        self.size
    }

    fn last_modified_time(&self) -> u64 {
        self.modified_at_ms
    }

    fn file_id(&self) -> u64 {
        self.file_id
    }

    fn flags(&self) -> FileOpenFlags {
        self.flags
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::Error;
    use crate::metadata::DEFAULT_CHUNK_SIZE;
    use crate::test_utils::TestDb;

    const FILE_ID: u64 = 7;

    /// Borrowed, not created here: dropping the fixture closes the database.
    fn open(
        fixture: &TestDb,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<SlateFileHandle> {
        SlateFileHandle::new(
            Arc::clone(&fixture.db),
            Arc::clone(&fixture.runtime),
            FILE_ID,
            metadata,
            flags,
        )
    }

    #[test]
    fn new_handle_reports_the_stored_metadata_and_starts_at_offset_zero() {
        let metadata = FileMetadata {
            size: 4096,
            modified_at_ms: 1_234_567_890,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };

        let handle = open(&TestDb::new(), metadata, FileOpenFlags::read_only()).expect("handle");

        assert_eq!(handle.seek_position(), 0);
        assert_eq!(handle.file_size(), 4096);
        assert_eq!(handle.last_modified_time(), 1_234_567_890);
        assert_eq!(handle.file_id(), FILE_ID);
        assert_eq!(handle.flags(), FileOpenFlags::read_only());
    }

    #[test]
    fn seek_and_reset_move_the_position() {
        let mut handle =
            open(&TestDb::new(), FileMetadata::new(), FileOpenFlags::create()).expect("handle");

        handle.seek(1234);
        assert_eq!(handle.seek_position(), 1234);

        handle.reset();
        assert_eq!(handle.seek_position(), 0);
    }

    #[test]
    fn contradictory_flags_are_rejected() {
        let flags = FileOpenFlags {
            read: false,
            write: false,
            create: false,
            truncate_existing: false,
        };

        let error =
            open(&TestDb::new(), FileMetadata::new(), flags).expect_err("flags should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("at least one of read or write"));
    }
}
