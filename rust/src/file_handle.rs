//! Handle-level I/O matching DuckDB's `FileHandle`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::{Db, DbReader, WriteBatch};

use crate::chunk_manager::ChunkManager;
use crate::chunk_store::{ChunkStore, SlateDbChunkStore, SlateDbReaderChunkStore};
use crate::error::{Error, Result};
use crate::file_metadata::FileMetadata;
use crate::flags::FileOpenFlags;
use crate::keys;
use crate::util::current_time_millis;

/// Sequential and positional I/O, matching DuckDB's `FileHandle` methods.
#[async_trait]
pub trait FileHandle: Debug + Send + Sync {
    /// Read at `offset` without moving the file position.
    async fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write at `offset` without moving the file position.
    async fn pwrite(&mut self, data: &[u8], offset: u64) -> Result<()>;

    /// Read at the current position and advance it.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Write at the current position and advance it.
    async fn write(&mut self, data: &[u8]) -> Result<usize>;

    /// Persist dirty chunks and metadata.
    async fn sync(&mut self) -> Result<()>;

    /// Set the file position.
    fn seek(&mut self, position: u64);

    /// Return the current file position.
    fn seek_position(&self) -> u64;

    /// Reset the file position to 0.
    fn reset(&mut self);

    /// Return the logical size, including unflushed writes.
    fn file_size(&self) -> u64;

    /// Shrink or grow the file to `new_size` bytes.
    async fn truncate(&mut self, new_size: u64) -> Result<()>;

    /// Last modification time in epoch milliseconds.
    fn get_last_modified_time(&self) -> u64;

    /// Flush dirty state. Calling `close` again is harmless.
    async fn close(&mut self) -> Result<()>;

    fn file_id(&self) -> u64;
    fn flags(&self) -> FileOpenFlags;
}

pub(crate) enum SlateFileClient {
    ReadWrite(Arc<Db>),
    ReadOnly(Arc<DbReader>),
}

impl SlateFileClient {
    fn chunk_store(&self) -> Arc<dyn ChunkStore> {
        match self {
            SlateFileClient::ReadWrite(db) => Arc::new(SlateDbChunkStore::new(Arc::clone(db))),
            SlateFileClient::ReadOnly(reader) => {
                Arc::new(SlateDbReaderChunkStore::new(Arc::clone(reader)))
            }
        }
    }

    fn writer(&self, file_id: u64) -> Result<&Db> {
        match self {
            SlateFileClient::ReadWrite(db) => Ok(db),
            SlateFileClient::ReadOnly(_) => Err(Error::read_only_violation(format!(
                "cannot sync file_id {file_id} through a read-only SlateDB client"
            ))),
        }
    }
}

pub struct SlateFileHandle {
    client: SlateFileClient,
    file_id: u64,
    position: u64,
    size: u64,
    modified_at_ms: u64,
    chunks: ChunkManager,
    metadata_dirty: bool,
    flags: FileOpenFlags,
}

impl SlateFileHandle {
    /// Open a handle over an existing metadata record.
    pub(crate) fn new(
        client: SlateFileClient,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        let store = client.chunk_store();
        Self::with_chunk_store_internal(client, store, file_id, metadata, flags)
    }

    /// Open a handle whose chunks are read through `store`; `db` still carries
    /// the metadata writes. Tests pass a store that fails on demand.
    #[cfg(test)]
    pub(crate) fn with_chunk_store(
        db: Arc<Db>,
        store: Arc<dyn ChunkStore>,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        Self::with_chunk_store_internal(
            SlateFileClient::ReadWrite(db),
            store,
            file_id,
            metadata,
            flags,
        )
    }

    fn with_chunk_store_internal(
        client: SlateFileClient,
        store: Arc<dyn ChunkStore>,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        flags.validate()?;
        if matches!(&client, SlateFileClient::ReadOnly(_)) {
            flags.ensure_readable(file_id)?;
            if flags.write {
                return Err(Error::read_only_violation(
                    "read-only SlateDB filesystem cannot create a writable file handle",
                ));
            }
        }
        let chunk_size = metadata.validated_chunk_size(file_id)?;
        let position = if flags.append { metadata.size } else { 0 };

        Ok(Self {
            client,
            file_id,
            position,
            size: metadata.size,
            modified_at_ms: metadata.modified_at_ms,
            chunks: ChunkManager::new(store, file_id, chunk_size),
            metadata_dirty: false,
            flags,
        })
    }

    fn set_metadata_dirty(&mut self) {
        self.modified_at_ms = current_time_millis();
        self.metadata_dirty = true;
    }

    fn metadata(&self) -> FileMetadata {
        FileMetadata {
            size: self.size,
            modified_at_ms: self.modified_at_ms,
            chunk_size: self.chunks.chunk_size(),
        }
    }

    fn write_overflow(&self) -> Error {
        Error::invalid_argument(format!("write overflow for file_id {}", self.file_id))
    }
}

impl Debug for SlateFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateFileHandle")
            .field("file_id", &self.file_id)
            .field("position", &self.position)
            .field("size", &self.size)
            .field("modified_at_ms", &self.modified_at_ms)
            .field("chunks", &self.chunks)
            .finish()
    }
}

#[async_trait]
impl FileHandle for SlateFileHandle {
    async fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.flags.ensure_readable(self.file_id)?;

        if buf.is_empty() || offset >= self.size {
            return Ok(0);
        }

        // Never read past the end of the file.
        let bytes_to_read = usize::try_from(self.size - offset)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        self.chunks.read(&mut buf[..bytes_to_read], offset).await?;
        Ok(bytes_to_read)
    }

    async fn pwrite(&mut self, data: &[u8], offset: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)?;

        if data.is_empty() {
            return Ok(());
        }

        let end_offset = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| self.write_overflow())?;

        self.chunks.write(data, offset).await?;

        if end_offset > self.size {
            self.size = end_offset;
        }
        self.set_metadata_dirty();
        Ok(())
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let available = usize::try_from(self.size.saturating_sub(self.position))
            .unwrap()
            .min(buf.len());

        if available == 0 {
            return Ok(0);
        }

        let bytes_read = self.pread(&mut buf[..available], self.position).await?;
        self.position += bytes_read as u64;
        Ok(bytes_read)
    }

    async fn write(&mut self, data: &[u8]) -> Result<usize> {
        let offset = if self.flags.append {
            self.size
        } else {
            self.position
        };
        self.pwrite(data, offset).await?;
        self.position = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| self.write_overflow())?;
        Ok(data.len())
    }

    async fn sync(&mut self) -> Result<()> {
        if !self.chunks.has_pending_changes() && !self.metadata_dirty {
            return Ok(());
        }

        // One batch, so a reader never sees a size its chunks do not back.
        let mut batch = WriteBatch::new();
        self.chunks.add_pending_to_batch(&mut batch);
        batch.put(
            keys::metadata_key(self.file_id),
            self.metadata().encode_to_bytes(),
        );

        let db = self.client.writer(self.file_id)?;
        db.write(batch).await?;
        db.flush().await?;

        self.chunks.mark_flushed();
        self.metadata_dirty = false;
        Ok(())
    }

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

    async fn truncate(&mut self, new_size: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)?;

        if new_size == self.size {
            return Ok(());
        }

        // Growing needs no chunk work: the new bytes read as zeroes.
        if new_size < self.size {
            self.chunks.truncate(new_size).await?;
        }

        self.size = new_size;
        self.set_metadata_dirty();
        Ok(())
    }

    fn get_last_modified_time(&self) -> u64 {
        self.modified_at_ms
    }

    async fn close(&mut self) -> Result<()> {
        self.sync().await
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

    use object_store_opendal::OpendalStore;
    use opendal::services::Memory;
    use opendal::Operator;
    use slatedb::Db;

    use super::*;
    use crate::chunk_store::FaultyChunkStore;
    use crate::file_metadata::FileMetadata;

    const SMALL_CHUNK: u64 = 8;

    fn new_metadata() -> FileMetadata {
        FileMetadata {
            size: 0,
            modified_at_ms: 1,
            chunk_size: SMALL_CHUNK,
        }
    }

    struct TestDb {
        db: Arc<Db>,
    }

    impl TestDb {
        async fn new() -> Self {
            let operator =
                Operator::new(Memory::default()).expect("create OpenDAL memory operator");
            let store = Arc::new(OpendalStore::new(operator));
            let db = Db::builder("handle-test", store)
                .build()
                .await
                .expect("open slatedb");
            Self { db: Arc::new(db) }
        }

        fn handle(&self, file_id: u64, flags: FileOpenFlags) -> SlateFileHandle {
            SlateFileHandle::new(
                SlateFileClient::ReadWrite(Arc::clone(&self.db)),
                file_id,
                new_metadata(),
                flags,
            )
            .unwrap()
        }

        /// A handle whose chunk reads can be made to fail.
        fn faulty_handle(
            &self,
            file_id: u64,
            flags: FileOpenFlags,
        ) -> (SlateFileHandle, Arc<FaultyChunkStore>) {
            let store = Arc::new(FaultyChunkStore::new(Arc::clone(&self.db)));
            let handle = SlateFileHandle::with_chunk_store(
                Arc::clone(&self.db),
                Arc::clone(&store) as Arc<dyn ChunkStore>,
                file_id,
                new_metadata(),
                flags,
            )
            .unwrap();
            (handle, store)
        }

        async fn reopen(&self, file_id: u64, flags: FileOpenFlags) -> SlateFileHandle {
            let bytes = self
                .db
                .get(keys::metadata_key(file_id))
                .await
                .unwrap()
                .expect("metadata");
            let metadata = FileMetadata::decode_from_bytes(&bytes).unwrap();
            SlateFileHandle::new(
                SlateFileClient::ReadWrite(Arc::clone(&self.db)),
                file_id,
                metadata,
                flags,
            )
            .unwrap()
        }

        async fn close(self) {
            self.db.close().await.expect("close slatedb");
        }
    }

    #[tokio::test]
    async fn sequential_io_tracks_position() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(1, FileOpenFlags::open_or_create());

        assert_eq!(handle.write(b"abcdef").await.unwrap(), 6);
        assert_eq!(handle.seek_position(), 6);

        handle.seek(2);
        let mut buf = [0u8; 3];
        assert_eq!(handle.read(&mut buf).await.unwrap(), 3);
        assert_eq!(&buf, b"cde");
        handle.reset();
        assert_eq!(handle.seek_position(), 0);

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn positional_reads_can_share_a_handle() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(11, FileOpenFlags::open_or_create());
        handle.pwrite(b"abcdefgh", 0).await.unwrap();
        handle.sync().await.unwrap();

        let mut first = [0; 4];
        let mut second = [0; 4];
        let (first_read, second_read) =
            tokio::join!(handle.pread(&mut first, 0), handle.pread(&mut second, 4));

        assert_eq!(first_read.unwrap(), first.len());
        assert_eq!(second_read.unwrap(), second.len());
        assert_eq!(&first, b"abcd");
        assert_eq!(&second, b"efgh");

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn writes_persist_across_chunks_and_reopen() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(2, FileOpenFlags::open_or_create());

        handle.pwrite(b"hello", 0).await.unwrap();
        handle.pwrite(b"xyz", SMALL_CHUNK - 1).await.unwrap();
        assert_eq!(handle.file_size(), SMALL_CHUNK + 2);
        handle.close().await.unwrap();
        drop(handle);

        let reopened = fixture.reopen(2, FileOpenFlags::read_only()).await;
        let mut boundary = [0u8; 3];
        reopened
            .pread(&mut boundary, SMALL_CHUNK - 1)
            .await
            .unwrap();
        assert_eq!(&boundary, b"xyz");

        drop(reopened);
        fixture.close().await;
    }

    #[tokio::test]
    async fn append_writes_at_end_after_reopen() {
        let fixture = TestDb::new().await;
        let mut writer = fixture.handle(6, FileOpenFlags::open_or_create());
        writer.write(b"abc").await.unwrap();
        writer.close().await.unwrap();
        drop(writer);

        let mut appender = fixture.reopen(6, FileOpenFlags::append()).await;
        assert_eq!(appender.seek_position(), 3);
        appender.seek(0);
        appender.write(b"def").await.unwrap();
        assert_eq!(appender.seek_position(), 6);
        appender.close().await.unwrap();
        drop(appender);

        let mut reader = fixture.reopen(6, FileOpenFlags::read_only()).await;
        let mut contents = [0; 6];
        assert_eq!(reader.read(&mut contents).await.unwrap(), contents.len());
        assert_eq!(&contents, b"abcdef");

        drop(reader);
        fixture.close().await;
    }

    #[tokio::test]
    async fn reads_stop_at_the_end_of_the_file() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(10, FileOpenFlags::open_or_create());

        handle.pwrite(b"abc", 0).await.unwrap();

        // The buffer outlives the file: only its own bytes are reported.
        let mut buf = [b'?'; 6];
        assert_eq!(handle.pread(&mut buf, 0).await.unwrap(), 3);
        assert_eq!(&buf, b"abc???");
        assert_eq!(handle.pread(&mut buf, 3).await.unwrap(), 0);

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn reads_span_persisted_and_pending_chunks() {
        let fixture = TestDb::new().await;
        let mut writer = fixture.handle(8, FileOpenFlags::open_or_create());
        writer
            .pwrite(b"aaaaaaaabbbbbbbbccccccccdddddddd", 0)
            .await
            .unwrap();
        writer.close().await.unwrap();
        drop(writer);

        // Chunk 2 is only pending, so the chunks either side form two ranges.
        let mut handle = fixture.reopen(8, FileOpenFlags::open_or_create()).await;
        handle.pwrite(b"CCCCCCCC", 2 * SMALL_CHUNK).await.unwrap();

        let mut buf = [0u8; 4 * SMALL_CHUNK as usize];
        assert_eq!(handle.pread(&mut buf, 0).await.unwrap(), buf.len());
        assert_eq!(&buf, b"aaaaaaaabbbbbbbbCCCCCCCCdddddddd");

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn truncate_hides_persisted_chunks_before_sync() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(9, FileOpenFlags::open_or_create());
        handle.pwrite(b"aaaaaaaabbbbbbbbcccccccc", 0).await.unwrap();
        handle.sync().await.unwrap();

        // Chunks 1 and 2 are persisted but pending deletion: a hole before flush.
        handle.truncate(SMALL_CHUNK).await.unwrap();
        handle.truncate(3 * SMALL_CHUNK).await.unwrap();
        let mut buf = [0u8; 3 * SMALL_CHUNK as usize];
        assert_eq!(handle.pread(&mut buf, 0).await.unwrap(), buf.len());
        assert_eq!(&buf[..SMALL_CHUNK as usize], b"aaaaaaaa");
        assert!(buf[SMALL_CHUNK as usize..].iter().all(|byte| *byte == 0));

        // Rewriting a chunk pending deletion replaces the delete.
        handle.pwrite(b"ZZZZZZZZ", SMALL_CHUNK).await.unwrap();
        handle.close().await.unwrap();
        drop(handle);

        let reopened = fixture.reopen(9, FileOpenFlags::read_only()).await;
        assert_eq!(reopened.pread(&mut buf, 0).await.unwrap(), buf.len());
        assert_eq!(&buf, b"aaaaaaaaZZZZZZZZ\0\0\0\0\0\0\0\0");

        drop(reopened);
        fixture.close().await;
    }

    #[tokio::test]
    async fn truncate_then_sparse_write_reads_as_zero() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(3, FileOpenFlags::open_or_create());

        handle.pwrite(b"abcdefghij", 0).await.unwrap();
        handle.sync().await.unwrap();
        handle.truncate(5).await.unwrap();
        handle.pwrite(b"Z", 8).await.unwrap();

        let mut buf = [0u8; 9];
        handle.pread(&mut buf, 0).await.unwrap();
        assert_eq!(&buf[..5], b"abcde");
        assert_eq!(&buf[5..8], &[0, 0, 0]);
        assert_eq!(buf[8], b'Z');

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn failed_truncate_preserves_unflushed_writes() {
        let fixture = TestDb::new().await;
        let (mut handle, store) = fixture.faulty_handle(5, FileOpenFlags::open_or_create());

        handle.pwrite(b"abcdefghij", 0).await.unwrap();
        assert_eq!(handle.file_size(), 10);

        // A truncation that cannot read the store changes nothing.
        store.fail_next_operation();
        assert!(matches!(handle.truncate(5).await, Err(Error::SlateDb(_))));
        assert_eq!(handle.file_size(), 10);

        store.fail_next_operation();
        assert!(matches!(handle.truncate(0).await, Err(Error::SlateDb(_))));
        assert_eq!(handle.file_size(), 10);

        let mut buf = [0u8; 10];
        assert_eq!(handle.pread(&mut buf, 0).await.unwrap(), 10);
        assert_eq!(&buf, b"abcdefghij");

        handle.close().await.unwrap();
        drop(handle);
        fixture.close().await;
    }

    #[tokio::test]
    async fn read_only_handle_rejects_writes() {
        let fixture = TestDb::new().await;
        let mut writer = fixture.handle(4, FileOpenFlags::open_or_create());
        writer.pwrite(b"abc", 0).await.unwrap();
        writer.close().await.unwrap();
        drop(writer);

        let mut read_only = fixture.reopen(4, FileOpenFlags::read_only()).await;
        assert!(matches!(
            read_only.pwrite(b"z", 0).await,
            Err(Error::ReadOnlyViolation(_))
        ));
        assert!(matches!(
            read_only.truncate(0).await,
            Err(Error::ReadOnlyViolation(_))
        ));

        drop(read_only);
        fixture.close().await;
    }
}
