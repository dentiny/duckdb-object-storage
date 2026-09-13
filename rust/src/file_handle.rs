//! Handle-level I/O matching DuckDB's `FileHandle`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use slatedb::{Db, WriteBatch};

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_metadata::FileMetadata;
use crate::flags::FileOpenFlags;
use crate::keys;
use crate::util::current_time_millis;

/// Sequential and positional I/O, matching DuckDB's `FileHandle` methods.
#[async_trait]
pub trait FileHandle: Debug {
    /// Read at `offset` without moving the file position.
    async fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;

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

pub struct SlateFileHandle {
    db: Arc<Db>,
    file_id: u64,
    position: u64,
    size: u64,
    modified_at_ms: u64,
    chunk_size: usize,
    dirty_chunks: BTreeMap<u64, Vec<u8>>,
    deleted_chunks: BTreeSet<u64>,
    metadata_dirty: bool,
    flags: FileOpenFlags,
    #[cfg(test)]
    fail_next_store_io: AtomicBool,
}

struct ChunkRead {
    chunk_idx: u64,
    buf_offset: usize,
    chunk_offset: usize,
    len: usize,
}

impl SlateFileHandle {
    /// Open a handle over an existing metadata record.
    pub fn new(
        db: Arc<Db>,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        flags.validate()?;
        let chunk_size = metadata.validated_chunk_size(file_id)?;
        let position = if flags.append { metadata.size } else { 0 };

        Ok(Self {
            db,
            file_id,
            position,
            size: metadata.size,
            modified_at_ms: metadata.modified_at_ms,
            chunk_size,
            dirty_chunks: BTreeMap::new(),
            deleted_chunks: BTreeSet::new(),
            metadata_dirty: false,
            flags,
            #[cfg(test)]
            fail_next_store_io: AtomicBool::new(false),
        })
    }

    fn check_injected_store_fault(&self) -> Result<()> {
        #[cfg(test)]
        {
            if self.fail_next_store_io.swap(false, Ordering::SeqCst) {
                return Err(Error::from(slatedb::Error::unavailable(
                    "injected store fault".to_string(),
                )));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn inject_store_fault(&self) {
        self.fail_next_store_io.store(true, Ordering::SeqCst);
    }

    fn chunk_size_u64(&self) -> u64 {
        self.chunk_size as u64
    }

    fn set_metadata_dirty(&mut self) {
        self.modified_at_ms = current_time_millis();
        self.metadata_dirty = true;
    }

    fn metadata(&self) -> FileMetadata {
        FileMetadata {
            size: self.size,
            modified_at_ms: self.modified_at_ms,
            chunk_size: self.chunk_size as u64,
        }
    }

    async fn read_chunk_from_store(&self, chunk_idx: u64) -> Result<Vec<u8>> {
        self.check_injected_store_fault()?;

        if self.deleted_chunks.contains(&chunk_idx) {
            return Ok(Vec::new());
        }

        let key = keys::chunk_key(self.file_id, chunk_idx);
        let bytes = self.db.get(&key).await?;
        Ok(bytes.map(|bytes| bytes.to_vec()).unwrap_or_default())
    }

    async fn read_chunks_from_store_parallel(&self, chunk_indices: &[u64]) -> Result<Vec<Vec<u8>>> {
        future::try_join_all(chunk_indices.iter().map(|chunk_idx| {
            let db = Arc::clone(&self.db);
            let key = keys::chunk_key(self.file_id, *chunk_idx);
            async move {
                let bytes = db.get(&key).await?;
                Ok::<Vec<u8>, Error>(bytes.map(|bytes| bytes.to_vec()).unwrap_or_default())
            }
        }))
        .await
    }

    async fn load_chunk_for_write(&self, chunk_idx: u64) -> Result<Vec<u8>> {
        if let Some(chunk) = self.dirty_chunks.get(&chunk_idx) {
            return Ok(chunk.clone());
        }

        self.read_chunk_from_store(chunk_idx).await
    }

    async fn collect_persisted_chunk_indices(&self, min_chunk_idx: u64) -> Result<Vec<u64>> {
        self.check_injected_store_fault()?;

        let prefix = keys::chunk_prefix(self.file_id);

        let mut iter = self.db.scan_prefix(prefix, ..).await?;
        let mut chunk_indices = Vec::new();
        while let Some(kv) = iter.next().await? {
            let chunk_idx = parse_chunk_idx(kv.key.as_ref())?;
            if chunk_idx >= min_chunk_idx {
                chunk_indices.push(chunk_idx);
            }
        }
        Ok(chunk_indices)
    }

    async fn truncate_internal(&mut self, new_size: u64) -> Result<()> {
        if new_size == self.size {
            return Ok(());
        }

        if new_size > self.size {
            self.size = new_size;
            self.set_metadata_dirty();
            return Ok(());
        }

        if new_size == 0 {
            let chunks_to_delete = self.collect_persisted_chunk_indices(0).await?;
            self.dirty_chunks.clear();
            self.deleted_chunks.extend(chunks_to_delete);
            self.size = new_size;
            self.set_metadata_dirty();
            return Ok(());
        }

        let chunk_size = self.chunk_size_u64();
        let last_byte = new_size - 1;
        let last_chunk = last_byte / chunk_size;
        let last_len = usize::try_from((last_byte % chunk_size) + 1).unwrap();

        let shortened_last_chunk = if self.dirty_chunks.contains_key(&last_chunk) {
            None
        } else {
            let mut persisted = self.read_chunk_from_store(last_chunk).await?;
            if persisted.len() > last_len {
                persisted.truncate(last_len);
                Some(persisted)
            } else {
                None
            }
        };
        let chunks_to_delete = self.collect_persisted_chunk_indices(last_chunk + 1).await?;

        self.dirty_chunks
            .retain(|chunk_idx, _| *chunk_idx <= last_chunk);
        if let Some(chunk) = self.dirty_chunks.get_mut(&last_chunk) {
            if chunk.len() > last_len {
                chunk.truncate(last_len);
            }
        } else if let Some(shortened) = shortened_last_chunk {
            self.dirty_chunks.insert(last_chunk, shortened);
        }

        self.deleted_chunks.extend(chunks_to_delete);
        self.size = new_size;
        self.set_metadata_dirty();
        Ok(())
    }
}

impl Debug for SlateFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateFileHandle")
            .field("file_id", &self.file_id)
            .field("position", &self.position)
            .field("size", &self.size)
            .field("modified_at_ms", &self.modified_at_ms)
            .field("dirty_chunks", &self.dirty_chunks.keys())
            .field("deleted_chunks", &self.deleted_chunks)
            .finish()
    }
}

#[async_trait]
impl FileHandle for SlateFileHandle {
    async fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.flags.ensure_readable(self.file_id)?;

        if buf.is_empty() || offset >= self.size {
            return Ok(0);
        }

        let bytes_to_read = usize::try_from(self.size - offset)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        buf[..bytes_to_read].fill(0);

        let mut copied = 0usize;
        let chunk_size = self.chunk_size_u64();
        let mut persisted_reads = Vec::new();

        while copied < bytes_to_read {
            let current_offset = offset + copied as u64;
            let chunk_idx = current_offset / chunk_size;
            let chunk_offset = usize::try_from(current_offset % chunk_size).unwrap();
            let chunk_remaining = self.chunk_size - chunk_offset;
            let to_copy = (bytes_to_read - copied).min(chunk_remaining);

            if let Some(chunk) = self.dirty_chunks.get(&chunk_idx) {
                let available_in_chunk = chunk.len().saturating_sub(chunk_offset).min(to_copy);
                buf[copied..copied + available_in_chunk]
                    .copy_from_slice(&chunk[chunk_offset..chunk_offset + available_in_chunk]);
            } else if !self.deleted_chunks.contains(&chunk_idx) {
                persisted_reads.push(ChunkRead {
                    chunk_idx,
                    buf_offset: copied,
                    chunk_offset,
                    len: to_copy,
                });
            }

            copied += to_copy;
        }

        let persisted_chunk_indices = persisted_reads
            .iter()
            .map(|read| read.chunk_idx)
            .collect::<Vec<_>>();
        let persisted_chunks = self
            .read_chunks_from_store_parallel(&persisted_chunk_indices)
            .await?;
        for (read, chunk) in persisted_reads.iter().zip(persisted_chunks) {
            let available_in_chunk = chunk.len().saturating_sub(read.chunk_offset).min(read.len);
            buf[read.buf_offset..read.buf_offset + available_in_chunk]
                .copy_from_slice(&chunk[read.chunk_offset..read.chunk_offset + available_in_chunk]);
        }

        Ok(bytes_to_read)
    }

    async fn pwrite(&mut self, data: &[u8], offset: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)?;

        if data.is_empty() {
            return Ok(());
        }

        let end_offset = offset.checked_add(data.len() as u64).ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                format!("write overflow for file_id {}", self.file_id),
                ErrorStatus::Permanent,
            ))
        })?;

        let mut written = 0usize;
        let chunk_size = self.chunk_size_u64();

        while written < data.len() {
            let current_offset = offset + written as u64;
            let chunk_idx = current_offset / chunk_size;
            let chunk_offset = usize::try_from(current_offset % chunk_size).unwrap();
            let chunk_remaining = self.chunk_size - chunk_offset;
            let to_copy = (data.len() - written).min(chunk_remaining);
            let required_len = chunk_offset + to_copy;

            let mut chunk = if chunk_offset == 0 && to_copy == self.chunk_size {
                vec![0; self.chunk_size]
            } else {
                self.load_chunk_for_write(chunk_idx).await?
            };

            if chunk.len() < required_len {
                chunk.resize(required_len, 0);
            }

            chunk[chunk_offset..required_len].copy_from_slice(&data[written..written + to_copy]);
            self.deleted_chunks.remove(&chunk_idx);
            self.dirty_chunks.insert(chunk_idx, chunk);
            written += to_copy;
        }

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
        self.position = offset.checked_add(data.len() as u64).ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                format!("write overflow for file_id {}", self.file_id),
                ErrorStatus::Permanent,
            ))
        })?;
        Ok(data.len())
    }

    async fn sync(&mut self) -> Result<()> {
        if self.dirty_chunks.is_empty() && self.deleted_chunks.is_empty() && !self.metadata_dirty {
            return Ok(());
        }

        let mut batch = WriteBatch::new();

        for chunk_idx in &self.deleted_chunks {
            batch.delete(keys::chunk_key(self.file_id, *chunk_idx));
        }

        for (chunk_idx, chunk) in &self.dirty_chunks {
            batch.put(keys::chunk_key(self.file_id, *chunk_idx), chunk);
        }

        batch.put(
            keys::metadata_key(self.file_id),
            self.metadata().encode_to_bytes(),
        );

        self.db.write(batch).await?;
        self.db.flush().await?;

        self.dirty_chunks.clear();
        self.deleted_chunks.clear();
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
        self.truncate_internal(new_size).await
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

fn parse_chunk_idx(key: &[u8]) -> Result<u64> {
    let key = std::str::from_utf8(key).map_err(|src| {
        Error::MetadataDecode(
            ErrorStruct::new(
                "chunk key is not valid utf-8".to_string(),
                ErrorStatus::Permanent,
            )
            .with_source(src),
        )
    })?;

    let chunk_idx = key.rsplit('/').next().ok_or_else(|| {
        Error::MetadataDecode(ErrorStruct::new(
            format!("invalid chunk key: {key}"),
            ErrorStatus::Permanent,
        ))
    })?;

    u64::from_str_radix(chunk_idx, 16).map_err(|src| {
        Error::MetadataDecode(
            ErrorStruct::new(
                format!("invalid chunk index in key: {key}"),
                ErrorStatus::Permanent,
            )
            .with_source(src),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store_opendal::OpendalStore;
    use opendal::services::Memory;
    use opendal::Operator;
    use slatedb::Db;

    use super::*;
    use crate::file_metadata::FileMetadata;

    const SMALL_CHUNK: u64 = 8;

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
            let metadata = FileMetadata {
                size: 0,
                modified_at_ms: 1,
                chunk_size: SMALL_CHUNK,
            };
            SlateFileHandle::new(Arc::clone(&self.db), file_id, metadata, flags).unwrap()
        }

        async fn reopen(&self, file_id: u64, flags: FileOpenFlags) -> SlateFileHandle {
            let bytes = self
                .db
                .get(keys::metadata_key(file_id))
                .await
                .unwrap()
                .expect("metadata");
            let metadata = FileMetadata::decode_from_bytes(&bytes).unwrap();
            SlateFileHandle::new(Arc::clone(&self.db), file_id, metadata, flags).unwrap()
        }

        async fn close(self) {
            self.db.close().await.expect("close slatedb");
        }
    }

    #[tokio::test]
    async fn sequential_io_tracks_position() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(1, FileOpenFlags::create());

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
    async fn writes_persist_across_chunks_and_reopen() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(2, FileOpenFlags::create());

        handle.pwrite(b"hello", 0).await.unwrap();
        handle.pwrite(b"xyz", SMALL_CHUNK - 1).await.unwrap();
        assert_eq!(handle.file_size(), SMALL_CHUNK + 2);
        handle.close().await.unwrap();
        drop(handle);

        let mut reopened = fixture.reopen(2, FileOpenFlags::read_only()).await;
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
        let mut writer = fixture.handle(6, FileOpenFlags::create());
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
    async fn truncate_then_sparse_write_reads_as_zero() {
        let fixture = TestDb::new().await;
        let mut handle = fixture.handle(3, FileOpenFlags::create());

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
        let mut handle = fixture.handle(5, FileOpenFlags::create());

        handle.pwrite(b"abcdefghij", 0).await.unwrap();
        assert_eq!(handle.file_size(), 10);

        handle.inject_store_fault();
        assert!(matches!(handle.truncate(5).await, Err(Error::SlateDb(_))));
        assert_eq!(handle.file_size(), 10);

        handle.inject_store_fault();
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
        let mut writer = fixture.handle(4, FileOpenFlags::create());
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
