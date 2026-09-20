//! Read-only handle backed by SlateDB's [`DbReader`].

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::DbReader;

use crate::chunk_manager::ChunkManager;
use crate::chunk_store::{ChunkStore, SlateDbReaderChunkStore};
use crate::error::Result;
use crate::file_handle::FileHandle;
use crate::file_metadata::FileMetadata;
use crate::flags::FileOpenFlags;

pub(crate) struct SlateReadOnlyFileHandle {
    file_id: u64,
    position: u64,
    size: u64,
    modified_at_ms: u64,
    chunks: ChunkManager,
    flags: FileOpenFlags,
}

impl SlateReadOnlyFileHandle {
    pub(crate) fn new(
        reader: Arc<DbReader>,
        file_id: u64,
        metadata: FileMetadata,
        flags: FileOpenFlags,
    ) -> Result<Self> {
        flags.validate()?;
        flags.ensure_readable(file_id)?;
        let chunk_size = metadata.validated_chunk_size(file_id)?;
        let store = Arc::new(SlateDbReaderChunkStore::new(reader));
        Ok(Self {
            file_id,
            position: 0,
            size: metadata.size,
            modified_at_ms: metadata.modified_at_ms,
            chunks: ChunkManager::new(store as Arc<dyn ChunkStore>, file_id, chunk_size),
            flags,
        })
    }
}

impl Debug for SlateReadOnlyFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateReadOnlyFileHandle")
            .field("file_id", &self.file_id)
            .field("position", &self.position)
            .field("size", &self.size)
            .field("modified_at_ms", &self.modified_at_ms)
            .field("chunks", &self.chunks)
            .finish()
    }
}

#[async_trait]
impl FileHandle for SlateReadOnlyFileHandle {
    async fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.flags.ensure_readable(self.file_id)?;
        if buf.is_empty() || offset >= self.size {
            return Ok(0);
        }

        let bytes_to_read = usize::try_from(self.size - offset)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        self.chunks.read(&mut buf[..bytes_to_read], offset).await?;
        Ok(bytes_to_read)
    }

    async fn pwrite(&mut self, _data: &[u8], _offset: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let available = usize::try_from(self.size.saturating_sub(self.position))
            .unwrap_or(usize::MAX)
            .min(buf.len());
        if available == 0 {
            return Ok(0);
        }

        let bytes_read = self.pread(&mut buf[..available], self.position).await?;
        self.position += bytes_read as u64;
        Ok(bytes_read)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<usize> {
        self.flags.ensure_writable(self.file_id)?;
        unreachable!("a read-only handle cannot be writable")
    }

    async fn sync(&mut self) -> Result<()> {
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

    async fn truncate(&mut self, _new_size: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)
    }

    fn get_last_modified_time(&self) -> u64 {
        self.modified_at_ms
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn file_id(&self) -> u64 {
        self.file_id
    }

    fn flags(&self) -> FileOpenFlags {
        self.flags
    }
}
