//! Handle for one open SlateFS file.
//!
//! A file is stored as a metadata record plus fixed-size chunks, addressed by
//! the keys in [`crate::keys`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use futures::future;
use slatedb::{Db, WriteBatch};
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::flags::FileOpenFlags;
use crate::keys;
use crate::metadata::FileMetadata;
use crate::util::current_time_millis;

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

    /// Reads at most `buf.len()` bytes starting at `offset`, returning how many
    /// bytes were read. A read past the end of the file returns 0, and a read
    /// that runs past it is truncated to what the file holds.
    ///
    /// Regions the file covers but no chunk backs read as zeros, so a file
    /// written sparsely reads back as if it had been zero-filled.
    fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Writes `data` at `offset`, extending the file if it runs past the end.
    /// The bytes are buffered until [`FileHandle::sync`]; they are visible to
    /// this handle's reads immediately, and to nobody else until then.
    fn pwrite(&mut self, data: &[u8], offset: u64) -> Result<()>;

    /// Persists buffered writes, deletions and metadata, then flushes, so a
    /// successful return means the data survives a crash.
    fn sync(&mut self) -> Result<()>;

    /// Flushes anything buffered and releases the handle. Taken by reference
    /// so it can be called through the `dyn FileHandle` the filesystem hands
    /// out, which is where DuckDB's `FileHandle::Close` will reach it.
    fn close(&mut self) -> Result<()>;

    /// Reads from the current position and advances it by the number of bytes
    /// read, which is 0 once the position is at or past the end of the file.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Writes at the current position and advances it past the written bytes.
    fn write(&mut self, data: &[u8]) -> Result<usize>;

    /// Resizes the file to `new_size`. Growing leaves the new region unbacked,
    /// so it reads as zeros; shrinking discards the chunks past the new end.
    fn truncate(&mut self, new_size: u64) -> Result<()>;
}

/// A file opened against a SlateDB instance.
///
/// The buffered state is a write-back cache over the persisted chunks: a chunk
/// in `dirty_chunks` shadows whatever is stored, and one in `deleted_chunks`
/// reads back as absent. Both are drained by `sync`.
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
    /// Chunk index to buffered contents, not yet persisted.
    dirty_chunks: BTreeMap<u64, Vec<u8>>,
    /// Chunk indices whose persisted contents are pending deletion.
    deleted_chunks: BTreeSet<u64>,
    metadata_dirty: bool,
    flags: FileOpenFlags,
}

/// One chunk's contribution to a `pread`, resolved before any I/O so the
/// chunks a read spans can be fetched together.
struct ChunkRead {
    chunk_index: u64,
    /// Where in the caller's buffer this chunk's bytes start.
    buf_offset: usize,
    /// Where in the chunk to start copying from.
    chunk_offset: usize,
    /// How many bytes the chunk may contribute; it may hold fewer.
    len: usize,
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

        let mut handle = Self {
            db,
            runtime,
            file_id,
            position: 0,
            size: metadata.size,
            modified_at_ms: metadata.modified_at_ms,
            chunk_size,
            dirty_chunks: BTreeMap::new(),
            deleted_chunks: BTreeSet::new(),
            metadata_dirty: false,
            flags,
        };

        if flags.truncate_existing && handle.size > 0 {
            handle.truncate_to(0)?;
        }

        Ok(handle)
    }

    /// Truncates without checking the open flags, so opening with
    /// `truncate_existing` can reuse it before the handle is handed out.
    fn truncate_to(&mut self, new_size: u64) -> Result<()> {
        if new_size == self.size {
            return Ok(());
        }

        // Growing backs nothing new: the added region has no chunks, which is
        // exactly how `pread` renders a hole.
        if new_size > self.size {
            self.size = new_size;
            self.set_metadata_dirty();
            return Ok(());
        }

        if new_size == 0 {
            self.dirty_chunks.clear();
            self.discard_persisted_chunks_from(0)?;
            self.size = 0;
            self.set_metadata_dirty();
            return Ok(());
        }

        let last_byte = new_size - 1;
        let last_chunk = last_byte / self.chunk_size_u64();
        let last_len = (last_byte % self.chunk_size_u64()) as usize + 1;

        self.dirty_chunks
            .retain(|chunk_index, _| *chunk_index <= last_chunk);
        match self.dirty_chunks.get_mut(&last_chunk) {
            Some(chunk) => chunk.truncate(last_len),
            None => {
                // The surviving prefix of a stored tail chunk has to be
                // buffered, since the chunk itself is about to be rewritten.
                let mut stored = self.read_chunk_from_store(last_chunk)?;
                if stored.len() > last_len {
                    stored.truncate(last_len);
                    self.dirty_chunks.insert(last_chunk, stored);
                }
            }
        }
        self.discard_persisted_chunks_from(last_chunk + 1)?;

        self.size = new_size;
        self.set_metadata_dirty();
        Ok(())
    }

    /// Marks every stored chunk from `first_chunk` onwards for deletion.
    fn discard_persisted_chunks_from(&mut self, first_chunk: u64) -> Result<()> {
        let prefix = keys::chunk_prefix(self.file_id);
        let stale = self.runtime.block_on(async {
            let mut iter = self.db.scan_prefix(prefix, ..).await?;
            let mut chunk_indices = Vec::new();
            while let Some(entry) = iter.next().await? {
                let chunk_index = parse_chunk_index(&entry.key)?;
                if chunk_index >= first_chunk {
                    chunk_indices.push(chunk_index);
                }
            }
            Ok::<Vec<u64>, Error>(chunk_indices)
        })?;

        self.deleted_chunks.extend(stale);
        Ok(())
    }

    fn chunk_size_u64(&self) -> u64 {
        self.chunk_size as u64
    }

    fn set_metadata_dirty(&mut self) {
        self.modified_at_ms = current_time_millis();
        self.metadata_dirty = true;
    }

    /// The record [`FileHandle::sync`] persists, reflecting buffered writes.
    fn metadata(&self) -> FileMetadata {
        FileMetadata {
            size: self.size,
            modified_at_ms: self.modified_at_ms,
            chunk_size: self.chunk_size_u64(),
        }
    }

    /// Reads one chunk, honouring the buffered deletions that make a stored
    /// chunk invisible.
    fn read_chunk_from_store(&self, chunk_index: u64) -> Result<Vec<u8>> {
        if self.deleted_chunks.contains(&chunk_index) {
            return Ok(Vec::new());
        }

        Ok(self
            .read_chunks_from_store(&[chunk_index])?
            .pop()
            .unwrap_or_default())
    }

    /// Returns the buffered chunk to write into, reading the stored contents
    /// first unless the write is about to replace the whole chunk anyway.
    fn chunk_for_write(
        &mut self,
        chunk_index: u64,
        overwrites_chunk: bool,
    ) -> Result<&mut Vec<u8>> {
        if !self.dirty_chunks.contains_key(&chunk_index) {
            let contents = if overwrites_chunk {
                vec![0; self.chunk_size]
            } else {
                self.read_chunk_from_store(chunk_index)?
            };
            self.dirty_chunks.insert(chunk_index, contents);
        }

        // A chunk is no longer pending deletion once it is written to again.
        self.deleted_chunks.remove(&chunk_index);
        Ok(self
            .dirty_chunks
            .get_mut(&chunk_index)
            .expect("chunk was just buffered"))
    }

    /// Fetches the given chunks concurrently, in the order requested. A chunk
    /// with no stored value comes back empty, which is how a sparse file's
    /// holes turn into zeros.
    fn read_chunks_from_store(&self, chunk_indices: &[u64]) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(async {
            future::try_join_all(chunk_indices.iter().map(|chunk_index| {
                let db = Arc::clone(&self.db);
                let key = keys::chunk_key(self.file_id, *chunk_index);
                async move {
                    let bytes = db.get(&key).await?;
                    Ok::<Vec<u8>, Error>(bytes.map(|bytes| bytes.to_vec()).unwrap_or_default())
                }
            }))
            .await
        })
    }

    /// Splits `[offset, offset + len)` into the chunks it spans, copying from
    /// buffered chunks directly and returning the reads that need the store.
    fn plan_read(&self, buf: &mut [u8], offset: u64, len: usize) -> Vec<ChunkRead> {
        let mut persisted_reads = Vec::new();
        let mut copied = 0usize;

        while copied < len {
            let current_offset = offset + copied as u64;
            let chunk_index = current_offset / self.chunk_size_u64();
            let chunk_offset = (current_offset % self.chunk_size_u64()) as usize;
            let to_copy = (len - copied).min(self.chunk_size - chunk_offset);

            if let Some(chunk) = self.dirty_chunks.get(&chunk_index) {
                copy_chunk(buf, copied, chunk, chunk_offset, to_copy);
            } else if !self.deleted_chunks.contains(&chunk_index) {
                persisted_reads.push(ChunkRead {
                    chunk_index,
                    buf_offset: copied,
                    chunk_offset,
                    len: to_copy,
                });
            }

            copied += to_copy;
        }

        persisted_reads
    }
}

/// Reads the chunk index back out of a key built by [`keys::chunk_key`].
fn parse_chunk_index(key: &[u8]) -> Result<u64> {
    let malformed = |reason: &str| {
        Error::MetadataDecode(ErrorStruct::new(
            format!("invalid chunk key: {reason}"),
            ErrorStatus::Permanent,
        ))
    };

    let key = std::str::from_utf8(key).map_err(|_| malformed("not valid utf-8"))?;
    let (_, chunk_index) = key
        .rsplit_once('/')
        .ok_or_else(|| malformed(&format!("{key} has no chunk index")))?;

    u64::from_str_radix(chunk_index, 16)
        .map_err(|_| malformed(&format!("{key} has a non-hex chunk index")))
}

/// Copies what `chunk` can supply into `buf`, leaving the rest untouched. A
/// stored chunk may be shorter than the region asked for, because only the
/// bytes actually written to it are persisted.
fn copy_chunk(buf: &mut [u8], buf_offset: usize, chunk: &[u8], chunk_offset: usize, len: usize) {
    let available = chunk.len().saturating_sub(chunk_offset).min(len);
    buf[buf_offset..buf_offset + available]
        .copy_from_slice(&chunk[chunk_offset..chunk_offset + available]);
}

impl Debug for SlateFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateFileHandle")
            .field("file_id", &self.file_id)
            .field("position", &self.position)
            .field("size", &self.size)
            .field("modified_at_ms", &self.modified_at_ms)
            .field("chunk_size", &self.chunk_size)
            .field("dirty_chunks", &self.dirty_chunks.keys())
            .field("deleted_chunks", &self.deleted_chunks)
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

    fn pwrite(&mut self, data: &[u8], offset: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)?;

        if data.is_empty() {
            return Ok(());
        }

        let end_offset = offset.checked_add(data.len() as u64).ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                format!(
                    "write past the end of the address space for file_id {}",
                    self.file_id
                ),
                ErrorStatus::Permanent,
            ))
        })?;

        let mut written = 0usize;
        while written < data.len() {
            let current_offset = offset + written as u64;
            let chunk_index = current_offset / self.chunk_size_u64();
            let chunk_offset = (current_offset % self.chunk_size_u64()) as usize;
            let to_copy = (data.len() - written).min(self.chunk_size - chunk_offset);
            let required_len = chunk_offset + to_copy;

            let chunk = self.chunk_for_write(
                chunk_index,
                required_len == self.chunk_size && chunk_offset == 0,
            )?;
            if chunk.len() < required_len {
                chunk.resize(required_len, 0);
            }
            chunk[chunk_offset..required_len].copy_from_slice(&data[written..written + to_copy]);

            written += to_copy;
        }

        self.size = self.size.max(end_offset);
        self.set_metadata_dirty();
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        if self.dirty_chunks.is_empty() && self.deleted_chunks.is_empty() && !self.metadata_dirty {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        // Deletions are staged first so a chunk that was deleted and written
        // again in the same session ends up with its new contents.
        for chunk_index in &self.deleted_chunks {
            batch.delete(keys::chunk_key(self.file_id, *chunk_index));
        }
        for (chunk_index, chunk) in &self.dirty_chunks {
            batch.put(keys::chunk_key(self.file_id, *chunk_index), chunk);
        }
        batch.put(
            keys::metadata_key(self.file_id),
            self.metadata().encode_to_bytes(),
        );

        self.runtime.block_on(async {
            self.db.write(batch).await?;
            self.db.flush().await?;
            Ok::<(), Error>(())
        })?;

        self.dirty_chunks.clear();
        self.deleted_chunks.clear();
        self.metadata_dirty = false;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.sync()
    }

    fn truncate(&mut self, new_size: u64) -> Result<()> {
        self.flags.ensure_writable(self.file_id)?;
        self.truncate_to(new_size)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_read = self.pread(buf, self.position)?;
        self.position += bytes_read as u64;
        Ok(bytes_read)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.pwrite(data, self.position)?;
        // `pwrite` already rejected a write that would run past the end of the
        // address space, so the position cannot overflow here.
        self.position += data.len() as u64;
        Ok(data.len())
    }

    fn pread(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.flags.ensure_readable(self.file_id)?;

        if buf.is_empty() || offset >= self.size {
            return Ok(0);
        }

        let bytes_to_read = usize::try_from(self.size - offset)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        // Pre-fill so unbacked regions, whether sparse holes, deleted chunks or
        // short stored chunks, read as zeros without a second pass.
        buf[..bytes_to_read].fill(0);

        let persisted_reads = self.plan_read(buf, offset, bytes_to_read);
        let chunk_indices: Vec<u64> = persisted_reads
            .iter()
            .map(|read| read.chunk_index)
            .collect();
        let chunks = self.read_chunks_from_store(&chunk_indices)?;

        for (read, chunk) in persisted_reads.iter().zip(chunks) {
            copy_chunk(buf, read.buf_offset, &chunk, read.chunk_offset, read.len);
        }

        Ok(bytes_to_read)
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
    /// Small enough to make chunk-spanning reads readable in tests.
    const TEST_CHUNK_SIZE: u64 = 8;

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

    /// Opens a read-only handle over `size` bytes of `chunks`, written straight
    /// to SlateDB so reads see stored data rather than buffered writes.
    fn open_with_stored_chunks(
        fixture: &TestDb,
        size: u64,
        chunks: &[(u64, &str)],
    ) -> SlateFileHandle {
        for (chunk_index, contents) in chunks {
            let key = keys::chunk_key(FILE_ID, *chunk_index);
            fixture
                .runtime
                .block_on(fixture.db.put(key, contents.as_bytes()))
                .expect("chunk should be stored");
        }

        open(
            fixture,
            FileMetadata {
                size,
                modified_at_ms: 0,
                chunk_size: TEST_CHUNK_SIZE,
            },
            FileOpenFlags::read_only(),
        )
        .expect("handle")
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

    #[test]
    fn pread_reads_stored_bytes_within_one_chunk() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);

        let mut buf = [0u8; 3];
        assert_eq!(handle.pread(&mut buf, 2).expect("pread"), 3);
        assert_eq!(&buf, b"cde");
        assert_eq!(
            handle.seek_position(),
            0,
            "a positional read moves no cursor"
        );
    }

    #[test]
    fn pread_spans_chunk_boundaries() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(
            &fixture,
            20,
            &[(0, "aaaaaaaa"), (1, "bbbbbbbb"), (2, "cccc")],
        );

        let mut buf = [0u8; 12];
        assert_eq!(handle.pread(&mut buf, 6).expect("pread"), 12);
        assert_eq!(&buf, b"aabbbbbbbbcc");
    }

    #[test]
    fn pread_stops_at_the_end_of_the_file() {
        let fixture = TestDb::new();
        // The stored chunk holds more than the file's recorded size covers.
        let mut handle = open_with_stored_chunks(&fixture, 5, &[(0, "abcdefgh")]);

        let mut buf = [0xffu8; 8];
        assert_eq!(handle.pread(&mut buf, 3).expect("pread"), 2);
        assert_eq!(&buf[..2], b"de");
        // Past the recorded size the caller's buffer is left alone, and a read
        // starting there returns nothing at all.
        assert_eq!(&buf[2..], &[0xff; 6]);
        assert_eq!(handle.pread(&mut buf, 5).expect("pread"), 0);
        assert_eq!(handle.pread(&mut [], 0).expect("pread"), 0);
        assert_eq!(&buf[2..], &[0xff; 6]);
    }

    #[test]
    fn write_only_handles_reject_reads() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);
        handle.flags = FileOpenFlags {
            read: false,
            ..FileOpenFlags::read_write()
        };

        let error = handle
            .pread(&mut [0u8; 4], 0)
            .expect_err("read should be rejected");

        assert!(error.to_string().contains("cannot read file_id"));
    }

    #[test]
    fn unbacked_regions_read_as_zeros() {
        let fixture = TestDb::new();
        // Chunk 1 was never written, and chunk 2 holds fewer bytes than the
        // file claims to cover.
        let mut handle = open_with_stored_chunks(&fixture, 22, &[(0, "aaaaaaaa"), (2, "cc")]);

        let mut buf = [0xffu8; 22];
        assert_eq!(handle.pread(&mut buf, 0).expect("pread"), 22);
        assert_eq!(&buf[..8], b"aaaaaaaa");
        assert_eq!(&buf[8..16], &[0; 8]);
        assert_eq!(&buf[16..18], b"cc");
        assert_eq!(&buf[18..], &[0; 4]);
    }

    #[test]
    fn buffered_state_shadows_what_is_stored() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 16, &[(0, "aaaaaaaa"), (1, "bbbbbbbb")]);
        handle.deleted_chunks.insert(0);
        handle.dirty_chunks.insert(1, b"BBBB".to_vec());
        assert!(format!("{handle:?}").contains("dirty_chunks: [1]"));

        let mut buf = [0xffu8; 16];
        assert_eq!(handle.pread(&mut buf, 0).expect("pread"), 16);
        // Chunk 0 is pending deletion; chunk 1's buffered bytes shadow the
        // stored ones and stop short, so the rest reads as zeros too.
        assert_eq!(&buf[..8], &[0; 8]);
        assert_eq!(&buf[8..12], b"BBBB");
        assert_eq!(&buf[12..], &[0; 4]);
    }

    /// Opens an empty writable handle over its own database.
    fn open_writable(fixture: &TestDb) -> SlateFileHandle {
        open(
            fixture,
            FileMetadata {
                size: 0,
                modified_at_ms: 0,
                chunk_size: TEST_CHUNK_SIZE,
            },
            FileOpenFlags::create(),
        )
        .expect("handle")
    }

    fn stored_chunk(fixture: &TestDb, chunk_index: u64) -> Option<Vec<u8>> {
        fixture
            .runtime
            .block_on(fixture.db.get(keys::chunk_key(FILE_ID, chunk_index)))
            .expect("get")
            .map(|bytes| bytes.to_vec())
    }

    fn stored_metadata(fixture: &TestDb) -> FileMetadata {
        let bytes = fixture
            .runtime
            .block_on(fixture.db.get(keys::metadata_key(FILE_ID)))
            .expect("get")
            .expect("metadata should be stored");
        FileMetadata::decode_from_bytes(&bytes).expect("metadata should decode")
    }

    #[test]
    fn writes_are_visible_to_reads_while_still_buffered() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        handle.pwrite(b"hello", 0).expect("pwrite");

        assert_eq!(handle.file_size(), 5);
        assert!(handle.metadata_dirty);
        let mut buf = [0u8; 5];
        assert_eq!(handle.pread(&mut buf, 0).expect("pread"), 5);
        assert_eq!(&buf, b"hello");
        assert!(stored_chunk(&fixture, 0).is_none(), "nothing persisted yet");
    }

    #[test]
    fn writes_spanning_chunks_are_split_across_them() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        handle.pwrite(b"0123456789abcdefghij", 4).expect("pwrite");
        handle.sync().expect("sync");

        assert_eq!(handle.file_size(), 24);
        // The hole ahead of the write is zero-filled, not left short.
        assert_eq!(
            stored_chunk(&fixture, 0).unwrap(),
            [&[0u8; 4], b"0123".as_slice()].concat()
        );
        assert_eq!(stored_chunk(&fixture, 1).unwrap(), b"456789ab");
        assert_eq!(stored_chunk(&fixture, 2).unwrap(), b"cdefghij");
    }

    #[test]
    fn writes_merge_into_an_already_buffered_chunk() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        handle.pwrite(b"aaaa", 0).expect("pwrite");
        handle.pwrite(b"bb", 2).expect("pwrite");

        let mut buf = [0u8; 4];
        handle.pread(&mut buf, 0).expect("pread");
        assert_eq!(&buf, b"aabb");
    }

    #[test]
    fn writes_merge_into_a_stored_chunk() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);
        handle.flags = FileOpenFlags::read_write();

        handle.pwrite(b"XY", 3).expect("pwrite");
        handle.sync().expect("sync");

        assert_eq!(stored_chunk(&fixture, 0).unwrap(), b"abcXYfgh");
    }

    #[test]
    fn writing_a_deleted_chunk_brings_it_back() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);
        handle.flags = FileOpenFlags::read_write();
        handle.deleted_chunks.insert(0);

        handle.pwrite(b"zz", 0).expect("pwrite");
        handle.sync().expect("sync");

        // The chunk is no longer pending deletion, and the bytes it held
        // before deletion stay gone.
        assert!(handle.deleted_chunks.is_empty());
        assert_eq!(stored_chunk(&fixture, 0).unwrap(), b"zz");
    }

    #[test]
    fn sync_persists_chunks_and_metadata_then_clears_the_buffer() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.pwrite(b"hello", 0).expect("pwrite");

        handle.sync().expect("sync");

        assert_eq!(stored_chunk(&fixture, 0).unwrap(), b"hello");
        let metadata = stored_metadata(&fixture);
        assert_eq!(metadata.size, 5);
        assert_eq!(metadata.chunk_size, TEST_CHUNK_SIZE);
        assert!(metadata.modified_at_ms > 0);
        assert!(handle.dirty_chunks.is_empty());
        assert!(!handle.metadata_dirty);
    }

    #[test]
    fn sync_without_changes_writes_nothing() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        handle.sync().expect("sync");

        assert!(fixture
            .runtime
            .block_on(fixture.db.get(keys::metadata_key(FILE_ID)))
            .expect("get")
            .is_none());
    }

    #[test]
    fn empty_writes_leave_the_file_untouched() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        handle.pwrite(b"", 100).expect("pwrite");

        assert_eq!(handle.file_size(), 0);
        assert!(!handle.metadata_dirty);
    }

    #[test]
    fn read_only_handles_reject_writes() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);

        let error = handle
            .pwrite(b"x", 0)
            .expect_err("write should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("cannot write to file_id"));
    }

    #[test]
    fn writes_past_the_end_of_the_address_space_are_rejected() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        let error = handle
            .pwrite(b"xy", u64::MAX)
            .expect_err("write should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn close_flushes_what_is_still_buffered() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.pwrite(b"hello", 0).expect("pwrite");

        FileHandle::close(&mut handle).expect("close");

        assert_eq!(stored_chunk(&fixture, 0).unwrap(), b"hello");
        // Nothing is left buffered, so closing again is a no-op rather than
        // a second write or a failure.
        FileHandle::close(&mut handle).expect("close again");
    }

    #[test]
    fn sequential_writes_append_at_the_moving_position() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);

        assert_eq!(handle.write(b"hello ").expect("write"), 6);
        assert_eq!(handle.write(b"world").expect("write"), 5);

        assert_eq!(handle.seek_position(), 11);
        assert_eq!(handle.file_size(), 11);
    }

    #[test]
    fn sequential_reads_walk_the_file_and_stop_at_the_end() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"hello world").expect("write");
        handle.reset();

        let mut buf = [0u8; 6];
        assert_eq!(handle.read(&mut buf).expect("read"), 6);
        assert_eq!(&buf, b"hello ");
        assert_eq!(handle.seek_position(), 6);

        // Only five bytes are left, so the read is short and the position
        // lands exactly on the end of the file.
        assert_eq!(handle.read(&mut buf).expect("read"), 5);
        assert_eq!(&buf[..5], b"world");
        assert_eq!(handle.seek_position(), 11);

        assert_eq!(handle.read(&mut buf).expect("read"), 0);
        assert_eq!(handle.seek_position(), 11);
    }

    #[test]
    fn seek_redirects_sequential_io() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"aaaaaaaaaaaa").expect("write");

        handle.seek(4);
        handle.write(b"BB").expect("write");
        handle.seek(3);

        let mut buf = [0u8; 4];
        assert_eq!(handle.read(&mut buf).expect("read"), 4);
        assert_eq!(&buf, b"aBBa");
        assert_eq!(handle.file_size(), 12, "an interior write does not extend");
    }

    #[test]
    fn reading_from_beyond_the_end_of_the_file_returns_nothing() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"hello").expect("write");
        handle.seek(9999);

        let mut buf = [0u8; 4];
        assert_eq!(handle.read(&mut buf).expect("read"), 0);
        assert_eq!(handle.seek_position(), 9999);
    }

    #[test]
    fn read_only_handles_reject_sequential_writes() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);

        let error = handle.write(b"x").expect_err("write should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(handle.seek_position(), 0);
    }

    #[test]
    fn truncating_to_zero_discards_every_chunk() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 16, &[(0, "aaaaaaaa"), (1, "bbbbbbbb")]);
        handle.flags = FileOpenFlags::read_write();
        handle.pwrite(b"cc", 8).expect("pwrite");

        handle.truncate(0).expect("truncate");
        handle.sync().expect("sync");

        assert_eq!(handle.file_size(), 0);
        assert!(handle.dirty_chunks.is_empty());
        assert!(stored_chunk(&fixture, 0).is_none());
        assert!(stored_chunk(&fixture, 1).is_none());
    }

    #[test]
    fn truncating_down_shortens_the_tail_chunk_and_drops_the_rest() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(
            &fixture,
            24,
            &[(0, "aaaaaaaa"), (1, "bbbbbbbb"), (2, "cccccccc")],
        );
        handle.flags = FileOpenFlags::read_write();

        handle.truncate(11).expect("truncate");
        handle.sync().expect("sync");

        assert_eq!(handle.file_size(), 11);
        assert_eq!(stored_chunk(&fixture, 0).unwrap(), b"aaaaaaaa");
        assert_eq!(stored_chunk(&fixture, 1).unwrap(), b"bbb");
        assert!(stored_chunk(&fixture, 2).is_none());
    }

    #[test]
    fn truncating_down_shortens_a_buffered_tail_chunk() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"0123456789ab").expect("write");

        handle.truncate(10).expect("truncate");

        assert_eq!(handle.dirty_chunks[&1], b"89");
        let mut buf = [0u8; 12];
        assert_eq!(handle.pread(&mut buf, 0).expect("pread"), 10);
    }

    #[test]
    fn truncating_up_leaves_the_new_region_reading_as_zeros() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"abc").expect("write");

        handle.truncate(10).expect("truncate");

        assert_eq!(handle.file_size(), 10);
        let mut buf = [0xffu8; 10];
        assert_eq!(handle.pread(&mut buf, 0).expect("pread"), 10);
        assert_eq!(&buf[..3], b"abc");
        assert_eq!(&buf[3..], &[0; 7]);
    }

    #[test]
    fn truncating_to_the_current_size_changes_nothing() {
        let fixture = TestDb::new();
        let mut handle = open_writable(&fixture);
        handle.write(b"abc").expect("write");
        handle.sync().expect("sync");

        handle.truncate(3).expect("truncate");

        assert!(!handle.metadata_dirty);
    }

    #[test]
    fn opening_with_truncate_existing_empties_a_stored_file() {
        let fixture = TestDb::new();
        let handle = open(
            &fixture,
            FileMetadata {
                size: 16,
                modified_at_ms: 0,
                chunk_size: TEST_CHUNK_SIZE,
            },
            FileOpenFlags {
                truncate_existing: true,
                ..FileOpenFlags::read_write()
            },
        )
        .expect("handle");

        assert_eq!(handle.file_size(), 0);
        assert!(handle.metadata_dirty);
    }

    #[test]
    fn read_only_handles_reject_truncate() {
        let fixture = TestDb::new();
        let mut handle = open_with_stored_chunks(&fixture, 8, &[(0, "abcdefgh")]);

        let error = handle.truncate(0).expect_err("truncate should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(handle.file_size(), 8);
    }

    #[test]
    fn chunk_indices_round_trip_through_their_keys() {
        assert_eq!(
            parse_chunk_index(&keys::chunk_key(3, 42)).expect("index"),
            42
        );
        assert!(parse_chunk_index(b"c/0000000000000003/zzzz").is_err());
        assert!(parse_chunk_index(b"no-separator").is_err());
        assert!(parse_chunk_index(b"c/\xff/0").is_err());
    }
}
