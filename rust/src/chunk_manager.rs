use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::future;
use slatedb::bytes::Bytes;
use slatedb::WriteBatch;

use crate::chunk_store::ChunkStore;
use crate::error::{Error, Result};
use crate::keys;

const MAX_SCAN_READ_AHEAD_BYTES: usize = 4 * 1024 * 1024;

enum PendingChunk {
    Written(Vec<u8>),
    Deleted,
}

impl Debug for PendingChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PendingChunk::Written(chunk) => write!(f, "Written({} bytes)", chunk.len()),
            PendingChunk::Deleted => f.write_str("Deleted"),
        }
    }
}

struct ChunkSlice {
    chunk_idx: u64,
    buf_offset: usize,
    chunk_offset: usize,
    len: usize,
}

pub(crate) struct ChunkManager {
    store: Arc<dyn ChunkStore>,
    file_id: u64,
    chunk_size: usize,
    pending: BTreeMap<u64, PendingChunk>,
}

impl ChunkManager {
    pub(crate) fn new(store: Arc<dyn ChunkStore>, file_id: u64, chunk_size: usize) -> Self {
        Self {
            store,
            file_id,
            chunk_size,
            pending: BTreeMap::new(),
        }
    }

    /// Bytes per chunk, as recorded in the file's metadata.
    pub(crate) fn chunk_size(&self) -> u64 {
        self.chunk_size as u64
    }

    /// Whether a sync has anything to write.
    pub(crate) fn has_pending_changes(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Fill `buf` from `offset`; bytes no chunk covers read as zero.
    pub(crate) async fn read(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        buf.fill(0);

        let mut persisted_slices = Vec::new();
        for slice in self.split_into_chunks(buf.len(), offset) {
            match self.pending.get(&slice.chunk_idx) {
                Some(PendingChunk::Written(chunk)) => copy_chunk_into(buf, &slice, chunk),
                Some(PendingChunk::Deleted) => {}
                None => persisted_slices.push(slice),
            }
        }

        let persisted_chunk_indices = persisted_slices
            .iter()
            .map(|slice| slice.chunk_idx)
            .collect::<Vec<_>>();
        let persisted_chunks = self
            .read_chunks_from_store(&persisted_chunk_indices)
            .await?;
        for slice in &persisted_slices {
            if let Some(chunk) = persisted_chunks.get(&slice.chunk_idx) {
                copy_chunk_into(buf, slice, chunk);
            }
        }

        Ok(())
    }

    /// Stage `data` at `offset` for the next sync.
    pub(crate) async fn write(&mut self, data: &[u8], offset: u64) -> Result<()> {
        for slice in self.split_into_chunks(data.len(), offset) {
            let required_len = slice.chunk_offset + slice.len;

            // A whole-chunk write replaces it, so skip fetching the old bytes.
            let mut chunk = if slice.chunk_offset == 0 && slice.len == self.chunk_size {
                vec![0; self.chunk_size]
            } else {
                self.chunk_contents(slice.chunk_idx).await?
            };

            if chunk.len() < required_len {
                chunk.resize(required_len, 0);
            }
            chunk[slice.chunk_offset..required_len]
                .copy_from_slice(&data[slice.buf_offset..slice.buf_offset + slice.len]);

            self.pending
                .insert(slice.chunk_idx, PendingChunk::Written(chunk));
        }

        Ok(())
    }

    /// Stage the removal of every byte at or past `new_size`.
    pub(crate) async fn truncate(&mut self, new_size: u64) -> Result<()> {
        if new_size == 0 {
            let chunks_to_delete = self.collect_persisted_chunk_indices(0).await?;
            self.pending.clear();
            self.mark_chunks_deleted(chunks_to_delete);
            return Ok(());
        }

        let last_byte = new_size - 1;
        let last_chunk = last_byte / self.chunk_size();
        let last_len = usize::try_from((last_byte % self.chunk_size()) + 1).unwrap();

        // Read before mutating, so a failed truncation keeps the staged writes.
        let shortened_last_chunk = if self.pending.contains_key(&last_chunk) {
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

        // Dropping pending state past the new end is safe: whatever reached
        // the store is already in `chunks_to_delete`.
        self.pending.retain(|chunk_idx, _| *chunk_idx <= last_chunk);
        if let Some(PendingChunk::Written(chunk)) = self.pending.get_mut(&last_chunk) {
            if chunk.len() > last_len {
                chunk.truncate(last_len);
            }
        } else if let Some(shortened) = shortened_last_chunk {
            self.pending
                .insert(last_chunk, PendingChunk::Written(shortened));
        }

        self.mark_chunks_deleted(chunks_to_delete);
        Ok(())
    }

    /// Add the staged writes and deletes to `batch`.
    pub(crate) fn add_pending_to_batch(&self, batch: &mut WriteBatch) {
        for (chunk_idx, pending) in &self.pending {
            let key = keys::chunk_key(self.file_id, *chunk_idx);
            match pending {
                PendingChunk::Written(chunk) => batch.put(key, chunk),
                PendingChunk::Deleted => batch.delete(key),
            }
        }
    }

    /// Drop the staged changes, once their batch is written.
    pub(crate) fn mark_flushed(&mut self) {
        self.pending.clear();
    }

    /// Split the `len` bytes at `offset` into per-chunk slices.
    fn split_into_chunks(&self, len: usize, offset: u64) -> Vec<ChunkSlice> {
        let mut slices = Vec::new();
        let mut covered = 0usize;

        while covered < len {
            let current_offset = offset + covered as u64;
            let chunk_offset = usize::try_from(current_offset % self.chunk_size()).unwrap();
            let slice_len = (len - covered).min(self.chunk_size - chunk_offset);
            slices.push(ChunkSlice {
                chunk_idx: current_offset / self.chunk_size(),
                buf_offset: covered,
                chunk_offset,
                len: slice_len,
            });
            covered += slice_len;
        }

        slices
    }

    /// Contents of `chunk_idx`: the staged write if any, else what is stored.
    async fn chunk_contents(&self, chunk_idx: u64) -> Result<Vec<u8>> {
        match self.pending.get(&chunk_idx) {
            Some(PendingChunk::Written(chunk)) => Ok(chunk.clone()),
            Some(PendingChunk::Deleted) => Ok(Vec::new()),
            None => self.read_chunk_from_store(chunk_idx).await,
        }
    }

    async fn read_chunk_from_store(&self, chunk_idx: u64) -> Result<Vec<u8>> {
        let key = keys::chunk_key(self.file_id, chunk_idx);
        let chunk = self.store.get(&key).await?;
        Ok(chunk.map(|chunk| chunk.to_vec()).unwrap_or_default())
    }

    /// Fetch `chunk_indices` (ascending, deduplicated), coalescing adjacent
    /// ones into range scans that are issued concurrently.
    async fn read_chunks_from_store(&self, chunk_indices: &[u64]) -> Result<BTreeMap<u64, Bytes>> {
        if chunk_indices.is_empty() {
            return Ok(BTreeMap::new());
        }

        let groups = coalesce_chunk_indices(chunk_indices);
        let fetched = future::try_join_all(
            groups
                .into_iter()
                .map(|group| self.read_chunk_group_from_store(group)),
        )
        .await?;
        Ok(fetched.into_iter().flatten().collect())
    }

    /// Fetch `group` with one range scan, or a point lookup when it holds a
    /// single chunk. Chunks with no persisted key are absent from the result.
    async fn read_chunk_group_from_store(
        &self,
        group: RangeInclusive<u64>,
    ) -> Result<Vec<(u64, Bytes)>> {
        let (first, last) = (*group.start(), *group.end());
        if first == last {
            let chunk = self
                .store
                .get(&keys::chunk_key(self.file_id, first))
                .await?;
            return Ok(chunk.map(|chunk| vec![(first, chunk)]).unwrap_or_default());
        }

        // Fixed width hex keys: adjacent indices are adjacent keys, and the
        // range cannot cross into another file.
        let read_ahead_bytes = scan_read_ahead_bytes(first, last, self.chunk_size);
        let scanned = self
            .store
            .scan_inclusive(
                &keys::chunk_key(self.file_id, first),
                &keys::chunk_key(self.file_id, last),
                read_ahead_bytes,
            )
            .await?;
        scanned
            .into_iter()
            .map(|(key, chunk)| Ok((parse_chunk_idx(&key)?, chunk)))
            .collect()
    }

    async fn collect_persisted_chunk_indices(&self, min_chunk_idx: u64) -> Result<Vec<u64>> {
        let keys = self
            .store
            .keys_with_prefix(&keys::chunk_prefix(self.file_id))
            .await?;

        let mut chunk_indices = Vec::new();
        for key in keys {
            let chunk_idx = parse_chunk_idx(&key)?;
            if chunk_idx >= min_chunk_idx {
                chunk_indices.push(chunk_idx);
            }
        }
        Ok(chunk_indices)
    }

    fn mark_chunks_deleted(&mut self, chunk_indices: impl IntoIterator<Item = u64>) {
        self.pending.extend(
            chunk_indices
                .into_iter()
                .map(|chunk_idx| (chunk_idx, PendingChunk::Deleted)),
        );
    }
}

impl Debug for ChunkManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkManager")
            .field("file_id", &self.file_id)
            .field("chunk_size", &self.chunk_size)
            .field("pending", &self.pending)
            .finish()
    }
}

fn scan_read_ahead_bytes(first: u64, last: u64, chunk_size: usize) -> usize {
    let chunk_count =
        usize::try_from(last.saturating_sub(first).saturating_add(1)).unwrap_or(usize::MAX);
    chunk_count
        .saturating_mul(chunk_size)
        .clamp(1, MAX_SCAN_READ_AHEAD_BYTES)
}

/// Copy the part of `chunk` that `slice` selects into `buf`. A short chunk
/// contributes only the bytes it has.
fn copy_chunk_into(buf: &mut [u8], slice: &ChunkSlice, chunk: &[u8]) {
    let available = chunk
        .len()
        .saturating_sub(slice.chunk_offset)
        .min(slice.len);
    buf[slice.buf_offset..slice.buf_offset + available]
        .copy_from_slice(&chunk[slice.chunk_offset..slice.chunk_offset + available]);
}

/// Group ascending, deduplicated indices into maximal adjacent runs. Each run
/// is one range scan, and separate runs can be fetched concurrently.
fn coalesce_chunk_indices(chunk_indices: &[u64]) -> Vec<RangeInclusive<u64>> {
    let mut groups: Vec<RangeInclusive<u64>> = Vec::new();

    for &chunk_idx in chunk_indices {
        match groups.last_mut() {
            Some(group) if group.end().checked_add(1) == Some(chunk_idx) => {
                *group = *group.start()..=chunk_idx;
            }
            _ => groups.push(chunk_idx..=chunk_idx),
        }
    }

    groups
}

fn parse_chunk_idx(key: &[u8]) -> Result<u64> {
    let key = std::str::from_utf8(key)
        .map_err(|src| Error::metadata_decode_with_source("chunk key is not valid utf-8", src))?;

    let chunk_idx = key
        .rsplit('/')
        .next()
        .ok_or_else(|| Error::metadata_decode(format!("invalid chunk key: {key}")))?;

    u64::from_str_radix(chunk_idx, 16).map_err(|src| {
        Error::metadata_decode_with_source(format!("invalid chunk index in key: {key}"), src)
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
    use crate::chunk_store::{FaultyChunkStore, SlateDbChunkStore};

    const SMALL_CHUNK: usize = 8;

    struct TestDb {
        db: Arc<Db>,
    }

    impl TestDb {
        async fn new() -> Self {
            let operator =
                Operator::new(Memory::default()).expect("create OpenDAL memory operator");
            let store = Arc::new(OpendalStore::new(operator));
            let db = Db::builder("chunk-manager-test", store)
                .build()
                .await
                .expect("open slatedb");
            Self { db: Arc::new(db) }
        }

        fn manager(&self, file_id: u64) -> ChunkManager {
            let store = Arc::new(SlateDbChunkStore::new(Arc::clone(&self.db)));
            ChunkManager::new(store, file_id, SMALL_CHUNK)
        }

        /// A manager whose store can be made to fail.
        fn faulty_manager(&self, file_id: u64) -> (ChunkManager, Arc<FaultyChunkStore>) {
            let store = Arc::new(FaultyChunkStore::new(Arc::clone(&self.db)));
            let manager = ChunkManager::new(
                Arc::clone(&store) as Arc<dyn ChunkStore>,
                file_id,
                SMALL_CHUNK,
            );
            (manager, store)
        }

        async fn close(self) {
            self.db.close().await.expect("close slatedb");
        }
    }

    /// Persist the staged changes, the way a handle's sync does.
    async fn flush(manager: &mut ChunkManager, db: &Db) {
        let mut batch = WriteBatch::new();
        manager.add_pending_to_batch(&mut batch);
        db.write(batch).await.expect("write batch");
        manager.mark_flushed();
    }

    async fn read_to_vec(manager: &ChunkManager, len: usize, offset: u64) -> Vec<u8> {
        let mut buf = vec![0; len];
        manager.read(&mut buf, offset).await.expect("read");
        buf
    }

    #[test]
    fn coalescing_groups_only_adjacent_chunk_indices() {
        assert!(coalesce_chunk_indices(&[]).is_empty());
        assert_eq!(coalesce_chunk_indices(&[7]), vec![7..=7]);
        assert_eq!(coalesce_chunk_indices(&[0, 1, 2]), vec![0..=2]);
        assert_eq!(
            coalesce_chunk_indices(&[0, 1, 3, 6, 7]),
            vec![0..=1, 3..=3, 6..=7]
        );
        assert_eq!(
            coalesce_chunk_indices(&[u64::MAX - 1, u64::MAX]),
            vec![u64::MAX - 1..=u64::MAX]
        );
    }

    #[test]
    fn scan_read_ahead_tracks_span_and_caps_at_four_mib() {
        assert_eq!(scan_read_ahead_bytes(4, 5, SMALL_CHUNK), 2 * SMALL_CHUNK);
        assert_eq!(
            scan_read_ahead_bytes(0, u64::MAX, usize::MAX),
            MAX_SCAN_READ_AHEAD_BYTES
        );
    }

    #[tokio::test]
    async fn writes_spanning_chunks_read_back_whole() {
        let fixture = TestDb::new().await;
        let mut manager = fixture.manager(1);

        assert!(!manager.has_pending_changes());
        manager.write(b"abcdefghij", 0).await.unwrap();
        assert!(manager.has_pending_changes());

        // Staged writes are visible before and after the flush.
        assert_eq!(read_to_vec(&manager, 10, 0).await, b"abcdefghij");
        flush(&mut manager, &fixture.db).await;
        assert!(!manager.has_pending_changes());
        assert_eq!(read_to_vec(&manager, 4, 6).await, b"ghij");

        // A write straddling the chunk boundary keeps the untouched bytes.
        manager.write(b"XY", SMALL_CHUNK as u64 - 1).await.unwrap();
        assert_eq!(read_to_vec(&manager, 10, 0).await, b"abcdefgXYj");

        drop(manager);
        fixture.close().await;
    }

    #[tokio::test]
    async fn reads_past_the_last_chunk_are_zero() {
        let fixture = TestDb::new().await;
        let mut manager = fixture.manager(2);

        manager.write(b"abc", 0).await.unwrap();
        flush(&mut manager, &fixture.db).await;

        // The manager has no notion of file length: uncovered bytes read as zero.
        assert_eq!(read_to_vec(&manager, 6, 0).await, b"abc\0\0\0");
        assert_eq!(read_to_vec(&manager, 4, 64).await, b"\0\0\0\0");

        drop(manager);
        fixture.close().await;
    }

    #[tokio::test]
    async fn reads_coalesced_chunks_skip_absent_keys() {
        let fixture = TestDb::new().await;
        let mut manager = fixture.manager(7);

        // Chunks 0, 1 and 3 are persisted; chunk 2 is a hole.
        manager.write(b"aaaaaaaabbbbbbbb", 0).await.unwrap();
        manager
            .write(b"dddddddd", 3 * SMALL_CHUNK as u64)
            .await
            .unwrap();
        flush(&mut manager, &fixture.db).await;

        // The next file's chunks sort right after; a scan must stop before them.
        let mut other = fixture.manager(8);
        other.write(b"xxxxxxxx", 0).await.unwrap();
        flush(&mut other, &fixture.db).await;

        let chunks = manager.read_chunks_from_store(&[0, 1, 2, 3]).await.unwrap();
        assert_eq!(chunks.keys().copied().collect::<Vec<_>>(), vec![0, 1, 3]);
        assert_eq!(chunks[&0].as_ref(), b"aaaaaaaa");
        assert_eq!(chunks[&3].as_ref(), b"dddddddd");

        // A lone index is fetched with a point lookup rather than a scan.
        let chunks = manager.read_chunks_from_store(&[3]).await.unwrap();
        assert_eq!(chunks.keys().copied().collect::<Vec<_>>(), vec![3]);

        // The hole between the two persisted ranges still reads as zeroes.
        assert_eq!(
            read_to_vec(&manager, 4 * SMALL_CHUNK, 0).await,
            b"aaaaaaaabbbbbbbb\0\0\0\0\0\0\0\0dddddddd"
        );

        drop(manager);
        fixture.close().await;
    }

    #[tokio::test]
    async fn truncate_shortens_the_last_chunk_and_deletes_the_rest() {
        let fixture = TestDb::new().await;
        let mut manager = fixture.manager(3);

        manager.write(b"aaaaaaaabbbbbbbbcccccccc", 0).await.unwrap();
        flush(&mut manager, &fixture.db).await;

        manager.truncate(SMALL_CHUNK as u64 + 3).await.unwrap();
        assert!(manager.has_pending_changes());
        assert_eq!(
            read_to_vec(&manager, 3 * SMALL_CHUNK, 0).await,
            b"aaaaaaaabbb\0\0\0\0\0\0\0\0\0\0\0\0\0"
        );

        // The deletes reach the store, so the dropped chunks stay gone.
        flush(&mut manager, &fixture.db).await;
        assert_eq!(
            manager
                .read_chunks_from_store(&[0, 1, 2])
                .await
                .unwrap()
                .len(),
            2
        );

        manager.truncate(0).await.unwrap();
        flush(&mut manager, &fixture.db).await;
        assert!(manager
            .read_chunks_from_store(&[0, 1, 2])
            .await
            .unwrap()
            .is_empty());

        drop(manager);
        fixture.close().await;
    }

    #[tokio::test]
    async fn failed_truncate_keeps_staged_writes() {
        let fixture = TestDb::new().await;
        let (mut manager, store) = fixture.faulty_manager(4);

        manager.write(b"abcdefghij", 0).await.unwrap();

        store.fail_next_operation();
        assert!(matches!(manager.truncate(5).await, Err(Error::SlateDb(_))));
        store.fail_next_operation();
        assert!(matches!(manager.truncate(0).await, Err(Error::SlateDb(_))));

        assert_eq!(read_to_vec(&manager, 10, 0).await, b"abcdefghij");

        drop(manager);
        fixture.close().await;
    }
}
