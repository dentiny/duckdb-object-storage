#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::bytes::Bytes;
use slatedb::{Db, DbReader};

#[cfg(test)]
use crate::error::Error;
use crate::error::Result;

/// Key/value reads a chunk manager issues. A trait rather than [`Db`] so tests
/// can wrap it in a store that fails on demand.
#[async_trait]
pub(crate) trait ChunkStore: Send + Sync {
    /// Value under `key`, if any.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Pairs from `first` to `last` inclusive, ascending.
    async fn scan_inclusive(&self, first: &[u8], last: &[u8]) -> Result<Vec<(Bytes, Bytes)>>;

    /// Keys under `prefix`, ascending.
    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>>;
}

/// Reads served straight from SlateDB.
pub(crate) struct SlateDbChunkStore {
    db: Arc<Db>,
}

impl SlateDbChunkStore {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl ChunkStore for SlateDbChunkStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.db.get(key).await?)
    }

    async fn scan_inclusive(&self, first: &[u8], last: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self.db.scan(first..=last).await?;
        let mut pairs = Vec::new();
        while let Some(kv) = iter.next().await? {
            pairs.push((kv.key, kv.value));
        }
        Ok(pairs)
    }

    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        let mut iter = self.db.scan_prefix(prefix, ..).await?;
        let mut keys = Vec::new();
        while let Some(kv) = iter.next().await? {
            keys.push(kv.key);
        }
        Ok(keys)
    }
}

/// Reads served through SlateDB's non-fencing read-only client.
pub(crate) struct SlateDbReaderChunkStore {
    reader: Arc<DbReader>,
}

impl SlateDbReaderChunkStore {
    pub(crate) fn new(reader: Arc<DbReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl ChunkStore for SlateDbReaderChunkStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.reader.get(key).await?)
    }

    async fn scan_inclusive(&self, first: &[u8], last: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self.reader.scan(first..=last).await?;
        let mut pairs = Vec::new();
        while let Some(kv) = iter.next().await? {
            pairs.push((kv.key, kv.value));
        }
        Ok(pairs)
    }

    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        let mut iter = self.reader.scan_prefix(prefix, ..).await?;
        let mut keys = Vec::new();
        while let Some(kv) = iter.next().await? {
            keys.push(kv.key);
        }
        Ok(keys)
    }
}

/// A real store that fails its next operation on demand.
#[cfg(test)]
pub(crate) struct FaultyChunkStore {
    inner: SlateDbChunkStore,
    fail_next: AtomicBool,
}

#[cfg(test)]
impl FaultyChunkStore {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self {
            inner: SlateDbChunkStore::new(db),
            fail_next: AtomicBool::new(false),
        }
    }

    /// Fail the next operation, once.
    pub(crate) fn fail_next_operation(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    fn take_fault(&self) -> Result<()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(Error::from(slatedb::Error::unavailable(
                "injected store fault".to_string(),
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[async_trait]
impl ChunkStore for FaultyChunkStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.take_fault()?;
        self.inner.get(key).await
    }

    async fn scan_inclusive(&self, first: &[u8], last: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.take_fault()?;
        self.inner.scan_inclusive(first, last).await
    }

    async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        self.take_fault()?;
        self.inner.keys_with_prefix(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use object_store_opendal::OpendalStore;
    use opendal::services::Memory;
    use opendal::Operator;

    use super::*;

    async fn test_db() -> Arc<Db> {
        let operator = Operator::new(Memory::default()).expect("create OpenDAL memory operator");
        let store = Arc::new(OpendalStore::new(operator));
        Arc::new(
            Db::builder("chunk-store-test", store)
                .build()
                .await
                .expect("open slatedb"),
        )
    }

    #[tokio::test]
    async fn scans_cover_both_ends_of_the_range() {
        let db = test_db().await;
        for key in [b"k/1", b"k/2", b"k/3", b"k/4"] {
            db.put(key, b"v").await.expect("put");
        }
        let store = SlateDbChunkStore::new(Arc::clone(&db));

        let scanned = store.scan_inclusive(b"k/2", b"k/3").await.unwrap();
        let keys = scanned
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![Bytes::from_static(b"k/2"), Bytes::from_static(b"k/3")]
        );

        assert!(store.get(b"k/1").await.unwrap().is_some());
        assert!(store.get(b"k/9").await.unwrap().is_none());
        assert_eq!(store.keys_with_prefix(b"k/").await.unwrap().len(), 4);

        db.close().await.expect("close slatedb");
    }

    #[tokio::test]
    async fn injected_fault_applies_once_to_whichever_operation_comes_next() {
        let db = test_db().await;
        db.put(b"k/1", b"v").await.expect("put");
        let store = FaultyChunkStore::new(Arc::clone(&db));

        store.fail_next_operation();
        assert!(matches!(store.get(b"k/1").await, Err(Error::SlateDb(_))));
        assert!(store.get(b"k/1").await.unwrap().is_some());

        store.fail_next_operation();
        assert!(matches!(
            store.keys_with_prefix(b"k/").await,
            Err(Error::SlateDb(_))
        ));
        assert_eq!(store.keys_with_prefix(b"k/").await.unwrap().len(), 1);

        store.fail_next_operation();
        assert!(matches!(
            store.scan_inclusive(b"k/0", b"k/9").await,
            Err(Error::SlateDb(_))
        ));
        assert_eq!(store.scan_inclusive(b"k/0", b"k/9").await.unwrap().len(), 1);

        db.close().await.expect("close slatedb");
    }
}
