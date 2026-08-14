use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use tokio::runtime::Runtime;

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "slatedb://";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

/// Distinctive error so SQL tests can confirm VFS routing.
pub const DUMMY_ERROR: &str =
    "SlateDBFileSystem is a dummy implementation and cannot open files yet";

/// SlateDB-backed filesystem. I/O is intentionally unimplemented; the
/// in-memory object store and tokio runtime exist so the SlateDB dependency
/// is actually linked and type-checked.
pub struct SlateDbFileSystem {
    #[allow(dead_code)]
    runtime: Runtime,
    /// Reserved for a real `Db` once the dummy VFS is replaced.
    #[allow(dead_code)]
    store: Arc<InMemory>,
}

impl SlateDbFileSystem {
    pub fn try_new() -> Result<Self, std::io::Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let store = Arc::new(InMemory::new());
        // Touch the SlateDB builder API so this crate fails to compile if the
        // dependency is incompatible. The resulting builder is discarded.
        slatedb_builder(&store);
        Ok(Self { runtime, store })
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

/// Type-checks `Db::builder` against the linked SlateDB crate.
fn slatedb_builder(store: &Arc<InMemory>) {
    let store: Arc<dyn slatedb::object_store::ObjectStore> = store.clone();
    let _builder = Db::builder("dummy", store);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_slatedb_urls() {
        let fs = SlateDbFileSystem::try_new().expect("filesystem");
        assert!(fs.can_handle("slatedb://bucket/key"));
        assert!(!fs.can_handle("s3://bucket/key"));
        assert!(!fs.can_handle("/tmp/foo"));
        assert_eq!(fs.name(), NAME);
    }
}
