//! Shared fixtures for tests that need a live SlateDB instance.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use tokio::runtime::Runtime;
use uuid::Uuid;

/// A SlateDB instance on an in-memory object store, isolated from every other
/// fixture by a unique path so tests can run in parallel.
pub(crate) struct TestDb {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) db: Arc<Db>,
}

impl TestDb {
    pub(crate) fn new() -> Self {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = format!("/slatefs-test-{}", Uuid::new_v4());
        let db = runtime
            .block_on(Db::open(path, object_store))
            .expect("slatedb should open");

        Self {
            runtime,
            db: Arc::new(db),
        }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        self.runtime
            .block_on(async { self.db.close().await })
            .expect("slatedb should close");
    }
}
