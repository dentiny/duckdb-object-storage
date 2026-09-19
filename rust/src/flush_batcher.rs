//! Coalesces database flushes in the calling tasks.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use slatedb::Db;

use crate::error::{Error, Result};

/// Persists all writes applied before the call.
#[async_trait]
pub(crate) trait Flush: Send + Sync {
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
impl Flush for Db {
    async fn flush(&self) -> Result<()> {
        Db::flush(self).await.map_err(Error::from)
    }
}

/// Callers queued before a flush starts share its result.
pub(crate) struct FlushBatcher {
    target: Arc<dyn Flush>,
    state: Mutex<FlushState>,
    turn: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct FlushState {
    /// ID of the most recently started flush
    started: u64,
    /// ID of the most recently completed flush, whether successful or failed
    finished: u64,
    outcome: Option<Error>,
}

impl FlushBatcher {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self::with_target(db)
    }

    fn with_target(target: Arc<dyn Flush>) -> Self {
        Self {
            target,
            state: Mutex::new(FlushState::default()),
            turn: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        // This caller’s snapshot of started
        let arrival = {
            let state = self.state.lock()?;
            state.started
        };

        // FIFO ordering keeps queued callers ahead of requests for later flushes.
        let _turn = self.turn.lock().await;

        let index = {
            let mut state = self.state.lock()?;
            if state.finished > arrival {
                // prior flush has covered this caller's flush when waiting for its turn, skipping a redundant flush.
                return state.outcome.clone().map_or(Ok(()), Err);
            }
            state.started += 1;
            state.started
        };

        let result = self.target.flush().await;
        {
            let mut state = self.state.lock()?;
            state.finished = index;
            state.outcome = result.as_ref().err().cloned();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use tokio::sync::{mpsc, Semaphore};

    use super::*;
    use crate::error_struct::{ErrorStatus, ErrorStruct};

    struct TestFlush {
        started: AtomicU64,
        release: Semaphore,
        fails: AtomicBool,
    }

    impl TestFlush {
        fn open() -> Arc<Self> {
            Self::new(Semaphore::MAX_PERMITS)
        }

        fn gated() -> Arc<Self> {
            Self::new(0)
        }

        fn new(permits: usize) -> Arc<Self> {
            Arc::new(Self {
                started: AtomicU64::new(0),
                release: Semaphore::new(permits),
                fails: AtomicBool::new(false),
            })
        }

        fn started(&self) -> u64 {
            self.started.load(Ordering::SeqCst)
        }

        fn fail(&self, fails: bool) {
            self.fails.store(fails, Ordering::SeqCst);
        }

        fn release_one(&self) {
            self.release.add_permits(1);
        }

        async fn await_started(&self, count: u64) {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while self.started() < count {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("flush started");
        }
    }

    #[async_trait]
    impl Flush for TestFlush {
        async fn flush(&self) -> Result<()> {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.release
                .acquire()
                .await
                .expect("semaphore is never closed")
                .forget();

            if self.fails.load(Ordering::SeqCst) {
                return Err(Error::SlateDb(ErrorStruct::new(
                    "injected flush failure".to_string(),
                    ErrorStatus::Temporary,
                )));
            }
            Ok(())
        }
    }

    fn batcher(target: Arc<TestFlush>) -> Arc<FlushBatcher> {
        Arc::new(FlushBatcher::with_target(target))
    }

    // On the current-thread runtime, each sender reaches flush's first await
    // before the receiver runs, so receiving all signals proves all arrivals.
    async fn spawn_arrived(
        batcher: &Arc<FlushBatcher>,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<()>>> {
        let (arrived_tx, mut arrived_rx) = mpsc::unbounded_channel();
        let callers = (0..count)
            .map(|_| {
                let batcher = Arc::clone(batcher);
                let arrived_tx = arrived_tx.clone();
                tokio::spawn(async move {
                    arrived_tx.send(()).expect("receiver is alive");
                    batcher.flush().await
                })
            })
            .collect::<Vec<_>>();

        for _ in 0..count {
            arrived_rx.recv().await.expect("caller arrival");
        }
        callers
    }

    #[tokio::test]
    async fn callers_queued_before_a_flush_share_its_result() {
        let target = TestFlush::gated();
        let batcher = batcher(Arc::clone(&target));
        let turn = batcher.turn.lock().await;
        let queued = spawn_arrived(&batcher, 4).await;
        drop(turn);

        target.await_started(1).await;
        target.release_one();
        for caller in queued {
            caller.await.expect("caller task").expect("shared flush");
        }
        assert_eq!(target.started(), 1);
    }

    #[tokio::test]
    async fn each_lone_caller_gets_its_own_flush() {
        let target = TestFlush::open();
        let batcher = batcher(Arc::clone(&target));

        batcher.flush().await.expect("first flush");
        assert_eq!(target.started(), 1);

        batcher.flush().await.expect("second flush");
        assert_eq!(target.started(), 2);
    }

    #[tokio::test]
    async fn callers_queued_behind_a_flush_share_one_follow_up() {
        let target = TestFlush::gated();
        let batcher = batcher(Arc::clone(&target));

        let leader = tokio::spawn({
            let batcher = Arc::clone(&batcher);
            async move { batcher.flush().await }
        });
        target.await_started(1).await;

        let queued = spawn_arrived(&batcher, 3).await;

        target.release_one();
        leader
            .await
            .expect("leader task")
            .expect("leader flush succeeds");

        target.await_started(2).await;
        target.release_one();
        for caller in queued {
            caller
                .await
                .expect("queued task")
                .expect("queued flush succeeds");
        }

        assert_eq!(target.started(), 2);
    }

    #[tokio::test]
    async fn a_failed_flush_is_reported_to_every_caller_it_serves() {
        let target = TestFlush::gated();
        target.fail(true);
        let batcher = batcher(Arc::clone(&target));

        let leader = tokio::spawn({
            let batcher = Arc::clone(&batcher);
            async move { batcher.flush().await }
        });
        target.await_started(1).await;
        let queued = spawn_arrived(&batcher, 2).await;

        target.release_one();
        assert!(matches!(
            leader.await.expect("leader task"),
            Err(Error::SlateDb(_))
        ));

        target.await_started(2).await;
        target.release_one();
        for caller in queued {
            assert!(matches!(
                caller.await.expect("queued task"),
                Err(Error::SlateDb(_))
            ));
        }

        assert_eq!(target.started(), 2);
    }

    #[tokio::test]
    async fn a_failure_does_not_poison_later_flushes() {
        let target = TestFlush::open();
        target.fail(true);
        let batcher = batcher(Arc::clone(&target));

        assert!(matches!(batcher.flush().await, Err(Error::SlateDb(_))));

        target.fail(false);
        batcher.flush().await.expect("retry after a failed flush");
        assert_eq!(target.started(), 2);
    }
}
