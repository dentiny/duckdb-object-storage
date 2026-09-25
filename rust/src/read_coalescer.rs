use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crate::{Error, Result};

const BATCH_WINDOW: Duration = Duration::from_micros(200);
const MAX_BATCH_REQUESTS: usize = 16;
pub(crate) const MAX_MERGED_READ_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadRequest {
    pub(crate) offset: u64,
    pub(crate) len: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MergedRead {
    pub(crate) offset: u64,
    pub(crate) len: usize,
    pub(crate) request_indices: Vec<usize>,
}

struct PendingRead {
    request: ReadRequest,
    result: Mutex<Option<Result<Vec<u8>>>>,
    ready: Condvar,
}

impl PendingRead {
    fn new(request: ReadRequest) -> Self {
        Self {
            request,
            result: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, result: Result<Vec<u8>>) {
        *lock_or_recover(&self.result) = Some(result);
        self.ready.notify_one();
    }

    fn wait(&self) -> Result<Vec<u8>> {
        let mut result = lock_or_recover(&self.result);
        while result.is_none() {
            result = self
                .ready
                .wait(result)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        result.take().expect("completed read must have a result")
    }
}

#[derive(Default)]
struct CoalescerState {
    leader_active: bool,
    queue: VecDeque<Arc<PendingRead>>,
}

/// Briefly batches synchronous positional reads so adjacent DuckDB blocks reach
/// the async file layer as one larger read.
///
/// This coordinator runs on DuckDB's calling threads, not Tokio workers. Its
/// mutexes are released before `execute` enters the async runtime.
pub(crate) struct ReadCoalescer {
    state: Mutex<CoalescerState>,
    queue_changed: Condvar,
    batch_window: Duration,
}

impl ReadCoalescer {
    pub(crate) fn new() -> Self {
        Self::with_batch_window(BATCH_WINDOW)
    }

    fn with_batch_window(batch_window: Duration) -> Self {
        Self {
            state: Mutex::new(CoalescerState::default()),
            queue_changed: Condvar::new(),
            batch_window,
        }
    }

    pub(crate) fn read(
        &self,
        request: ReadRequest,
        mut execute: impl FnMut(&[ReadRequest]) -> Vec<Result<Vec<u8>>>,
    ) -> Result<Vec<u8>> {
        let pending = Arc::new(PendingRead::new(request));
        let is_leader = {
            let mut state = lock_or_recover(&self.state);
            state.queue.push_back(Arc::clone(&pending));
            let is_leader = !state.leader_active;
            if is_leader {
                state.leader_active = true;
            }
            self.queue_changed.notify_one();
            is_leader
        };

        if is_leader {
            self.run_leader(&mut execute);
        }
        pending.wait()
    }

    fn run_leader(&self, execute: &mut impl FnMut(&[ReadRequest]) -> Vec<Result<Vec<u8>>>) {
        let mut first_batch = true;
        loop {
            let batch = self.take_batch(first_batch);
            first_batch = false;
            let requests = batch
                .iter()
                .map(|pending| pending.request)
                .collect::<Vec<_>>();
            let results = catch_unwind(AssertUnwindSafe(|| execute(&requests)));

            match results {
                Ok(results) if results.len() == batch.len() => {
                    for (pending, result) in batch.into_iter().zip(results) {
                        pending.complete(result);
                    }
                }
                Ok(results) => {
                    let error = Error::invalid_argument(format!(
                        "coalesced read returned {} results for {} requests",
                        results.len(),
                        batch.len()
                    ));
                    for pending in batch {
                        pending.complete(Err(error.clone()));
                    }
                }
                Err(_) => {
                    let error = Error::invalid_argument("coalesced read executor panicked");
                    for pending in batch {
                        pending.complete(Err(error.clone()));
                    }
                    self.fail_queued(error);
                    return;
                }
            }

            let mut state = lock_or_recover(&self.state);
            if state.queue.is_empty() {
                state.leader_active = false;
                return;
            }
        }
    }

    fn take_batch(&self, wait_for_more: bool) -> Vec<Arc<PendingRead>> {
        let mut state = lock_or_recover(&self.state);
        if wait_for_more && state.queue.len() < MAX_BATCH_REQUESTS {
            let (next, _) = self
                .queue_changed
                .wait_timeout_while(state, self.batch_window, |state| {
                    state.queue.len() < MAX_BATCH_REQUESTS
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
        }
        let count = state.queue.len().min(MAX_BATCH_REQUESTS);
        state.queue.drain(..count).collect()
    }

    fn fail_queued(&self, error: Error) {
        let queued = {
            let mut state = lock_or_recover(&self.state);
            state.leader_active = false;
            state.queue.drain(..).collect::<Vec<_>>()
        };
        for pending in queued {
            pending.complete(Err(error.clone()));
        }
    }
}

pub(crate) fn merge_read_requests(requests: &[ReadRequest]) -> Result<Vec<MergedRead>> {
    let mut order = (0..requests.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|index| requests[*index].offset);
    let mut merged: Vec<MergedRead> = Vec::new();

    for index in order {
        let request = requests[index];
        let request_end = request
            .offset
            .checked_add(request.len as u64)
            .ok_or_else(|| Error::invalid_argument("coalesced read offset overflow"))?;
        let can_merge = merged.last().is_some_and(|current| {
            let current_end = current.offset + current.len as u64;
            let merged_end = current_end.max(request_end);
            request.offset <= current_end
                && merged_end - current.offset <= MAX_MERGED_READ_BYTES as u64
        });

        if can_merge {
            let current = merged.last_mut().expect("checked above");
            let merged_end = (current.offset + current.len as u64).max(request_end);
            current.len = usize::try_from(merged_end - current.offset)
                .expect("merged read is bounded by usize constant");
            current.request_indices.push(index);
        } else {
            merged.push(MergedRead {
                offset: request.offset,
                len: request.len,
                request_indices: vec![index],
            });
        }
    }
    Ok(merged)
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn merges_adjacent_overlapping_and_out_of_order_requests() {
        let requests = [
            ReadRequest { offset: 16, len: 8 },
            ReadRequest { offset: 0, len: 8 },
            ReadRequest { offset: 8, len: 8 },
            ReadRequest { offset: 40, len: 4 },
            ReadRequest { offset: 14, len: 4 },
        ];

        assert_eq!(
            merge_read_requests(&requests).unwrap(),
            vec![
                MergedRead {
                    offset: 0,
                    len: 24,
                    request_indices: vec![1, 2, 4, 0],
                },
                MergedRead {
                    offset: 40,
                    len: 4,
                    request_indices: vec![3],
                },
            ]
        );
    }

    #[test]
    fn does_not_merge_across_a_gap_or_the_size_limit() {
        let requests = [
            ReadRequest {
                offset: 0,
                len: MAX_MERGED_READ_BYTES,
            },
            ReadRequest {
                offset: MAX_MERGED_READ_BYTES as u64,
                len: 1,
            },
            ReadRequest {
                offset: (MAX_MERGED_READ_BYTES + 2) as u64,
                len: 1,
            },
        ];

        assert_eq!(merge_read_requests(&requests).unwrap().len(), 3);
    }

    #[test]
    fn merges_duckdb_blocks_despite_the_twelve_kib_file_header() {
        const DUCKDB_BLOCK_START: u64 = 3 * 4096;
        const DUCKDB_BLOCK_SIZE: usize = 256 * 1024;
        let requests = (0..4)
            .map(|block| ReadRequest {
                offset: DUCKDB_BLOCK_START + block * DUCKDB_BLOCK_SIZE as u64,
                len: DUCKDB_BLOCK_SIZE,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            merge_read_requests(&requests).unwrap(),
            vec![MergedRead {
                offset: DUCKDB_BLOCK_START,
                len: 4 * DUCKDB_BLOCK_SIZE,
                request_indices: vec![0, 1, 2, 3],
            }]
        );
    }

    #[test]
    fn batches_concurrent_callers_and_returns_their_own_data() {
        let coalescer = Arc::new(ReadCoalescer::with_batch_window(Duration::from_millis(50)));
        let barrier = Arc::new(Barrier::new(9));
        let executions = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();

        for index in 0..8 {
            let coalescer = Arc::clone(&coalescer);
            let barrier = Arc::clone(&barrier);
            let executions = Arc::clone(&executions);
            threads.push(thread::spawn(move || {
                barrier.wait();
                coalescer
                    .read(
                        ReadRequest {
                            offset: index * 8,
                            len: 8,
                        },
                        |requests| {
                            executions.fetch_add(1, Ordering::SeqCst);
                            requests
                                .iter()
                                .map(|request| Ok(vec![request.offset as u8; request.len]))
                                .collect()
                        },
                    )
                    .unwrap()
            }));
        }

        barrier.wait();
        for (index, thread) in threads.into_iter().enumerate() {
            assert_eq!(thread.join().unwrap(), vec![(index * 8) as u8; 8]);
        }
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rejects_executor_result_count_mismatch() {
        let coalescer = ReadCoalescer::new();
        let error = coalescer
            .read(ReadRequest { offset: 0, len: 1 }, |_| Vec::new())
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("returned 0 results for 1 requests"));
    }
}
