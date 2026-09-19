use std::sync::Mutex;
use std::time::Duration;

use crate::fs::SlateDbFileSystem;

#[derive(Clone, Copy, Debug, Default)]
pub struct IoOperationStats {
    pub request_count: u64,
    pub average_latency: Duration,
    pub stddev_latency: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IoStats {
    pub read: IoOperationStats,
    pub write: IoOperationStats,
}

#[derive(Debug, Default)]
struct RunningStats {
    count: u64,
    mean: f64,
    squared_deviation_sum: f64,
}

impl RunningStats {
    fn record(&mut self, duration: Duration) {
        let latency = duration.as_secs_f64();
        self.count += 1;
        let delta = latency - self.mean;
        self.mean += delta / self.count as f64;
        let delta_after_mean_update = latency - self.mean;
        self.squared_deviation_sum += delta * delta_after_mean_update;
    }

    fn snapshot(&self) -> IoOperationStats {
        let variance = if self.count == 0 {
            0.0
        } else {
            (self.squared_deviation_sum / self.count as f64).max(0.0)
        };
        IoOperationStats {
            request_count: self.count,
            average_latency: Duration::from_secs_f64(self.mean),
            stddev_latency: Duration::from_secs_f64(variance.sqrt()),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct IoMetrics {
    read: Mutex<RunningStats>,
    write: Mutex<RunningStats>,
}

impl IoMetrics {
    pub(crate) fn snapshot(&self) -> IoStats {
        IoStats {
            read: lock_or_recover(&self.read).snapshot(),
            write: lock_or_recover(&self.write).snapshot(),
        }
    }

    pub(crate) fn record_read(&self, duration: Duration) {
        lock_or_recover(&self.read).record(duration);
    }

    pub(crate) fn record_write(&self, duration: Duration) {
        lock_or_recover(&self.write).record(duration);
    }
}

impl SlateDbFileSystem {
    pub fn io_stats(&self) -> IoStats {
        self.io_metrics.snapshot()
    }
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_population_average_and_standard_deviation() {
        let mut stats = RunningStats::default();
        stats.record(Duration::from_secs(1));
        stats.record(Duration::from_secs(3));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.request_count, 2);
        assert_eq!(snapshot.average_latency, Duration::from_secs(2));
        assert_eq!(snapshot.stddev_latency, Duration::from_secs(1));
    }
}
