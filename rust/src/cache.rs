use std::sync::Arc;

use slatedb::config::{DbReaderOptions, Settings};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue, Metrics};

use crate::fs::{SlateDbFileSystem, SlateDbReadOnlyFileSystem};

#[derive(Clone)]
pub struct CacheConfig {
    /// Maximum bytes retained in the in-memory data-block cache; zero disables it.
    pub block_cache_size_bytes: u64,
    /// Maximum bytes retained in the in-memory SST metadata cache; zero disables it.
    pub metadata_cache_size_bytes: u64,
    /// Number of cache shards, or `None` to use the implementation default.
    pub cache_shards: Option<usize>,
    /// Local directory for persistent cached SST parts, or `None` to disable persistence.
    pub persistent_cache_path: Option<std::path::PathBuf>,
    /// Maximum total size of the persistent cache.
    pub persistent_cache_size_bytes: usize,
    /// Size of each persistent cache part; must be a multiple of 1024 bytes.
    pub persistent_cache_part_size_bytes: usize,
    /// Whether memtable flush output should be inserted into the persistent cache.
    pub persistent_cache_on_flush: bool,
    /// Whether compaction output should be inserted into the persistent cache.
    pub persistent_cache_on_compaction: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            block_cache_size_bytes: slatedb::db_cache::DEFAULT_BLOCK_CACHE_CAPACITY,
            metadata_cache_size_bytes: slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY,
            cache_shards: None,
            persistent_cache_path: None,
            persistent_cache_size_bytes: 16 * 1024 * 1024 * 1024,
            persistent_cache_part_size_bytes: 4 * 1024 * 1024,
            persistent_cache_on_flush: false,
            persistent_cache_on_compaction: false,
        }
    }
}

impl CacheConfig {
    pub(crate) fn apply_to_settings(&self, settings: &mut Settings) {
        let Some(path) = &self.persistent_cache_path else {
            return;
        };
        settings.object_store_cache_options.root_folder = Some(path.clone());
        settings.object_store_cache_options.max_cache_size_bytes =
            Some(self.persistent_cache_size_bytes);
        settings.object_store_cache_options.part_size_bytes = self.persistent_cache_part_size_bytes;
        settings.object_store_cache_options.cache_on_flush = self.persistent_cache_on_flush;
        settings.object_store_cache_options.cache_on_compaction =
            self.persistent_cache_on_compaction;
    }

    pub(crate) fn apply_to_reader_options(&self, options: &mut DbReaderOptions) {
        let Some(path) = &self.persistent_cache_path else {
            return;
        };
        options.object_store_cache_options.root_folder = Some(path.clone());
        options.object_store_cache_options.max_cache_size_bytes =
            Some(self.persistent_cache_size_bytes);
        options.object_store_cache_options.part_size_bytes = self.persistent_cache_part_size_bytes;
    }

    pub(crate) fn build_db_cache(&self) -> Option<Arc<dyn DbCache>> {
        if self.block_cache_size_bytes == 0 && self.metadata_cache_size_bytes == 0 {
            return None;
        }
        let block_cache = build_foyer_cache(self.block_cache_size_bytes, self.cache_shards);
        let metadata_cache = build_foyer_cache(self.metadata_cache_size_bytes, self.cache_shards);
        Some(Arc::new(
            SplitCache::new()
                .with_block_cache(block_cache)
                .with_meta_cache(metadata_cache)
                .build(),
        ))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CacheStats {
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub persistent_cache_hits: u64,
    pub persistent_cache_misses: u64,
    pub persistent_cache_entries: u64,
    pub persistent_cache_size_bytes: u64,
    pub persistent_cache_evictions: u64,
    pub persistent_cache_evicted_bytes: u64,
}

pub(crate) struct CacheMetrics {
    recorder: Arc<DefaultMetricsRecorder>,
}

impl CacheMetrics {
    pub(crate) fn new() -> Self {
        Self {
            recorder: Arc::new(DefaultMetricsRecorder::new()),
        }
    }

    pub(crate) fn recorder(&self) -> Arc<DefaultMetricsRecorder> {
        Arc::clone(&self.recorder)
    }
}

impl SlateDbFileSystem {
    pub fn cache_stats(&self) -> CacheStats {
        cache_stats(&self.cache_metrics)
    }
}

impl SlateDbReadOnlyFileSystem {
    pub fn cache_stats(&self) -> CacheStats {
        cache_stats(&self.cache_metrics)
    }
}

fn cache_stats(cache_metrics: &CacheMetrics) -> CacheStats {
    const DB_CACHE_ACCESS_COUNT: &str = "slatedb.db_cache.access_count";
    const PERSISTENT_HIT_COUNT: &str = "slatedb.object_store_cache.part_hit_count";
    const PERSISTENT_ACCESS_COUNT: &str = "slatedb.object_store_cache.part_access_count";
    const PERSISTENT_CACHE_KEYS: &str = "slatedb.object_store_cache.cache_keys";
    const PERSISTENT_CACHE_BYTES: &str = "slatedb.object_store_cache.cache_bytes";
    const PERSISTENT_EVICTED_KEYS: &str = "slatedb.object_store_cache.evicted_keys";
    const PERSISTENT_EVICTED_BYTES: &str = "slatedb.object_store_cache.evicted_bytes";

    let metrics = cache_metrics.recorder.snapshot();
    let block_hits = metric_counter(
        &metrics,
        DB_CACHE_ACCESS_COUNT,
        &[("entry_kind", "data_block"), ("result", "hit")],
    );
    let block_misses = metric_counter(
        &metrics,
        DB_CACHE_ACCESS_COUNT,
        &[("entry_kind", "data_block"), ("result", "miss")],
    );
    let metadata_hits = ["filter", "index", "stats"]
        .iter()
        .map(|entry_kind| {
            metric_counter(
                &metrics,
                DB_CACHE_ACCESS_COUNT,
                &[("entry_kind", entry_kind), ("result", "hit")],
            )
        })
        .sum();
    let metadata_misses = ["filter", "index", "stats"]
        .iter()
        .map(|entry_kind| {
            metric_counter(
                &metrics,
                DB_CACHE_ACCESS_COUNT,
                &[("entry_kind", entry_kind), ("result", "miss")],
            )
        })
        .sum();
    let persistent_hits = metric_counter(&metrics, PERSISTENT_HIT_COUNT, &[]);
    let persistent_accesses = metric_counter(&metrics, PERSISTENT_ACCESS_COUNT, &[]);

    CacheStats {
        block_cache_hits: block_hits,
        block_cache_misses: block_misses,
        metadata_cache_hits: metadata_hits,
        metadata_cache_misses: metadata_misses,
        persistent_cache_hits: persistent_hits,
        persistent_cache_misses: persistent_accesses.saturating_sub(persistent_hits),
        persistent_cache_entries: metric_gauge(&metrics, PERSISTENT_CACHE_KEYS, &[]),
        persistent_cache_size_bytes: metric_gauge(&metrics, PERSISTENT_CACHE_BYTES, &[]),
        persistent_cache_evictions: metric_counter(&metrics, PERSISTENT_EVICTED_KEYS, &[]),
        persistent_cache_evicted_bytes: metric_counter(&metrics, PERSISTENT_EVICTED_BYTES, &[]),
    }
}

fn build_foyer_cache(max_capacity: u64, shards: Option<usize>) -> Option<Arc<dyn DbCache>> {
    if max_capacity == 0 {
        return None;
    }
    let mut options = FoyerCacheOptions {
        max_capacity,
        ..Default::default()
    };
    if let Some(shards) = shards {
        options.shards = shards;
    }
    Some(Arc::new(FoyerCache::new_with_opts(options)))
}

fn metric_counter(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> u64 {
    metrics
        .by_name_and_labels(name, labels)
        .and_then(|metric| match metric.value {
            MetricValue::Counter(value) => Some(value),
            _ => None,
        })
        .unwrap_or_default()
}

fn metric_gauge(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> u64 {
    metrics
        .by_name_and_labels(name, labels)
        .and_then(|metric| match metric.value {
            MetricValue::Gauge(value) => u64::try_from(value).ok(),
            _ => None,
        })
        .unwrap_or_default()
}
