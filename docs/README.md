# DuckDB Object Storage

`duckdb_object_storage` provides a DuckDB virtual filesystem backed by
SlateDB. DuckDB databases attached through `duckdb_objfs://` can be stored in
memory, on the local filesystem, or in S3-compatible object storage.

## Local filesystem backend

Local storage is the default backend and requires no configuration:

```sql
LOAD duckdb_object_storage;

ATTACH 'duckdb_objfs://database.db' AS object_db;
CREATE TABLE object_db.items(i INTEGER);
INSERT INTO object_db.items VALUES (1), (2);
CHECKPOINT object_db;
```

By default, SlateDB stores its data under `.duckdb_objfs` in the current
working directory. Set `duckdb_objfs_root` before the first
`duckdb_objfs://` access to choose another directory:

```sql
SET duckdb_objfs_root = '/var/lib/duckdb-objfs';
ATTACH 'duckdb_objfs://database.db' AS object_db;
```

## S3 backend

The extension uses DuckDB's Secret Manager for credentials and endpoint
configuration. Install and load `cache_httpfs` to register the standard
`TYPE S3` secret:

```sql
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
LOAD duckdb_object_storage;

CREATE OR REPLACE SECRET duckdb_objfs_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID getenv('AWS_ACCESS_KEY_ID'),
    SECRET getenv('AWS_SECRET_ACCESS_KEY'),
    REGION 'us-east-1',
    SCOPE 's3://my-bucket/duckdb-data'
);

SET duckdb_objfs_backend = 's3';
SET duckdb_objfs_bucket = 'my-bucket';
SET duckdb_objfs_root = 'duckdb-data';

ATTACH 'duckdb_objfs://database.db' AS object_db;
```

For MinIO or another S3-compatible service, add endpoint options to the
secret:

```sql
CREATE OR REPLACE SECRET duckdb_objfs_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID getenv('AWS_ACCESS_KEY_ID'),
    SECRET getenv('AWS_SECRET_ACCESS_KEY'),
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9000',
    USE_SSL false,
    URL_STYLE 'path',
    SCOPE 's3://my-bucket/duckdb-data'
);
```

Credentials, session tokens, regions, endpoints, and URL style come from the
scope-matching S3 secret. They are not stored in extension settings.

## Cache configuration

The extension enables SlateDB's Foyer in-memory cache by default. Data blocks
use up to 512 MiB and SST metadata uses up to 128 MiB. These limits are
independent of DuckDB's buffer-manager memory limit:

Default cache settings are:

- `duckdb_objfs_memory_cache_size`: 536870912 bytes (512 MiB);
- `duckdb_objfs_metadata_cache_size`: 134217728 bytes (128 MiB);
- `duckdb_objfs_cache_shards`: `0` (automatic);
- `duckdb_objfs_persistent_cache_path`: empty (persistent cache disabled);
- `duckdb_objfs_persistent_cache_size`: 17179869184 bytes (16 GiB, used only
  when a persistent cache path is set);
- `duckdb_objfs_persistent_cache_part_size`: 4194304 bytes (4 MiB);
- `duckdb_objfs_persistent_cache_on_flush`: `false`;
- `duckdb_objfs_persistent_cache_on_compaction`: `false`.

For example, to reduce the in-memory limits:

```sql
SET duckdb_objfs_memory_cache_size = 268435456;   -- 256 MiB
SET duckdb_objfs_metadata_cache_size = 67108864; -- 64 MiB
SET duckdb_objfs_cache_shards = 0;                -- automatic
```

Set either cache size to zero to disable that part of the in-memory cache.

The persistent local SST cache is disabled by default. Set a local path to
enable it, which is most useful with the S3 backend:

```sql
SET duckdb_objfs_persistent_cache_path = '/var/cache/duckdb-objfs';
SET duckdb_objfs_persistent_cache_size = 17179869184;     -- 16 GiB
SET duckdb_objfs_persistent_cache_part_size = 4194304;    -- 4 MiB
SET duckdb_objfs_persistent_cache_on_flush = false;
SET duckdb_objfs_persistent_cache_on_compaction = false;
```

The part size must be a non-zero multiple of 1024 bytes. Cache settings are
read once when the filesystem is initialized. Configure them before the first
`duckdb_objfs://` access; changing these settings afterward does not
reconfigure the running cache.

## Cache statistics

After the first `duckdb_objfs://` access, query cumulative cache statistics
with:

```sql
SELECT * FROM duckdb_objfs_cache_stats();
```

The function returns rows for `memory_data`, `memory_metadata`, and
`persistent` caches. It reports hit and miss counts and hit rate. The
persistent row also reports its current entry count, size, and eviction
totals. Metadata statistics aggregate SlateDB's index, filter, and
SST-statistics cache entries.

`hit_rate` is `NULL` until a cache has been accessed. In-memory `entry_count`,
`size_bytes`, and eviction fields are `NULL` because SlateDB's Foyer cache
adapter does not expose those values. The function returns no rows before the
filesystem is initialized.

## I/O statistics

Query cumulative OpenDAL I/O statistics after the filesystem is initialized:

```sql
SELECT * FROM duckdb_objfs_io_stats();
```

The function returns one row each for `read`, `write`, `stat`, `delete`, and
`list`, with `request_count`, `average_latency_ms`, and `stddev_latency_ms`.
Standard deviation is calculated over the complete observed population.
Streaming read and list latency runs until the stream finishes or is dropped;
write and delete latency runs until the operation is closed or dropped. The
function returns no rows before the first `duckdb_objfs://` access.

## Path semantics

`duckdb_objfs://` paths identify logical DuckDB files:

```text
duckdb_objfs://analytics/report.db
             -> analytics/report.db
```

The URI does not contain the local directory, S3 bucket, or physical object
key. `duckdb_objfs_root` selects the local directory or S3 object prefix;
SlateDB owns the physical layout below that location.

## File placement

The native DuckDB database and every file needed for durable recovery stay in
object storage:

- the main database file;
- the write-ahead log (`.wal`);
- checkpoint and recovery WAL files (`.wal.checkpoint` and `.wal.recovery`).

Machine-local runtime files do not belong in object storage. DuckDB spill
files (`duckdb_temp_storage_*.tmp` and `duckdb_temp_block-*.block`) continue to
use its `temp_directory`. If that setting points at a `duckdb_objfs://` path,
the extension replaces it with a unique directory under the platform's local
temporary directory before opening the object database. Set `temp_directory`
to an explicit local path before `ATTACH` to control its location.

Other files are routed by their own path. Extension binaries, persisted
secrets, logs, and local `COPY` outputs stay local unless their path explicitly
uses `duckdb_objfs://`.

## Read-only reopen

```sql
DETACH object_db;
ATTACH 'duckdb_objfs://database.db' AS object_db (READ_ONLY);
SELECT * FROM object_db.items;
```

## Read consistency

Read-only attachments share a cached SlateDB reader that follows the writer's
manifest roughly every 10 seconds
([`DbReaderOptions::manifest_poll_interval`](https://slatedb.io/docs/design/readers/)).
Data committed by another process can therefore stay invisible for up to that
interval — including across `DETACH`/`ATTACH`, which reuses the cached reader
instead of rebuilding it.

While a read-write attachment exists in the same process, read-only opens are
served by the read-write SlateDB instance and always see the latest committed
state.

## Single writer

Only one process may hold a read-write attachment of a `duckdb_objfs://`
database at a time. The extension does not implement file locking, so a second
read-write `ATTACH` does not fail up front. Instead, SlateDB's manifest
fencing is the backstop: the second writer bumps the manifest's writer epoch
and takes over, and the first process starts failing with I/O errors on its
next flush or checkpoint.

Fencing keeps the stored data consistent, but the delayed failure is confusing
enough that concurrent writers must be treated as a configuration error, not a
supported mode. Use external coordination or a single-writer deployment to
guarantee exclusivity. Read-only attachments are unaffected: any number of
processes may attach read-only alongside the writer and each other.

