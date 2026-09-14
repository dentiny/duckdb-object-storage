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

## Path semantics

`duckdb_objfs://` paths identify logical DuckDB files:

```text
duckdb_objfs://analytics/report.db
             -> analytics/report.db
```

The URI does not contain the local directory, S3 bucket, or physical object
key. `duckdb_objfs_root` selects the local directory or S3 object prefix;
SlateDB owns the physical layout below that location.

## Read-only reopen

```sql
DETACH object_db;
ATTACH 'duckdb_objfs://database.db' AS object_db (READ_ONLY);
SELECT * FROM object_db.items;
```

