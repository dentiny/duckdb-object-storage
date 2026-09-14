# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or
```bash
make test_debug
```

## S3 end-to-end test

The S3 test starts a temporary MinIO server, creates a standard DuckDB `TYPE
S3` secret, and verifies create, checkpoint, detach, and read-only reattach
through `duckdb_objfs://`.

Build the RelWithDebInfo DuckDB binary and start Docker before running:

```bash
make reldebug
make test-s3
```

The extension reads `duckdb_objfs_bucket` and `duckdb_objfs_root` as
non-secret settings. Endpoint and credentials are resolved from the
scope-matching S3 secret and are never placed in extension settings.

## Local filesystem backend

Set the backend and a storage directory before the first `duckdb_objfs://`
access:

```sql
SET duckdb_objfs_backend = 'local';
SET duckdb_objfs_local_path = '/var/lib/duckdb-objfs';
ATTACH 'duckdb_objfs://database.db' AS object_db;
```

The `memory` backend remains available for tests. The default backend is `s3`.
