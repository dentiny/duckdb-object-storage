#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Extracts the backend-independent logical file name from a duckdb_objfs URI.
//!
//! The URI identifies a logical DuckDB file, not a local directory, S3 bucket, or S3 object key. Backend location is
//! configured separately with duckdb_objfs_root and, for S3, duckdb_objfs_bucket.
//!
//! Examples:
//!   duckdb_objfs://database.db     -> database.db
//!   duckdb_objfs:/dir/database.db  -> dir/database.db
string GetLogicalPath(const string &path);

//! Returns the platform's default machine-local temporary directory.
string GetDefaultTemporaryDirectory();

} // namespace duckdb
