use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::services::{Fs, Memory, S3};
use opendal::Operator;
use slatedb::config::Settings;
use slatedb::Db;

use crate::cache::{CacheConfig, CacheMetrics};
use crate::database_metadata::DatabaseMetadata;
use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_handle::SlateFileHandle;
use crate::flags::FileOpenFlags;
use crate::io_metrics::IoMetrics;
use crate::opendal_io_metrics_layer::IoMetricsLayer;

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "duckdb_objfs:";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

/// SlateDB-backed filesystem owner.
///
/// One live database is kept for the lifetime of the filesystem. The FFI layer
/// owns the runtime used to drive these asynchronous operations.
pub struct SlateDbFileSystem {
    db: Option<Arc<Db>>,
    pub(crate) cache_metrics: CacheMetrics,
    pub(crate) io_metrics: Arc<IoMetrics>,
}

pub struct S3StorageConfig {
    /// S3 bucket that stores SlateDB objects.
    pub bucket: String,
    /// Optional prefix within the bucket reserved for this filesystem.
    pub root: Option<String>,
    /// Optional S3-compatible service endpoint.
    pub endpoint: Option<String>,
    /// AWS region used to sign S3 requests.
    pub region: Option<String>,
    /// Optional access-key identifier.
    pub key_id: Option<String>,
    /// Optional secret access key.
    pub secret: Option<String>,
    /// Optional temporary-credential session token.
    pub session_token: Option<String>,
    /// Whether the endpoint should use HTTPS.
    pub use_ssl: bool,
    /// Whether S3 requests should use virtual-host-style addressing.
    pub virtual_host_style: bool,
}

impl SlateDbFileSystem {
    /// Opens SlateDB on top of an OpenDAL storage operator.
    pub async fn open(database_path: &str, operator: Operator) -> Result<Self> {
        Self::open_with_cache_config(database_path, operator, CacheConfig::default()).await
    }

    pub async fn open_with_cache_config(
        database_path: &str,
        operator: Operator,
        cache_config: CacheConfig,
    ) -> Result<Self> {
        if database_path.is_empty() {
            return Err(Error::InvalidArgument(ErrorStruct::new(
                "database path must not be empty".to_string(),
                ErrorStatus::Permanent,
            )));
        }

        let io_metrics = Arc::new(IoMetrics::default());
        let operator = operator.layer(IoMetricsLayer::new(Arc::clone(&io_metrics)));
        let object_store = Arc::new(OpendalStore::new(operator));
        let cache_metrics = CacheMetrics::new();
        let mut settings = Settings::default();
        cache_config.apply_to_settings(&mut settings);

        let mut builder = Db::builder(database_path, object_store)
            .with_settings(settings)
            .with_metrics_recorder(cache_metrics.recorder());
        match cache_config.build_db_cache() {
            Some(cache) => builder = builder.with_db_cache(cache),
            None => builder = builder.with_db_cache_disabled(),
        }
        let db = builder.build().await?;
        Ok(Self {
            db: Some(Arc::new(db)),
            cache_metrics,
            io_metrics,
        })
    }

    pub async fn open_s3(database_path: &str, config: S3StorageConfig) -> Result<Self> {
        let operator = build_s3_operator(config)?;
        Self::open(database_path, operator).await
    }

    pub async fn open_s3_with_cache_config(
        database_path: &str,
        config: S3StorageConfig,
        cache_config: CacheConfig,
    ) -> Result<Self> {
        let operator = build_s3_operator(config)?;
        Self::open_with_cache_config(database_path, operator, cache_config).await
    }

    pub async fn open_in_memory(database_path: &str) -> Result<Self> {
        Self::open_in_memory_with_cache_config(database_path, CacheConfig::default()).await
    }

    pub async fn open_in_memory_with_cache_config(
        database_path: &str,
        cache_config: CacheConfig,
    ) -> Result<Self> {
        let operator = Operator::new(Memory::default()).map_err(|source| {
            Error::Io(
                ErrorStruct::new(
                    "failed to initialize OpenDAL memory storage".to_string(),
                    ErrorStatus::Permanent,
                )
                .with_source(source),
            )
        })?;
        Self::open_with_cache_config(database_path, operator, cache_config).await
    }

    pub async fn open_local(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        Self::open_local_with_cache_config(database_path, root, CacheConfig::default()).await
    }

    pub async fn open_local_with_cache_config(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
        cache_config: CacheConfig,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let root_str = root.to_str().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                format!("local OpenDAL root is not valid UTF-8: {}", root.display()),
                ErrorStatus::Permanent,
            ))
        })?;
        tokio::fs::create_dir_all(&root).await?;

        let operator = Operator::new(Fs::default().root(root_str)).map_err(|source| {
            Error::Io(
                ErrorStruct::new(
                    format!("failed to initialize OpenDAL storage at {}", root.display()),
                    ErrorStatus::Permanent,
                )
                .with_source(source),
            )
        })?;
        Self::open_with_cache_config(database_path, operator, cache_config).await
    }

    /// Open a logical file, creating its persistent catalog entry when allowed.
    ///
    // TODO: Evaluate adding a read-through in-memory path catalog; SlateDB must remain the source of truth.
    pub async fn open_file(&self, path: &str, flags: FileOpenFlags) -> Result<SlateFileHandle> {
        flags.validate()?;

        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        let database_metadata = DatabaseMetadata::new(Arc::clone(&db));
        let (file_id, metadata) = database_metadata
            .prepare_file_for_open(
                path,
                flags.create,
                flags.truncate_existing,
                flags.exclusive_create,
            )
            .await?;

        SlateFileHandle::new(db, file_id, metadata, flags)
    }

    /// Returns whether a logical path is present in the database catalog.
    pub async fn file_exists(&self, path: &str) -> Result<bool> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).file_exists(path).await
    }

    /// Atomically removes a logical file and all of its persisted state.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).remove_file(path).await
    }

    /// Atomically moves a logical file, replacing the destination if present.
    pub async fn move_file(&self, source: &str, target: &str) -> Result<()> {
        let db = Arc::clone(self.db.as_ref().ok_or_else(|| {
            Error::InvalidArgument(ErrorStruct::new(
                "filesystem is closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?);
        DatabaseMetadata::new(db).move_file(source, target).await
    }

    /// Flush and close the owned database.
    ///
    /// Calling `close` more than once is harmless. SlateDB marks all clones of
    /// the database closed.
    pub async fn close(&mut self) -> Result<()> {
        let Some(db) = self.db.take() else {
            return Ok(());
        };

        db.close().await?;
        Ok(())
    }

    pub fn name(&self) -> &'static str {
        NAME
    }

    pub fn can_handle(&self, path: &str) -> bool {
        path.starts_with(PREFIX)
    }
}

fn build_s3_operator(config: S3StorageConfig) -> Result<Operator> {
    if config.bucket.is_empty() {
        return Err(Error::InvalidArgument(ErrorStruct::new(
            "S3 bucket must not be empty".to_string(),
            ErrorStatus::Permanent,
        )));
    }
    if config.key_id.is_some() != config.secret.is_some() {
        return Err(Error::InvalidArgument(ErrorStruct::new(
            "S3 key ID and secret must be provided together".to_string(),
            ErrorStatus::Permanent,
        )));
    }
    if config.session_token.is_some() && config.key_id.is_none() {
        return Err(Error::InvalidArgument(ErrorStruct::new(
            "S3 session token requires a key ID and secret".to_string(),
            ErrorStatus::Permanent,
        )));
    }

    // Static libraries do not reliably run OpenDAL's process constructor.
    opendal::install_default();

    let mut builder = S3::default()
        .bucket(&config.bucket)
        .disable_config_load()
        .disable_ec2_metadata();

    if let Some(root) = config.root {
        builder = builder.root(&root);
    }
    if let Some(endpoint) = config.endpoint {
        let endpoint = if endpoint.contains("://") {
            endpoint
        } else if config.use_ssl {
            format!("https://{endpoint}")
        } else {
            format!("http://{endpoint}")
        };
        builder = builder.endpoint(&endpoint);
    }
    if let Some(region) = config.region {
        builder = builder.region(&region);
    }
    if let (Some(key_id), Some(secret)) = (config.key_id, config.secret) {
        builder = builder.access_key_id(&key_id).secret_access_key(&secret);
        if let Some(session_token) = config.session_token {
            builder = builder.session_token(&session_token);
        }
    } else {
        builder = builder.skip_signature();
    }
    if config.virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }

    Operator::new(builder).map_err(|source| {
        Error::Io(
            ErrorStruct::new(
                "failed to initialize OpenDAL S3 storage".to_string(),
                ErrorStatus::Permanent,
            )
            .with_source(source),
        )
    })
}

#[cfg(test)]
mod tests {
    use slatedb::WriteBatch;
    use tempfile::tempdir;

    use super::*;
    use crate::file_handle::FileHandle;
    use crate::keys;

    #[tokio::test]
    async fn creates_and_reopens_file_by_path() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let mut created = fs
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .expect("create file");
        assert_eq!(created.file_id(), 1);
        created
            .write(b"database contents")
            .await
            .expect("write file");
        created.close().await.expect("close created file");
        drop(created);

        let mut other = fs
            .open_file("other.db", FileOpenFlags::open_or_create())
            .await
            .expect("create second file");
        assert_eq!(other.file_id(), 2);
        other.close().await.expect("close second file");
        drop(other);

        let mut reopened = fs
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .expect("reopen file");
        assert_eq!(reopened.file_id(), 1);
        let mut contents = vec![0; "database contents".len()];
        assert_eq!(
            reopened.read(&mut contents).await.expect("read file"),
            contents.len()
        );
        assert_eq!(contents, b"database contents");
        reopened.close().await.expect("close reopened file");
        drop(reopened);

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn local_database_persists_across_reopen() {
        let root = tempdir().expect("temporary object-store root");

        let mut first = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("first open");
        let mut created = first
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .expect("create file");
        created.write(b"persisted").await.expect("write file");
        created.close().await.expect("close file");
        drop(created);
        first.close().await.expect("first close");

        let mut second = SlateDbFileSystem::open_local("persistent-db", root.path())
            .await
            .expect("second open");
        let mut reopened = second
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .expect("reopen file");
        let mut contents = vec![0; "persisted".len()];
        assert_eq!(
            reopened.read(&mut contents).await.expect("read file"),
            contents.len()
        );
        assert_eq!(contents, b"persisted");
        reopened.close().await.expect("close file");
        drop(reopened);
        second.close().await.expect("second close");
    }

    #[tokio::test]
    async fn opening_missing_file_without_create_fails() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let error = fs
            .open_file("missing.db", FileOpenFlags::read_only())
            .await
            .expect_err("missing file should fail");
        assert!(matches!(error, Error::FileNotFound(_)));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn create_or_truncate_clears_an_existing_file() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        let mut original = fs
            .open_file("recovery.wal", FileOpenFlags::open_or_create())
            .await
            .expect("create original");
        let file_id = original.file_id();
        original
            .write(b"stale wal contents")
            .await
            .expect("write original");
        original.close().await.expect("close original");
        drop(original);

        let mut replaced = fs
            .open_file("recovery.wal", FileOpenFlags::create_or_truncate())
            .await
            .expect("replace file");
        assert_eq!(replaced.file_id(), file_id);
        assert_eq!(replaced.file_size(), 0);
        assert!(fs
            .db
            .as_ref()
            .expect("open database")
            .scan_prefix(keys::chunk_prefix(file_id), ..)
            .await
            .expect("scan old chunks")
            .next()
            .await
            .expect("read old chunk")
            .is_none());
        replaced.write(b"new wal").await.expect("write replacement");
        replaced.close().await.expect("close replacement");
        drop(replaced);

        let mut reopened = fs
            .open_file("recovery.wal", FileOpenFlags::read_only())
            .await
            .expect("reopen replacement");
        let mut contents = [0; 7];
        assert_eq!(
            reopened
                .read(&mut contents)
                .await
                .expect("read replacement"),
            contents.len()
        );
        assert_eq!(&contents, b"new wal");
        reopened.close().await.expect("close reopened file");
        drop(reopened);

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn file_exists_uses_path_mapping_only() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        assert!(!fs.file_exists("database.db").await.expect("missing lookup"));

        let mut file = fs
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .expect("create file");
        let file_id = file.file_id();
        file.close().await.expect("close file");
        drop(file);

        assert!(fs
            .file_exists("database.db")
            .await
            .expect("existing lookup"));

        let mut batch = WriteBatch::new();
        batch.delete(keys::metadata_key(file_id));
        fs.db
            .as_ref()
            .expect("open database")
            .write(batch)
            .await
            .expect("delete metadata");

        assert!(fs.file_exists("database.db").await.expect("path lookup"));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn remove_file_deletes_catalog_metadata_and_chunks() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        let mut file = fs
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .expect("create file");
        let file_id = file.file_id();
        file.write(b"contents").await.expect("write file");
        file.close().await.expect("close file");
        drop(file);

        fs.remove_file("database.db").await.expect("remove file");

        assert!(!fs.file_exists("database.db").await.expect("path lookup"));
        let db = fs.db.as_ref().expect("open database");
        assert!(db
            .get(keys::metadata_key(file_id))
            .await
            .expect("read metadata")
            .is_none());
        assert!(db
            .scan_prefix(keys::chunk_prefix(file_id), ..)
            .await
            .expect("scan chunks")
            .next()
            .await
            .expect("read chunk")
            .is_none());
        assert!(matches!(
            fs.remove_file("database.db").await,
            Err(Error::FileNotFound(_))
        ));

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn move_file_preserves_source_and_replaces_destination() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");

        let mut source = fs
            .open_file("source.db", FileOpenFlags::open_or_create())
            .await
            .expect("create source");
        let source_file_id = source.file_id();
        source.write(b"source").await.expect("write source");
        source.close().await.expect("close source");
        drop(source);

        let mut target = fs
            .open_file("target.db", FileOpenFlags::open_or_create())
            .await
            .expect("create target");
        let replaced_file_id = target.file_id();
        target.write(b"target").await.expect("write target");
        target.close().await.expect("close target");
        drop(target);

        fs.move_file("source.db", "target.db")
            .await
            .expect("move file");

        assert!(!fs.file_exists("source.db").await.expect("source lookup"));
        let mut moved = fs
            .open_file("target.db", FileOpenFlags::read_only())
            .await
            .expect("open moved file");
        assert_eq!(moved.file_id(), source_file_id);
        let mut contents = [0; 6];
        assert_eq!(
            moved.read(&mut contents).await.expect("read moved file"),
            contents.len()
        );
        assert_eq!(&contents, b"source");
        moved.close().await.expect("close moved file");
        drop(moved);

        let db = fs.db.as_ref().expect("open database");
        assert!(db
            .get(keys::metadata_key(replaced_file_id))
            .await
            .expect("read replaced metadata")
            .is_none());
        assert!(db
            .scan_prefix(keys::chunk_prefix(replaced_file_id), ..)
            .await
            .expect("scan replaced chunks")
            .next()
            .await
            .expect("read replaced chunk")
            .is_none());

        fs.close().await.expect("close");
    }

    #[tokio::test]
    async fn claims_slatedb_urls() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        assert!(fs.can_handle("duckdb_objfs://bucket/key"));
        assert!(fs.can_handle("duckdb_objfs:/bucket/key"));
        assert!(!fs.can_handle("s3://bucket/key"));
        assert!(!fs.can_handle("/tmp/foo"));
        assert_eq!(fs.name(), NAME);
        fs.close().await.expect("close");
    }
}
