use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::services::{Fs, Memory, S3};
use opendal::Operator;
use slatedb::config::{DbReaderOptions, Settings};
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind as SlateDbErrorKind};

use crate::cache::{CacheConfig, CacheMetrics};
use crate::database_metadata::DatabaseMetadata;
use crate::error::{Error, Result};
use crate::file_handle::{FileHandle, SlateFileHandle};
use crate::flags::FileOpenFlags;
use crate::io_metrics::IoMetrics;
use crate::opendal_io_metrics_layer::IoMetricsLayer;
use crate::read_only_file_handle::SlateReadOnlyFileHandle;

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "duckdb_objfs:";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

/// SlateDB-backed filesystem owner.
///
/// One live database is kept for the lifetime of the filesystem. The FFI layer
/// owns the runtime used to drive these asynchronous operations.
pub struct SlateDbFileSystem {
    client: Option<SlateDbClient>,
    pub(crate) cache_metrics: CacheMetrics,
    pub(crate) io_metrics: Arc<IoMetrics>,
}

enum SlateDbClient {
    ReadWrite(Arc<Db>),
    ReadOnly(Option<Arc<DbReader>>),
}

#[derive(Clone, Copy)]
pub(crate) enum SlateDbAccessMode {
    ReadWrite,
    ReadOnly,
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
        Self::open_with_cache_config(
            database_path,
            operator,
            CacheConfig::default(),
            SlateDbAccessMode::ReadWrite,
        )
        .await
    }

    pub(crate) async fn open_with_cache_config(
        database_path: &str,
        operator: Operator,
        cache_config: CacheConfig,
        access_mode: SlateDbAccessMode,
    ) -> Result<Self> {
        if database_path.is_empty() {
            return Err(Error::invalid_argument("database path must not be empty"));
        }

        let io_metrics = Arc::new(IoMetrics::default());
        let operator = operator.layer(IoMetricsLayer::new(Arc::clone(&io_metrics)));
        let object_store = Arc::new(OpendalStore::new(operator));
        let cache_metrics = CacheMetrics::new();
        let db_cache = cache_config.build_db_cache();
        let object_store_cache_options = cache_config.object_store_cache_options();
        let client = match access_mode {
            SlateDbAccessMode::ReadWrite => {
                let mut settings = Settings::default();
                settings.object_store_cache_options = object_store_cache_options;
                let mut builder = Db::builder(database_path, object_store)
                    .with_settings(settings)
                    .with_metrics_recorder(cache_metrics.recorder());
                match db_cache {
                    Some(cache) => builder = builder.with_db_cache(cache),
                    None => builder = builder.with_db_cache_disabled(),
                }
                SlateDbClient::ReadWrite(Arc::new(builder.build().await?))
            }
            SlateDbAccessMode::ReadOnly => {
                let mut options = DbReaderOptions::default();
                options.object_store_cache_options = object_store_cache_options;
                let mut builder = DbReader::builder(database_path, object_store)
                    .with_reader_mode(DbReaderMode::ManagedCheckpoint)
                    .with_options(options)
                    .with_metrics_recorder(cache_metrics.recorder());
                match db_cache {
                    Some(cache) => builder = builder.with_db_cache(cache),
                    None => builder = builder.with_db_cache_disabled(),
                }
                let reader = match builder.build().await {
                    Ok(reader) => Some(Arc::new(reader)),
                    Err(error)
                        if error.kind() == SlateDbErrorKind::Data
                            && error
                                .to_string()
                                .contains("failed to find latest transactional object") =>
                    {
                        None
                    }
                    Err(error) => return Err(error.into()),
                };
                SlateDbClient::ReadOnly(reader)
            }
        };
        Ok(Self {
            client: Some(client),
            cache_metrics,
            io_metrics,
        })
    }

    pub async fn open_s3(database_path: &str, config: S3StorageConfig) -> Result<Self> {
        let operator = build_s3_operator(config)?;
        Self::open(database_path, operator).await
    }

    pub(crate) async fn open_s3_with_cache_config(
        database_path: &str,
        config: S3StorageConfig,
        cache_config: CacheConfig,
        access_mode: SlateDbAccessMode,
    ) -> Result<Self> {
        let operator = build_s3_operator(config)?;
        Self::open_with_cache_config(database_path, operator, cache_config, access_mode).await
    }

    pub async fn open_in_memory(database_path: &str) -> Result<Self> {
        Self::open_in_memory_with_cache_config(
            database_path,
            CacheConfig::default(),
            SlateDbAccessMode::ReadWrite,
        )
        .await
    }

    pub(crate) async fn open_in_memory_with_cache_config(
        database_path: &str,
        cache_config: CacheConfig,
        access_mode: SlateDbAccessMode,
    ) -> Result<Self> {
        let operator = Operator::new(Memory::default()).map_err(|source| {
            Error::io_with_source("failed to initialize OpenDAL memory storage", source)
        })?;
        Self::open_with_cache_config(database_path, operator, cache_config, access_mode).await
    }

    pub async fn open_local(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        Self::open_local_with_cache_config(
            database_path,
            root,
            CacheConfig::default(),
            SlateDbAccessMode::ReadWrite,
        )
        .await
    }

    pub(crate) async fn open_local_with_cache_config(
        database_path: &str,
        root: impl AsRef<std::path::Path>,
        cache_config: CacheConfig,
        access_mode: SlateDbAccessMode,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let root_str = root.to_str().ok_or_else(|| {
            Error::invalid_argument(format!(
                "local OpenDAL root is not valid UTF-8: {}",
                root.display()
            ))
        })?;
        tokio::fs::create_dir_all(&root).await?;

        let operator = Operator::new(Fs::default().root(root_str)).map_err(|source| {
            Error::io_with_source(
                format!("failed to initialize OpenDAL storage at {}", root.display()),
                source,
            )
        })?;
        Self::open_with_cache_config(database_path, operator, cache_config, access_mode).await
    }

    /// Open a logical file, creating its persistent catalog entry when allowed.
    ///
    // TODO: Evaluate adding a read-through in-memory path catalog; SlateDB must remain the source of truth.
    pub async fn open_file(
        &self,
        path: &str,
        flags: FileOpenFlags,
    ) -> Result<Box<dyn FileHandle + Send>> {
        flags.validate()?;
        match self.client.as_ref().ok_or_else(filesystem_closed)? {
            SlateDbClient::ReadWrite(db) => {
                let db = Arc::clone(db);
                let (file_id, metadata) = DatabaseMetadata::new(Arc::clone(&db))
                    .prepare_file_for_open(path, flags.create, flags.truncate_existing)
                    .await?;
                Ok(Box::new(SlateFileHandle::new(
                    db, file_id, metadata, flags,
                )?))
            }
            SlateDbClient::ReadOnly(reader) => {
                if flags.write || flags.create || flags.append || flags.truncate_existing {
                    return Err(read_only_violation("open a writable file"));
                }
                let reader =
                    Arc::clone(reader.as_ref().ok_or_else(|| Error::file_not_found(path))?);
                let (file_id, metadata) = DatabaseMetadata::new(Arc::clone(&reader))
                    .open_file(path)
                    .await?;
                Ok(Box::new(SlateReadOnlyFileHandle::new(
                    reader, file_id, metadata, flags,
                )?))
            }
        }
    }

    /// Returns whether a logical path is present in the database catalog.
    pub async fn file_exists(&self, path: &str) -> Result<bool> {
        match self.client.as_ref().ok_or_else(filesystem_closed)? {
            SlateDbClient::ReadWrite(db) => {
                DatabaseMetadata::new(Arc::clone(db))
                    .file_exists(path)
                    .await
            }
            SlateDbClient::ReadOnly(Some(reader)) => {
                DatabaseMetadata::new(Arc::clone(reader))
                    .file_exists(path)
                    .await
            }
            SlateDbClient::ReadOnly(None) => Ok(false),
        }
    }

    /// Atomically removes a logical file and all of its persisted state.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        let db = self.read_write_db("remove a file")?;
        DatabaseMetadata::new(db).remove_file(path).await
    }

    /// Atomically moves a logical file, replacing the destination if present.
    pub async fn move_file(&self, source: &str, target: &str) -> Result<()> {
        let db = self.read_write_db("move a file")?;
        DatabaseMetadata::new(db).move_file(source, target).await
    }

    /// Flush and close the owned database.
    ///
    /// Calling `close` more than once is harmless. SlateDB marks all clones of
    /// the database closed.
    pub async fn close(&mut self) -> Result<()> {
        let Some(client) = self.client.take() else {
            return Ok(());
        };
        match client {
            SlateDbClient::ReadWrite(db) => db.close().await?,
            SlateDbClient::ReadOnly(Some(reader)) => reader.close().await?,
            SlateDbClient::ReadOnly(None) => {}
        }
        Ok(())
    }

    pub fn name(&self) -> &'static str {
        NAME
    }

    pub fn can_handle(&self, path: &str) -> bool {
        path.starts_with(PREFIX)
    }

    fn read_write_db(&self, operation: &str) -> Result<Arc<Db>> {
        match self.client.as_ref().ok_or_else(filesystem_closed)? {
            SlateDbClient::ReadWrite(db) => Ok(Arc::clone(db)),
            SlateDbClient::ReadOnly(_) => Err(read_only_violation(operation)),
        }
    }
}

fn filesystem_closed() -> Error {
    Error::invalid_argument("filesystem is closed")
}

fn read_only_violation(operation: &str) -> Error {
    Error::read_only_violation(format!("read-only SlateDB filesystem cannot {operation}"))
}

fn build_s3_operator(config: S3StorageConfig) -> Result<Operator> {
    if config.bucket.is_empty() {
        return Err(Error::invalid_argument("S3 bucket must not be empty"));
    }
    if config.key_id.is_some() != config.secret.is_some() {
        return Err(Error::invalid_argument(
            "S3 key ID and secret must be provided together",
        ));
    }
    if config.session_token.is_some() && config.key_id.is_none() {
        return Err(Error::invalid_argument(
            "S3 session token requires a key ID and secret",
        ));
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

    Operator::new(builder)
        .map_err(|source| Error::io_with_source("failed to initialize OpenDAL S3 storage", source))
}

#[cfg(test)]
mod tests {
    use slatedb::WriteBatch;
    use tempfile::tempdir;

    use super::*;
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
    async fn read_only_client_does_not_fence_active_writer() {
        let root = tempdir().expect("temporary object-store root");
        let mut writer = SlateDbFileSystem::open_local("shared-db", root.path())
            .await
            .expect("writer");
        let mut created = writer
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .expect("create file");
        created.write(b"before").await.expect("write file");
        created.close().await.expect("close file");
        drop(created);

        let mut reader = SlateDbFileSystem::open_local_with_cache_config(
            "shared-db",
            root.path(),
            CacheConfig::default(),
            SlateDbAccessMode::ReadOnly,
        )
        .await
        .expect("reader");
        let mut read_handle = reader
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .expect("open through reader");
        let mut contents = [0; 6];
        assert_eq!(
            read_handle.read(&mut contents).await.expect("read file"),
            contents.len()
        );
        assert_eq!(&contents, b"before");
        read_handle.close().await.expect("close reader handle");
        drop(read_handle);

        let mut writable = writer
            .open_file("database.db", FileOpenFlags::read_write())
            .await
            .expect("writer remains usable");
        writable
            .pwrite(b"after", 0)
            .await
            .expect("write after reader opens");
        writable.close().await.expect("close writer handle");
        drop(writable);

        reader.close().await.expect("close reader");
        writer.close().await.expect("close writer");
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
            .read_write_db("inspect database")
            .expect("read-write database")
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
        fs.read_write_db("inspect database")
            .expect("read-write database")
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
        let db = fs
            .read_write_db("inspect database")
            .expect("read-write database");
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

        let db = fs
            .read_write_db("inspect database")
            .expect("read-write database");
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
