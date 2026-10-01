use std::collections::HashMap;
use std::sync::Arc;

use opendal::services::{Fs, Memory, S3};
use opendal::Operator;
use slatedb::config::{DbReaderOptions, Settings};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind as SlateDbErrorKind};
use tokio::sync::Mutex;

use crate::cache::{CacheConfig, CacheMetrics};
use crate::database_metadata::DatabaseMetadata;
use crate::error::{Error, Result};
use crate::file_handle::{FileHandle, SlateFileClient, SlateFileHandle};
use crate::flags::FileOpenFlags;
use crate::io_metrics::IoMetrics;
use crate::opendal_io_metrics_layer::IoMetricsLayer;
use crate::slatedb_object_store::SlateDbObjectStore;

/// URL scheme claimed by this filesystem in DuckDB's virtual filesystem.
pub const PREFIX: &str = "duckdb_objfs:";

/// Name reported to DuckDB via `FileSystem::GetName`.
pub const NAME: &str = "SlateDBFileSystem";

/// Suffixes of the files DuckDB keeps next to a database file. DuckDB moves
/// them onto each other, so they live in the database file's SlateDB.
const DATABASE_COMPANION_SUFFIXES: [&str; 3] = [".wal", ".wal.checkpoint", ".wal.recovery"];

/// SlateDB-backed filesystem owner.
///
/// Every database file gets its own SlateDB, shared with its companion files,
/// so a writer only excludes other writers of the same file, as with DuckDB's
/// file locks. SlateDBs are opened on first use and stay open until the
/// filesystem closes. The FFI layer owns the runtime used to drive these
/// asynchronous operations.
pub struct SlateDbFileSystem {
    database_path: String,
    object_store: Arc<dyn ObjectStore>,
    cache_config: CacheConfig,
    access_mode: SlateDbAccessMode,
    /// Keyed by `database_group`.
    databases: Mutex<HashMap<String, SlateDbClient>>,
    closed: bool,
    pub(crate) cache_metrics: CacheMetrics,
    pub(crate) io_metrics: Arc<IoMetrics>,
}

#[derive(Clone)]
enum SlateDbClient {
    ReadWrite(Arc<Db>),
    ReadOnly(Arc<DbReader>),
}

#[derive(Clone, Copy)]
enum Access {
    Read,
    Write { create: bool },
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
        Ok(Self {
            database_path: database_path.to_string(),
            object_store: Arc::new(SlateDbObjectStore::new(operator)),
            cache_config,
            access_mode,
            databases: Mutex::new(HashMap::new()),
            closed: false,
            cache_metrics: CacheMetrics::new(),
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

        // SlateDB updates manifests while background tasks may read them. OpenDAL's
        // FS service otherwise truncates the destination before writing, exposing
        // partial (including empty) manifests to concurrent readers.
        let atomic_write_dir = root.join(".atomic-write");
        let atomic_write_dir_str = atomic_write_dir.to_str().ok_or_else(|| {
            Error::invalid_argument(format!(
                "local OpenDAL atomic write directory is not valid UTF-8: {}",
                atomic_write_dir.display()
            ))
        })?;
        let operator = Operator::new(
            Fs::default()
                .root(root_str)
                .atomic_write_dir(atomic_write_dir_str),
        )
        .map_err(|source| {
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
        let access = if flags.write || flags.create || flags.append || flags.truncate_existing {
            Access::Write {
                create: flags.create,
            }
        } else {
            Access::Read
        };
        match self.client(path, access, "open a writable file").await? {
            SlateDbClient::ReadWrite(db) => {
                let (file_id, metadata) = DatabaseMetadata::new(Arc::clone(&db))
                    .prepare_file_for_open(path, flags.create, flags.truncate_existing)
                    .await?;
                Ok(Box::new(SlateFileHandle::new(
                    SlateFileClient::ReadWrite(db),
                    file_id,
                    metadata,
                    flags,
                )?))
            }
            SlateDbClient::ReadOnly(reader) => {
                let (file_id, metadata) = DatabaseMetadata::new(Arc::clone(&reader))
                    .open_file(path)
                    .await?;
                Ok(Box::new(SlateFileHandle::new(
                    SlateFileClient::ReadOnly(reader),
                    file_id,
                    metadata,
                    flags,
                )?))
            }
        }
    }

    /// Returns whether a logical path is present in the database catalog.
    pub async fn file_exists(&self, path: &str) -> Result<bool> {
        match self.client(path, Access::Read, "").await {
            Ok(SlateDbClient::ReadWrite(db)) => DatabaseMetadata::new(db).file_exists(path).await,
            Ok(SlateDbClient::ReadOnly(reader)) => {
                DatabaseMetadata::new(reader).file_exists(path).await
            }
            Err(Error::FileNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Atomically removes a logical file and all of its persisted state.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        let db = self.read_write_db(path, "remove a file").await?;
        DatabaseMetadata::new(db).remove_file(path).await
    }

    /// Atomically moves a logical file within its database, replacing the
    /// destination if present.
    pub async fn move_file(&self, source: &str, target: &str) -> Result<()> {
        if database_group(source) != database_group(target) {
            return Err(Error::invalid_argument(format!(
                "cannot move '{source}' to '{target}': files of different databases are stored separately"
            )));
        }
        let db = self.read_write_db(source, "move a file").await?;
        DatabaseMetadata::new(db).move_file(source, target).await
    }

    /// Flush and close every open database.
    ///
    /// Calling `close` more than once is harmless. SlateDB marks all clones of
    /// a database closed.
    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        let mut result = Ok(());
        for client in std::mem::take(self.databases.get_mut()).into_values() {
            let closed = match client {
                SlateDbClient::ReadWrite(db) => db.close().await,
                SlateDbClient::ReadOnly(reader) => reader.close().await,
            };
            if let (Ok(()), Err(error)) = (&result, closed) {
                result = Err(error.into());
            }
        }
        result
    }

    pub fn name(&self) -> &'static str {
        NAME
    }

    pub fn can_handle(&self, path: &str) -> bool {
        path.starts_with(PREFIX)
    }

    async fn read_write_db(&self, path: &str, operation: &str) -> Result<Arc<Db>> {
        match self
            .client(path, Access::Write { create: false }, operation)
            .await?
        {
            SlateDbClient::ReadWrite(db) => Ok(db),
            SlateDbClient::ReadOnly(_) => Err(read_only_violation(operation)),
        }
    }

    /// Returns the SlateDB of the database `path` belongs to, opening it on
    /// first use. Reads reuse any open client, including this process's writer,
    /// so they see its writes; only writes open a writer and fence other
    /// writers of the same database.
    async fn client(&self, path: &str, access: Access, operation: &str) -> Result<SlateDbClient> {
        if self.closed {
            return Err(filesystem_closed());
        }
        let group = database_group(path);
        let mut databases = self.databases.lock().await;
        let existing = databases.get(&group).cloned();
        match (access, &existing) {
            (Access::Read, Some(client))
            | (Access::Write { .. }, Some(client @ SlateDbClient::ReadWrite(_))) => {
                return Ok(client.clone());
            }
            _ => {}
        }

        let client = match access {
            Access::Read => SlateDbClient::ReadOnly(
                self.open_reader(&group)
                    .await?
                    .ok_or_else(|| Error::file_not_found(path))?,
            ),
            Access::Write { create } => {
                if matches!(self.access_mode, SlateDbAccessMode::ReadOnly) {
                    return Err(read_only_violation(operation));
                }
                // Opening a writer creates the database, so check that it exists first.
                if !create && existing.is_none() {
                    match self.open_reader(&group).await? {
                        Some(reader) => reader.close().await?,
                        None => return Err(Error::file_not_found(path)),
                    }
                }
                SlateDbClient::ReadWrite(self.open_writer(&group).await?)
            }
        };
        if let Some(SlateDbClient::ReadOnly(reader)) = databases.insert(group, client.clone()) {
            // A reader still used by open file handles is left to them.
            if Arc::strong_count(&reader) == 1 {
                reader.close().await?;
            }
        }
        Ok(client)
    }

    fn slatedb_path(&self, group: &str) -> String {
        // Flatten the group so one database's SlateDB never nests inside another's.
        let group = group.replace('%', "%25").replace('/', "%2F");
        format!("{}/{group}", self.database_path)
    }

    async fn open_writer(&self, group: &str) -> Result<Arc<Db>> {
        let settings = Settings {
            object_store_cache_options: self.cache_config.object_store_cache_options(),
            ..Settings::default()
        };
        let mut builder = Db::builder(self.slatedb_path(group), Arc::clone(&self.object_store))
            .with_settings(settings)
            .with_metrics_recorder(self.cache_metrics.recorder());
        // Block caches must not be shared: WAL SST ids, part of the cache key, repeat across SlateDBs.
        match self.cache_config.build_db_cache() {
            Some(cache) => builder = builder.with_db_cache(cache),
            None => builder = builder.with_db_cache_disabled(),
        }
        Ok(Arc::new(builder.build().await?))
    }

    /// Returns `None` if the database has never been written.
    async fn open_reader(&self, group: &str) -> Result<Option<Arc<DbReader>>> {
        let options = DbReaderOptions {
            object_store_cache_options: self.cache_config.object_store_cache_options(),
            ..DbReaderOptions::default()
        };
        let mut builder =
            DbReader::builder(self.slatedb_path(group), Arc::clone(&self.object_store))
                .with_reader_mode(DbReaderMode::ManagedCheckpoint)
                .with_options(options)
                .with_metrics_recorder(self.cache_metrics.recorder());
        match self.cache_config.build_db_cache() {
            Some(cache) => builder = builder.with_db_cache(cache),
            None => builder = builder.with_db_cache_disabled(),
        }
        match builder.build().await {
            Ok(reader) => Ok(Some(Arc::new(reader))),
            Err(error)
                if error.kind() == SlateDbErrorKind::Data
                    && error
                        .to_string()
                        .contains("failed to find latest transactional object") =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Name of the database a logical file belongs to. Known DuckDB companion
/// files share a SlateDB with the path obtained by removing their suffix;
/// every other path is its own group.
fn database_group(path: &str) -> String {
    for suffix in DATABASE_COMPANION_SUFFIXES {
        if let Some(database) = path.strip_suffix(suffix) {
            if !database.is_empty() && !database.ends_with('/') {
                return database.to_string();
            }
        }
    }
    path.to_string()
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
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnectionBuilder;
    use s3s::auth::SimpleAuth;
    use s3s::host::SingleDomain;
    use s3s::service::S3ServiceBuilder;
    use s3s_fs::FileSystem as S3FileSystem;
    use slatedb::WriteBatch;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::keys;

    const S3_ACCESS_KEY: &str = "test-access-key";
    const S3_BUCKET: &str = "test-bucket";
    const S3_SECRET_KEY: &str = "test-secret-key";

    async fn start_fake_s3(root: &std::path::Path) -> (String, JoinHandle<()>) {
        std::fs::create_dir(root.join(S3_BUCKET)).unwrap();
        let storage = S3FileSystem::new(root).unwrap();
        let mut builder = S3ServiceBuilder::new(storage);
        builder.set_auth(SimpleAuth::from_single(S3_ACCESS_KEY, S3_SECRET_KEY));
        builder.set_host(SingleDomain::new("localhost").unwrap());
        let service = builder.build();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let connection = ConnectionBuilder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(socket), service.clone())
                    .into_owned();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
            }
        });
        (format!("http://{address}"), handle)
    }

    fn fake_s3_config(endpoint: &str) -> S3StorageConfig {
        S3StorageConfig {
            bucket: S3_BUCKET.to_string(),
            root: Some("slatedb-test".to_string()),
            endpoint: Some(endpoint.to_string()),
            region: Some("us-east-1".to_string()),
            key_id: Some(S3_ACCESS_KEY.to_string()),
            secret: Some(S3_SECRET_KEY.to_string()),
            session_token: None,
            use_ssl: false,
            virtual_host_style: false,
        }
    }

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
        // Another database has its own SlateDB and file ids.
        assert_eq!(other.file_id(), 1);
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
    async fn s3_database_persists_across_reopen() {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            check_s3_database_persists_across_reopen(),
        )
        .await
        .unwrap();
    }

    async fn check_s3_database_persists_across_reopen() {
        let root = tempdir().unwrap();
        let (endpoint, server) = start_fake_s3(root.path()).await;

        let mut first = SlateDbFileSystem::open_s3("s3-persistent-db", fake_s3_config(&endpoint))
            .await
            .unwrap();
        let mut created = first
            .open_file("database.db", FileOpenFlags::open_or_create())
            .await
            .unwrap();
        created.write(b"persisted through S3").await.unwrap();
        created.close().await.unwrap();
        drop(created);
        first.close().await.unwrap();

        let mut second = SlateDbFileSystem::open_s3("s3-persistent-db", fake_s3_config(&endpoint))
            .await
            .unwrap();
        assert!(second.file_exists("database.db").await.unwrap());
        let mut reopened = second
            .open_file("database.db", FileOpenFlags::read_only())
            .await
            .unwrap();
        let mut contents = vec![0; "persisted through S3".len()];
        assert_eq!(reopened.read(&mut contents).await.unwrap(), contents.len());
        assert_eq!(contents, b"persisted through S3");
        reopened.close().await.unwrap();
        drop(reopened);

        let mut checkpoint_wal = second
            .open_file(
                "database.db.wal.checkpoint",
                FileOpenFlags::open_or_create(),
            )
            .await
            .unwrap();
        checkpoint_wal.close().await.unwrap();
        drop(checkpoint_wal);
        second
            .move_file("database.db.wal.checkpoint", "database.db.wal")
            .await
            .unwrap();
        assert!(!second
            .file_exists("database.db.wal.checkpoint")
            .await
            .unwrap());
        assert!(second.file_exists("database.db.wal").await.unwrap());
        second.remove_file("database.db.wal").await.unwrap();
        assert!(!second.file_exists("database.db.wal").await.unwrap());
        second.close().await.unwrap();

        server.abort();
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
            .read_write_db("recovery.wal", "inspect database")
            .await
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
        fs.read_write_db("database.db", "inspect database")
            .await
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
            .read_write_db("database.db", "inspect database")
            .await
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
            .open_file(
                "database.db.wal.checkpoint",
                FileOpenFlags::open_or_create(),
            )
            .await
            .expect("create source");
        let source_file_id = source.file_id();
        source.write(b"source").await.expect("write source");
        source.close().await.expect("close source");
        drop(source);

        let mut target = fs
            .open_file("database.db.wal", FileOpenFlags::open_or_create())
            .await
            .expect("create target");
        let replaced_file_id = target.file_id();
        target.write(b"target").await.expect("write target");
        target.close().await.expect("close target");
        drop(target);

        fs.move_file("database.db.wal.checkpoint", "database.db.wal")
            .await
            .expect("move file");

        assert!(!fs
            .file_exists("database.db.wal.checkpoint")
            .await
            .expect("source lookup"));
        let mut moved = fs
            .open_file("database.db.wal", FileOpenFlags::read_only())
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
            .read_write_db("database.db", "inspect database")
            .await
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

    #[test]
    fn database_group_keeps_companion_files_with_their_database() {
        for (path, group) in [
            ("database.db", "database.db"),
            ("database.db.wal", "database.db"),
            ("database.db.wal.checkpoint", "database.db"),
            ("database.db.wal.recovery", "database.db"),
            ("dir/database.db.wal", "dir/database.db"),
            ("database", "database"),
            ("database.wal", "database"),
            ("data.csv.wal", "data.csv"),
            ("tmp_data.csv", "tmp_data.csv"),
            ("dir/tmp_data.csv", "dir/tmp_data.csv"),
            (".wal", ".wal"),
            ("dir/.wal", "dir/.wal"),
            ("tmp_", "tmp_"),
            ("dir/tmp_", "dir/tmp_"),
        ] {
            assert_eq!(database_group(path), group, "{path}");
        }
    }

    #[tokio::test]
    async fn move_file_rejects_moves_between_databases() {
        let mut fs = SlateDbFileSystem::open_in_memory("test-db")
            .await
            .expect("filesystem");
        let mut source = fs
            .open_file("source.db", FileOpenFlags::open_or_create())
            .await
            .expect("create source");
        source.close().await.expect("close source");
        drop(source);

        assert!(matches!(
            fs.move_file("source.db", "target.db").await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(fs.file_exists("source.db").await.expect("source lookup"));

        fs.close().await.expect("close");
    }

    async fn write_file(fs: &SlateDbFileSystem, path: &str, contents: &[u8]) {
        let mut file = fs
            .open_file(path, FileOpenFlags::open_or_create())
            .await
            .expect("open file for writing");
        file.pwrite(contents, 0).await.expect("write file");
        file.close().await.expect("close file");
    }

    async fn read_file(fs: &SlateDbFileSystem, path: &str, len: usize) -> Vec<u8> {
        let mut file = fs
            .open_file(path, FileOpenFlags::read_only())
            .await
            .expect("open file for reading");
        let mut contents = vec![0; len];
        assert_eq!(file.pread(&mut contents, 0).await.expect("read file"), len);
        file.close().await.expect("close file");
        contents
    }

    #[tokio::test]
    async fn writers_of_different_databases_share_a_root() {
        let root = tempdir().expect("temporary object-store root");
        let mut first = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("first writer");
        let mut second = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("second writer");

        write_file(&first, "a.db", b"a1").await;
        write_file(&second, "b.db", b"b1").await;
        // Opening b.db's writer must not fence a.db's writer.
        write_file(&first, "a.db", b"a2").await;
        write_file(&first, "a.db.wal", b"wal").await;
        write_file(&second, "b.db", b"b2").await;

        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let mut reopened = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("reopen");
        assert_eq!(read_file(&reopened, "a.db", 2).await, b"a2");
        assert_eq!(read_file(&reopened, "a.db.wal", 3).await, b"wal");
        assert_eq!(read_file(&reopened, "b.db", 2).await, b"b2");
        reopened.close().await.expect("close reopened");
    }

    #[tokio::test]
    async fn reads_on_a_writable_filesystem_do_not_fence_the_writer() {
        let root = tempdir().expect("temporary object-store root");
        let mut writer = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("writer");
        let mut other = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("other process");

        write_file(&writer, "a.db", b"before").await;
        assert!(other.file_exists("a.db").await.expect("exists"));
        assert!(!other.file_exists("a.db.wal").await.expect("missing wal"));
        assert_eq!(read_file(&other, "a.db", 6).await, b"before");
        write_file(&writer, "a.db", b"after!").await;

        other.close().await.expect("close other");
        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn missing_database_is_not_created_by_lookups() {
        let root = tempdir().expect("temporary object-store root");
        let mut fs = SlateDbFileSystem::open_local("shared-root", root.path())
            .await
            .expect("filesystem");

        assert!(!fs.file_exists("missing.db").await.expect("exists"));
        assert!(matches!(
            fs.remove_file("missing.db").await,
            Err(Error::FileNotFound(_))
        ));
        assert!(matches!(
            fs.open_file("missing.db", FileOpenFlags::read_write())
                .await,
            Err(Error::FileNotFound(_))
        ));
        fs.close().await.expect("close");
        assert!(!root.path().join("shared-root").exists());
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
