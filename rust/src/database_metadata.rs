//! Database-level metadata such as path mappings and file ID allocation.

use std::sync::Arc;

use slatedb::{Db, IsolationLevel};

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::file_metadata::FileMetadata;
use crate::keys::{chunk_prefix, metadata_key};

const NEXT_FILE_ID_KEY: &[u8] = b"m/0000000000000000/next_file_id";
const PATH_PREFIX: &[u8] = b"p/";

/// Reads and updates metadata shared by every logical file in a database.
pub(crate) struct DatabaseMetadata {
    db: Arc<Db>,
}

impl DatabaseMetadata {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Returns whether a path has a catalog entry.
    ///
    /// Existence is intentionally determined by the path mapping alone. It
    /// does not read file metadata or scan chunks.
    pub(crate) async fn file_exists(&self, path: &str) -> Result<bool> {
        validate_path(path)?;
        Ok(self.db.get(path_key(path)).await?.is_some())
    }

    /// Atomically removes a path mapping, its metadata, and all content chunks.
    pub(crate) async fn remove_file(&self, path: &str) -> Result<()> {
        validate_path(path)?;

        let path_key = path_key(path);
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let file_id = transaction
            .get(&path_key)
            .await?
            .ok_or_else(|| {
                Error::FileNotFound(ErrorStruct::new(
                    format!("file not found: {path}"),
                    ErrorStatus::Permanent,
                ))
            })
            .and_then(|bytes| decode_file_id(&bytes, "path mapping"))?;

        transaction.delete(&path_key)?;
        transaction.delete(metadata_key(file_id))?;

        let mut chunks = transaction.scan_prefix(chunk_prefix(file_id), ..).await?;
        while let Some(chunk) = chunks.next().await? {
            transaction.delete(chunk.key)?;
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Atomically moves a path mapping, replacing the destination when present.
    pub(crate) async fn move_file(&self, source: &str, target: &str) -> Result<()> {
        validate_path(source)?;
        validate_path(target)?;

        if source == target {
            return Ok(());
        }

        let source_key = path_key(source);
        let target_key = path_key(target);
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let source_file_id = transaction
            .get(&source_key)
            .await?
            .ok_or_else(|| {
                Error::FileNotFound(ErrorStruct::new(
                    format!("file not found: {source}"),
                    ErrorStatus::Permanent,
                ))
            })
            .and_then(|bytes| decode_file_id(&bytes, "source path mapping"))?;

        if let Some(target_file_id_bytes) = transaction.get(&target_key).await? {
            let target_file_id = decode_file_id(&target_file_id_bytes, "target path mapping")?;

            if target_file_id != source_file_id {
                transaction.delete(metadata_key(target_file_id))?;

                let mut chunks = transaction
                    .scan_prefix(chunk_prefix(target_file_id), ..)
                    .await?;
                while let Some(chunk) = chunks.next().await? {
                    transaction.delete(chunk.key)?;
                }
            }
        }

        transaction.delete(&source_key)?;
        transaction.put(&target_key, source_file_id.to_be_bytes())?;
        transaction.commit().await?;
        Ok(())
    }

    /// Resolves a path to its file ID and metadata, creating both when allowed.
    pub(crate) async fn get_or_create_file(
        &self,
        path: &str,
        create: bool,
        truncate_existing: bool,
    ) -> Result<(u64, FileMetadata)> {
        validate_path(path)?;

        let path_key = path_key(path);
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        if let Some(file_id_bytes) = transaction.get(&path_key).await? {
            let file_id = decode_file_id(&file_id_bytes, "path mapping")?;
            let metadata = transaction
                .get(metadata_key(file_id))
                .await?
                .ok_or_else(|| {
                    Error::MetadataDecode(ErrorStruct::new(
                        format!("metadata is missing for file_id {file_id}"),
                        ErrorStatus::Permanent,
                    ))
                })
                .and_then(|bytes| FileMetadata::decode_from_bytes(&bytes))?;

            if truncate_existing {
                let mut chunks = transaction.scan_prefix(chunk_prefix(file_id), ..).await?;
                while let Some(chunk) = chunks.next().await? {
                    transaction.delete(chunk.key)?;
                }

                let metadata = FileMetadata::new();
                transaction.put(metadata_key(file_id), metadata.encode_to_bytes())?;
                transaction.commit().await?;
                return Ok((file_id, metadata));
            }

            return Ok((file_id, metadata));
        }

        if !create {
            return Err(Error::FileNotFound(ErrorStruct::new(
                format!("file not found: {path}"),
                ErrorStatus::Permanent,
            )));
        }

        let file_id = match transaction.get(NEXT_FILE_ID_KEY).await? {
            Some(bytes) => decode_file_id(&bytes, "next file ID")?,
            None => 1,
        };
        let next_file_id = file_id.checked_add(1).ok_or_else(|| {
            Error::MetadataDecode(ErrorStruct::new(
                "file ID space is exhausted".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;
        let metadata = FileMetadata::new();

        transaction.put(&path_key, file_id.to_be_bytes())?;
        transaction.put(metadata_key(file_id), metadata.encode_to_bytes())?;
        transaction.put(NEXT_FILE_ID_KEY, next_file_id.to_be_bytes())?;
        transaction.commit().await?;

        Ok((file_id, metadata))
    }
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::InvalidArgument(ErrorStruct::new(
            "file path must not be empty".to_string(),
            ErrorStatus::Permanent,
        )));
    }
    Ok(())
}

fn path_key(path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(PATH_PREFIX.len() + path.len());
    key.extend_from_slice(PATH_PREFIX);
    key.extend_from_slice(path.as_bytes());
    key
}

fn decode_file_id(bytes: &[u8], record: &str) -> Result<u64> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::MetadataDecode(ErrorStruct::new(
            format!("{record} must contain an 8-byte file ID"),
            ErrorStatus::Permanent,
        ))
    })?;
    let file_id = u64::from_be_bytes(encoded);
    if file_id == 0 {
        return Err(Error::MetadataDecode(ErrorStruct::new(
            format!("{record} contains reserved file ID 0"),
            ErrorStatus::Permanent,
        )));
    }
    Ok(file_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_key_uses_database_metadata_prefix() {
        assert_eq!(path_key("some/file.db"), b"p/some/file.db");
    }

    #[test]
    fn file_id_round_trips() {
        assert_eq!(
            decode_file_id(&42_u64.to_be_bytes(), "file ID").expect("file ID"),
            42
        );
    }

    #[test]
    fn malformed_file_id_is_rejected() {
        assert!(matches!(
            decode_file_id(&[1, 2, 3], "file ID"),
            Err(Error::MetadataDecode(_))
        ));
    }

    #[test]
    fn reserved_file_id_is_rejected() {
        assert!(matches!(
            decode_file_id(&0_u64.to_be_bytes(), "file ID"),
            Err(Error::MetadataDecode(_))
        ));
    }
}
