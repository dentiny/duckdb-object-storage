//! Persisted metadata for a logical SlateFS file.

use prost::Message;

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};
use crate::util::current_time_millis;

/// Matches DuckDB's `DEFAULT_BLOCK_ALLOC_SIZE` of 262144.
#[allow(dead_code)]
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;

include!(concat!(env!("OUT_DIR"), "/slatefs.rs"));

impl FileMetadata {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            size: 0,
            modified_at_ms: current_time_millis(),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub(crate) fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    #[allow(dead_code)]
    pub(crate) fn decode_from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::decode(bytes)?)
    }

    /// Returns the chunk size as a `usize`, rejecting values that would break
    /// the offset arithmetic built on it. The record comes back from storage,
    /// so a zero would divide by zero on every read and a value past `usize`
    /// would overflow; neither is worth discovering mid-operation.
    pub(crate) fn validated_chunk_size(&self, file_id: u64) -> Result<usize> {
        let invalid = |reason: String| {
            Error::InvalidArgument(ErrorStruct::new(
                format!("invalid chunk size for file_id {file_id}: {reason}"),
                ErrorStatus::Permanent,
            ))
        };

        if self.chunk_size == 0 {
            return Err(invalid("must be non-zero".to_string()));
        }
        usize::try_from(self.chunk_size)
            .map_err(|_| invalid(format!("{} exceeds usize", self.chunk_size)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protobuf_roundtrip_preserves_metadata() {
        let metadata = FileMetadata {
            size: 8192,
            modified_at_ms: 1_234_567_890,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };

        let decoded = FileMetadata::decode_from_bytes(&metadata.encode_to_bytes())
            .expect("metadata should decode");

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn malformed_metadata_is_rejected() {
        let error = FileMetadata::decode_from_bytes(&[0x08, 0x80])
            .expect_err("truncated varint should not decode");

        assert!(matches!(error, Error::MetadataDecode(_)));
        assert_eq!(error.status(), ErrorStatus::Permanent);
    }

    #[test]
    fn new_metadata_starts_empty_at_the_default_chunk_size() {
        let metadata = FileMetadata::new();

        assert_eq!(metadata.size, 0);
        assert_eq!(metadata.chunk_size, DEFAULT_CHUNK_SIZE);
        assert!(metadata.modified_at_ms > 0);
        assert_eq!(
            metadata.validated_chunk_size(7).expect("chunk size"),
            DEFAULT_CHUNK_SIZE as usize
        );
    }

    #[test]
    fn zero_chunk_size_is_rejected() {
        let metadata = FileMetadata {
            size: 0,
            modified_at_ms: 0,
            chunk_size: 0,
        };

        let error = metadata
            .validated_chunk_size(7)
            .expect_err("zero chunk size should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("file_id 7: must be non-zero"));
    }
}
