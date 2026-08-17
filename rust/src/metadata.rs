//! Persisted metadata for a logical SlateFS file.

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;

pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;

/// Metadata stored at `m/<file_id:016x>`.
///
/// The protobuf tags match the original SlateFS format.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct FileMetadata {
    #[prost(uint64, tag = "1")]
    pub(crate) size: u64,
    #[prost(uint64, tag = "2")]
    pub(crate) modified_at: u64,
    #[prost(uint64, tag = "3")]
    pub(crate) chunk_size: u64,
}

impl FileMetadata {
    pub(crate) fn new() -> Self {
        Self {
            size: 0,
            modified_at: current_time_millis(),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub(crate) fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub(crate) fn decode_from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
    }
}

fn current_time_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_metadata_has_defaults() {
        let before = current_time_millis();
        let metadata = FileMetadata::new();
        let after = current_time_millis();

        assert_eq!(metadata.size, 0);
        assert_eq!(metadata.chunk_size, DEFAULT_CHUNK_SIZE);
        assert!((before..=after).contains(&metadata.modified_at));
    }

    #[test]
    fn protobuf_encoding_uses_stable_field_tags() {
        let metadata = FileMetadata {
            size: 1024,
            modified_at: 123,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };

        assert_eq!(
            metadata.encode_to_bytes(),
            vec![0x08, 0x80, 0x08, 0x10, 0x7b, 0x18, 0x80, 0x80, 0x10]
        );
    }

    #[test]
    fn protobuf_roundtrip_preserves_metadata() {
        let metadata = FileMetadata {
            size: 8192,
            modified_at: 1_234_567_890,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };

        let decoded = FileMetadata::decode_from_bytes(&metadata.encode_to_bytes())
            .expect("metadata should decode");

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn malformed_metadata_is_rejected() {
        let error = FileMetadata::decode_from_bytes(&[0x08, 0x80]);

        assert!(error.is_err());
    }
}
