//! Persisted metadata for a logical SlateFS file.

use prost::Message;

use crate::util::current_time_millis;

/// Matches DuckDB's `DEFAULT_BLOCK_ALLOC_SIZE` of 262144.
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;

include!(concat!(env!("OUT_DIR"), "/slatefs.rs"));

impl FileMetadata {
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

    pub(crate) fn decode_from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
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
        let error = FileMetadata::decode_from_bytes(&[0x08, 0x80]);

        assert!(error.is_err());
    }
}
