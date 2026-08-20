//! Stable SlateDB key encoding for logical files and their chunks.

const CHUNK_PREFIX: &str = "c/";
const METADATA_PREFIX: &str = "m/";
const PATH_PREFIX: &str = "p/";

/// Returns `c/<file_id:016x>/<chunk_index:016x>`.
pub(crate) fn chunk_key(file_id: u64, chunk_index: u64) -> Vec<u8> {
    let mut key = chunk_prefix(file_id);
    key.extend_from_slice(format!("{chunk_index:016x}").as_bytes());
    key
}

/// Returns `c/<file_id:016x>/` for scanning every chunk in a file.
pub(crate) fn chunk_prefix(file_id: u64) -> Vec<u8> {
    format!("{CHUNK_PREFIX}{file_id:016x}/").into_bytes()
}

/// Returns `m/<file_id:016x>`.
pub(crate) fn metadata_key(file_id: u64) -> Vec<u8> {
    format!("{METADATA_PREFIX}{file_id:016x}").into_bytes()
}

/// Returns the reserved metadata key holding the next available file ID.
pub(crate) fn next_file_id_key() -> Vec<u8> {
    b"m/0000000000000000/next_file_id".to_vec()
}

/// Returns `p/<path>`.
pub(crate) fn path_key(path: &str) -> Vec<u8> {
    format!("{PATH_PREFIX}{path}").into_bytes()
}
