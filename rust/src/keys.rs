//! Stable SlateDB key encoding for logical files and their chunks.

const CHUNK_PREFIX: &str = "c/";
const METADATA_PREFIX: &str = "m/";
const PATH_PREFIX: &str = "p/";

/// Returns `c/<file_id:016x>/<chunk_index:016x>`.
pub(crate) fn chunk_key(file_id: u64, chunk_index: u64) -> Vec<u8> {
    format!("{CHUNK_PREFIX}{file_id:016x}/{chunk_index:016x}").into_bytes()
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
    format!("{METADATA_PREFIX}0000000000000000/next_file_id").into_bytes()
}

/// Returns `p/<path>`.
pub(crate) fn path_key(path: &str) -> Vec<u8> {
    format!("{PATH_PREFIX}{path}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_chunk_key() {
        assert_eq!(chunk_key(1, 2), b"c/0000000000000001/0000000000000002");
    }

    #[test]
    fn chunk_keys_sort_by_file_then_index() {
        assert!(chunk_key(1, 1) < chunk_key(1, 2));
        assert!(chunk_key(1, u64::MAX) < chunk_key(2, 0));
    }

    #[test]
    fn chunk_prefix_matches_only_its_file() {
        let prefix = chunk_prefix(42);

        assert!(chunk_key(42, 0).starts_with(&prefix));
        assert!(chunk_key(42, u64::MAX).starts_with(&prefix));
        assert!(!chunk_key(43, 0).starts_with(&prefix));
    }

    #[test]
    fn encodes_metadata_key() {
        assert_eq!(metadata_key(1), b"m/0000000000000001");
    }

    #[test]
    fn allocator_key_sorts_before_file_metadata() {
        assert_eq!(next_file_id_key(), b"m/0000000000000000/next_file_id");
        assert!(next_file_id_key() < metadata_key(1));
    }

    #[test]
    fn encodes_nested_path_without_normalizing_it() {
        assert_eq!(
            path_key("warehouse/main.duckdb"),
            b"p/warehouse/main.duckdb"
        );
    }

    #[test]
    fn key_spaces_are_disjoint() {
        let chunk = chunk_key(1, 0);
        let metadata = metadata_key(1);
        let path = path_key("db");

        assert_ne!(chunk[0], metadata[0]);
        assert_ne!(chunk[0], path[0]);
        assert_ne!(metadata[0], path[0]);
    }
}
