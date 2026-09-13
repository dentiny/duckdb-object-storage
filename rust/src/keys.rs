//! Stable SlateDB key encoding for logical files and their chunks.

use std::fmt;

/// Key family each SlateFS key belongs to. Keeping the prefixes distinct lets a
/// single SlateDB instance hold every family without a scan for one family ever
/// crossing into another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prefix {
    Chunk,
    Metadata,
    Path,
}

impl Prefix {
    const fn as_str(self) -> &'static str {
        match self {
            Prefix::Chunk => "c/",
            Prefix::Metadata => "m/",
            Prefix::Path => "p/",
        }
    }
}

impl fmt::Display for Prefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns `c/<file_id:016x>/<chunk_index:016x>`.
pub(crate) fn chunk_key(file_id: u64, chunk_index: u64) -> Vec<u8> {
    let mut key = chunk_prefix(file_id);
    key.extend_from_slice(format!("{chunk_index:016x}").as_bytes());
    key
}

/// Returns `c/<file_id:016x>/` for scanning every chunk in a file.
pub(crate) fn chunk_prefix(file_id: u64) -> Vec<u8> {
    format!("{}{file_id:016x}/", Prefix::Chunk).into_bytes()
}

/// Returns `m/<file_id:016x>`.
pub(crate) fn metadata_key(file_id: u64) -> Vec<u8> {
    format!("{}{file_id:016x}", Prefix::Metadata).into_bytes()
}

/// Returns the reserved metadata key holding the next available file ID.
pub(crate) fn next_file_id_key() -> Vec<u8> {
    format!("{}0000000000000000/next_file_id", Prefix::Metadata).into_bytes()
}

/// Returns `p/<path>`.
pub(crate) fn path_key(path: &str) -> Vec<u8> {
    format!("{}{path}", Prefix::Path).into_bytes()
}

/// Returns the path encoded in a key produced by [`path_key`], or `None` if
/// `key` is not valid UTF-8 or belongs to another key family.
#[allow(dead_code)]
pub(crate) fn parse_path_from_key(key: &[u8]) -> Option<&str> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix(Prefix::Path.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_is_zero_padded_hex() {
        assert_eq!(chunk_key(1, 0), b"c/0000000000000001/0000000000000000");
    }

    #[test]
    fn chunk_keys_sort_by_file_then_index() {
        assert!(chunk_key(5, 0) < chunk_key(5, 1));
        assert!(chunk_key(1, 100) < chunk_key(2, 0));
    }

    #[test]
    fn chunk_prefix_matches_only_its_own_file() {
        let prefix = chunk_prefix(42);

        assert!(chunk_key(42, 7).starts_with(&prefix));
        assert!(!chunk_key(43, 0).starts_with(&prefix));
    }

    #[test]
    fn metadata_key_is_zero_padded_hex() {
        assert_eq!(metadata_key(1), b"m/0000000000000001");
    }

    #[test]
    fn next_file_id_key_sorts_before_every_file_metadata_key() {
        assert_eq!(next_file_id_key(), b"m/0000000000000000/next_file_id");
        assert!(next_file_id_key() < metadata_key(1));
    }

    #[test]
    fn path_key_round_trips() {
        let path = "some/file.db";

        let key = path_key(path);

        assert!(key.starts_with(b"p/"));
        assert_eq!(parse_path_from_key(&key), Some(path));
    }

    #[test]
    fn parse_path_rejects_other_key_families() {
        assert_eq!(parse_path_from_key(&chunk_key(1, 0)), None);
        assert_eq!(parse_path_from_key(&metadata_key(1)), None);
    }

    #[test]
    fn parse_path_rejects_invalid_utf8() {
        assert_eq!(parse_path_from_key(b"p/\xff\xfe"), None);
    }

    #[test]
    fn key_families_are_disjoint() {
        let keys = [chunk_key(0, 0), metadata_key(0), path_key("")];

        // Families are discriminated by their leading byte, so no scan can
        // straddle two of them however large the file ids or paths get.
        let leading: std::collections::HashSet<u8> = keys.iter().map(|key| key[0]).collect();
        assert_eq!(leading.len(), keys.len());
    }
}
