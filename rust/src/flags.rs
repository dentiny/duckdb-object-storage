//! How a file was opened, mirroring the subset of DuckDB's `FileOpenFlags`
//! that SlateFS acts on.

use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};

/// Access mode requested when opening a file.
///
/// `read_only` and `write` are tracked separately rather than as one mode
/// because DuckDB passes them as independent bits; the combinations that make
/// no sense are rejected when a handle is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOpenFlags {
    /// Create the file if no path mapping exists.
    pub create: bool,
    /// Allow writes through the handle.
    pub write: bool,
    /// Reject every mutating operation on the handle.
    pub read_only: bool,
    /// Discard the contents of an existing file on open.
    pub truncate_existing: bool,
}

impl FileOpenFlags {
    /// Opens an existing file for reading only.
    pub fn read_only() -> Self {
        Self {
            create: false,
            write: false,
            read_only: true,
            truncate_existing: false,
        }
    }

    /// Opens a file for writing, creating it when it does not exist.
    pub fn create_new() -> Self {
        Self {
            create: true,
            write: true,
            read_only: false,
            truncate_existing: false,
        }
    }

    /// Opens an existing file for reading and writing.
    pub fn read_write() -> Self {
        Self {
            create: false,
            write: true,
            read_only: false,
            truncate_existing: false,
        }
    }

    /// Rejects the bit combinations that contradict each other. DuckDB passes
    /// the bits independently, so this is checked once when a file is opened
    /// rather than at every operation that consults them.
    pub fn validate(&self, file_id: u64) -> Result<()> {
        let reason = if self.read_only && self.write {
            "read_only and write cannot both be set"
        } else if self.read_only && self.truncate_existing {
            "read_only and truncate_existing cannot both be set"
        } else if self.truncate_existing && !self.write {
            "truncate_existing requires write access"
        } else {
            return Ok(());
        };

        Err(Error::InvalidArgument(ErrorStruct::new(
            format!("invalid flags for file_id {file_id}: {reason}"),
            ErrorStatus::Permanent,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_grants_no_write_access() {
        let flags = FileOpenFlags::read_only();

        assert!(flags.read_only);
        assert!(!flags.write);
        assert!(!flags.create);
        assert!(!flags.truncate_existing);
    }

    #[test]
    fn create_new_grants_write_access_and_creates() {
        let flags = FileOpenFlags::create_new();

        assert!(flags.create);
        assert!(flags.write);
        assert!(!flags.read_only);
    }

    #[test]
    fn read_write_grants_write_access_without_creating() {
        let flags = FileOpenFlags::read_write();

        assert!(!flags.create);
        assert!(flags.write);
        assert!(!flags.read_only);
    }

    #[test]
    fn every_constructor_produces_valid_flags() {
        for flags in [
            FileOpenFlags::read_only(),
            FileOpenFlags::create_new(),
            FileOpenFlags::read_write(),
        ] {
            flags.validate(7).unwrap_or_else(|_| panic!("{flags:?}"));
        }
    }

    #[test]
    fn contradictory_flag_combinations_are_rejected() {
        let cases = [
            (
                FileOpenFlags {
                    create: false,
                    write: true,
                    read_only: true,
                    truncate_existing: false,
                },
                "read_only and write",
            ),
            (
                FileOpenFlags {
                    create: false,
                    write: true,
                    read_only: true,
                    truncate_existing: true,
                },
                "read_only and write",
            ),
            (
                FileOpenFlags {
                    create: false,
                    write: false,
                    read_only: true,
                    truncate_existing: true,
                },
                "read_only and truncate_existing",
            ),
            (
                FileOpenFlags {
                    create: false,
                    write: false,
                    read_only: false,
                    truncate_existing: true,
                },
                "truncate_existing requires write access",
            ),
        ];

        for (flags, expected) in cases {
            let error = flags.validate(7).expect_err("flags should be rejected");

            assert!(matches!(error, Error::InvalidArgument(_)), "{flags:?}");
            assert!(error.to_string().contains(expected), "{flags:?}");
            assert!(error.to_string().contains("file_id 7"), "{flags:?}");
        }
    }
}
