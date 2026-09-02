use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};

/// Access requested when opening a file.
///
/// `read` and `write` are independent, exactly as in DuckDB: there is no
/// read-only bit, only `FILE_FLAGS_READ` without `FILE_FLAGS_WRITE`. A handle
/// may be opened for either or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOpenFlags {
    /// Allow reads through the handle. DuckDB's `FILE_FLAGS_READ`.
    pub read: bool,
    /// Allow writes through the handle. DuckDB's `FILE_FLAGS_WRITE`.
    pub write: bool,
    /// Create the file when no path mapping exists. DuckDB's
    /// `FILE_FLAGS_FILE_CREATE`.
    pub create: bool,
    /// Discard the contents of an existing file on open. Together with
    /// `create` this is DuckDB's `FILE_FLAGS_FILE_CREATE_NEW`, which creates
    /// the file and overwrites it if it was already there.
    pub truncate_existing: bool,
}

impl FileOpenFlags {
    /// Opens an existing file for reading.
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate_existing: false,
        }
    }

    /// Opens an existing file for reading and writing.
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: false,
            truncate_existing: false,
        }
    }

    /// Opens a file for reading and writing, creating it if it is not there.
    pub fn create() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            truncate_existing: false,
        }
    }

    /// DuckDB's `FileOpenFlags::Verify` treats some combinations as invalid.
    // Checked once when a file is opened
    pub fn validate(&self, file_id: u64) -> Result<()> {
        let reason = if !self.read && !self.write {
            "at least one of read or write must be set"
        } else if self.create && !self.write {
            "create requires write access"
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

    /// Rejects a read on a handle opened write-only.
    pub fn ensure_readable(&self, file_id: u64) -> Result<()> {
        self.ensure(self.read, "read", file_id)
    }

    /// Rejects a mutating operation on a handle opened read-only.
    pub fn ensure_writable(&self, file_id: u64) -> Result<()> {
        self.ensure(self.write, "write to", file_id)
    }

    fn ensure(&self, granted: bool, operation: &str, file_id: u64) -> Result<()> {
        if granted {
            return Ok(());
        }

        Err(Error::InvalidArgument(ErrorStruct::new(
            format!("cannot {operation} file_id {file_id}: not opened for that access"),
            ErrorStatus::Permanent,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE_ID: u64 = 7;

    #[test]
    fn every_constructor_produces_valid_flags() {
        for flags in [
            FileOpenFlags::read_only(),
            FileOpenFlags::read_write(),
            FileOpenFlags::create(),
        ] {
            flags
                .validate(FILE_ID)
                .unwrap_or_else(|_| panic!("{flags:?}"));
        }
    }

    #[test]
    fn access_with_neither_read_nor_write_is_rejected() {
        let flags = FileOpenFlags {
            read: false,
            write: false,
            create: false,
            truncate_existing: false,
        };

        let error = flags
            .validate(FILE_ID)
            .expect_err("flags should be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("at least one of read or write"));
        assert!(error.to_string().contains("file_id 7"));
    }

    #[test]
    fn creating_and_truncating_both_require_write_access() {
        let read_only_create = FileOpenFlags {
            create: true,
            ..FileOpenFlags::read_only()
        };
        let read_only_truncate = FileOpenFlags {
            truncate_existing: true,
            ..FileOpenFlags::read_only()
        };

        assert!(read_only_create
            .validate(FILE_ID)
            .expect_err("create needs write")
            .to_string()
            .contains("create requires write access"));
        assert!(read_only_truncate
            .validate(FILE_ID)
            .expect_err("truncate needs write")
            .to_string()
            .contains("truncate_existing requires write access"));
    }

    #[test]
    fn access_checks_follow_the_granted_bits() {
        let read_only = FileOpenFlags::read_only();
        assert!(read_only.ensure_readable(FILE_ID).is_ok());
        assert!(read_only
            .ensure_writable(FILE_ID)
            .expect_err("read-only cannot write")
            .to_string()
            .contains("cannot write to file_id 7"));

        let write_only = FileOpenFlags {
            read: false,
            ..FileOpenFlags::read_write()
        };
        assert!(write_only.ensure_writable(FILE_ID).is_ok());
        assert!(write_only
            .ensure_readable(FILE_ID)
            .expect_err("write-only cannot read")
            .to_string()
            .contains("cannot read file_id 7"));
    }
}
