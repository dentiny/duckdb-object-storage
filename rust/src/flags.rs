use crate::error::{Error, Result};
use crate::error_struct::{ErrorStatus, ErrorStruct};

/// Access requested when opening a file.
///
/// `read` and `write` are independent flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOpenFlags {
    /// Allow reads through the handle.
    pub read: bool,
    /// Allow writes through the handle.
    pub write: bool,
    /// Create the file when it does not exist.
    pub create: bool,
}

impl FileOpenFlags {
    /// Opens an existing file for reading.
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
        }
    }

    /// Opens an existing file for reading and writing.
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: false,
        }
    }

    /// Opens a file for reading and writing, creating it if it is not there.
    pub fn create() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
        }
    }

    /// Rejects invalid flag combinations.
    pub fn validate(&self) -> Result<()> {
        let reason = if !self.read && !self.write {
            "at least one of read or write must be set"
        } else if self.create && !self.write {
            "create requires write access"
        } else {
            return Ok(());
        };

        Err(Error::InvalidArgument(ErrorStruct::new(
            format!("invalid file open flags: {reason}"),
            ErrorStatus::Permanent,
        )))
    }

    /// Rejects a read on a handle opened write-only.
    pub fn ensure_readable(&self, file_id: u64) -> Result<()> {
        self.ensure(self.read, "read", file_id)
    }

    /// Rejects a mutating operation on a handle opened read-only.
    pub fn ensure_writable(&self, file_id: u64) -> Result<()> {
        if self.write {
            return Ok(());
        }

        Err(Error::ReadOnlyViolation(ErrorStruct::new(
            format!("cannot write to file_id {file_id}: not opened for that access"),
            ErrorStatus::Permanent,
        )))
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
