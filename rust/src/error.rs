//! Errors returned by SlateFS operations.

use thiserror::Error as ThisError;

use crate::error_struct::{ErrorStatus, ErrorStruct};

/// Stable error codes exposed through the C ABI.
#[repr(i32)]
pub enum ErrorCode {
    MetadataDecode = 1,
    FileNotFound = 2,
    FileAlreadyExists = 3,
    ReadOnlyViolation = 4,
    SlateDb = 5,
    InvalidArgument = 6,
    Io = 7,
}

/// All errors returned by SlateFS operations.
#[derive(Clone, Debug, ThisError)]
pub enum Error {
    /// Stored bytes could not be decoded into a known format.
    #[error("{0}")]
    MetadataDecode(ErrorStruct),

    /// The requested file was not found.
    #[error("{0}")]
    FileNotFound(ErrorStruct),

    /// A file already exists at the given path.
    #[error("{0}")]
    FileAlreadyExists(ErrorStruct),

    /// A mutating operation was attempted through a read-only handle.
    #[error("{0}")]
    ReadOnlyViolation(ErrorStruct),

    /// An error from the underlying SlateDB.
    #[error("{0}")]
    SlateDb(ErrorStruct),

    /// An invalid argument was provided.
    #[error("{0}")]
    InvalidArgument(ErrorStruct),

    /// An I/O error occurred.
    #[error("{0}")]
    Io(ErrorStruct),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Returns the stable code used by C ABI consumers.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::MetadataDecode(_) => ErrorCode::MetadataDecode,
            Error::FileNotFound(_) => ErrorCode::FileNotFound,
            Error::FileAlreadyExists(_) => ErrorCode::FileAlreadyExists,
            Error::ReadOnlyViolation(_) => ErrorCode::ReadOnlyViolation,
            Error::SlateDb(_) => ErrorCode::SlateDb,
            Error::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Error::Io(_) => ErrorCode::Io,
        }
    }

    /// Returns whether retrying the failed operation could succeed.
    pub fn status(&self) -> ErrorStatus {
        match self {
            Error::MetadataDecode(inner)
            | Error::FileNotFound(inner)
            | Error::FileAlreadyExists(inner)
            | Error::ReadOnlyViolation(inner)
            | Error::SlateDb(inner)
            | Error::InvalidArgument(inner)
            | Error::Io(inner) => inner.status,
        }
    }
}

impl From<prost::DecodeError> for Error {
    #[track_caller]
    fn from(source: prost::DecodeError) -> Self {
        Error::MetadataDecode(
            ErrorStruct::new("metadata decode error".to_string(), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl From<slatedb::Error> for Error {
    #[track_caller]
    fn from(source: slatedb::Error) -> Self {
        // `ErrorKind` is `#[non_exhaustive]`, so unknown kinds fall back to
        // permanent: a caller that keeps retrying an unknown failure is worse
        // than one that surfaces it.
        let status = match source.kind() {
            slatedb::ErrorKind::Unavailable | slatedb::ErrorKind::Transaction => {
                ErrorStatus::Temporary
            }
            _ => ErrorStatus::Permanent,
        };
        Error::SlateDb(ErrorStruct::new("slatedb error".to_string(), status).with_source(source))
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    #[track_caller]
    fn from(_: std::sync::PoisonError<T>) -> Self {
        Error::Io(ErrorStruct::new(
            "lock is poisoned".to_string(),
            ErrorStatus::Permanent,
        ))
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = match source.kind() {
            std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut => ErrorStatus::Temporary,
            _ => ErrorStatus::Permanent,
        };
        Error::Io(ErrorStruct::new("io error".to_string(), status).with_source(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_errors_are_permanent() {
        // A varint with the continuation bit set but no following byte.
        let decode_error = <u64 as prost::Message>::decode([0x08, 0x80].as_slice())
            .expect_err("truncated varint should not decode");

        let error = Error::from(decode_error);

        assert!(matches!(error, Error::MetadataDecode(_)));
        assert_eq!(error.status(), ErrorStatus::Permanent);
        assert!(error.to_string().contains("metadata decode error"));
    }

    #[test]
    fn unavailable_slatedb_errors_are_temporary() {
        let error = Error::from(slatedb::Error::unavailable("object store down".to_string()));

        assert!(matches!(error, Error::SlateDb(_)));
        assert_eq!(error.status(), ErrorStatus::Temporary);
    }

    #[test]
    fn invalid_slatedb_errors_are_permanent() {
        let error = Error::from(slatedb::Error::invalid("bad argument".to_string()));

        assert_eq!(error.status(), ErrorStatus::Permanent);
    }

    #[test]
    fn interrupted_io_errors_are_temporary() {
        let error = Error::from(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "try again",
        ));

        assert!(matches!(error, Error::Io(_)));
        assert_eq!(error.status(), ErrorStatus::Temporary);
        assert!(error.to_string().contains("io error"));
    }

    #[test]
    fn poisoned_locks_are_permanent_io_errors() {
        let lock = std::sync::Arc::new(std::sync::Mutex::new(0));
        let poisoner = std::sync::Arc::clone(&lock);
        // A panic while the guard is held is the only way to poison a lock.
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().expect("uncontended lock");
            panic!("poisoning the lock under test");
        })
        .join();

        let error = Error::from(lock.lock().expect_err("lock should be poisoned"));

        assert!(matches!(error, Error::Io(_)));
        assert_eq!(error.status(), ErrorStatus::Permanent);
        assert!(error.to_string().contains("lock is poisoned"));
    }

    #[test]
    fn not_found_io_errors_are_permanent() {
        let error = Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));

        assert!(matches!(error, Error::Io(_)));
        assert_eq!(error.status(), ErrorStatus::Permanent);
    }
}
