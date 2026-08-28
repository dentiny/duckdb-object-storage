//! Errors returned by SlateFS operations.

use thiserror::Error as ThisError;

use crate::error_struct::{ErrorStatus, ErrorStruct};

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
    /// Returns whether retrying the failed operation could succeed.
    pub fn status(&self) -> ErrorStatus {
        match self {
            Error::MetadataDecode(inner)
            | Error::FileNotFound(inner)
            | Error::FileAlreadyExists(inner)
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
}
