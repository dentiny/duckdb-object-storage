//! Payload shared by every [`crate::Error`] variant.

use std::fmt;
use std::panic::Location;
use std::sync::Arc;

/// Whether an operation is worth retrying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorStatus {
    /// May resolve on retry (e.g. storage unavailable, rate-limited).
    Temporary,
    /// Cannot be resolved by retrying (e.g. file not found, bad data).
    Permanent,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorStatus::Temporary => write!(f, "temporary"),
            ErrorStatus::Permanent => write!(f, "permanent"),
        }
    }
}

/// Message, retryability, source error and the call site that produced it.
#[derive(Clone, Debug)]
pub struct ErrorStruct {
    pub message: String,
    pub status: ErrorStatus,
    /// `Arc` so the enclosing [`crate::Error`] stays `Clone`.
    pub source: Option<Arc<anyhow::Error>>,
    pub location: Option<String>,
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)?;
        if let Some(location) = &self.location {
            write!(f, " at {location}")?;
        }
        if let Some(source) = &self.source {
            write!(f, ", caused by: {source}")?;
        }
        Ok(())
    }
}

impl ErrorStruct {
    /// Captures the caller's location, so error sites stay traceable across the
    /// C ABI where Rust backtraces are unavailable.
    #[track_caller]
    pub fn new(message: String, status: ErrorStatus) -> Self {
        let location = Location::caller();
        Self {
            message,
            status,
            source: None,
            location: Some(format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )),
        }
    }

    /// Attaches the underlying cause.
    ///
    /// # Panics
    ///
    /// Panics if a source error has already been set.
    pub fn with_source(mut self, source: impl Into<anyhow::Error>) -> Self {
        assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(Arc::new(source.into()));
        self
    }

    /// Returns the underlying cause, for downcasting to a concrete error type.
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| source.as_ref().as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_status_and_location() {
        let error = ErrorStruct::new("boom".to_string(), ErrorStatus::Permanent);

        let rendered = error.to_string();
        assert!(rendered.starts_with("boom (permanent) at "));
        assert!(rendered.contains("error_struct.rs"));
    }

    #[test]
    fn display_includes_source() {
        let error = ErrorStruct::new("boom".to_string(), ErrorStatus::Temporary)
            .with_source(std::io::Error::other("disk on fire"));

        assert!(error.to_string().contains("caused by: disk on fire"));
    }

    #[test]
    fn source_downcasts_to_the_original_error() {
        let error = ErrorStruct::new("boom".to_string(), ErrorStatus::Permanent)
            .with_source(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));

        let source = error.source().expect("source should be set");
        let io_error = source
            .downcast_ref::<std::io::Error>()
            .expect("source should be an io error");
        assert_eq!(io_error.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    #[should_panic(expected = "the source error has been set")]
    fn setting_the_source_twice_panics() {
        ErrorStruct::new("boom".to_string(), ErrorStatus::Permanent)
            .with_source(std::io::Error::other("first"))
            .with_source(std::io::Error::other("second"));
    }
}
