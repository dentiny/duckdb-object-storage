//! Helper functions for the filesystem.

use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current time in epoch milliseconds, saturating instead of
/// panicking if the clock is before the epoch or beyond `u64`.
pub(crate) fn current_time_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}
