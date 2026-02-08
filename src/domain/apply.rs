//! Domain layer for apply command.
//!
//! Pure types and functions for resume mode classification.
//! No I/O - all filesystem and database checks happen in the caller.

/// The state of a destination path for resume classification.
///
/// Used by work planning to determine what action to take for each source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationState {
    /// Not in DB, not on disk — needs transfer
    Available,
    /// In DB with present=1 — fully archived
    Archived,
    /// On disk but not in DB, size matches — resumed (needs scan to register)
    Resumed,
    /// On disk but not in DB, size mismatch — partial/corrupted
    SizeMismatch { expected: u64, actual: u64 },
}

#[allow(dead_code)] // Used in tests, kept as complete API
impl DestinationState {
    /// Returns true if this destination needs a file transfer.
    pub fn needs_transfer(&self) -> bool {
        matches!(self, DestinationState::Available)
    }

    /// Returns true if this is an error state (size mismatch).
    pub fn is_error(&self) -> bool {
        matches!(self, DestinationState::SizeMismatch { .. })
    }
}

/// Classify a destination's state for resume mode.
///
/// This is a pure function — all I/O happens in the caller.
///
/// # Arguments
/// * `in_db` - Whether the destination path exists in DB with present=1
/// * `on_disk` - If file exists on disk: Some(actual_size), else None
/// * `expected_size` - The expected file size from the manifest
///
/// # Returns
/// The classification of this destination's state.
///
/// # State Detection Table
///
/// | in_db | on_disk | Result |
/// |-------|---------|--------|
/// | true  | _       | `Archived` (DB is authoritative) |
/// | false | None    | `Available` (ready for transfer) |
/// | false | Some(s) where s == expected | `Resumed` (copy done, needs scan) |
/// | false | Some(s) where s != expected | `SizeMismatch` (partial/corrupted) |
pub fn classify_destination(
    in_db: bool,
    on_disk: Option<u64>,
    expected_size: u64,
) -> DestinationState {
    if in_db {
        // D4: If DB says present=1, trust it without disk check
        DestinationState::Archived
    } else if let Some(actual_size) = on_disk {
        // File exists on disk but not in DB
        if actual_size == expected_size {
            // D6: Size match is sufficient for resume
            DestinationState::Resumed
        } else {
            DestinationState::SizeMismatch {
                expected: expected_size,
                actual: actual_size,
            }
        }
    } else {
        // Not in DB, not on disk — available for transfer
        DestinationState::Available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_not_in_db_not_on_disk() {
        let state = classify_destination(false, None, 1024);
        assert_eq!(state, DestinationState::Available);
        assert!(state.needs_transfer());
        assert!(!state.is_error());
    }

    #[test]
    fn classify_in_db() {
        // When in DB, we trust the DB regardless of disk state
        let state = classify_destination(true, None, 1024);
        assert_eq!(state, DestinationState::Archived);
        assert!(!state.needs_transfer());
        assert!(!state.is_error());

        // Even if there's something on disk, DB takes precedence
        let state = classify_destination(true, Some(1024), 1024);
        assert_eq!(state, DestinationState::Archived);
    }

    #[test]
    fn classify_on_disk_size_matches() {
        let state = classify_destination(false, Some(1024), 1024);
        assert_eq!(state, DestinationState::Resumed);
        assert!(!state.needs_transfer());
        assert!(!state.is_error());
    }

    #[test]
    fn classify_on_disk_size_smaller() {
        // Partial copy scenario - file is smaller than expected
        let state = classify_destination(false, Some(512), 1024);
        assert_eq!(
            state,
            DestinationState::SizeMismatch {
                expected: 1024,
                actual: 512
            }
        );
        assert!(!state.needs_transfer());
        assert!(state.is_error());
    }

    #[test]
    fn classify_on_disk_size_larger() {
        // Different file scenario - file is larger than expected
        let state = classify_destination(false, Some(2048), 1024);
        assert_eq!(
            state,
            DestinationState::SizeMismatch {
                expected: 1024,
                actual: 2048
            }
        );
        assert!(!state.needs_transfer());
        assert!(state.is_error());
    }
}
