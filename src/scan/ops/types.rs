//! Scan's shared result/parameter types: the pipeline's per-file outcome and
//! stats types, options, the walk-observability trait, and the shared
//! timestamp helper the other four stratum files call.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ops::receipt::DeletionReceiptItem;

/// Classification of a source's fate during scan.
pub enum SourceOutcome {
    Seen,
    Missing,
    Disconnected,
}

/// Action taken for a processed file.
pub enum FileAction {
    New,
    Modified,
    Moved,
    Unchanged,
}

/// Accumulated scan statistics.
#[derive(Default)]
pub struct ScanStats {
    pub scanned: u64,
    pub new: u64,
    pub updated: u64,
    pub moved: u64,
    pub unchanged: u64,
    pub missing: u64,
    pub disconnected: u64,
    pub skipped: u64,
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
    /// Number of walk roots where missing detection was skipped (mount guard).
    /// Counted in the stats — and thus the durable decision summary — so a scan
    /// that *couldn't verify* absence is distinguishable from one that verified
    /// nothing was missing.
    pub missing_detection_skipped: u64,
    /// Number of walk entries that could not be read (permissions, I/O). A
    /// non-zero count gates missing detection for the affected root — part of
    /// the tree went unseen, and unseen must never read as deleted — and lands
    /// in the durable decision summary like the mount-guard skip.
    pub walk_errors: u64,
}

impl ScanStats {
    /// Compose the scan summary message.
    pub fn compose_summary(&self) -> String {
        let mut summary = format!(
            "Scanned {} files: {} new, {} updated, {} moved, {} unchanged, {} missing",
            self.scanned, self.new, self.updated, self.moved, self.unchanged, self.missing
        );
        if self.missing_detection_skipped == 1 {
            summary.push_str(", missing detection skipped (mount unstable)");
        } else if self.missing_detection_skipped > 1 {
            summary.push_str(&format!(
                ", missing detection skipped on {} roots (mount unstable)",
                self.missing_detection_skipped
            ));
        }
        if self.walk_errors > 0 {
            summary.push_str(&format!(
                ", {} walk errors (missing detection skipped)",
                self.walk_errors
            ));
        }
        if self.skipped > 0 {
            summary.push_str(&format!(", {} skipped (read errors)", self.skipped));
        }
        if self.disconnected > 0 {
            summary.push_str(&format!(", {} skipped (disconnected)", self.disconnected));
        }
        if self.hashed > 0 {
            summary.push_str(&format!("\nHashed {} files", self.hashed));
        }
        summary
    }
}

/// A file that needs full hashing after the walk completes.
pub struct FileToHash {
    pub source_id: i64,
    pub full_path: PathBuf,
    pub old_object_id: Option<i64>,
    pub basis_changed: bool,
}

/// Result of scanning a single root.
pub struct ScanRootResult {
    pub stats: ScanStats,
    pub files_to_hash: Vec<FileToHash>,
    /// Sources that went missing during this scan, captured before the
    /// `present → absent` flip for the deletion receipt. Empty when receipt
    /// capture is off (`capture_deletions = false`) or nothing was deleted.
    pub deleted_items: Vec<DeletionReceiptItem>,
    /// Warnings collected during scan (disconnected storage, errors).
    pub warnings: Vec<String>,
}

/// Result of marking a path's sources deleted via `--missing`.
#[derive(Debug)]
pub struct MarkMissingPathResult {
    /// The root that contained the deleted sources.
    pub root_id: i64,
    /// Absolute path of that root (for source-local receipt placement).
    pub root_path: String,
    /// How many present sources were flipped to absent.
    pub missing_count: u64,
    /// Deletion-receipt items captured before the flip. Empty when receipt
    /// capture is off (`capture_deletions = false`) or nothing was present.
    pub deleted_items: Vec<DeletionReceiptItem>,
}

/// Parameters controlling scan behavior.
pub struct ScanOptions {
    /// Whether to compute partial hashes during the walk.
    pub hash: bool,
    /// Whether to re-hash files that already have a hash.
    pub hash_all: bool,
    /// Whether to treat device ID mismatches as missing (--ignore-device-id).
    pub ignore_device_id: bool,
}

/// Observability for the scan pipeline. The interface implements this
/// to update progress bars, emit warnings, etc.
pub trait ScanProgress {
    /// Called after each file is processed.
    fn on_file(&self, path: &str, action: &FileAction);
    /// Called when a walk error is encountered (e.g., permission denied).
    fn on_walk_error(&self, error: &str);
    /// Called when process_file fails for a specific file.
    fn on_process_error(&self, path: &str, error: &str);
}

pub fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_summary_records_missing_detection_skip() {
        // The skip must reach the durable decision summary — a scan that
        // couldn't verify absence must not read like one that verified
        // nothing was missing.
        let stats = ScanStats {
            scanned: 10,
            missing_detection_skipped: 1,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("missing detection skipped (mount unstable)"));

        let stats = ScanStats {
            missing_detection_skipped: 2,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("missing detection skipped on 2 roots (mount unstable)"));

        assert!(!ScanStats::default()
            .compose_summary()
            .contains("missing detection"));
    }

    #[test]
    fn compose_summary_records_walk_errors() {
        // Walk errors reach the durable summary with the skip stated — an
        // incomplete walk must not read like a complete one.
        let stats = ScanStats {
            scanned: 5,
            walk_errors: 3,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("3 walk errors (missing detection skipped)"));
        assert!(!ScanStats::default()
            .compose_summary()
            .contains("walk errors"));
    }
}
