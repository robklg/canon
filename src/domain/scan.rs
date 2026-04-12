//! Scan domain model for canon.
//!
//! This module defines the domain concepts for filesystem scanning:
//! - `FileObservation`: What scan observes about a file on disk
//! - `Reconciliation`: The outcome of comparing an observation to database state
//! - `reconcile()`: Pure function encoding the reconciliation rules
//! - `find_missing()`: Pure function for detecting missing files
//!
//! ## Design Principles
//!
//! 1. **Pure functions**: All logic is pure — no I/O, no side effects
//! 2. **Testable in isolation**: Can unit test with constructed inputs
//! 3. **Domain describes "what happened"**: Not how to persist it
//! 4. **Command layer applies policy**: User config affects behavior via parameters

use super::source::Source;
use std::collections::HashSet;

/// What scan observes about a file on disk.
///
/// This is pure data captured from filesystem metadata. The `partial_hash`
/// field is computed after reconciliation when needed (for New or Modified files).
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used in Phase 3 integration
pub struct FileObservation {
    /// ID of the root being scanned
    pub root_id: i64,
    /// Path relative to root
    pub rel_path: String,
    /// Device ID from filesystem (st_dev)
    pub device: u64,
    /// Inode number from filesystem (st_ino)
    pub inode: u64,
    /// File size in bytes
    pub size: i64,
    /// Modification time (Unix timestamp)
    pub mtime: i64,
    /// Partial hash — computed after reconciliation if needed
    pub partial_hash: Option<String>,
}

/// The outcome of reconciling a file observation with database state.
///
/// This enum describes what happened semantically, not how to persist it.
/// The repo layer translates these outcomes into appropriate SQL operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    /// File is new — no existing source at this path or with this inode.
    /// Requires: partial_hash must be computed before persistence.
    /// Action: INSERT new source record.
    New,

    /// File exists and is unchanged — same path, same size+mtime.
    /// Action: UPDATE last_seen_at only.
    Unchanged { source_id: i64 },

    /// File exists but content may have changed — same path, different size or mtime.
    /// Requires: partial_hash must be computed before persistence.
    /// Action: UPDATE with new size, mtime, partial_hash, increment basis_rev.
    Modified {
        source_id: i64,
        /// Previous object_id for detecting unexpected hash changes during hashing phase
        old_object_id: Option<i64>,
    },

    /// File was moved — different path, but same device+inode.
    /// Action: UPDATE path (and possibly root_id for cross-root moves).
    Moved {
        source_id: i64,
        from_root_id: i64,
        from_path: String,
        /// Previous object_id for reference
        old_object_id: Option<i64>,
    },
}

impl Reconciliation {
    /// Whether this reconciliation requires partial hash computation.
    ///
    /// This is an infrastructure requirement for persisting the source record.
    /// New and Modified files need a partial hash for the INSERT/UPDATE.
    pub fn needs_partial_hash(&self) -> bool {
        matches!(self, Reconciliation::New | Reconciliation::Modified { .. })
    }

    /// The source ID affected by this reconciliation, if any.
    ///
    /// Returns None for New files (no existing source ID yet).
    ///
    /// Note: Currently unused but kept as part of the complete domain API.
    /// Provides a convenient accessor without pattern matching.
    #[allow(dead_code)]
    pub fn source_id(&self) -> Option<i64> {
        match self {
            Reconciliation::New => None,
            Reconciliation::Unchanged { source_id }
            | Reconciliation::Modified { source_id, .. }
            | Reconciliation::Moved { source_id, .. } => Some(*source_id),
        }
    }
}

/// Determine the reconciliation outcome for a file observation.
///
/// This function ONLY processes files that exist on disk. It does NOT perform
/// device ID checking — that happens at the command layer for empty directories
/// via `classify_sources_in_empty_dir()`.
///
/// # Arguments
/// - `observation`: What we observed on disk (the file EXISTS)
/// - `source_at_path`: Existing source at this (root_id, rel_path), if any
/// - `source_by_inode`: Existing source with this (device, inode), if any
///
/// # Behavior
///
/// The decision tree:
///
/// 1. If source exists at path:
///     - a. If same inode (or inode not tracked): check size+mtime
///         - Match: Unchanged
///         - Differ: Modified
///     - b. If different inode: New (replacement, old file handled by `mark_missing`)
/// 2. Else if source exists with same inode (different path): Moved
/// 3. Else: New
///
/// # Note on "Replaced" case
///
/// If a source exists at the path but with different inode, this means:
/// - Old file at this path was deleted
/// - New file was created at same path
///
/// This is handled as: New (for the new file) + Missing (old file detected at end of scan).
/// We don't need a Replaced variant because mark_missing handles the old file.
///
/// # Note on device ID changes
///
/// If a file is present on disk, it is always processed — even if its device ID
/// differs from what was stored. Device ID changes are legitimate (e.g., NAS remount,
/// drive replacement). The device ID will be updated in the source record.
///
/// # Note on device/inode tracking
///
/// In the Source struct, device=0 and inode=0 indicate "not tracked" (e.g., non-Unix platform).
/// When not tracked, we fall back to size+mtime comparison only.
pub fn reconcile(
    observation: &FileObservation,
    source_at_path: Option<&Source>,
    source_by_inode: Option<&Source>,
) -> Reconciliation {
    // Source exists at this path?
    if let Some(existing) = source_at_path {
        // Check if this is actually the same file (by inode) or a replacement
        // inode=0 means "not tracked"
        let inode_tracked = existing.inode != 0;
        let same_inode = inode_tracked && existing.inode as u64 == observation.inode;

        if same_inode || !inode_tracked {
            // Same file (or inode not tracked) — check for modifications
            let fingerprint_matches =
                existing.size == observation.size && existing.mtime == observation.mtime;

            if fingerprint_matches {
                Reconciliation::Unchanged {
                    source_id: existing.id,
                }
            } else {
                Reconciliation::Modified {
                    source_id: existing.id,
                    old_object_id: existing.object_id,
                }
            }
        } else {
            // Different inode at same path = replacement
            // The old file will be caught by mark_missing
            // This observation is treated as New
            Reconciliation::New
        }
    } else if let Some(existing) = source_by_inode {
        // No source at path, but found one with same inode = moved
        Reconciliation::Moved {
            source_id: existing.id,
            from_root_id: existing.root_id,
            from_path: existing.rel_path.clone(),
            old_object_id: existing.object_id,
        }
    } else {
        // No existing source at path or by inode = new file
        Reconciliation::New
    }
}

/// Identify source IDs that were expected but not seen during the walk.
///
/// # Arguments
/// - `expected_ids`: Source IDs that existed at start of scan (present=1)
/// - `seen_ids`: Source IDs we encountered during the walk
///
/// # Returns
/// Source IDs that should be marked as missing (present=0).
///
/// # Note
/// Sources that were moved will be in seen_ids (we saw them at their new path),
/// so they won't appear in the missing list.
pub fn find_missing(expected_ids: &HashSet<i64>, seen_ids: &HashSet<i64>) -> Vec<i64> {
    expected_ids.difference(seen_ids).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a FileObservation with defaults for testing.
    fn make_observation(rel_path: &str) -> FileObservation {
        FileObservation {
            root_id: 1,
            rel_path: rel_path.to_string(),
            device: 100,
            inode: 1000,
            size: 1024,
            mtime: 1700000000,
            partial_hash: None,
        }
    }

    /// Helper to create a Source with defaults for testing.
    fn make_source(id: i64, rel_path: &str) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: "/test".to_string(),
            rel_path: rel_path.to_string(),
            object_id: Some(42),
            size: 1024,
            mtime: 1700000000,
            excluded: false,
            object_excluded: None,
            device: 100,
            inode: 1000,
            partial_hash: "abc123".to_string(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    // =========================================================================
    // reconcile() tests
    // =========================================================================

    #[test]
    fn reconcile_new_file() {
        // No source at path, no source by inode → New
        let obs = make_observation("new_file.txt");
        let result = reconcile(&obs, None, None);
        assert_eq!(result, Reconciliation::New);
    }

    #[test]
    fn reconcile_unchanged_file() {
        // Source at path, same size+mtime → Unchanged
        let obs = make_observation("existing.txt");
        let existing = make_source(1, "existing.txt");

        let result = reconcile(&obs, Some(&existing), Some(&existing));
        assert_eq!(result, Reconciliation::Unchanged { source_id: 1 });
    }

    #[test]
    fn reconcile_modified_file_size() {
        // Source at path, different size → Modified
        let mut obs = make_observation("existing.txt");
        obs.size = 2048; // Different size

        let existing = make_source(1, "existing.txt");

        let result = reconcile(&obs, Some(&existing), Some(&existing));
        assert_eq!(
            result,
            Reconciliation::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_modified_file_mtime() {
        // Source at path, different mtime → Modified
        let mut obs = make_observation("existing.txt");
        obs.mtime = 1800000000; // Different mtime

        let existing = make_source(1, "existing.txt");

        let result = reconcile(&obs, Some(&existing), Some(&existing));
        assert_eq!(
            result,
            Reconciliation::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_moved_file() {
        // No source at path, source by inode exists → Moved
        let obs = make_observation("new_location.txt");
        let existing = make_source(1, "old_location.txt");

        let result = reconcile(&obs, None, Some(&existing));
        assert_eq!(
            result,
            Reconciliation::Moved {
                source_id: 1,
                from_root_id: 1,
                from_path: "old_location.txt".to_string(),
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_moved_cross_root() {
        // Source by inode in different root → Moved with different from_root_id
        let obs = make_observation("imported.txt");
        let mut existing = make_source(1, "original.txt");
        existing.root_id = 2; // Different root

        let result = reconcile(&obs, None, Some(&existing));
        assert_eq!(
            result,
            Reconciliation::Moved {
                source_id: 1,
                from_root_id: 2,
                from_path: "original.txt".to_string(),
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_replaced_file() {
        // Source at path with different inode → New (old handled by missing)
        let mut obs = make_observation("replaced.txt");
        obs.inode = 9999; // Different inode

        let existing = make_source(1, "replaced.txt");

        let result = reconcile(&obs, Some(&existing), None);
        assert_eq!(result, Reconciliation::New);
    }

    #[test]
    fn reconcile_inode_not_tracked() {
        // Source at path with inode=0 (not tracked) → uses size+mtime only
        let obs = make_observation("file.txt");
        let mut existing = make_source(1, "file.txt");
        existing.inode = 0; // Not tracked
        existing.device = 0; // Not tracked

        // Same size+mtime → Unchanged
        let result = reconcile(&obs, Some(&existing), None);
        assert_eq!(result, Reconciliation::Unchanged { source_id: 1 });
    }

    #[test]
    fn reconcile_inode_not_tracked_modified() {
        // Source at path with inode=0, different size → Modified
        let mut obs = make_observation("file.txt");
        obs.size = 2048;

        let mut existing = make_source(1, "file.txt");
        existing.inode = 0;
        existing.device = 0;

        let result = reconcile(&obs, Some(&existing), None);
        assert_eq!(
            result,
            Reconciliation::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_device_changed() {
        // Source at path, different device, same inode → processed normally (Unchanged)
        // Device ID changes are legitimate (e.g., NAS remount, drive replacement)
        let mut obs = make_observation("file.txt");
        obs.device = 200; // Different device

        let existing = make_source(1, "file.txt");

        let result = reconcile(&obs, Some(&existing), Some(&existing));
        // Should proceed to Unchanged since inode and size+mtime match
        assert_eq!(result, Reconciliation::Unchanged { source_id: 1 });
    }

    #[test]
    fn reconcile_device_changed_modified() {
        // Source at path, different device, same inode, different size → Modified
        let mut obs = make_observation("file.txt");
        obs.device = 200; // Different device
        obs.size = 2048; // Different size

        let existing = make_source(1, "file.txt");

        let result = reconcile(&obs, Some(&existing), Some(&existing));
        assert_eq!(
            result,
            Reconciliation::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    // =========================================================================
    // needs_partial_hash() tests
    // =========================================================================

    #[test]
    fn needs_partial_hash_new() {
        assert!(Reconciliation::New.needs_partial_hash());
    }

    #[test]
    fn needs_partial_hash_modified() {
        let r = Reconciliation::Modified {
            source_id: 1,
            old_object_id: None,
        };
        assert!(r.needs_partial_hash());
    }

    #[test]
    fn needs_partial_hash_moved() {
        let r = Reconciliation::Moved {
            source_id: 1,
            from_root_id: 1,
            from_path: "old.txt".to_string(),
            old_object_id: None,
        };
        assert!(!r.needs_partial_hash());
    }

    #[test]
    fn needs_partial_hash_unchanged() {
        let r = Reconciliation::Unchanged { source_id: 1 };
        assert!(!r.needs_partial_hash());
    }

    // =========================================================================
    // source_id() tests
    // =========================================================================

    #[test]
    fn source_id_new() {
        assert_eq!(Reconciliation::New.source_id(), None);
    }

    #[test]
    fn source_id_unchanged() {
        let r = Reconciliation::Unchanged { source_id: 42 };
        assert_eq!(r.source_id(), Some(42));
    }

    #[test]
    fn source_id_modified() {
        let r = Reconciliation::Modified {
            source_id: 42,
            old_object_id: None,
        };
        assert_eq!(r.source_id(), Some(42));
    }

    #[test]
    fn source_id_moved() {
        let r = Reconciliation::Moved {
            source_id: 42,
            from_root_id: 1,
            from_path: "old.txt".to_string(),
            old_object_id: None,
        };
        assert_eq!(r.source_id(), Some(42));
    }

    // =========================================================================
    // find_missing() tests
    // =========================================================================

    #[test]
    fn find_missing_empty_sets() {
        let expected: HashSet<i64> = HashSet::new();
        let seen: HashSet<i64> = HashSet::new();
        let result = find_missing(&expected, &seen);
        assert!(result.is_empty());
    }

    #[test]
    fn find_missing_all_seen() {
        let expected: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let seen: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let result = find_missing(&expected, &seen);
        assert!(result.is_empty());
    }

    #[test]
    fn find_missing_none_seen() {
        let expected: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let seen: HashSet<i64> = HashSet::new();
        let mut result = find_missing(&expected, &seen);
        result.sort();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn find_missing_partial() {
        let expected: HashSet<i64> = [1, 2, 3, 4, 5].into_iter().collect();
        let seen: HashSet<i64> = [1, 3, 5].into_iter().collect();
        let mut result = find_missing(&expected, &seen);
        result.sort();
        assert_eq!(result, vec![2, 4]);
    }

    #[test]
    fn find_missing_seen_has_extra() {
        // seen_ids can have IDs not in expected (e.g., new files)
        let expected: HashSet<i64> = [1, 2].into_iter().collect();
        let seen: HashSet<i64> = [1, 2, 99, 100].into_iter().collect();
        let result = find_missing(&expected, &seen);
        assert!(result.is_empty());
    }
}
