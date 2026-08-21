//! Scan domain model for canon.
//!
//! This module defines the domain concepts for filesystem scanning:
//! - `FileObservation`: What scan observes about a file on disk
//! - `Reconciliation`: The outcome of comparing an observation to database state
//! - `same_physical_file()`: The physical-identity law — the one place Canon
//!   decides whether an observation and a stored row are the same file
//! - `reconcile_at_path()`: The same-path arm, deciding unchanged vs. updated
//! - `reconcile()`: Pure function encoding the reconciliation rules
//! - `find_missing()`: Pure function for detecting missing files
//! - `check_no_overlap()`: Pure predicate guarding new-root creation
//!
//! ## Design Principles
//!
//! 1. **Pure functions**: All logic is pure — no I/O, no side effects
//! 2. **Testable in isolation**: Can unit test with constructed inputs
//! 3. **Domain describes "what happened"**: Not how to persist it
//! 4. **Command layer applies policy**: User config affects behavior via parameters

use crate::core::domain::root::Root;
use crate::core::domain::source::Source;
use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::path::Path;

/// What scan observes about a file on disk.
///
/// This is pure data captured from filesystem metadata. The `partial_hash`
/// field is computed after reconciliation when needed (for New or Modified files).
#[derive(Debug, Clone)]
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

    /// File exists and is unchanged — the row standing at this path satisfies
    /// the identity law.
    /// Action: UPDATE last_seen_at, and device+inode — location metadata, which
    /// may have moved beneath a standing path (the silent refresh).
    Unchanged { source_id: i64 },

    /// The content at this path changed — the row standing there does not
    /// satisfy the identity law. "Updated", however the editor wrote it: in
    /// place, or by renaming a temp file over the path.
    /// Requires: partial_hash must be computed before persistence.
    /// Action: UPDATE size, mtime, partial_hash, device, inode; clear object_id
    /// (identity unknown until the hash pass); increment basis_rev.
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

/// What an identity claim would *do*, which sets the evidence it must clear.
///
/// The grade is about the claim, never about the file: the same pair of
/// observation and row can be the same file for one purpose and not enough
/// for another, because the two claims cost different things when wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityClaim {
    /// Continuity at a standing path: the row and the observation share a
    /// path, and the question is only whether the content moved under it.
    /// Being wrong costs a mislabelled report line — the path, and every
    /// judgment attached to it, stands either way.
    SamePath,

    /// A claim that would relocate a row to a different path. Being wrong
    /// rewrites where content was, which no later scan self-heals — so this
    /// grade demands evidence that actually says something.
    Relocation,
}

/// The physical-identity law: is this observation the same file as this row?
///
/// **(device, inode) is a hint, never identity.** It nominates candidates —
/// that is its whole job — and this predicate deliberately never reads it: a
/// remount that renumbers a device, or a network filesystem that synthesizes
/// inodes afresh per session, must not be able to make Canon report events the
/// user's own actions did not cause. What decides is content evidence:
///
/// 1. A **relocating** claim about a contentless candidate is refused outright.
///    An empty source is all shape, no content, so "the content agrees" agrees
///    about nothing — vacuous evidence is failed corroboration for a claim that
///    moves a row (the contentless law's one predicate is asked here).
/// 2. The **fingerprint** — size and mtime — must agree. Floor grade for either
///    claim.
/// 3. With a **head-read in hand**, the observed partial hash must equal the
///    row's stored one. This is the strong corroborator, and the only evidence
///    that separates a metadata refresh from a real edit at a standing path.
///    Without one, a `SamePath` claim still stands (the path itself carries
///    continuity, and the fingerprint is exactly the evidence an unchanged file
///    has always been trusted on), while a `Relocation` is refused — a claim
///    with no anchor is a guess, and this is the class of guess Canon does not
///    make.
///
/// The corroboration is strong, not absolute: a partial hash reads a file's
/// head and tail, so a middle-of-file change that preserves size *and* mtime
/// reads as continuity. Two things follow, both accepted deliberately: the edit
/// goes unreported, and the row keeps an object link that no longer describes
/// its content until something re-hashes it (`--verify` does, and still warns
/// about the change; a plain scan does not, because a link that is merely stale
/// is indistinguishable from a good one without reading the file). This is an
/// evidence limit, not a new one — the same corner exists wherever a fingerprint
/// is preserved across an edit — and it is never a relocated row.
pub fn same_physical_file(
    candidate: &Source,
    observation: &FileObservation,
    observed_partial_hash: Option<&str>,
    claim: IdentityClaim,
) -> bool {
    if claim == IdentityClaim::Relocation && candidate.is_contentless() {
        return false;
    }

    if candidate.size != observation.size || candidate.mtime != observation.mtime {
        return false;
    }

    match (claim, observed_partial_hash) {
        (_, Some(observed)) => candidate.partial_hash == observed,
        (IdentityClaim::SamePath, None) => true,
        (IdentityClaim::Relocation, None) => false,
    }
}

/// What the same-path arm decided about a standing row.
///
/// Two outcomes only: the file at this path is the one Canon already knows
/// (whatever the filesystem renumbered underneath it), or its content moved
/// and the row is updated in place. A path that holds a row is never new.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtPathOutcome {
    /// Same file, nothing to report. The stored device and inode are refreshed
    /// as ordinary location metadata — a silent refresh when they moved.
    Unchanged { source_id: i64 },

    /// The content at this path changed — "updated", however the editor wrote
    /// it: in place, or by renaming a temp file over the path.
    Modified {
        source_id: i64,
        /// Previous object_id, for detecting unexpected hash changes during
        /// the hashing phase.
        old_object_id: Option<i64>,
    },
}

impl AtPathOutcome {
    /// Map onto the persistence vocabulary the repo layer speaks.
    pub fn into_reconciliation(self) -> Reconciliation {
        match self {
            AtPathOutcome::Unchanged { source_id } => Reconciliation::Unchanged { source_id },
            AtPathOutcome::Modified {
                source_id,
                old_object_id,
            } => Reconciliation::Modified {
                source_id,
                old_object_id,
            },
        }
    }
}

/// Reconcile an observation against the row standing at its path.
///
/// The path is the identity here; the law decides only whether the content
/// held there is still the same. `observed_partial_hash` is the caller's head
/// read, demanded exactly when the inode changed under a standing path —
/// without it, an atomic-save editor's replacement and a bit-identical
/// recreation are indistinguishable, and one of those is an event while the
/// other is not.
pub fn reconcile_at_path(
    observation: &FileObservation,
    existing: &Source,
    observed_partial_hash: Option<&str>,
) -> AtPathOutcome {
    if same_physical_file(
        existing,
        observation,
        observed_partial_hash,
        IdentityClaim::SamePath,
    ) {
        AtPathOutcome::Unchanged {
            source_id: existing.id,
        }
    } else {
        AtPathOutcome::Modified {
            source_id: existing.id,
            old_object_id: existing.object_id,
        }
    }
}

/// Determine the reconciliation outcome for an observation whose path holds no
/// source — the other half of `reconcile_at_path`.
///
/// A path Canon already holds a row for is that row's, whatever the filesystem
/// renumbered underneath it; this arm is for the paths it does not. A stored
/// row carrying the same (device, inode) is the one thread suggesting the file
/// was moved here from somewhere else; with no such nomination, the path is new.
///
/// This function ONLY processes files that exist on disk. It does NOT perform
/// device ID checking — that happens at the command layer for empty directories
/// via `classify_sources_in_empty_dir()`.
///
/// # Arguments
/// - `source_by_inode`: Existing source with this (device, inode), if any
pub fn reconcile(source_by_inode: Option<&Source>) -> Reconciliation {
    if let Some(existing) = source_by_inode {
        // Nominated by inode from another path = moved
        Reconciliation::Moved {
            source_id: existing.id,
            from_root_id: existing.root_id,
            from_path: existing.rel_path.clone(),
            old_object_id: existing.object_id,
        }
    } else {
        // No row at this path and none nominated = a path Canon has not held
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

/// Check that a new root path does not overlap with any existing root.
///
/// Two roots overlap if one is a parent of the other. The same path is
/// allowed (handled elsewhere as a no-op or error). This is a pure
/// predicate — no I/O.
pub fn check_no_overlap(roots: &[Root], new_path: &Path) -> Result<()> {
    let new_path_str = new_path.to_str().context("Path is not valid UTF-8")?;

    for root in roots {
        if root.path == new_path_str {
            continue; // Same path, not overlapping
        }

        let existing_path = Path::new(&root.path);

        if new_path.starts_with(existing_path) {
            bail!(
                "Path {} overlaps with existing root {}",
                new_path.display(),
                root.path
            );
        }

        if existing_path.starts_with(new_path) {
            bail!(
                "Path {} overlaps with existing root {}",
                new_path.display(),
                root.path
            );
        }
    }

    Ok(())
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
    // same_physical_file() — the physical-identity law
    // =========================================================================

    #[test]
    fn same_path_continuity_needs_no_head_read() {
        // The evidence an unchanged file has always been trusted on: the path
        // holds the row, and the fingerprint agrees. No file is opened.
        let obs = make_observation("file.txt");
        let existing = make_source(1, "file.txt");
        assert!(same_physical_file(
            &existing,
            &obs,
            None,
            IdentityClaim::SamePath
        ));
    }

    #[test]
    fn a_fingerprint_difference_refuses_either_claim() {
        let existing = make_source(1, "file.txt");

        let mut bigger = make_observation("file.txt");
        bigger.size = 2048;
        let mut later = make_observation("file.txt");
        later.mtime = 1800000000;

        for obs in [&bigger, &later] {
            for claim in [IdentityClaim::SamePath, IdentityClaim::Relocation] {
                assert!(
                    !same_physical_file(&existing, obs, Some("abc123"), claim),
                    "a changed fingerprint is not continuity, whatever the claim"
                );
            }
        }
    }

    #[test]
    fn a_disagreeing_head_read_refuses_either_claim() {
        // Size and mtime can agree by coincidence or by an editor preserving
        // them; the head read is what settles it.
        let obs = make_observation("file.txt");
        let existing = make_source(1, "file.txt");

        for claim in [IdentityClaim::SamePath, IdentityClaim::Relocation] {
            assert!(!same_physical_file(
                &existing,
                &obs,
                Some("different"),
                claim
            ));
        }
    }

    #[test]
    fn an_agreeing_head_read_corroborates_a_relocation() {
        let obs = make_observation("moved_here.txt");
        let existing = make_source(1, "was_here.txt");
        assert!(same_physical_file(
            &existing,
            &obs,
            Some("abc123"),
            IdentityClaim::Relocation
        ));
    }

    #[test]
    fn a_relocation_without_a_head_read_is_refused() {
        // Moving a row rewrites where content was. Fingerprint agreement alone
        // never buys that claim — refuse rather than guess.
        let obs = make_observation("moved_here.txt");
        let existing = make_source(1, "was_here.txt");
        assert!(!same_physical_file(
            &existing,
            &obs,
            None,
            IdentityClaim::Relocation
        ));
    }

    #[test]
    fn a_relocation_of_a_contentless_candidate_is_refused() {
        // Every empty file agrees with every other empty file. Agreement that
        // cannot distinguish is no evidence at all.
        let mut obs = make_observation("moved_here.txt");
        obs.size = 0;
        let mut existing = make_source(1, "was_here.txt");
        existing.size = 0;

        assert!(!same_physical_file(
            &existing,
            &obs,
            Some("abc123"),
            IdentityClaim::Relocation
        ));
    }

    #[test]
    fn a_contentless_source_still_holds_continuity_at_its_own_path() {
        // The refusal is the relocating claim's alone: an empty file at its own
        // path is the same file, and rescanning it is not an event.
        let mut obs = make_observation("empty.log");
        obs.size = 0;
        let mut existing = make_source(1, "empty.log");
        existing.size = 0;

        assert!(same_physical_file(
            &existing,
            &obs,
            Some("abc123"),
            IdentityClaim::SamePath
        ));
    }

    #[test]
    fn the_law_reads_content_evidence_never_device_or_inode() {
        // The demotion, made structural. A wholesale remount plus inode
        // renumbering leaves identity intact...
        let obs = make_observation("file.txt");
        let mut renumbered = make_source(1, "file.txt");
        renumbered.device = 999_999;
        renumbered.inode = 888_888;
        assert!(same_physical_file(
            &renumbered,
            &obs,
            Some("abc123"),
            IdentityClaim::SamePath
        ));
        assert!(same_physical_file(
            &renumbered,
            &obs,
            Some("abc123"),
            IdentityClaim::Relocation
        ));

        // ...and a device+inode match buys nothing when the content disagrees
        // (an inode reused after delete+create is exactly this shape).
        let mut recycled = make_source(1, "file.txt");
        recycled.size = 4096;
        assert_eq!(recycled.device, obs.device as i64);
        assert_eq!(recycled.inode, obs.inode as i64);
        assert!(!same_physical_file(
            &recycled,
            &obs,
            Some("abc123"),
            IdentityClaim::SamePath
        ));
        assert!(!same_physical_file(
            &recycled,
            &obs,
            Some("abc123"),
            IdentityClaim::Relocation
        ));
    }

    // =========================================================================
    // reconcile_at_path() tests
    // =========================================================================

    #[test]
    fn reconcile_at_path_is_unchanged_when_the_fingerprint_stands() {
        let obs = make_observation("file.txt");
        let existing = make_source(1, "file.txt");
        assert_eq!(
            reconcile_at_path(&obs, &existing, None),
            AtPathOutcome::Unchanged { source_id: 1 }
        );
    }

    #[test]
    fn reconcile_at_path_is_modified_when_the_size_changed() {
        let mut obs = make_observation("file.txt");
        obs.size = 2048;
        let existing = make_source(1, "file.txt");
        assert_eq!(
            reconcile_at_path(&obs, &existing, None),
            AtPathOutcome::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn reconcile_at_path_is_modified_when_only_the_mtime_changed() {
        // A touched file reads as updated: source.mtime is a fact users filter
        // and pattern on, so a changed mtime changes what Canon would do with
        // the file even when the bytes did not move.
        let mut obs = make_observation("file.txt");
        obs.mtime = 1800000000;
        let existing = make_source(1, "file.txt");
        assert_eq!(
            reconcile_at_path(&obs, &existing, Some("abc123")),
            AtPathOutcome::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn an_identical_content_replacement_is_a_silent_refresh() {
        // A restore or a dedup pass recreates a file byte for byte: new inode,
        // same content. Nothing happened to the user's data, so nothing is
        // reported — the location metadata is refreshed and that is all.
        let mut obs = make_observation("file.txt");
        obs.inode = 9999;
        obs.device = 200;
        let existing = make_source(1, "file.txt");

        assert_eq!(
            reconcile_at_path(&obs, &existing, Some("abc123")),
            AtPathOutcome::Unchanged { source_id: 1 }
        );
    }

    #[test]
    fn a_changed_content_replacement_is_modified() {
        // What an atomic-save editor leaves behind: a different inode at the
        // same path, holding different content. The user edited a file, so the
        // report says updated — never new.
        let mut obs = make_observation("file.txt");
        obs.inode = 9999;
        let existing = make_source(1, "file.txt");

        assert_eq!(
            reconcile_at_path(&obs, &existing, Some("rewritten")),
            AtPathOutcome::Modified {
                source_id: 1,
                old_object_id: Some(42)
            }
        );
    }

    #[test]
    fn modified_carries_the_previous_object_link() {
        // The hashing phase compares against it to spot content changing under
        // an unchanged fingerprint.
        let mut obs = make_observation("file.txt");
        obs.size = 2048;
        let mut existing = make_source(1, "file.txt");
        existing.object_id = Some(7);

        match reconcile_at_path(&obs, &existing, None) {
            AtPathOutcome::Modified { old_object_id, .. } => {
                assert_eq!(old_object_id, Some(7));
            }
            other => panic!("expected Modified, got {other:?}"),
        }
    }

    #[test]
    fn at_path_outcomes_map_onto_the_persistence_vocabulary() {
        assert_eq!(
            AtPathOutcome::Unchanged { source_id: 5 }.into_reconciliation(),
            Reconciliation::Unchanged { source_id: 5 }
        );
        assert_eq!(
            AtPathOutcome::Modified {
                source_id: 5,
                old_object_id: Some(9)
            }
            .into_reconciliation(),
            Reconciliation::Modified {
                source_id: 5,
                old_object_id: Some(9)
            }
        );
    }

    // =========================================================================
    // reconcile() tests
    // =========================================================================

    #[test]
    fn reconcile_new_file() {
        // Nothing nominated → a path Canon has not held is new
        let result = reconcile(None);
        assert_eq!(result, Reconciliation::New);
    }

    #[test]
    fn reconcile_moved_file() {
        // A row nominated by inode from another path → Moved
        let existing = make_source(1, "old_location.txt");

        let result = reconcile(Some(&existing));
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
        // A nomination from a different root → Moved carrying that root
        let mut existing = make_source(1, "original.txt");
        existing.root_id = 2; // Different root

        let result = reconcile(Some(&existing));
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

    // =========================================================================
    // check_no_overlap() tests
    // =========================================================================

    /// Helper to create a Root with a specific id, path, and role.
    fn make_root_with(id: i64, path: &str, role: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: role.to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn check_no_overlap_empty_roots_ok() {
        let roots: Vec<Root> = vec![];
        assert!(check_no_overlap(&roots, Path::new("/a/b")).is_ok());
    }

    #[test]
    fn check_no_overlap_accepts_disjoint_paths() {
        let roots = vec![
            make_root_with(1, "/a", "source"),
            make_root_with(2, "/b", "archive"),
        ];
        assert!(check_no_overlap(&roots, Path::new("/c")).is_ok());
    }

    #[test]
    fn check_no_overlap_rejects_new_under_existing() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let err = check_no_overlap(&roots, Path::new("/a/b/c"))
            .expect_err("a path inside an existing root must be refused");
        let msg = err.to_string();
        assert!(msg.contains("/a/b/c"), "message names the new path: {msg}");
        assert!(
            msg.contains("/a/b"),
            "message names the existing root: {msg}"
        );
    }

    #[test]
    fn check_no_overlap_rejects_existing_under_new() {
        // The other direction: adding a parent of an existing root would nest
        // them just the same.
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let err = check_no_overlap(&roots, Path::new("/a"))
            .expect_err("a path containing an existing root must be refused");
        assert!(err.to_string().contains("/a/b"));
    }

    #[test]
    fn check_no_overlap_allows_identical_path() {
        // Re-adding the same path is not an overlap — it is handled elsewhere
        // as a no-op or a separate error.
        let roots = vec![make_root_with(1, "/a/b", "source")];
        assert!(check_no_overlap(&roots, Path::new("/a/b")).is_ok());
    }

    #[test]
    fn check_no_overlap_sibling_prefix_is_not_overlap() {
        // /a/bc is NOT under /a/b — the same component-boundary rule
        // find_containing_root follows. A string-prefix test would refuse this
        // pair and block a legitimate root.
        let roots = vec![make_root_with(1, "/a/b", "source")];
        assert!(check_no_overlap(&roots, Path::new("/a/bc")).is_ok());

        let roots = vec![make_root_with(1, "/a/bc", "source")];
        assert!(check_no_overlap(&roots, Path::new("/a/b")).is_ok());
    }
}
