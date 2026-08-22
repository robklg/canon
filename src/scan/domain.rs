//! Scan domain model for canon.
//!
//! This module defines the domain concepts for filesystem scanning:
//! - `FileObservation`: What scan observes about a file on disk
//! - `Reconciliation`: The outcome of comparing an observation to database state
//! - `same_physical_file()`: The physical-identity law — the one place Canon
//!   decides whether an observation and a stored row are the same file
//! - `reconcile_at_path()`: The same-path arm, deciding unchanged vs. updated
//! - `reconcile_pathless()`: The pathless arm, deciding new vs. deferred move
//! - `resolve_moves()`: Deterministic end-of-walk pairing of moves to rows
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
use std::collections::{HashMap, HashSet};
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

/// What the disk says about a candidate's own path, checked at nomination time.
///
/// The question a content comparison cannot answer: a file that still stands
/// where Canon recorded it did not move, however perfectly its evidence agrees
/// with something found elsewhere. Hardlink twins are exactly this — one inode,
/// many paths, all of them real — and without this check the first twin walked
/// would steal the other's row every scan, forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldPathCheck {
    /// The candidate's file is still at its recorded path. It is a sibling of
    /// what was observed, not its former self.
    Present,

    /// The candidate's path is empty and its root answered — the file that was
    /// there is gone from there. This is the one state a move can be claimed
    /// from, and only with corroboration on top.
    Vacated,

    /// The check could not be made: the root is unreachable, or the stat failed
    /// for a reason that is not absence. Ignorance, not evidence — never
    /// claimed, always counted.
    Unverifiable,
}

/// A stored row nominated by inode, paired with what the disk says about the
/// path it claims to occupy.
#[derive(Debug, Clone)]
pub struct MoveCandidate {
    pub source: Source,
    pub old_path: OldPathCheck,
}

/// What the pathless arm decided about an observation whose path holds no row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathlessOutcome {
    /// A path Canon has not held. Reached four ways — nothing was nominated,
    /// every nomination is still on disk (companions), the evidence failed, or
    /// the old paths could not be checked — and the two counts are the ones the
    /// summary owes the user when the number is large.
    New { companions: u32, unverified: u32 },

    /// At least one nominated row is gone from its own path and its content
    /// corroborates. Not yet a move: which observation takes which row is a
    /// question about the whole walk, answered once at the end.
    Deferred {
        corroborated_candidate_ids: Vec<i64>,
    },
}

/// Reconcile an observation whose path holds no source — the other half of
/// `reconcile_at_path`.
///
/// A move is the only reason a path Canon has never held should take over an
/// existing row rather than start a new one, and claiming one wrongly rewrites
/// where content was. So the claim is built from three independent things, and
/// this function owns the first two:
///
/// 1. **Nomination** — the caller's inode lookup. A hint, and the only thread
///    that suggests looking at all; it decides nothing.
/// 2. **Disk truth** — a nominated file still standing at its own path is a
///    sibling, not a former self. Refused before evidence is even consulted,
///    because no amount of agreement makes two files that both exist into one.
/// 3. **Corroboration** — the physical-identity law at `Relocation` grade, which
///    is where a contentless candidate's vacuous evidence is refused.
///
/// The third gate — a re-check when the row is actually claimed — lives at
/// resolution, because it is about time passing, not about this observation.
///
/// Anything that fails a gate is simply a new path, which is the truthful
/// reading: content Canon cannot account for elsewhere is content it is seeing
/// for the first time. Two failure modes are counted rather than silent — a
/// companion, because during convergence there will be tens of thousands of them
/// and an unexplained flood of "new" reads like data loss; and an unverifiable
/// check, because ignorance stated is not the same as absence observed.
pub fn reconcile_pathless(
    observation: &FileObservation,
    candidates: &[MoveCandidate],
    observed_partial_hash: &str,
) -> PathlessOutcome {
    let mut companions = 0u32;
    let mut unverified = 0u32;
    let mut corroborated_candidate_ids = Vec::new();

    for candidate in candidates {
        // Disk truth is read first and short-circuits: a candidate still
        // standing at its own path never reaches the law at all. The ordering
        // is the rule, not an optimisation — asking the law first and applying
        // the disk check to its verdict would make the two co-equal filters,
        // and "disk truth outranks evidence" would be a convention held by call
        // order rather than a shape. Guard: `a_present_twin_is_never_a_move_donor`,
        // which corroborates perfectly and must still be refused.
        match candidate.old_path {
            OldPathCheck::Present => {
                // The refusal above is unconditional; the *count* is a separate
                // claim and is not. Calling this new path a companion says its
                // content is shared with a file already indexed, and two rows
                // agreeing on an inode number agree on nothing when they sit on
                // different volumes — inode numbers are small integers, reused
                // freely across filesystems. So the count is made only where
                // the evidence supports it, at the grade that refuses vacuous
                // agreement: an empty file matches every other empty file, and
                // "companion" is exactly the kind of identity claim the
                // contentless law says may not rest on that.
                if same_physical_file(
                    &candidate.source,
                    observation,
                    Some(observed_partial_hash),
                    IdentityClaim::Relocation,
                ) {
                    companions += 1;
                }
            }
            OldPathCheck::Unverifiable => unverified += 1,
            OldPathCheck::Vacated => {
                if same_physical_file(
                    &candidate.source,
                    observation,
                    Some(observed_partial_hash),
                    IdentityClaim::Relocation,
                ) {
                    corroborated_candidate_ids.push(candidate.source.id);
                }
            }
        }
    }

    if corroborated_candidate_ids.is_empty() {
        PathlessOutcome::New {
            companions,
            unverified,
        }
    } else {
        PathlessOutcome::Deferred {
            corroborated_candidate_ids,
        }
    }
}

/// An observation held back from the walk because at least one nominated row
/// corroborated it. Carries the observation itself — including the partial hash
/// already computed for it — so resolution needs nothing from the walk that has
/// since ended.
#[derive(Debug, Clone)]
pub struct DeferredMove {
    pub observation: FileObservation,
    pub candidate_ids: Vec<i64>,
}

/// What pairing decided for one held-back observation.
#[derive(Debug, Clone)]
pub enum MoveResolution {
    /// This observation takes that row: the file moved.
    Moved {
        source_id: i64,
        observation: FileObservation,
    },
    /// No row was left for it — every corroborating candidate went to a better
    /// claimant. An ordinary new path, decided rather than defaulted.
    New { observation: FileObservation },
}

/// How many trailing path components two relative paths share.
///
/// The similarity that survives a move: renaming a folder changes the head of
/// every path under it and leaves the tail alone, so agreement counted from the
/// end is what tells `old/a.jpg → new/a.jpg` from `old/a.jpg → new/b.jpg`.
fn trailing_components_in_common(a: &str, b: &str) -> usize {
    a.rsplit('/')
        .zip(b.rsplit('/'))
        .take_while(|(x, y)| x == y)
        .count()
}

/// Pair held-back observations with the rows they claim, deterministically.
///
/// When a whole folder of hardlinked or identical files is renamed at once,
/// every observation corroborates every candidate — the content genuinely
/// cannot tell them apart. Something must still decide which row lands at which
/// path, and if that something is iteration order, two scans of the same disk
/// produce different histories. So the rule is fixed and total:
///
/// 1. Observations are considered in `(root_id, rel_path)` order — the walk's
///    order never reaches this decision.
/// 2. Each takes its best **unclaimed** candidate, preferring more shared
///    trailing path components; a file renamed within its folder keeps its own
///    row rather than swapping with a sibling.
/// 3. Ties go to the candidate whose **stored device** matches the observation.
///    This is device's one remaining job — a hint that breaks a tie it could
///    never have decided, and which disqualifies nothing when it disagrees.
/// 4. Anything still tied goes to the lowest source id, which is arbitrary and
///    stable, and the point is the second half.
///
/// Every candidate is claimed at most once: two paths cannot be the same row.
/// An observation left with nothing is New — the honest reading when the rows
/// it could have taken were better claimed elsewhere.
///
/// **Deliberately nomination-agnostic**: nothing here knows candidates arrived
/// by inode. A future assisted move-suggestion tool nominating by content reuses
/// this unchanged, adding only its own ceremony.
pub fn resolve_moves(
    mut deferred: Vec<DeferredMove>,
    candidates: &HashMap<i64, Source>,
) -> Vec<MoveResolution> {
    deferred.sort_by(|a, b| {
        (a.observation.root_id, &a.observation.rel_path)
            .cmp(&(b.observation.root_id, &b.observation.rel_path))
    });

    let mut claimed: HashSet<i64> = HashSet::new();
    let mut resolutions = Vec::with_capacity(deferred.len());

    for item in deferred {
        let best = item
            .candidate_ids
            .iter()
            .filter(|id| !claimed.contains(*id))
            .filter_map(|id| candidates.get(id).map(|source| (*id, source)))
            .min_by_key(|(id, source)| {
                (
                    // Reversed: more agreement is a better claim.
                    std::cmp::Reverse(trailing_components_in_common(
                        &item.observation.rel_path,
                        &source.rel_path,
                    )),
                    std::cmp::Reverse(source.device as u64 == item.observation.device),
                    *id,
                )
            })
            .map(|(id, _)| id);

        resolutions.push(match best {
            Some(source_id) => {
                claimed.insert(source_id);
                MoveResolution::Moved {
                    source_id,
                    observation: item.observation,
                }
            }
            None => MoveResolution::New {
                observation: item.observation,
            },
        });
    }

    resolutions
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
    // reconcile_pathless() — the pathless arm
    // =========================================================================

    /// A candidate whose stored evidence agrees with `make_observation`'s, so
    /// each test below varies exactly one thing: what the disk says, or what
    /// the content says.
    fn candidate(id: i64, rel_path: &str, old_path: OldPathCheck) -> MoveCandidate {
        MoveCandidate {
            source: make_source(id, rel_path),
            old_path,
        }
    }

    #[test]
    fn an_observation_with_nothing_nominated_is_new() {
        let obs = make_observation("fresh.txt");
        assert_eq!(
            reconcile_pathless(&obs, &[], "abc123"),
            PathlessOutcome::New {
                companions: 0,
                unverified: 0
            }
        );
    }

    #[test]
    fn a_present_twin_is_never_a_move_donor() {
        // The hardlink case, and the reason disk truth outranks evidence: both
        // paths hold the same inode and the same bytes, so the content agrees
        // perfectly — and agreement is beside the point, because the candidate
        // is still standing where Canon left it. Claiming a move here would
        // relocate a row away from a file that exists, and the twin would steal
        // it back on the next scan, forever.
        let obs = make_observation("by-year/2024/trip.jpg");
        let candidates = [
            candidate(1, "albums/trip.jpg", OldPathCheck::Present),
            candidate(2, "originals/trip.jpg", OldPathCheck::Present),
        ];

        assert_eq!(
            reconcile_pathless(&obs, &candidates, "abc123"),
            PathlessOutcome::New {
                companions: 2,
                unverified: 0
            },
            "a file that still exists is a sibling, not a former self"
        );
    }

    #[test]
    fn a_vacated_corroborated_candidate_is_deferred() {
        // Gone from its own path, and the content says it is the same file.
        // Still not a move yet — which observation takes this row is a question
        // about the whole walk.
        let obs = make_observation("sorted/2024/photo.jpg");
        let candidates = [candidate(7, "inbox/photo.jpg", OldPathCheck::Vacated)];

        assert_eq!(
            reconcile_pathless(&obs, &candidates, "abc123"),
            PathlessOutcome::Deferred {
                corroborated_candidate_ids: vec![7]
            }
        );
    }

    #[test]
    fn an_uncorroborated_nomination_degrades_to_new() {
        // Inode reuse: the number came back around after a delete, and now
        // names different content. The nomination is real and worthless — a
        // path holding content Canon cannot account for elsewhere is a path it
        // is seeing for the first time.
        let obs = make_observation("recycled.bin");
        let mut stale = candidate(7, "deleted-last-week.bin", OldPathCheck::Vacated);
        stale.source.size = 999_999;

        assert_eq!(
            reconcile_pathless(&obs, &[stale], "abc123"),
            PathlessOutcome::New {
                companions: 0,
                unverified: 0
            }
        );

        // A head read that disagrees refuses it just as flatly, with the
        // fingerprint agreeing exactly — the strong corroborator doing its job.
        let matching_shape = candidate(8, "coincidence.bin", OldPathCheck::Vacated);
        assert_eq!(
            reconcile_pathless(&obs, &[matching_shape], "different-content"),
            PathlessOutcome::New {
                companions: 0,
                unverified: 0
            }
        );
    }

    #[test]
    fn an_unverifiable_old_path_is_counted_never_claimed() {
        // The root is unreachable, so Canon does not know whether the old file
        // is gone. Ignorance is not evidence: no claim, and the count carries
        // to the summary so silence never passes for certainty.
        let obs = make_observation("maybe-moved.txt");
        let candidates = [
            candidate(1, "offline-drive/a.txt", OldPathCheck::Unverifiable),
            candidate(2, "offline-drive/b.txt", OldPathCheck::Unverifiable),
        ];

        assert_eq!(
            reconcile_pathless(&obs, &candidates, "abc123"),
            PathlessOutcome::New {
                companions: 0,
                unverified: 2
            }
        );
    }

    #[test]
    fn candidates_partition_by_what_the_disk_says() {
        // One of each, with content that would corroborate all three: only the
        // vacated one can be claimed, and the other two are counted separately
        // because they mean different things to the reader.
        let obs = make_observation("destination.txt");
        let mixed = [
            candidate(1, "still-here.txt", OldPathCheck::Present),
            candidate(2, "cannot-say.txt", OldPathCheck::Unverifiable),
            candidate(3, "was-here.txt", OldPathCheck::Vacated),
        ];

        assert_eq!(
            reconcile_pathless(&obs, &mixed, "abc123"),
            PathlessOutcome::Deferred {
                corroborated_candidate_ids: vec![3]
            }
        );

        // Drop the vacated one and the same set reads as new, each reason
        // counted in its own register.
        assert_eq!(
            reconcile_pathless(&obs, &mixed[..2], "abc123"),
            PathlessOutcome::New {
                companions: 1,
                unverified: 1
            }
        );
    }

    #[test]
    fn every_corroborated_candidate_is_offered_to_resolution() {
        // A whole hardlink group renamed at once: several rows are vacated and
        // every one of them corroborates, because they are the same content.
        // Choosing between them here — by row order, or by taking the first —
        // is exactly the decision that belongs to deterministic pairing.
        let obs = make_observation("renamed/trip.jpg");
        let group = [
            candidate(3, "old/c.jpg", OldPathCheck::Vacated),
            candidate(1, "old/a.jpg", OldPathCheck::Vacated),
            candidate(2, "old/b.jpg", OldPathCheck::Vacated),
        ];

        assert_eq!(
            reconcile_pathless(&obs, &group, "abc123"),
            PathlessOutcome::Deferred {
                corroborated_candidate_ids: vec![3, 1, 2]
            },
            "all of them, in the order nominated — resolution decides"
        );
    }

    #[test]
    fn a_contentless_candidate_is_never_a_donor() {
        // Every empty file's content agrees with every other's, so a vacated
        // empty row would corroborate any empty file appearing anywhere — the
        // law refuses the relocating claim outright. The control below carries
        // the identical evidence shape with actual content and is claimed, so
        // what the refusal turns on is the emptiness and nothing else.
        let mut obs = make_observation("moved/empty.log");
        obs.size = 0;
        let mut empty = candidate(1, "was/empty.log", OldPathCheck::Vacated);
        empty.source.size = 0;

        assert_eq!(
            reconcile_pathless(&obs, &[empty], "abc123"),
            PathlessOutcome::New {
                companions: 0,
                unverified: 0
            }
        );

        let obs = make_observation("moved/data.bin");
        let data = candidate(1, "was/data.bin", OldPathCheck::Vacated);
        assert_eq!(
            reconcile_pathless(&obs, &[data], "abc123"),
            PathlessOutcome::Deferred {
                corroborated_candidate_ids: vec![1]
            }
        );
    }

    // =========================================================================
    // resolve_moves() — deterministic pairing
    // =========================================================================

    /// A deferred observation at `rel_path` corroborated by `candidate_ids`.
    fn deferred(rel_path: &str, candidate_ids: &[i64]) -> DeferredMove {
        DeferredMove {
            observation: make_observation(rel_path),
            candidate_ids: candidate_ids.to_vec(),
        }
    }

    /// Build the candidate map from (id, rel_path) pairs, in the given order —
    /// insertion order is a variable the determinism test permutes.
    fn candidate_map(rows: &[(i64, &str)]) -> HashMap<i64, Source> {
        let mut map = HashMap::new();
        for (id, rel_path) in rows {
            map.insert(*id, make_source(*id, rel_path));
        }
        map
    }

    /// Each resolution as (destination path, claimed row) — `None` for New.
    fn pairs(resolutions: &[MoveResolution]) -> Vec<(String, Option<i64>)> {
        resolutions
            .iter()
            .map(|r| match r {
                MoveResolution::Moved {
                    source_id,
                    observation,
                } => (observation.rel_path.clone(), Some(*source_id)),
                MoveResolution::New { observation } => (observation.rel_path.clone(), None),
            })
            .collect()
    }

    #[test]
    fn resolve_moves_pairs_a_single_move() {
        let candidates = candidate_map(&[(7, "inbox/photo.jpg")]);
        let resolved = resolve_moves(vec![deferred("sorted/photo.jpg", &[7])], &candidates);
        assert_eq!(
            pairs(&resolved),
            [("sorted/photo.jpg".to_string(), Some(7))]
        );
    }

    #[test]
    fn resolve_moves_handles_empty_input() {
        assert!(resolve_moves(vec![], &candidate_map(&[])).is_empty());
    }

    #[test]
    fn resolve_moves_prefers_the_more_similar_path() {
        // Two files swapped folders. Content cannot tell them apart — both
        // corroborate both — so the tail of the path decides, and each keeps
        // its own row instead of the two rows crossing.
        let candidates = candidate_map(&[(1, "old/a.jpg"), (2, "old/b.jpg")]);
        let resolved = resolve_moves(
            vec![
                deferred("new/a.jpg", &[1, 2]),
                deferred("new/b.jpg", &[1, 2]),
            ],
            &candidates,
        );
        assert_eq!(
            pairs(&resolved),
            [
                ("new/a.jpg".to_string(), Some(1)),
                ("new/b.jpg".to_string(), Some(2)),
            ]
        );
    }

    #[test]
    fn resolve_moves_counts_agreement_from_the_end() {
        // A renamed parent folder changes the head of every path under it and
        // leaves the tail alone — which is why similarity is counted backwards.
        // Here the deeper tail agreement wins over a shallower one.
        let candidates = candidate_map(&[(1, "old/2024/trip.jpg"), (2, "elsewhere/trip.jpg")]);
        let resolved = resolve_moves(vec![deferred("new/2024/trip.jpg", &[1, 2])], &candidates);
        assert_eq!(
            pairs(&resolved),
            [("new/2024/trip.jpg".to_string(), Some(1))]
        );
    }

    #[test]
    fn resolve_moves_breaks_a_path_tie_on_the_stored_device() {
        // Device's one remaining job: both candidates agree on the path score
        // and on content, so the hint breaks a tie it could never have decided
        // by itself. Note the lower id is the *loser* here — without the device
        // rule the final tiebreak would have taken it.
        let mut candidates = candidate_map(&[(1, "old/x.jpg"), (2, "other/x.jpg")]);
        candidates.get_mut(&1).unwrap().device = 999;
        candidates.get_mut(&2).unwrap().device = 100; // the observation's device

        let resolved = resolve_moves(vec![deferred("new/x.jpg", &[1, 2])], &candidates);
        assert_eq!(pairs(&resolved), [("new/x.jpg".to_string(), Some(2))]);
    }

    #[test]
    fn a_disagreeing_stored_device_never_disqualifies_a_candidate() {
        // The demotion, at the pairing rung: after a remount every stored
        // device disagrees, and the only candidate must still be claimable.
        let mut candidates = candidate_map(&[(1, "old/x.jpg")]);
        candidates.get_mut(&1).unwrap().device = 999_999;

        let resolved = resolve_moves(vec![deferred("new/x.jpg", &[1])], &candidates);
        assert_eq!(pairs(&resolved), [("new/x.jpg".to_string(), Some(1))]);
    }

    #[test]
    fn resolve_moves_claims_each_candidate_at_most_once() {
        // Two observations, one row: two paths cannot be the same row, so the
        // second is New rather than a duplicate claim.
        let candidates = candidate_map(&[(1, "old/x.jpg")]);
        let resolved = resolve_moves(
            vec![deferred("new/a.jpg", &[1]), deferred("new/b.jpg", &[1])],
            &candidates,
        );
        assert_eq!(
            pairs(&resolved),
            [
                ("new/a.jpg".to_string(), Some(1)),
                ("new/b.jpg".to_string(), None),
            ]
        );
    }

    #[test]
    fn resolve_moves_leaves_surplus_candidates_unclaimed() {
        // Three rows vacated, one file observed: the two unclaimed rows are
        // untouched here and fall through to ordinary missing detection.
        let candidates = candidate_map(&[(1, "old/a.jpg"), (2, "old/b.jpg"), (3, "old/c.jpg")]);
        let resolved = resolve_moves(vec![deferred("new/b.jpg", &[1, 2, 3])], &candidates);
        assert_eq!(pairs(&resolved), [("new/b.jpg".to_string(), Some(2))]);
    }

    #[test]
    fn resolve_moves_is_identical_under_permuted_input_order() {
        // The determinism pin. A whole hardlink group renamed at once: every
        // observation corroborates every candidate, so *something* must choose,
        // and if that something is iteration order then two scans of the same
        // disk write two different histories. Every order the caller could hand
        // in — the walk's entry order, the map's iteration order, the
        // nomination list's row order — is permuted here, and the pairing must
        // not move.
        let rows: [(i64, &str); 3] = [(11, "old/a.jpg"), (22, "old/b.jpg"), (33, "old/c.jpg")];
        let all_ids = [11, 22, 33];
        let paths = ["new/a.jpg", "new/b.jpg", "new/c.jpg"];

        let expected = vec![
            ("new/a.jpg".to_string(), Some(11)),
            ("new/b.jpg".to_string(), Some(22)),
            ("new/c.jpg".to_string(), Some(33)),
        ];

        // Six permutations of the observation order, each crossed with a
        // reversed candidate-id list and a reversed map insertion order.
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for order in orders {
            for reversed_ids in [false, true] {
                for reversed_rows in [false, true] {
                    let mut ids = all_ids;
                    if reversed_ids {
                        ids.reverse();
                    }
                    let mut row_order = rows;
                    if reversed_rows {
                        row_order.reverse();
                    }
                    let candidates = candidate_map(&row_order);
                    let deferred_moves: Vec<DeferredMove> =
                        order.iter().map(|i| deferred(paths[*i], &ids)).collect();

                    let mut got = pairs(&resolve_moves(deferred_moves, &candidates));
                    got.sort();
                    assert_eq!(
                        got, expected,
                        "order {order:?}, ids reversed {reversed_ids}, rows reversed {reversed_rows}"
                    );
                }
            }
        }
    }

    #[test]
    fn resolve_moves_orders_observations_by_root_then_path() {
        // The sort is (root_id, rel_path), and root leads: two observations at
        // the same rel_path in different roots must resolve in root order, not
        // by whichever the walk reached first.
        let candidates = candidate_map(&[(1, "shared.jpg")]);
        let mut first = deferred("shared.jpg", &[1]);
        first.observation.root_id = 1;
        let mut second = deferred("shared.jpg", &[1]);
        second.observation.root_id = 2;

        // Handed in reversed; the lower root still takes the row.
        let resolved = resolve_moves(vec![second, first], &candidates);
        let claimed: Vec<Option<i64>> = resolved
            .iter()
            .map(|r| match r {
                MoveResolution::Moved {
                    source_id,
                    observation,
                } => {
                    assert_eq!(observation.root_id, 1);
                    Some(*source_id)
                }
                MoveResolution::New { observation } => {
                    assert_eq!(observation.root_id, 2);
                    None
                }
            })
            .collect();
        assert_eq!(claimed, [Some(1), None]);
    }

    #[test]
    fn resolve_moves_new_when_every_candidate_is_taken() {
        // A candidate id that names no known row is not a claim either — the
        // observation resolves as New rather than silently pointing at nothing.
        let resolved = resolve_moves(vec![deferred("new/x.jpg", &[404])], &candidate_map(&[]));
        assert_eq!(pairs(&resolved), [("new/x.jpg".to_string(), None)]);
    }

    #[test]
    fn trailing_agreement_counts_whole_components() {
        // "photo.jpg" and "other-photo.jpg" share a suffix as text and nothing
        // as a path — the count must be of components, not characters.
        assert_eq!(trailing_components_in_common("a/b/c.jpg", "x/b/c.jpg"), 2);
        assert_eq!(
            trailing_components_in_common("a/photo.jpg", "a/x-photo.jpg"),
            0
        );
        assert_eq!(trailing_components_in_common("same.jpg", "same.jpg"), 1);
        assert_eq!(trailing_components_in_common("a/b.jpg", "b.jpg"), 1);
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
