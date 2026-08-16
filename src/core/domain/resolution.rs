//! The two-register resolution account — shared substrate independently
//! computed over by `retire`'s readiness gate and `story`'s place lens.
//! Neither subsystem owns it; both interpret it for their own purposes,
//! which is exactly why it lives here rather than in either.
//!
//! - **The story so far** counts whole-history events: what was archived from
//!   here (extraction-recorded, copies and moves alike — the trail rollup's
//!   established meaning of "archived from here"), what was deleted
//!   (scan-observed), and what is missing without a recorded deletion.
//! - **Standing here now** partitions the present rows: archived from here
//!   (the surviving originals of copy-mode applies — extraction-linked,
//!   object-grain), covered (copies elsewhere, nobody chose it), excluded,
//!   contentless (empty files — all shape, no content; stated, never
//!   blocking), unresolved.
//!
//! Copies overlap the two registers deliberately (a file copied to the
//! archive is both "archived from here" and typically standing `covered`);
//! the moved/copied split on the archived line is what makes that overlap
//! readable rather than confusing.
//!
//! `retire`'s own verdict (`Readiness`, derived from this account) and its
//! book-compile fate model stay in `retire/domain.rs` — only one subsystem
//! consumes those.

use std::collections::{HashMap, HashSet};

use super::extraction::{DecisionExtraction, OriginDisposition};
use super::fate::DecisionFamily;
use crate::core::domain::source::Source;

/// The two-register resolution account of a root.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolutionAccount {
    // The story so far (event register, whole-history).
    /// Files archived from this root, extraction-recorded, all dispositions.
    pub archived_files: i64,
    /// All-or-omitted: `None` if any contributing row lacks a size.
    pub archived_bytes: Option<i64>,
    pub archived_moved: i64,
    pub archived_copied: i64,
    /// Files whose extraction rows carry no disposition (pre-vocabulary
    /// records) — the moved/copied split degrades explicitly, never guessed.
    pub archived_unrecorded: i64,
    /// Absent rows whose stamp is the Observe-family decision that witnessed
    /// the loss.
    pub deleted: i64,
    /// Absent rows whose stamp no longer supports a deletion explanation
    /// (recording was off, or a later transition overwrote it) — a
    /// record-quality fact, not a distinct physical state.
    pub unexplained_missing: i64,
    // Standing here now (state register, present rows).
    /// Present rows whose content was archived *from here* and still stands
    /// in the archive — the deliberate act, told apart from mere coverage.
    pub archived_standing: i64,
    pub covered: i64,
    pub excluded: i64,
    /// Empty files — contentless: nothing to cover, nothing to verify;
    /// stated, never silent, never blocking.
    pub contentless: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: present, non-excluded, never hashed — they
    /// cannot be verified covered, which is exactly why they block.
    pub unhashed_unresolved: i64,
}

impl ResolutionAccount {
    /// Present rows, partitioned exactly: archived + covered + excluded +
    /// contentless + unresolved.
    pub fn standing(&self) -> i64 {
        self.archived_standing + self.covered + self.excluded + self.contentless + self.unresolved
    }

    /// Every source ever indexed here: standing + absent + moved away.
    /// `None` when any extraction row lacks a disposition — without it the
    /// moved count is unsupported by the record (omitted, never guessed).
    pub fn ever_indexed(&self) -> Option<i64> {
        if self.archived_unrecorded > 0 {
            return None;
        }
        Some(self.standing() + self.deleted + self.unexplained_missing + self.archived_moved)
    }
}

/// Where a present row stands, by priority excluded > contentless >
/// archived > covered > unresolved. Exclusion first: dismissal is a
/// judgment, and judgment covers shape too. Contentless second, before any
/// identity test (including the hash test — an unhashed empty source is
/// Contentless, never unresolved): identity evidence about no-content is
/// vacuous (the contentless law). Archived over covered: the
/// deliberate act wins mixed evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandingBucket {
    /// Archived from here and the copy still stands in the archive —
    /// extraction-linked, object-grain (`batch_check_archived_from_root`).
    Archived,
    Covered,
    Excluded,
    /// Empty — all shape, no content; carried with its place.
    Contentless,
    Unresolved {
        unhashed: bool,
    },
}

/// What an absent row's stamp still explains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentBucket {
    Deleted,
    Unexplained,
}

pub fn classify_present(
    source: &Source,
    archived: &HashSet<i64>,
    archived_from_here: &HashSet<i64>,
) -> StandingBucket {
    if source.is_excluded() {
        return StandingBucket::Excluded;
    }
    if source.is_contentless() {
        return StandingBucket::Contentless;
    }
    match source.object_id {
        Some(object_id) if archived_from_here.contains(&object_id) => StandingBucket::Archived,
        Some(object_id) if archived.contains(&object_id) => StandingBucket::Covered,
        Some(_) => StandingBucket::Unresolved { unhashed: false },
        None => StandingBucket::Unresolved { unhashed: true },
    }
}

/// Deleted iff the stamp is Observe-family — the trail's presence-axis rule:
/// "deleted reads only the absent bucket of Observe-family decisions".
pub fn classify_absent(family: Option<DecisionFamily>) -> AbsentBucket {
    match family {
        Some(DecisionFamily::Observe) => AbsentBucket::Deleted,
        _ => AbsentBucket::Unexplained,
    }
}

/// Build the account from the two presence classes, the archive membership
/// sets (all archive copies; the extraction-linked archived-from-here
/// subset), the root's origin extraction rows, and the absent rows' stamp
/// families (`decision_id` → family).
pub fn build_account(
    present: &[Source],
    absent: &[Source],
    archived: &HashSet<i64>,
    archived_from_here: &HashSet<i64>,
    extractions: &[DecisionExtraction],
    stamp_families: &HashMap<i64, DecisionFamily>,
) -> ResolutionAccount {
    let mut account = ResolutionAccount {
        archived_bytes: Some(0),
        ..Default::default()
    };

    for source in present {
        match classify_present(source, archived, archived_from_here) {
            StandingBucket::Archived => account.archived_standing += 1,
            StandingBucket::Covered => account.covered += 1,
            StandingBucket::Excluded => account.excluded += 1,
            StandingBucket::Contentless => account.contentless += 1,
            StandingBucket::Unresolved { unhashed } => {
                account.unresolved += 1;
                if unhashed {
                    account.unhashed_unresolved += 1;
                }
            }
        }
    }

    for source in absent {
        let family = source
            .decision_id
            .and_then(|id| stamp_families.get(&id).copied());
        match classify_absent(family) {
            AbsentBucket::Deleted => account.deleted += 1,
            AbsentBucket::Unexplained => account.unexplained_missing += 1,
        }
    }

    for row in extractions {
        account.archived_files += row.files;
        account.archived_bytes = match (account.archived_bytes, row.bytes) {
            (Some(total), Some(bytes)) => Some(total + bytes),
            _ => None,
        };
        match row.disposition {
            Some(OriginDisposition::Relocated) => account.archived_moved += row.files,
            Some(OriginDisposition::Retained) => account.archived_copied += row.files,
            None => account.archived_unrecorded += row.files,
        }
    }
    if account.archived_files == 0 {
        account.archived_bytes = Some(0);
    }

    account
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: i64, object_id: Option<i64>, excluded: bool, object_excluded: bool) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: "/r".to_string(),
            rel_path: format!("f{id}.jpg"),
            object_id,
            size: 100,
            mtime: 0,
            excluded,
            object_excluded: object_id.map(|_| object_excluded),
            device: 1,
            inode: id,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    fn stamped(mut s: Source, decision_id: i64) -> Source {
        s.decision_id = Some(decision_id);
        s
    }

    fn extraction(
        files: i64,
        bytes: Option<i64>,
        disposition: Option<OriginDisposition>,
    ) -> DecisionExtraction {
        // Rows shaped directly rather than via build_extraction_rows: the
        // legacy no-disposition case the builder never emits is under test.
        DecisionExtraction {
            decision_id: 1,
            root_id: 1,
            root_path: "/r".to_string(),
            rel_prefix: String::new(),
            files,
            bytes,
            destination_root_id: Some(2),
            destination_path: "/archive".to_string(),
            disposition,
        }
    }

    fn archived_set(ids: &[i64]) -> HashSet<i64> {
        ids.iter().copied().collect()
    }

    fn mixed_account() -> ResolutionAccount {
        let present = vec![
            source(1, Some(10), false, false), // covered
            source(2, Some(11), false, false), // covered
            source(3, Some(12), true, false),  // excluded
            source(4, Some(13), false, false), // unresolved, hashed
            source(5, None, false, false),     // unresolved, unhashed
        ];
        let absent = vec![
            stamped(source(6, Some(14), false, false), 100), // Observe → deleted
            stamped(source(7, None, false, false), 200),     // Exclude → unexplained
            source(8, None, false, false),                   // no stamp → unexplained
        ];
        let families = HashMap::from([
            (100, DecisionFamily::Observe),
            (200, DecisionFamily::Exclude),
        ]);
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(2, Some(200), Some(OriginDisposition::Retained)),
        ];
        build_account(
            &present,
            &absent,
            &archived_set(&[10, 11]),
            &archived_set(&[]),
            &extractions,
            &families,
        )
    }

    // classify_present — priority excluded > covered > unresolved

    #[test]
    fn present_source_excluded_beats_archived_object() {
        let s = source(1, Some(10), true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_object_excluded_beats_archived_object() {
        let s = source(1, Some(10), false, true);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_hashed_in_archive_is_covered() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[])),
            StandingBucket::Covered
        );
    }

    #[test]
    fn present_hashed_uncovered_is_unresolved() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: false }
        );
    }

    #[test]
    fn present_unhashed_is_unresolved_unhashed() {
        let s = source(1, None, false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: true }
        );
    }

    #[test]
    fn present_unhashed_but_excluded_is_excluded() {
        let s = source(1, None, true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), &archived_set(&[])),
            StandingBucket::Excluded
        );
    }

    // The contentless law + the standing split: precedence
    // excluded > contentless > archived > covered > unresolved.

    #[test]
    fn empty_source_is_contentless_even_when_its_object_is_archived() {
        let mut s = source(1, Some(10), false, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[10])),
            StandingBucket::Contentless,
            "identity evidence about no-content is vacuous"
        );
    }

    #[test]
    fn unhashed_empty_source_is_contentless_not_unresolved() {
        // Contentless precedes the hash test — empty files on a
        // never-enriched root must not block retirement.
        let mut s = source(1, None, false, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[]), &archived_set(&[])),
            StandingBucket::Contentless
        );
    }

    #[test]
    fn excluded_empty_source_is_excluded() {
        // Judgment covers shape: exclusion outranks contentless.
        let mut s = source(1, Some(10), true, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn archived_from_here_beats_covered() {
        // The deliberate act wins mixed evidence: an object both archived
        // from here and standing in the archive reads Archived, not Covered.
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), &archived_set(&[10])),
            StandingBucket::Archived
        );
    }

    // classify_absent — Deleted iff Observe

    #[test]
    fn absent_observe_stamp_is_deleted() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Observe)),
            AbsentBucket::Deleted
        );
    }

    #[test]
    fn absent_archive_stamp_is_unexplained() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Archive)),
            AbsentBucket::Unexplained
        );
    }

    #[test]
    fn absent_exclude_stamp_is_unexplained() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Exclude)),
            AbsentBucket::Unexplained
        );
    }

    #[test]
    fn absent_without_stamp_is_unexplained() {
        assert_eq!(classify_absent(None), AbsentBucket::Unexplained);
    }

    // build_account

    #[test]
    fn account_buckets_every_class() {
        let a = mixed_account();
        assert_eq!(a.covered, 2);
        assert_eq!(a.excluded, 1);
        assert_eq!(a.unresolved, 2);
        assert_eq!(a.unhashed_unresolved, 1);
        assert_eq!(a.deleted, 1);
        assert_eq!(a.unexplained_missing, 2);
        assert_eq!(a.archived_files, 5);
        assert_eq!(a.archived_bytes, Some(500));
        assert_eq!(a.archived_moved, 3);
        assert_eq!(a.archived_copied, 2);
        assert_eq!(a.archived_unrecorded, 0);
    }

    #[test]
    fn sum_invariants_hold() {
        let a = mixed_account();
        assert_eq!(a.standing(), a.covered + a.excluded + a.unresolved);
        assert_eq!(
            a.ever_indexed(),
            Some(a.standing() + a.deleted + a.unexplained_missing + a.archived_moved)
        );
    }

    #[test]
    fn unrecorded_disposition_degrades_split_and_ever_indexed() {
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(4, Some(400), None), // legacy row
        ];
        let a = build_account(
            &[],
            &[],
            &archived_set(&[]),
            &archived_set(&[]),
            &extractions,
            &HashMap::new(),
        );
        assert_eq!(a.archived_files, 7);
        assert_eq!(a.archived_unrecorded, 4);
        assert_eq!(a.ever_indexed(), None, "moved count unsupported → omitted");
    }

    #[test]
    fn archived_bytes_are_all_or_omitted() {
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(2, None, Some(OriginDisposition::Retained)),
        ];
        let a = build_account(
            &[],
            &[],
            &archived_set(&[]),
            &archived_set(&[]),
            &extractions,
            &HashMap::new(),
        );
        assert_eq!(a.archived_bytes, None);
    }
}
