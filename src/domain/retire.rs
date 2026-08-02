//! Retirement readiness: the resolution account and the asymmetric verdict.
//!
//! The account has two registers, never reconciled — the same discipline as
//! the trail's "Arrived here" (event total) beside the card's "Standing here"
//! (state total):
//!
//! - **The story so far** counts whole-history events: what was archived from
//!   here (extraction-recorded, copies and moves alike — the trail rollup's
//!   established meaning of "archived from here"), what was deleted
//!   (scan-observed), and what is missing without a recorded deletion.
//! - **Standing here now** partitions the present rows: covered, excluded,
//!   unresolved.
//!
//! Copies overlap the two registers deliberately (a file copied to the
//! archive is both "archived from here" and typically standing `covered`);
//! the moved/copied split on the archived line is what makes that overlap
//! readable rather than confusing.

use std::collections::{HashMap, HashSet};

use super::extraction::{DecisionExtraction, OriginDisposition};
use super::source::Source;
use super::trail::DecisionFamily;

/// The asymmetric verdict. There is deliberately no `Ready` variant: Canon
/// can know NOT READY — present sources neither archived nor excluded — but
/// whether a story is complete is the user's judgment, never certified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    NotReady { unresolved: i64, unhashed: i64 },
    NoBlockersFound,
}

impl Readiness {
    /// The gate: does the ceremony refuse to proceed? Takes only the
    /// `--allow unresolved` acknowledgment — never `--yes`: the prompt-skip
    /// and the acknowledgment are orthogonal, and this signature is what
    /// enforces it.
    pub fn blocks(&self, allow_unresolved: bool) -> bool {
        match self {
            Readiness::NotReady { .. } => !allow_unresolved,
            Readiness::NoBlockersFound => false,
        }
    }
}

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
    pub covered: i64,
    pub excluded: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: present, non-excluded, never hashed — they
    /// cannot be verified covered, which is exactly why they block.
    pub unhashed_unresolved: i64,
}

impl ResolutionAccount {
    /// Present rows, partitioned exactly: covered + excluded + unresolved.
    pub fn standing(&self) -> i64 {
        self.covered + self.excluded + self.unresolved
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

/// Where a present row stands, by priority excluded > covered > unresolved:
/// an excluded-and-covered source counts excluded — its resolution is
/// already recorded, which is the fact the account reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandingBucket {
    Covered,
    Excluded,
    Unresolved { unhashed: bool },
}

/// What an absent row's stamp still explains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentBucket {
    Deleted,
    Unexplained,
}

pub fn classify_present(source: &Source, archived: &HashSet<i64>) -> StandingBucket {
    if source.is_excluded() {
        return StandingBucket::Excluded;
    }
    match source.object_id {
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
/// set, the root's origin extraction rows, and the absent rows' stamp
/// families (`decision_id` → family).
pub fn build_account(
    present: &[Source],
    absent: &[Source],
    archived: &HashSet<i64>,
    extractions: &[DecisionExtraction],
    stamp_families: &HashMap<i64, DecisionFamily>,
) -> ResolutionAccount {
    let mut account = ResolutionAccount {
        archived_bytes: Some(0),
        ..Default::default()
    };

    for source in present {
        match classify_present(source, archived) {
            StandingBucket::Covered => account.covered += 1,
            StandingBucket::Excluded => account.excluded += 1,
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

pub fn derive_readiness(account: &ResolutionAccount) -> Readiness {
    if account.unresolved > 0 {
        Readiness::NotReady {
            unresolved: account.unresolved,
            unhashed: account.unhashed_unresolved,
        }
    } else {
        Readiness::NoBlockersFound
    }
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

    // classify_present — priority excluded > covered > unresolved

    #[test]
    fn present_source_excluded_beats_archived_object() {
        let s = source(1, Some(10), true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_object_excluded_beats_archived_object() {
        let s = source(1, Some(10), false, true);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_hashed_in_archive_is_covered() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Covered
        );
    }

    #[test]
    fn present_hashed_uncovered_is_unresolved() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: false }
        );
    }

    #[test]
    fn present_unhashed_is_unresolved_unhashed() {
        let s = source(1, None, false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: true }
        );
    }

    #[test]
    fn present_unhashed_but_excluded_is_excluded() {
        let s = source(1, None, true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Excluded
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
            &extractions,
            &families,
        )
    }

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
        let a = build_account(&[], &[], &archived_set(&[]), &extractions, &HashMap::new());
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
        let a = build_account(&[], &[], &archived_set(&[]), &extractions, &HashMap::new());
        assert_eq!(a.archived_bytes, None);
    }

    #[test]
    fn empty_account_is_all_zero_with_zero_bytes() {
        let a = build_account(&[], &[], &archived_set(&[]), &[], &HashMap::new());
        assert_eq!(a.archived_files, 0);
        assert_eq!(a.archived_bytes, Some(0), "zero known bytes, not omitted");
        assert_eq!(a.standing(), 0);
        assert_eq!(a.ever_indexed(), Some(0));
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    // derive_readiness + blocks

    #[test]
    fn unresolved_sources_make_not_ready_with_both_counts() {
        let a = mixed_account();
        assert_eq!(
            derive_readiness(&a),
            Readiness::NotReady {
                unresolved: 2,
                unhashed: 1
            }
        );
    }

    #[test]
    fn no_unresolved_is_no_blockers_found_never_ready() {
        let present = vec![source(1, Some(10), false, false)];
        let a = build_account(&present, &[], &archived_set(&[10]), &[], &HashMap::new());
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    #[test]
    fn unhashed_only_root_still_blocks() {
        // The "user forgot to hash" case: unhashed ⊆ unresolved, blocks.
        let present = vec![source(1, None, false, false)];
        let a = build_account(&present, &[], &archived_set(&[]), &[], &HashMap::new());
        assert_eq!(
            derive_readiness(&a),
            Readiness::NotReady {
                unresolved: 1,
                unhashed: 1
            }
        );
    }

    #[test]
    fn gate_blocks_without_allow_and_passes_with() {
        let not_ready = Readiness::NotReady {
            unresolved: 5,
            unhashed: 0,
        };
        assert!(not_ready.blocks(false));
        assert!(!not_ready.blocks(true));
        assert!(!Readiness::NoBlockersFound.blocks(false));
        assert!(!Readiness::NoBlockersFound.blocks(true));
    }
}
