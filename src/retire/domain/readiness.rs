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

use crate::core::domain::resolution::ResolutionAccount;

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
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::core::domain::resolution::build_account;
    use crate::domain::extraction::{DecisionExtraction, OriginDisposition};
    use crate::domain::source::Source;
    use crate::domain::trail::DecisionFamily;

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

    // classify_present, classify_absent, build_account are now tested in
    // core/domain/resolution.rs, alongside their definitions — these
    // fixtures stay here (duplicated, deliberately: no cross-subsystem test
    // sharing) because the tests below need them. `book.rs`'s test module
    // keeps its own copy of `source`/`stamped`/`archived_set` for the same
    // reason: sharing four one-line helpers across files would need
    // cross-file test-only visibility, more machinery than the duplication
    // costs.

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

    #[test]
    fn standing_sum_holds_across_all_five_buckets() {
        // The sum invariant with the split and the contentless standing:
        // standing() = archived + covered + excluded + contentless +
        // unresolved, exactly.
        let mut empty = source(5, Some(12), false, false);
        empty.size = 0;
        let present = vec![
            source(1, Some(10), false, false), // archived from here
            source(2, Some(11), false, false), // covered
            source(3, Some(11), true, false),  // excluded
            source(4, None, false, false),     // unresolved (unhashed)
            empty,                             // contentless
        ];
        let a = build_account(
            &present,
            &[],
            &archived_set(&[10, 11]),
            &archived_set(&[10]),
            &[],
            &HashMap::new(),
        );
        assert_eq!(a.archived_standing, 1);
        assert_eq!(a.covered, 1);
        assert_eq!(a.excluded, 1);
        assert_eq!(a.contentless, 1);
        assert_eq!(a.unresolved, 1);
        assert_eq!(a.standing(), 5);
        assert_eq!(
            derive_readiness(&a),
            Readiness::NotReady {
                unresolved: 1,
                unhashed: 1
            },
            "contentless never blocks — only the unresolved source does"
        );
    }

    #[test]
    fn contentless_only_root_has_no_blockers() {
        // A root of pure empty files retires with no blockers: there is no
        // content to lose. The account still states them (never silent).
        let mut e1 = source(1, Some(10), false, false);
        e1.size = 0;
        let mut e2 = source(2, None, false, false);
        e2.size = 0;
        let a = build_account(
            &[e1, e2],
            &[],
            &archived_set(&[]),
            &archived_set(&[]),
            &[],
            &HashMap::new(),
        );
        assert_eq!(a.contentless, 2);
        assert_eq!(a.unresolved, 0);
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    #[test]
    fn empty_account_is_all_zero_with_zero_bytes() {
        let a = build_account(
            &[],
            &[],
            &archived_set(&[]),
            &archived_set(&[]),
            &[],
            &HashMap::new(),
        );
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
        let a = build_account(
            &present,
            &[],
            &archived_set(&[10]),
            &archived_set(&[]),
            &[],
            &HashMap::new(),
        );
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    #[test]
    fn unhashed_only_root_still_blocks() {
        // The "user forgot to hash" case: unhashed ⊆ unresolved, blocks.
        let present = vec![source(1, None, false, false)];
        let a = build_account(
            &present,
            &[],
            &archived_set(&[]),
            &archived_set(&[]),
            &[],
            &HashMap::new(),
        );
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
