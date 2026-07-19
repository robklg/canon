//! The archive composition card — a present-tense "Standing here" statement
//! of what a location is made of, read from the last transitions of its
//! surviving sources.
//!
//! A statement of state, not an event log: distinct from the trail ("what
//! happened here?") and from item lineage ("what happened to this one
//! thing" — direction not yet chosen). Deleted content is absent (the trail
//! holds the loss); curation renames don't erase attribution (the
//! `decision_id` set/preserve rule already guarantees this — no code here
//! re-derives it).

use std::collections::{HashMap, HashSet};

use super::decision::Decision;
use super::extraction::DecisionExtraction;
use super::trail::{decision_family, fate_transition, DecisionFamily, FateAspect};

/// Files/bytes for one bucket of the card.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketCount {
    pub files: i64,
    pub bytes: i64,
}

/// One line of the origins section: content this location received from
/// elsewhere, attributed at decision granularity (the extraction ledger's
/// index ceiling — no per-item origin here, that's a receipt's job).
#[derive(Debug, Clone, PartialEq)]
pub enum OriginLine {
    /// A single-origin apply (or several, merged): every contributing
    /// decision drew from exactly one source root, so the line is anchored
    /// on that root rather than on any one decision.
    FromRoot {
        root_path: String,
        /// Whether the origin root is still known to the live index.
        root_removed: bool,
        files: i64,
        bytes: i64,
        /// Every contributing decision, ascending.
        decision_ids: Vec<i64>,
        first_at: i64,
        last_at: i64,
    },
    /// A multi-origin apply: drew from more than one source root in a
    /// single decision, so origin roots aren't merge-worthy — one line per
    /// decision.
    MultiOrigin {
        decision_id: i64,
        origin_count: usize,
        files: i64,
        bytes: i64,
        at: i64,
    },
}

impl OriginLine {
    pub fn files(&self) -> i64 {
        match self {
            OriginLine::FromRoot { files, .. } => *files,
            OriginLine::MultiOrigin { files, .. } => *files,
        }
    }

    /// Deterministic tie-break for equal-`files` ordering: the earliest
    /// decision id this line carries.
    fn tie_break(&self) -> i64 {
        match self {
            OriginLine::FromRoot { decision_ids, .. } => {
                decision_ids.iter().copied().min().unwrap_or(0)
            }
            OriginLine::MultiOrigin { decision_id, .. } => *decision_id,
        }
    }
}

/// One line of the transitioned section: a present source last touched by a
/// decision that isn't an apply or a scan — an exclusion, a restore, or any
/// other command that can stamp `decision_id`. Origin is untracked here (the
/// extraction ledger only attributes apply decisions).
#[derive(Debug, Clone, PartialEq)]
pub struct TransitionedLine {
    pub decision_id: i64,
    /// The registered transition word (`excluded`, `restored`, ...) when one
    /// applies, else the raw command — self-explaining, never guessed.
    pub label: String,
    pub files: i64,
    pub bytes: i64,
}

/// The composition card: what a location is made of, right now.
///
/// Invariant: `files`/`bytes` equal the sum across every bucket
/// (`origins` + `transitioned` + `indexed_here` + `untracked`) — every
/// present source in scope lands in exactly one bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositionCard {
    pub files: i64,
    pub bytes: i64,
    pub origins: Vec<OriginLine>,
    pub transitioned: Vec<TransitionedLine>,
    /// Present sources last touched by a scan (first indexed here, never
    /// archived from elsewhere).
    pub indexed_here: Option<BucketCount>,
    /// Present sources with no stamp at all (`decision_id IS NULL`) —
    /// predate recording. Real data, not an error.
    pub untracked: Option<BucketCount>,
}

impl CompositionCard {
    /// The omission predicate: render the card only when it has something to
    /// say beyond "some files are here, first-indexed or untracked" — i.e.
    /// at least one origin or transitioned line. Lives on the domain type so
    /// every future surface (subcommand, survey, TUI) applies the same rule.
    pub fn has_origin_story(&self) -> bool {
        !self.origins.is_empty() || !self.transitioned.is_empty()
    }
}

struct FromRootAcc {
    root_removed: bool,
    files: i64,
    bytes: i64,
    decision_ids: Vec<i64>,
    first_at: i64,
    last_at: i64,
}

/// Build a composition card from present-source groups, keyed by the
/// decision that stamped each group (`None` = untracked).
///
/// Pure — `decisions` and `extractions_by_decision` must already carry every
/// id referenced by `groups` (the ops layer fetches them); a `Some(id)` group
/// with no matching entry in `decisions` is a caller precondition violation,
/// not a runtime possibility to guess around.
pub fn build_card(
    groups: &HashMap<Option<i64>, BucketCount>,
    decisions: &HashMap<i64, Decision>,
    extractions_by_decision: &HashMap<i64, Vec<DecisionExtraction>>,
    live_root_ids: &HashSet<i64>,
) -> CompositionCard {
    let mut files = 0i64;
    let mut bytes = 0i64;
    let mut from_roots: HashMap<String, FromRootAcc> = HashMap::new();
    let mut origins: Vec<OriginLine> = Vec::new();
    let mut transitioned: Vec<TransitionedLine> = Vec::new();
    let mut indexed_here: Option<BucketCount> = None;
    let mut untracked: Option<BucketCount> = None;

    for (decision_id, bucket) in groups {
        files += bucket.files;
        bytes += bucket.bytes;
        match decision_id {
            None => {
                let entry = untracked.get_or_insert_with(BucketCount::default);
                entry.files += bucket.files;
                entry.bytes += bucket.bytes;
            }
            Some(id) => {
                let decision = decisions
                    .get(id)
                    .expect("composition card: decision must be fetched for every stamped id");
                match decision_family(&decision.command) {
                    DecisionFamily::Archive => {
                        let rows = extractions_by_decision
                            .get(id)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        match rows.len() {
                            0 => {
                                // The stamp says "archived", but no extraction
                                // row exists to say from where — a gap must
                                // read as a gap, not be silently folded into
                                // another bucket or dropped.
                                transitioned.push(TransitionedLine {
                                    decision_id: *id,
                                    label: "archived (origin unknown)".to_string(),
                                    files: bucket.files,
                                    bytes: bucket.bytes,
                                });
                            }
                            1 => {
                                let row = &rows[0];
                                let entry =
                                    from_roots.entry(row.root_path.clone()).or_insert_with(|| {
                                        FromRootAcc {
                                            root_removed: !live_root_ids.contains(&row.root_id),
                                            files: 0,
                                            bytes: 0,
                                            decision_ids: Vec::new(),
                                            first_at: decision.created_at,
                                            last_at: decision.created_at,
                                        }
                                    });
                                entry.files += bucket.files;
                                entry.bytes += bucket.bytes;
                                entry.decision_ids.push(*id);
                                entry.first_at = entry.first_at.min(decision.created_at);
                                entry.last_at = entry.last_at.max(decision.created_at);
                            }
                            origin_count => {
                                origins.push(OriginLine::MultiOrigin {
                                    decision_id: *id,
                                    origin_count,
                                    files: bucket.files,
                                    bytes: bucket.bytes,
                                    at: decision.created_at,
                                });
                            }
                        }
                    }
                    DecisionFamily::Observe => {
                        let entry = indexed_here.get_or_insert_with(BucketCount::default);
                        entry.files += bucket.files;
                        entry.bytes += bucket.bytes;
                    }
                    family => {
                        let label = fate_transition(family, FateAspect::Present)
                            .map(|t| t.as_str().to_string())
                            .unwrap_or_else(|| decision.command.clone());
                        transitioned.push(TransitionedLine {
                            decision_id: *id,
                            label,
                            files: bucket.files,
                            bytes: bucket.bytes,
                        });
                    }
                }
            }
        }
    }

    for (root_path, acc) in from_roots {
        let mut decision_ids = acc.decision_ids;
        decision_ids.sort_unstable();
        origins.push(OriginLine::FromRoot {
            root_path,
            root_removed: acc.root_removed,
            files: acc.files,
            bytes: acc.bytes,
            decision_ids,
            first_at: acc.first_at,
            last_at: acc.last_at,
        });
    }
    origins.sort_by(|a, b| {
        b.files()
            .cmp(&a.files())
            .then(a.tie_break().cmp(&b.tie_break()))
    });
    transitioned.sort_by(|a, b| {
        b.files
            .cmp(&a.files)
            .then(a.decision_id.cmp(&b.decision_id))
    });

    CompositionCard {
        files,
        bytes,
        origins,
        transitioned,
        indexed_here,
        untracked,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_decision(id: i64, command: &str, created_at: i64) -> Decision {
        Decision {
            id,
            command: command.to_string(),
            scope: None,
            command_line: format!("canon {command}"),
            reason: None,
            status: "completed".to_string(),
            count_attempted: None,
            count_completed: None,
            count_failed: None,
            count_skipped: None,
            summary: None,
            canon_version: "test".to_string(),
            created_at,
            receipt_root_id: None,
            receipt_rel_path: None,
        }
    }

    fn mk_extraction(decision_id: i64, root_id: i64, root_path: &str) -> DecisionExtraction {
        DecisionExtraction {
            decision_id,
            root_id,
            root_path: root_path.to_string(),
            rel_prefix: "".to_string(),
            files: 1,
            bytes: Some(1),
            destination_root_id: Some(99),
            destination_path: "/archive".to_string(),
            disposition: None,
        }
    }

    fn bucket(files: i64, bytes: i64) -> BucketCount {
        BucketCount { files, bytes }
    }

    #[test]
    fn single_origin_applies_from_one_root_merge_across_decisions() {
        let d1 = mk_decision(1, "apply", 100);
        let d2 = mk_decision(2, "apply", 200);
        let groups = HashMap::from([(Some(1), bucket(10, 1_000)), (Some(2), bucket(5, 500))]);
        let decisions = HashMap::from([(1, d1), (2, d2)]);
        let extractions = HashMap::from([
            (1, vec![mk_extraction(1, 7, "/vol/a")]),
            (2, vec![mk_extraction(2, 7, "/vol/a")]),
        ]);
        let live = HashSet::from([7]);

        let card = build_card(&groups, &decisions, &extractions, &live);
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_path,
                root_removed,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
            } => {
                assert_eq!(root_path, "/vol/a");
                assert!(!root_removed);
                assert_eq!(*files, 15);
                assert_eq!(*bytes, 1_500);
                assert_eq!(decision_ids, &vec![1, 2]);
                assert_eq!(*first_at, 100);
                assert_eq!(*last_at, 200);
            }
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn multi_origin_apply_gets_its_own_line() {
        let d = mk_decision(1, "apply", 100);
        let groups = HashMap::from([(Some(1), bucket(12, 1_200))]);
        let decisions = HashMap::from([(1, d)]);
        let extractions = HashMap::from([(
            1,
            vec![mk_extraction(1, 7, "/vol/a"), mk_extraction(1, 8, "/vol/b")],
        )]);
        let live = HashSet::from([7, 8]);

        let card = build_card(&groups, &decisions, &extractions, &live);
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::MultiOrigin {
                decision_id,
                origin_count,
                files,
                bytes,
                at,
            } => {
                assert_eq!(*decision_id, 1);
                assert_eq!(*origin_count, 2);
                assert_eq!(*files, 12);
                assert_eq!(*bytes, 1_200);
                assert_eq!(*at, 100);
            }
            OriginLine::FromRoot { .. } => panic!("expected MultiOrigin"),
        }
    }

    #[test]
    fn archive_decision_with_no_extraction_row_is_transitioned_not_dropped() {
        // A gap in the extraction ledger (pre-ledger apply) must still be
        // counted somewhere and must say so, not silently vanish.
        let d = mk_decision(1, "apply", 100);
        let groups = HashMap::from([(Some(1), bucket(3, 300))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(&groups, &decisions, &HashMap::new(), &HashSet::new());

        assert!(card.origins.is_empty());
        assert_eq!(card.transitioned.len(), 1);
        assert_eq!(card.transitioned[0].label, "archived (origin unknown)");
        assert_eq!(card.transitioned[0].files, 3);
        assert_eq!(card.files, 3);
    }

    #[test]
    fn scan_stamp_is_indexed_here() {
        let d = mk_decision(1, "scan", 100);
        let groups = HashMap::from([(Some(1), bucket(20, 2_000))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(&groups, &decisions, &HashMap::new(), &HashSet::new());

        assert_eq!(card.indexed_here, Some(bucket(20, 2_000)));
        assert!(card.origins.is_empty());
        assert!(card.transitioned.is_empty());
    }

    #[test]
    fn exclusion_stamp_is_transitioned_with_registered_label() {
        let d = mk_decision(1, "exclude_set", 100);
        let groups = HashMap::from([(Some(1), bucket(4, 400))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(&groups, &decisions, &HashMap::new(), &HashSet::new());

        assert_eq!(card.transitioned.len(), 1);
        assert_eq!(card.transitioned[0].label, "excluded");
        assert_eq!(card.transitioned[0].decision_id, 1);
        assert_eq!(card.transitioned[0].files, 4);
    }

    #[test]
    fn other_family_transitioned_falls_back_to_raw_command() {
        // No registered transition word for this family/aspect — the raw
        // command name is the self-explaining fallback, never a guess.
        let d = mk_decision(1, "roots_rm", 100);
        let groups = HashMap::from([(Some(1), bucket(1, 1))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(&groups, &decisions, &HashMap::new(), &HashSet::new());

        assert_eq!(card.transitioned[0].label, "roots_rm");
    }

    #[test]
    fn null_stamp_is_untracked() {
        let groups = HashMap::from([(None, bucket(7, 700))]);
        let card = build_card(&groups, &HashMap::new(), &HashMap::new(), &HashSet::new());

        assert_eq!(card.untracked, Some(bucket(7, 700)));
        assert!(!card.has_origin_story());
    }

    #[test]
    fn sum_invariant_across_every_bucket() {
        let apply = mk_decision(1, "apply", 100);
        let scan = mk_decision(2, "scan", 200);
        let exclude = mk_decision(3, "exclude_set", 300);
        let groups = HashMap::from([
            (Some(1), bucket(10, 1_000)),
            (Some(2), bucket(20, 2_000)),
            (Some(3), bucket(30, 3_000)),
            (None, bucket(40, 4_000)),
        ]);
        let decisions = HashMap::from([(1, apply), (2, scan), (3, exclude)]);
        let extractions = HashMap::from([(1, vec![mk_extraction(1, 7, "/vol/a")])]);
        let live = HashSet::from([7]);

        let card = build_card(&groups, &decisions, &extractions, &live);
        assert_eq!(card.files, 100);
        assert_eq!(card.bytes, 10_000);

        let bucket_sum_files: i64 = card.origins.iter().map(|o| o.files()).sum::<i64>()
            + card.transitioned.iter().map(|t| t.files).sum::<i64>()
            + card.indexed_here.map(|b| b.files).unwrap_or(0)
            + card.untracked.map(|b| b.files).unwrap_or(0);
        assert_eq!(bucket_sum_files, card.files);
    }

    #[test]
    fn has_origin_story_false_for_indexed_here_and_untracked_only() {
        let scan = mk_decision(1, "scan", 100);
        let groups = HashMap::from([(Some(1), bucket(5, 500)), (None, bucket(2, 200))]);
        let decisions = HashMap::from([(1, scan)]);
        let card = build_card(&groups, &decisions, &HashMap::new(), &HashSet::new());

        assert!(card.indexed_here.is_some());
        assert!(card.untracked.is_some());
        assert!(!card.has_origin_story());
    }

    #[test]
    fn origins_ordered_by_files_descending() {
        let d1 = mk_decision(1, "apply", 100);
        let d2 = mk_decision(2, "apply", 200);
        let groups = HashMap::from([(Some(1), bucket(5, 500)), (Some(2), bucket(50, 5_000))]);
        let decisions = HashMap::from([(1, d1), (2, d2)]);
        let extractions = HashMap::from([
            (1, vec![mk_extraction(1, 7, "/vol/small")]),
            (2, vec![mk_extraction(2, 8, "/vol/big")]),
        ]);
        let live = HashSet::from([7, 8]);

        let card = build_card(&groups, &decisions, &extractions, &live);
        assert_eq!(card.origins.len(), 2);
        assert_eq!(card.origins[0].files(), 50);
        assert_eq!(card.origins[1].files(), 5);
    }

    #[test]
    fn removed_origin_root_marked() {
        let d = mk_decision(1, "apply", 100);
        let groups = HashMap::from([(Some(1), bucket(1, 1))]);
        let decisions = HashMap::from([(1, d)]);
        let extractions = HashMap::from([(1, vec![mk_extraction(1, 7, "/vol/gone")])]);
        // root_id 7 is absent from the live set.
        let card = build_card(&groups, &decisions, &extractions, &HashSet::new());

        match &card.origins[0] {
            OriginLine::FromRoot { root_removed, .. } => assert!(root_removed),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }
}
