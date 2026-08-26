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

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::fate::{decision_family, fate_transition, DecisionFamily, FateAspect};
use crate::core::domain::path::path_is_under;

use super::placement::{classify_row, RowAspect};

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
        /// Whether the origin root *contains the viewed scope* — the content
        /// came from elsewhere within this same root, not in from outside.
        ///
        /// A real crossing (the origin sits outside the view, so it is not a
        /// rearrangement *here*), but naming the root alone would read as
        /// "from the place I'm already standing in". Constant across a merge
        /// group: the group's key is the root path, and roots cannot nest, so
        /// at most one root can contain any view.
        from_within: bool,
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

/// One line of the transitioned section: present sources last touched by a
/// decision that isn't an apply or a scan — an exclusion, a restore, or any
/// other command that can stamp `decision_id`. Origin is untracked here (the
/// extraction ledger only attributes apply decisions).
///
/// Two shapes, because two of the four things this section says are not
/// standings at all. The card is a **state** statement — the present-tense
/// fact about a place — so a standing is *what stands here*, merged across
/// every decision that produced it and carrying no decision id: the acts
/// behind it are the timeline's to hold, directly above. A **gap** is the
/// exception: a hole in the record *about one specific decision*, which is
/// the whole reason it exists (self-explaining gaps, never silent), so it
/// keeps its id and is never merged.
#[derive(Debug, Clone, PartialEq)]
pub enum TransitionedLine {
    /// A standing, merged across every decision that produced it.
    /// `label` is the registered transition word where one applies, the raw
    /// command otherwise — derived, never a literal.
    Standing {
        label: String,
        files: i64,
        bytes: i64,
    },
    /// A gap in the record, about one specific decision — which is what makes
    /// it self-explaining. Never merged.
    Gap {
        decision_id: i64,
        label: String,
        files: i64,
        bytes: i64,
    },
}

impl TransitionedLine {
    pub fn label(&self) -> &str {
        match self {
            TransitionedLine::Standing { label, .. } => label,
            TransitionedLine::Gap { label, .. } => label,
        }
    }

    /// The line's counts, in the shape every other bucket of the card uses —
    /// one accessor rather than a `files`/`bytes` pair, so the sum invariant
    /// reads over lines exactly as it reads over the other buckets.
    pub fn bucket(&self) -> BucketCount {
        match self {
            TransitionedLine::Standing { files, bytes, .. }
            | TransitionedLine::Gap { files, bytes, .. } => BucketCount {
                files: *files,
                bytes: *bytes,
            },
        }
    }
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

/// The shape of a view, as far as the composition card is concerned.
///
/// Named fields rather than three positional bools: the three conditions are
/// interchangeable at a call site and a swapped pair would silently invert a
/// gate that no output makes obvious.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewShape {
    /// `--jsonl` — machine output.
    pub machine_output: bool,
    /// No scope prefixes: every root at once.
    pub global: bool,
    /// `--today` / `--since` / `--on` — the day-grouped story view.
    pub time_lens: bool,
}

/// Whether a view carries a composition card at all.
///
/// The card is a **scoped, present-tense** reading, so all three exclusions
/// follow from what it is rather than from display taste:
/// - **global** — no single "here" whose composition could be stated;
/// - **time lens** — a day-grouped event story; a state statement has no day
///   to belong to;
/// - **`--jsonl`** — machine output, whose provenance surface is the
///   view-independent `extractions` field; a view-dependent card would make
///   scripted output vary by where it was run.
///
/// Lives beside [`CompositionCard::has_origin_story`] so both omission rules
/// have one home: this one decides whether to *ask*, that one whether there
/// is anything to *say*.
pub fn card_applies(view: ViewShape) -> bool {
    !view.machine_output && !view.global && !view.time_lens
}

/// Accumulator for one merged origin-root line.
///
/// Deliberately holds neither `root_removed` nor anything else derivable from
/// the group key: the key *is* the root path, so both live-ness and
/// within-ness are properties of the group, decided once after merging rather
/// than by whichever row happened to arrive first.
struct FromRootAcc {
    from_within: bool,
    files: i64,
    bytes: i64,
    decision_ids: Vec<i64>,
    first_at: i64,
    last_at: i64,
}

/// Fold one decision's bucket into the standing its label names.
fn add_standing(standings: &mut HashMap<String, BucketCount>, label: &str, bucket: &BucketCount) {
    let entry = standings.entry(label.to_string()).or_default();
    entry.files += bucket.files;
    entry.bytes += bucket.bytes;
}

/// A gap line's decision id, for its deterministic tie-break. Total by
/// construction — only [`TransitionedLine::Gap`] ever reaches the gap
/// ordering — but written as a match rather than an `unreachable!` so a
/// future variant is a compile error, not a panic in a display path.
fn gap_id(line: &TransitionedLine) -> i64 {
    match line {
        TransitionedLine::Gap { decision_id, .. } => *decision_id,
        TransitionedLine::Standing { .. } => i64::MAX,
    }
}

/// Whether an origin root contains the viewed scope — i.e. the content came
/// from elsewhere *within this same root* rather than in from outside it.
///
/// Distinct from the rearrangement guard: this row's origin sits outside the
/// view (or the guard would have caught it), so content really did arrive.
/// Only the display is at stake — `from /archive` while standing in
/// `/archive/2020` reads as "from the place I'm already in".
fn contains_view(root_path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|p| path_is_under(p, root_path))
}

/// Build a composition card from present-source groups, keyed by the
/// decision that stamped each group (`None` = untracked).
///
/// Pure — the ops layer fetches `decisions` and `extractions_by_decision` for
/// every id referenced by `groups`. A `Some(id)` group with no matching entry
/// in `decisions` is nonetheless representable: `sources.decision_id` carries
/// no foreign key, and the DB is a rebuildable index whose decision rows may
/// not have survived. Such a stamp becomes its own transitioned line rather
/// than a panic or a silent fold into another bucket — a gap must read as a
/// gap.
///
/// `prefixes` is the viewed scope, used only to tell a real origin from this
/// place itself (see the rearrangement guard below).
pub fn build_card(
    groups: &HashMap<Option<i64>, BucketCount>,
    decisions: &HashMap<i64, Decision>,
    extractions_by_decision: &HashMap<i64, Vec<DecisionExtraction>>,
    live_root_paths: &HashSet<String>,
    prefixes: &[String],
) -> CompositionCard {
    let mut files = 0i64;
    let mut bytes = 0i64;
    let mut from_roots: HashMap<String, FromRootAcc> = HashMap::new();
    let mut origins: Vec<OriginLine> = Vec::new();
    // Standings merge on their label — the card states what stands here, and
    // "excluded" is one fact however many decisions produced it. Gaps stay
    // per-decision: naming the one decision is what makes a gap
    // self-explaining rather than a shrug.
    let mut standings: HashMap<String, BucketCount> = HashMap::new();
    let mut gaps: Vec<TransitionedLine> = Vec::new();
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
                let Some(decision) = decisions.get(id) else {
                    // The stamp names a decision the index no longer holds.
                    // Say so on its own line: the content's standing is real,
                    // only the story behind it was lost.
                    gaps.push(TransitionedLine::Gap {
                        decision_id: *id,
                        label: "transition unrecorded".to_string(),
                        files: bucket.files,
                        bytes: bucket.bytes,
                    });
                    continue;
                };
                match decision_family(&decision.command) {
                    DecisionFamily::Archive => {
                        let rows = extractions_by_decision
                            .get(id)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        // A place is not its own origin. When every row of
                        // this apply was drawn from inside the view, nothing
                        // arrived — content was rearranged here, and an
                        // origin line naming this very place would occupy the
                        // line that should hold the answer.
                        //
                        // The card classifies **per decision** (`all` rows)
                        // while the trail's rollups classify per row. That
                        // asymmetry is forced, not an oversight: a source's
                        // stamp names a *decision*, not a row, so for an apply
                        // that drew from several roots the card cannot know
                        // which surviving files came from which side. Any row
                        // from outside means real content arrived, so the
                        // origin line stays rather than claiming a
                        // rearrangement it can't substantiate. Do not "fix"
                        // this into per-row — it would fake attribution the
                        // index cannot support.
                        if !rows.is_empty()
                            && rows
                                .iter()
                                .all(|row| classify_row(row, prefixes) == RowAspect::Rearrangement)
                        {
                            add_standing(&mut standings, "rearranged", bucket);
                            continue;
                        }
                        // Origin multiplicity is counted over distinct origin
                        // *roots*, never raw rows: directory-precision
                        // recording gives one apply several rows per origin
                        // root (one per placement pair), and a single-drive
                        // delivery must not read as "from N origins" just
                        // because it fanned out into N folders.
                        let mut origin_roots: Vec<&str> =
                            rows.iter().map(|r| r.root_path.as_str()).collect();
                        origin_roots.sort_unstable();
                        origin_roots.dedup();
                        match origin_roots.len() {
                            0 => {
                                // The stamp says "archived", but no extraction
                                // row exists to say from where — a gap must
                                // read as a gap, not be silently folded into
                                // another bucket or dropped.
                                gaps.push(TransitionedLine::Gap {
                                    decision_id: *id,
                                    label: "archived (origin unknown)".to_string(),
                                    files: bucket.files,
                                    bytes: bucket.bytes,
                                });
                            }
                            1 => {
                                let root_path = origin_roots[0];
                                let entry =
                                    from_roots.entry(root_path.to_string()).or_insert_with(|| {
                                        FromRootAcc {
                                            from_within: contains_view(root_path, prefixes),
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
                        add_standing(&mut standings, &label, bucket);
                    }
                }
            }
        }
    }

    for (root_path, acc) in from_roots {
        let mut decision_ids = acc.decision_ids;
        decision_ids.sort_unstable();
        // Decided from the group key, so a root removed and re-added can't be
        // called removed on the strength of whichever stale row merged first.
        let root_removed = !live_root_paths.iter().any(|p| p == &root_path);
        origins.push(OriginLine::FromRoot {
            root_path,
            root_removed,
            from_within: acc.from_within,
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
    // Standings first, then gaps: a gap is an exception and belongs after
    // the ordinary reading. Within each group, files descending — the card's
    // established ordering — with a deterministic tie-break (label for a
    // merged standing, which has no id; decision id for a gap, which does).
    let mut transitioned: Vec<TransitionedLine> = standings
        .into_iter()
        .map(|(label, bucket)| TransitionedLine::Standing {
            label,
            files: bucket.files,
            bytes: bucket.bytes,
        })
        .collect();
    transitioned.sort_by(|a, b| {
        b.bucket()
            .files
            .cmp(&a.bucket().files)
            .then(a.label().cmp(b.label()))
    });
    gaps.sort_by(|a, b| {
        b.bucket()
            .files
            .cmp(&a.bucket().files)
            .then(gap_id(a).cmp(&gap_id(b)))
    });
    transitioned.extend(gaps);

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

    /// Standing at the archive that `mk_extraction`'s rows land in — every
    /// origin below is a real crossing unless a test says otherwise.
    fn view() -> Vec<String> {
        vec!["/archive".to_string()]
    }

    /// The live roots, by path — `build_card` decides removed-ness by
    /// location, not by id.
    fn live(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    /// An origin line's bytes. A test helper rather than a public accessor
    /// beside `files()`: only the sum-invariant tests need it, and the domain
    /// API shouldn't grow a method production never calls.
    fn origin_bytes(line: &OriginLine) -> i64 {
        match line {
            OriginLine::FromRoot { bytes, .. } => *bytes,
            OriginLine::MultiOrigin { bytes, .. } => *bytes,
        }
    }

    #[test]
    fn several_placement_rows_from_one_root_are_still_a_single_origin() {
        // Directory-precision recording gives one apply a row per placement
        // pair. Two rows, one origin root: a single-drive delivery that
        // fanned out into two folders — a `from <root>` line, never
        // "via apply #N from 2 origins".
        let d = mk_decision(7, "apply", 100);
        let groups = HashMap::from([(Some(7), bucket(2, 12))]);
        let decisions = HashMap::from([(7, d)]);
        let mut to_a = mk_extraction(7, 3, "/Volumes/sd");
        to_a.destination_path = "/archive/n/dir-d".to_string();
        let mut to_b = mk_extraction(7, 3, "/Volumes/sd");
        to_b.destination_path = "/archive/n/dir-e".to_string();
        let extractions = HashMap::from([(7, vec![to_a, to_b])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/Volumes/sd", "/archive"]),
            &view(),
        );
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_path, files, ..
            } => {
                assert_eq!(root_path, "/Volumes/sd");
                assert_eq!(*files, 2);
            }
            OriginLine::MultiOrigin { .. } => {
                panic!("one origin root fanned across folders is not multi-origin")
            }
        }
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
        let live = live(&["/vol/a"]);

        let card = build_card(&groups, &decisions, &extractions, &live, &view());
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_path,
                root_removed,
                from_within,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
            } => {
                assert_eq!(root_path, "/vol/a");
                assert!(!root_removed);
                assert!(!from_within, "/vol/a does not contain /archive");
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
        let live = live(&["/vol/a", "/vol/b", "/vol/small", "/vol/big"]);

        let card = build_card(&groups, &decisions, &extractions, &live, &view());
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
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert!(card.origins.is_empty());
        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Gap {
                decision_id: 1,
                label: "archived (origin unknown)".to_string(),
                files: 3,
                bytes: 300,
            }]
        );
        assert_eq!(card.files, 3);
    }

    // card_applies — the gate on whether a view asks for a card at all

    /// A scoped, human, scope-lens view: the one shape that carries a card.
    fn scoped_human_view() -> ViewShape {
        ViewShape {
            machine_output: false,
            global: false,
            time_lens: false,
        }
    }

    #[test]
    fn card_applies_only_to_a_scoped_human_scope_lens_view() {
        assert!(card_applies(scoped_human_view()));
    }

    #[test]
    fn each_condition_alone_suppresses_the_card() {
        // Each exclusion must stand on its own — a gate that only fires when
        // two conditions coincide would let the card leak into, say, a global
        // human view.
        for (name, view) in [
            (
                "--jsonl",
                ViewShape {
                    machine_output: true,
                    ..scoped_human_view()
                },
            ),
            (
                "global",
                ViewShape {
                    global: true,
                    ..scoped_human_view()
                },
            ),
            (
                "time lens",
                ViewShape {
                    time_lens: true,
                    ..scoped_human_view()
                },
            ),
        ] {
            assert!(!card_applies(view), "{name} must suppress the card");
        }
    }

    #[test]
    fn conditions_combine_without_cancelling() {
        // No pair or triple of exclusions may re-enable the card.
        for machine_output in [false, true] {
            for global in [false, true] {
                for time_lens in [false, true] {
                    let view = ViewShape {
                        machine_output,
                        global,
                        time_lens,
                    };
                    let any_excluded = machine_output || global || time_lens;
                    assert_eq!(card_applies(view), !any_excluded, "{view:?}");
                }
            }
        }
    }

    #[test]
    fn origin_inside_the_view_is_rearranged_not_an_origin_line() {
        // The pathology: standing at /archive after a curation pass that
        // moved content from /archive/2016 to /archive/2020, the card used to
        // answer "from /archive" — this place naming itself, a non-answer in
        // the line that should hold the answer.
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(47, 3_900))]);
        let decisions = HashMap::from([(42, d)]);
        let mut row = mk_extraction(42, 7, "/archive");
        row.rel_prefix = "2016".to_string();
        row.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([(42, vec![row])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive", "/vol/a", "/Volumes/sd"]),
            &view(),
        );
        assert!(card.origins.is_empty(), "a place is not its own origin");
        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Standing {
                label: "rearranged".to_string(),
                files: 47,
                bytes: 3_900,
            }]
        );
        // Rearranged content still stands here.
        assert_eq!(card.files, 47);
        assert!(card.has_origin_story());
    }

    #[test]
    fn narrower_view_keeps_the_origin_line_for_the_same_decision() {
        // Viewed from /archive/2020 the origin /archive/2016 is outside, so
        // content genuinely arrived — the same scope-dependence the rollups
        // have, from the same rule.
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(47, 3_900))]);
        let decisions = HashMap::from([(42, d)]);
        let mut row = mk_extraction(42, 7, "/archive");
        row.rel_prefix = "2016".to_string();
        row.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([(42, vec![row])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive", "/vol/a", "/Volumes/sd"]),
            &["/archive/2020".to_string()],
        );
        assert!(card.transitioned.is_empty());
        assert_eq!(card.origins.len(), 1);
    }

    #[test]
    fn origin_root_containing_the_view_is_marked_from_within() {
        // Standing at /archive/2020, content drawn from /archive/2016 really
        // did arrive (its origin is outside this view) — but the origin line
        // is anchored on the *root*, so a bare "from /archive" would name the
        // place the reader is already standing in.
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(47, 3_900))]);
        let decisions = HashMap::from([(42, d)]);
        let mut row = mk_extraction(42, 7, "/archive");
        row.rel_prefix = "2016".to_string();
        row.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([(42, vec![row])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive", "/vol/a", "/Volumes/sd"]),
            &["/archive/2020".to_string()],
        );
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_path,
                from_within,
                ..
            } => {
                assert_eq!(root_path, "/archive");
                assert!(from_within);
            }
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn origin_root_outside_the_view_is_not_from_within() {
        // The ordinary case: a source drive is not an ancestor of the view.
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(47, 3_900))]);
        let decisions = HashMap::from([(42, d)]);
        let extractions = HashMap::from([(42, vec![mk_extraction(42, 7, "/Volumes/sd")])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive", "/vol/a", "/Volumes/sd"]),
            &["/archive/2020".to_string()],
        );
        match &card.origins[0] {
            OriginLine::FromRoot { from_within, .. } => assert!(!from_within),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn contains_view_respects_segment_boundaries() {
        // /archive-old does not contain /archive/2020, however similar the
        // strings look — the same segment rule the rest of the trail obeys.
        assert!(contains_view("/archive", &["/archive/2020".to_string()]));
        assert!(contains_view("/archive", &["/archive".to_string()]));
        assert!(!contains_view(
            "/archive-old",
            &["/archive/2020".to_string()]
        ));
        assert!(!contains_view("/archive/2020", &["/archive".to_string()]));
        // Multi-prefix view: within-ness holds if any prefix is under it.
        assert!(contains_view(
            "/archive",
            &["/Volumes/sd".to_string(), "/archive/2020".to_string()]
        ));
    }

    #[test]
    fn any_row_from_outside_keeps_the_origin_line() {
        // The per-decision ceiling, stated as a test: this apply drew from
        // inside *and* outside the view, and the stamp names the decision,
        // not the row — the card cannot know which surviving files came from
        // which side, so it keeps the origin line rather than claiming a
        // rearrangement it can't substantiate.
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(55, 4_700))]);
        let decisions = HashMap::from([(42, d)]);
        let mut inside = mk_extraction(42, 7, "/archive");
        inside.rel_prefix = "2016".to_string();
        inside.destination_path = "/archive/2020".to_string();
        let mut outside = mk_extraction(42, 8, "/Volumes/sd");
        outside.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([(42, vec![inside, outside])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/vol/a", "/vol/b", "/Volumes/sd", "/archive"]),
            &view(),
        );
        assert!(card.transitioned.is_empty(), "not a rearrangement");
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::MultiOrigin { origin_count, .. } => assert_eq!(*origin_count, 2),
            OriginLine::FromRoot { .. } => panic!("expected MultiOrigin"),
        }
    }

    #[test]
    fn sum_invariant_holds_with_a_rearranged_bucket() {
        let apply = mk_decision(1, "apply", 100);
        let curation = mk_decision(2, "apply", 200);
        let scan = mk_decision(3, "scan", 300);
        let groups = HashMap::from([
            (Some(1), bucket(10, 1_000)),
            (Some(2), bucket(47, 3_900)),
            (Some(3), bucket(20, 2_000)),
            (None, bucket(5, 500)),
        ]);
        let decisions = HashMap::from([(1, apply), (2, curation), (3, scan)]);
        let mut rearranged = mk_extraction(2, 7, "/archive");
        rearranged.rel_prefix = "2016".to_string();
        rearranged.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([
            (1, vec![mk_extraction(1, 8, "/vol/a")]),
            (2, vec![rearranged]),
        ]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/vol/a", "/vol/b", "/Volumes/sd", "/archive"]),
            &view(),
        );
        assert_eq!(card.files, 82);
        assert_eq!(card.bytes, 7_400);

        let sum_files: i64 = card.origins.iter().map(|o| o.files()).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().files)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.files).unwrap_or(0)
            + card.untracked.map(|b| b.files).unwrap_or(0);
        let sum_bytes: i64 = card.origins.iter().map(origin_bytes).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().bytes)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.bytes).unwrap_or(0)
            + card.untracked.map(|b| b.bytes).unwrap_or(0);
        assert_eq!(sum_files, card.files);
        assert_eq!(sum_bytes, card.bytes);
    }

    #[test]
    fn a_card_whose_only_story_is_rearrangement_still_renders() {
        let d = mk_decision(42, "apply", 100);
        let groups = HashMap::from([(Some(42), bucket(47, 3_900)), (None, bucket(3, 300))]);
        let decisions = HashMap::from([(42, d)]);
        let mut row = mk_extraction(42, 7, "/archive");
        row.rel_prefix = "2016".to_string();
        row.destination_path = "/archive/2020".to_string();
        let extractions = HashMap::from([(42, vec![row])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive", "/vol/a", "/Volumes/sd"]),
            &view(),
        );
        assert!(card.origins.is_empty());
        assert!(
            card.has_origin_story(),
            "a rearrangement is a story worth showing"
        );
    }

    #[test]
    fn scan_stamp_is_indexed_here() {
        let d = mk_decision(1, "scan", 100);
        let groups = HashMap::from([(Some(1), bucket(20, 2_000))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(card.indexed_here, Some(bucket(20, 2_000)));
        assert!(card.origins.is_empty());
        assert!(card.transitioned.is_empty());
    }

    #[test]
    fn exclusion_stamp_is_transitioned_with_registered_label() {
        let d = mk_decision(1, "exclude_set", 100);
        let groups = HashMap::from([(Some(1), bucket(4, 400))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        // A standing, not an event: the registered word, no decision id.
        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Standing {
                label: "excluded".to_string(),
                files: 4,
                bytes: 400,
            }]
        );
    }

    #[test]
    fn other_family_transitioned_falls_back_to_raw_command() {
        // No registered transition word for this family/aspect — the raw
        // command name is the self-explaining fallback, never a guess.
        let d = mk_decision(1, "roots_rm", 100);
        let groups = HashMap::from([(Some(1), bucket(1, 1))]);
        let decisions = HashMap::from([(1, d)]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(card.transitioned[0].label(), "roots_rm");
    }

    // The transitioned section's grain: standings merge, gaps do not.

    /// The card is a state statement. Three exclude sessions leave one
    /// standing — the acts behind it are the timeline's, directly above.
    #[test]
    fn standings_merge_across_decisions_and_carry_no_ids() {
        let groups = HashMap::from([
            (Some(1), bucket(20, 2_000)),
            (Some(2), bucket(3, 300)),
            (Some(3), bucket(5, 500)),
        ]);
        let decisions = HashMap::from([
            (1, mk_decision(1, "exclude_set", 100)),
            (2, mk_decision(2, "exclude_set", 200)),
            (3, mk_decision(3, "exclude_set_object", 300)),
        ]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Standing {
                label: "excluded".to_string(),
                files: 28,
                bytes: 2_800,
            }]
        );
    }

    /// Both gap labels stay per-decision: naming the one decision is what
    /// makes the gap self-explaining rather than a shrug.
    #[test]
    fn a_gap_line_keeps_its_decision_id_and_is_never_merged() {
        // #1 and #2: applies with no extraction row (the ledger gap).
        // #404 and #405: stamps naming decisions the index no longer holds.
        let groups = HashMap::from([
            (Some(1), bucket(9, 900)),
            (Some(2), bucket(8, 800)),
            (Some(404), bucket(7, 700)),
            (Some(405), bucket(6, 600)),
        ]);
        let decisions = HashMap::from([
            (1, mk_decision(1, "apply", 100)),
            (2, mk_decision(2, "apply", 200)),
        ]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(
            card.transitioned,
            vec![
                TransitionedLine::Gap {
                    decision_id: 1,
                    label: "archived (origin unknown)".to_string(),
                    files: 9,
                    bytes: 900,
                },
                TransitionedLine::Gap {
                    decision_id: 2,
                    label: "archived (origin unknown)".to_string(),
                    files: 8,
                    bytes: 800,
                },
                TransitionedLine::Gap {
                    decision_id: 404,
                    label: "transition unrecorded".to_string(),
                    files: 7,
                    bytes: 700,
                },
                TransitionedLine::Gap {
                    decision_id: 405,
                    label: "transition unrecorded".to_string(),
                    files: 6,
                    bytes: 600,
                },
            ]
        );
    }

    /// "rearranged" is a statement about how much of what stands here was
    /// moved within it — a standing, not a fact about one apply.
    #[test]
    fn rearranged_merges_like_any_other_standing() {
        let groups = HashMap::from([(Some(41), bucket(10, 1_000)), (Some(42), bucket(4, 400))]);
        let decisions = HashMap::from([
            (41, mk_decision(41, "apply", 100)),
            (42, mk_decision(42, "apply", 200)),
        ]);
        let mut row_a = mk_extraction(41, 7, "/archive");
        row_a.rel_prefix = "2016".to_string();
        row_a.destination_path = "/archive/2020".to_string();
        let mut row_b = mk_extraction(42, 7, "/archive");
        row_b.rel_prefix = "2017".to_string();
        row_b.destination_path = "/archive/2021".to_string();
        let extractions = HashMap::from([(41, vec![row_a]), (42, vec![row_b])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/archive"]),
            &view(),
        );

        assert!(card.origins.is_empty());
        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Standing {
                label: "rearranged".to_string(),
                files: 14,
                bytes: 1_400,
            }]
        );
    }

    /// The raw-command fallback merges by command name — it is still a
    /// standing, just one the fate vocabulary has no word for yet.
    #[test]
    fn an_unregistered_command_merges_by_its_command_name() {
        let groups = HashMap::from([(Some(1), bucket(2, 200)), (Some(2), bucket(3, 300))]);
        let decisions = HashMap::from([
            (1, mk_decision(1, "roots_rm", 100)),
            (2, mk_decision(2, "roots_rm", 200)),
        ]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Standing {
                label: "roots_rm".to_string(),
                files: 5,
                bytes: 500,
            }]
        );
    }

    /// The sum invariant is a law, and merging must not leak a bucket.
    #[test]
    fn the_sum_invariant_holds_with_merged_standings() {
        let groups = HashMap::from([
            (Some(1), bucket(10, 1_000)), // apply, real origin
            (Some(2), bucket(20, 2_000)), // scan -> indexed_here
            (Some(3), bucket(30, 3_000)), // exclude_set  \ merge
            (Some(4), bucket(5, 500)),    // exclude_set  /
            (Some(5), bucket(7, 700)),    // apply, no rows -> gap
            (Some(404), bucket(6, 600)),  // missing decision -> gap
            (None, bucket(40, 4_000)),    // untracked
        ]);
        let decisions = HashMap::from([
            (1, mk_decision(1, "apply", 100)),
            (2, mk_decision(2, "scan", 200)),
            (3, mk_decision(3, "exclude_set", 300)),
            (4, mk_decision(4, "exclude_set", 400)),
            (5, mk_decision(5, "apply", 500)),
        ]);
        let extractions = HashMap::from([(1, vec![mk_extraction(1, 8, "/vol/a")])]);

        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/vol/a"]),
            &view(),
        );

        assert_eq!(card.files, 118);
        assert_eq!(card.bytes, 11_800);
        let sum_files: i64 = card.origins.iter().map(|o| o.files()).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().files)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.files).unwrap_or(0)
            + card.untracked.map(|b| b.files).unwrap_or(0);
        let sum_bytes: i64 = card.origins.iter().map(origin_bytes).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().bytes)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.bytes).unwrap_or(0)
            + card.untracked.map(|b| b.bytes).unwrap_or(0);
        assert_eq!(sum_files, card.files);
        assert_eq!(sum_bytes, card.bytes);
    }

    /// Files descending, label ascending as the deterministic tie-break — a
    /// merged standing has no id to break the tie with.
    #[test]
    fn standings_sort_by_files_then_label() {
        let groups = HashMap::from([
            (Some(1), bucket(5, 500)),
            (Some(2), bucket(5, 500)),
            (Some(3), bucket(9, 900)),
        ]);
        let decisions = HashMap::from([
            (1, mk_decision(1, "exclude_clear", 100)), // restored
            (2, mk_decision(2, "exclude_set", 200)),   // excluded
            (3, mk_decision(3, "roots_rm", 300)),      // roots_rm
        ]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        let labels: Vec<&str> = card.transitioned.iter().map(|t| t.label()).collect();
        assert_eq!(labels, vec!["roots_rm", "excluded", "restored"]);
    }

    /// A gap is an exception and belongs after the ordinary reading, however
    /// many files it carries.
    #[test]
    fn gap_lines_render_after_standings() {
        let groups = HashMap::from([
            (Some(1), bucket(1, 100)),       // exclude -> standing, 1 file
            (Some(404), bucket(999, 9_900)), // gap, far more files
        ]);
        let decisions = HashMap::from([(1, mk_decision(1, "exclude_set", 100))]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(card.transitioned.len(), 2);
        assert!(matches!(
            card.transitioned[0],
            TransitionedLine::Standing { .. }
        ));
        assert!(matches!(card.transitioned[1], TransitionedLine::Gap { .. }));
    }

    #[test]
    fn null_stamp_is_untracked() {
        let groups = HashMap::from([(None, bucket(7, 700))]);
        let card = build_card(
            &groups,
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

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
        let live = live(&["/vol/a"]);

        let card = build_card(&groups, &decisions, &extractions, &live, &view());
        assert_eq!(card.files, 100);
        assert_eq!(card.bytes, 10_000);

        let bucket_sum_files: i64 = card.origins.iter().map(|o| o.files()).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().files)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.files).unwrap_or(0)
            + card.untracked.map(|b| b.files).unwrap_or(0);
        assert_eq!(bucket_sum_files, card.files);

        // Bytes too: a card whose files add up but whose sizes don't would
        // still be misreporting what stands here.
        let bucket_sum_bytes: i64 = card.origins.iter().map(origin_bytes).sum::<i64>()
            + card
                .transitioned
                .iter()
                .map(|t| t.bucket().bytes)
                .sum::<i64>()
            + card.indexed_here.map(|b| b.bytes).unwrap_or(0)
            + card.untracked.map(|b| b.bytes).unwrap_or(0);
        assert_eq!(bucket_sum_bytes, card.bytes);
    }

    #[test]
    fn stamp_naming_a_missing_decision_becomes_its_own_line() {
        // `sources.decision_id` has no foreign key and the DB is a
        // rebuildable index, so a stamp can outlive its decision row. The
        // card must survive that, keep the files standing, and say the story
        // is missing — never panic, never quietly bucket it as untracked
        // (which means "no stamp at all", a different fact).
        let groups = HashMap::from([(Some(404), bucket(6, 600)), (None, bucket(1, 100))]);
        let card = build_card(
            &groups,
            &HashMap::new(),
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

        assert_eq!(
            card.transitioned,
            vec![TransitionedLine::Gap {
                decision_id: 404,
                label: "transition unrecorded".to_string(),
                files: 6,
                bytes: 600,
            }]
        );
        assert_eq!(card.untracked, Some(bucket(1, 100)));
        assert_eq!(card.files, 7);
        assert_eq!(card.bytes, 700);
    }

    #[test]
    fn has_origin_story_false_for_indexed_here_and_untracked_only() {
        let scan = mk_decision(1, "scan", 100);
        let groups = HashMap::from([(Some(1), bucket(5, 500)), (None, bucket(2, 200))]);
        let decisions = HashMap::from([(1, scan)]);
        let card = build_card(
            &groups,
            &decisions,
            &HashMap::new(),
            &HashSet::new(),
            &view(),
        );

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
        let live = live(&["/vol/a", "/vol/b", "/vol/small", "/vol/big"]);

        let card = build_card(&groups, &decisions, &extractions, &live, &view());
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
        // No live root at that path.
        let card = build_card(&groups, &decisions, &extractions, &HashSet::new(), &view());

        match &card.origins[0] {
            OriginLine::FromRoot { root_removed, .. } => assert!(root_removed),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn re_added_origin_root_is_not_marked_removed() {
        // Removed and re-added, so the live index knows this location under a
        // new id while the snapshot row still carries the old one. The drive
        // is on the desk; saying "(root removed)" would be a lie.
        let d = mk_decision(1, "apply", 100);
        let groups = HashMap::from([(Some(1), bucket(1, 1))]);
        let decisions = HashMap::from([(1, d)]);
        let extractions = HashMap::from([(1, vec![mk_extraction(1, 7, "/vol/a")])]);
        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/vol/a"]), // live under some other id now
            &view(),
        );

        match &card.origins[0] {
            OriginLine::FromRoot { root_removed, .. } => assert!(!root_removed),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn merged_origin_liveness_is_decided_by_the_group_not_the_first_row() {
        // Two applies from the same location, recorded under different ids
        // because the root was removed and re-added between them. Merging
        // keys on the path, so both rows are one line — and its liveness must
        // come from the path, not from whichever row merged first.
        let d1 = mk_decision(1, "apply", 100);
        let d2 = mk_decision(2, "apply", 200);
        let groups = HashMap::from([(Some(1), bucket(10, 1_000)), (Some(2), bucket(5, 500))]);
        let decisions = HashMap::from([(1, d1), (2, d2)]);
        let extractions = HashMap::from([
            (1, vec![mk_extraction(1, 7, "/vol/a")]),  // stale id
            (2, vec![mk_extraction(2, 42, "/vol/a")]), // current id
        ]);
        let card = build_card(
            &groups,
            &decisions,
            &extractions,
            &live(&["/vol/a"]),
            &view(),
        );

        assert_eq!(card.origins.len(), 1, "one location, one line");
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_removed,
                files,
                ..
            } => {
                assert!(!root_removed);
                assert_eq!(*files, 15);
            }
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }
}
