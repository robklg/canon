//! Pure logic for the story review — the judgment instrument that renders a
//! root's resolution story as a map of places (`canon roots story`).
//!
//! The organizing axis is deliberate versus never-explicitly-decided: acts
//! (decisions with their reasons) versus standings that carry no trace of
//! judgment. This module holds the leaf helpers: location aggregation (the
//! one "where" derivation, shared by act destinations and covered-copy
//! locations) and act grouping (the what/why register). The place splitter
//! builds on both.
//!
//! No I/O anywhere here; callers supply everything fetched.

use std::collections::HashMap;

use super::folder_tree::FolderTree;
use super::path::path_is_under;

/// Named calibratable constants (the sweep discipline). Defaults are initial
/// guesses until the first calibration pass against the real archive locks
/// them; recalibration changes constants, never code paths.
pub struct StoryParams {
    /// Children whose standing proportions differ by no more than this tell
    /// the same line and merge into their parent place.
    pub signature_tolerance: f64,
    /// A child below both dust floors lifts into its parent rather than
    /// earning a place line of its own.
    pub dust_floor_files: i64,
    pub dust_floor_bytes: i64,
    /// Location entries per "where" line before the counted remainder.
    pub where_cap: usize,
}

impl Default for StoryParams {
    fn default() -> Self {
        Self {
            signature_tolerance: 0.10,
            dust_floor_files: 10,
            dust_floor_bytes: 1_000_000,
            where_cap: 3,
        }
    }
}

/// One location entry on a "where" line: a directory prefix and how many of
/// the line's files it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationCount {
    pub path: String,
    pub files: i64,
}

/// A set of locations aggregated to the fewest legible prefixes, capped with
/// a counted remainder — never a silent truncation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocationAggregate {
    pub locations: Vec<LocationCount>,
    /// Location groups beyond the cap ("… and N more locations").
    pub omitted_locations: usize,
}

impl LocationAggregate {
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// The location paths alone — the merge identity for act grouping.
    fn paths(&self) -> Vec<String> {
        self.locations.iter().map(|l| l.path.clone()).collect()
    }
}

/// Aggregate a set of directories (with per-directory file counts) to the
/// fewest legible prefixes.
///
/// `bases` are the path prefixes below which an answer starts to say
/// something (archive root paths for covered locations, known root paths for
/// act destinations): a prefix at or above every base is vacuous — "in the
/// archive" answers nothing — so the walk splits below it instead.
///
/// The rule: collapse single-branch chains from the top to the deepest
/// common node. If that node is strictly below a base, it alone is the
/// answer (the widest honest one-line where — fan-out beneath it is shape,
/// not information). Otherwise emit its branch groups, each chain-collapsed
/// to where it first branches or holds files, ordered by weight then path,
/// capped with a counted remainder.
pub fn aggregate_locations(dirs: &[(&str, i64)], bases: &[&str], cap: usize) -> LocationAggregate {
    if dirs.is_empty() {
        return LocationAggregate::default();
    }

    let mut tree = FolderTree::new();
    let mut direct: Vec<i64> = Vec::new();
    for (dir, files) in dirs {
        let fid = tree.intern(dir) as usize;
        if direct.len() < tree.len() {
            direct.resize(tree.len(), 0);
        }
        direct[fid] += files;
    }
    direct.resize(tree.len(), 0);

    // Subtree sums: ids are parents-first, so one reverse pass suffices.
    let mut sub = direct.clone();
    for fid in (0..tree.len() as u32).rev() {
        if let Some(parent) = tree.parent(fid) {
            sub[parent as usize] += sub[fid as usize];
        }
    }

    let collapse = |mut fid: u32| -> u32 {
        while direct[fid as usize] == 0 && tree.children(fid).len() == 1 {
            fid = tree.children(fid)[0];
        }
        fid
    };

    let top = collapse(0);
    let top_path = tree.path(top);
    let legible = bases
        .iter()
        .any(|base| top_path != *base && path_is_under(top_path, base));
    if legible {
        return LocationAggregate {
            locations: vec![LocationCount {
                path: top_path.to_string(),
                files: sub[top as usize],
            }],
            omitted_locations: 0,
        };
    }

    // Vacuous common node: emit its branch groups instead.
    let mut groups: Vec<LocationCount> = Vec::new();
    if direct[top as usize] > 0 {
        groups.push(LocationCount {
            path: tree.path(top).to_string(),
            files: direct[top as usize],
        });
    }
    for &child in tree.children(top) {
        let group = collapse(child);
        groups.push(LocationCount {
            path: tree.path(group).to_string(),
            files: sub[child as usize],
        });
    }
    groups.sort_by(|a, b| b.files.cmp(&a.files).then_with(|| a.path.cmp(&b.path)));
    let omitted_locations = groups.len().saturating_sub(cap);
    groups.truncate(cap);

    LocationAggregate {
        locations: groups,
        omitted_locations,
    }
}

/// One decision's contribution to a place, before grouping — the atom the
/// splitter derives from stamps and extraction rows.
#[derive(Debug, Clone)]
pub struct ActAtom<'a> {
    pub decision_id: i64,
    pub created_at: i64,
    pub reason: Option<&'a str>,
    /// Registered transition word, derived via `fate_transition` — never a
    /// literal at the call site.
    pub transition: &'static str,
    /// Scan-observed (a deletion the world made) as opposed to performed.
    pub observed: bool,
    pub files: i64,
    /// `None` when the record cannot say — all-or-omitted at group level.
    pub bytes: Option<i64>,
    /// Disposition split for archivals; `None` when any contributing row
    /// predates the vocabulary — omitted, never guessed.
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    /// Destination directories with per-directory file counts (archivals
    /// only; empty for exclusions and deletions — nothing went anywhere).
    pub destination_dirs: Vec<(&'a str, i64)>,
}

/// One decision inside an act group, oldest-first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActDecision {
    pub id: i64,
    pub created_at: i64,
    pub reason: Option<String>,
}

/// Acts aggregated for one place line: same transition, same destination
/// aggregate. The what compresses; the whys (reasons per decision) never
/// disappear; the where never blurs — acts that went to different
/// destinations stay separate lines.
#[derive(Debug, Clone, PartialEq)]
pub struct ActGroup {
    pub transition: &'static str,
    pub observed: bool,
    pub destination: LocationAggregate,
    pub files: i64,
    /// All-or-omitted: `Some` only when every grouped decision knew.
    pub bytes: Option<i64>,
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    pub decisions: Vec<ActDecision>,
}

/// The whys of an act group, ready to render: distinct reasons in
/// first-seen order with the decisions that gave them, and the count that
/// recorded none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasonSummary {
    pub reasons: Vec<(String, Vec<i64>)>,
    pub without_reason: usize,
}

impl ActGroup {
    pub fn reason_summary(&self) -> ReasonSummary {
        let mut reasons: Vec<(String, Vec<i64>)> = Vec::new();
        let mut without_reason = 0;
        for decision in &self.decisions {
            match &decision.reason {
                Some(reason) => match reasons.iter_mut().find(|(r, _)| r == reason) {
                    Some((_, ids)) => ids.push(decision.id),
                    None => reasons.push((reason.clone(), vec![decision.id])),
                },
                None => without_reason += 1,
            }
        }
        ReasonSummary {
            reasons,
            without_reason,
        }
    }
}

/// Group a place's act atoms by (transition, posture, destination
/// aggregate). Groups order by their earliest decision; decisions within a
/// group are oldest-first.
pub fn group_acts(atoms: &[ActAtom], bases: &[&str], cap: usize) -> Vec<ActGroup> {
    struct Accum<'a> {
        transition: &'static str,
        observed: bool,
        pooled_dirs: HashMap<&'a str, i64>,
        files: i64,
        bytes: Option<i64>,
        bytes_complete: bool,
        moved: Option<i64>,
        moved_complete: bool,
        copied: Option<i64>,
        copied_complete: bool,
        decisions: Vec<ActDecision>,
    }

    let mut order: Vec<(String, usize)> = Vec::new();
    let mut accums: HashMap<String, Accum> = HashMap::new();

    for atom in atoms {
        let dest = aggregate_locations(&atom.destination_dirs, bases, cap);
        let key = format!(
            "{}|{}|{}",
            atom.transition,
            atom.observed,
            dest.paths().join("\n")
        );
        let accum = accums.entry(key.clone()).or_insert_with(|| {
            order.push((key, order.len()));
            Accum {
                transition: atom.transition,
                observed: atom.observed,
                pooled_dirs: HashMap::new(),
                files: 0,
                bytes: Some(0),
                bytes_complete: true,
                moved: Some(0),
                moved_complete: true,
                copied: Some(0),
                copied_complete: true,
                decisions: Vec::new(),
            }
        });
        for (dir, files) in &atom.destination_dirs {
            *accum.pooled_dirs.entry(dir).or_insert(0) += files;
        }
        accum.files += atom.files;
        match atom.bytes {
            Some(b) => accum.bytes = accum.bytes.map(|acc| acc + b),
            None => accum.bytes_complete = false,
        }
        match atom.moved {
            Some(m) => accum.moved = accum.moved.map(|acc| acc + m),
            None => accum.moved_complete = false,
        }
        match atom.copied {
            Some(c) => accum.copied = accum.copied.map(|acc| acc + c),
            None => accum.copied_complete = false,
        }
        accum.decisions.push(ActDecision {
            id: atom.decision_id,
            created_at: atom.created_at,
            reason: atom.reason.map(str::to_string),
        });
    }

    let mut groups: Vec<ActGroup> = order
        .into_iter()
        .map(|(key, _)| {
            let mut accum = accums.remove(&key).expect("accumulated key");
            accum
                .decisions
                .sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
            let pooled: Vec<(&str, i64)> = {
                let mut dirs: Vec<(&str, i64)> =
                    accum.pooled_dirs.iter().map(|(d, f)| (*d, *f)).collect();
                dirs.sort();
                dirs
            };
            ActGroup {
                transition: accum.transition,
                observed: accum.observed,
                destination: aggregate_locations(&pooled, bases, cap),
                files: accum.files,
                bytes: if accum.bytes_complete {
                    accum.bytes
                } else {
                    None
                },
                moved: if accum.moved_complete {
                    accum.moved
                } else {
                    None
                },
                copied: if accum.copied_complete {
                    accum.copied
                } else {
                    None
                },
                decisions: accum.decisions,
            }
        })
        .collect();

    groups.sort_by_key(|g| {
        g.decisions
            .first()
            .map(|d| (d.created_at, d.id))
            .unwrap_or((i64::MAX, i64::MAX))
    });
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(dirs: &[(&str, i64)], bases: &[&str]) -> LocationAggregate {
        aggregate_locations(dirs, bases, 3)
    }

    #[test]
    fn empty_dirs_aggregate_to_nothing() {
        let out = agg(&[], &["/archive"]);
        assert!(out.is_empty());
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn single_directory_is_its_own_answer() {
        let out = agg(&[("/archive/media/2016", 640)], &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 640
            }]
        );
    }

    #[test]
    fn coherent_fanout_collapses_to_the_common_prefix() {
        // Twelve month dirs under one year: the year is the answer; the
        // fan-out beneath it is shape, not information.
        let dirs: Vec<(String, i64)> = (1..=12)
            .map(|m| (format!("/archive/media/2016/{m:02}"), 10))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 120
            }]
        );
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn divergence_at_the_base_lists_the_groups() {
        // The common node is the archive root itself — vacuous ("in the
        // archive" answers nothing) — so the branch groups are the answer.
        let out = agg(
            &[
                ("/archive/staging-2019/worlds", 3401),
                ("/archive/games", 11),
            ],
            &["/archive"],
        );
        assert_eq!(
            out.locations,
            vec![
                LocationCount {
                    path: "/archive/staging-2019/worlds".into(),
                    files: 3401
                },
                LocationCount {
                    path: "/archive/games".into(),
                    files: 11
                },
            ]
        );
    }

    #[test]
    fn groups_beyond_the_cap_are_counted_never_dropped_silently() {
        let dirs: Vec<(String, i64)> = (0..6)
            .map(|i| (format!("/archive/spot-{i}"), 10 + i as i64))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(out.locations.len(), 3);
        assert_eq!(out.omitted_locations, 3);
        // Heaviest first.
        assert_eq!(out.locations[0].path, "/archive/spot-5");
    }

    #[test]
    fn ordering_is_weight_then_path() {
        let out = agg(
            &[("/archive/b", 10), ("/archive/a", 10), ("/archive/c", 20)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive/c", "/archive/a", "/archive/b"]);
    }

    #[test]
    fn no_matching_base_splits_at_the_first_branching() {
        // A destination whose root is gone from the fleet: nothing is
        // legible, so the top-level branch groups are the answer.
        let out = agg(
            &[("/old-archive/media", 5), ("/old-archive/docs", 3)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/old-archive/media", "/old-archive/docs"]);
    }

    #[test]
    fn base_itself_carrying_files_is_its_own_group() {
        let out = agg(&[("/archive", 7), ("/archive/media", 5)], &["/archive"]);
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive", "/archive/media"]);
    }

    fn atom<'a>(
        id: i64,
        at: i64,
        reason: Option<&'a str>,
        transition: &'static str,
        files: i64,
    ) -> ActAtom<'a> {
        ActAtom {
            decision_id: id,
            created_at: at,
            reason,
            transition,
            observed: false,
            files,
            bytes: None,
            moved: None,
            copied: None,
            destination_dirs: vec![],
        }
    }

    #[test]
    fn iterative_exclusions_merge_into_one_group_with_reasons_enumerated() {
        let atoms = vec![
            atom(57, 100, Some("installer junk"), "excluded", 200),
            atom(61, 200, Some("installer junk"), "excluded", 300),
            atom(63, 300, Some("old exports"), "excluded", 90),
            atom(64, 400, None, "excluded", 4300),
        ];
        let groups = group_acts(&atoms, &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.files, 4890);
        assert_eq!(g.decisions.len(), 4);
        let summary = g.reason_summary();
        assert_eq!(
            summary.reasons,
            vec![
                ("installer junk".to_string(), vec![57, 61]),
                ("old exports".to_string(), vec![63]),
            ]
        );
        assert_eq!(summary.without_reason, 1);
    }

    #[test]
    fn archivals_to_different_destinations_never_merge() {
        let mut a = atom(42, 100, Some("the Italy trip"), "archived", 640);
        a.destination_dirs = vec![("/archive/media/2016-italy", 640)];
        a.bytes = Some(1000);
        let mut b = atom(51, 200, None, "archived", 4102);
        b.destination_dirs = vec![("/archive/media/2017", 4102)];
        b.bytes = Some(2000);
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].decisions[0].id, 42);
        assert_eq!(groups[1].decisions[0].id, 51);
    }

    #[test]
    fn archivals_to_the_same_destination_merge_and_sum() {
        let mut a = atom(42, 100, None, "archived", 100);
        a.destination_dirs = vec![("/archive/media/2016", 100)];
        a.bytes = Some(1_000);
        a.moved = Some(100);
        a.copied = Some(0);
        let mut b = atom(48, 200, None, "archived", 50);
        b.destination_dirs = vec![("/archive/media/2016", 50)];
        b.bytes = Some(500);
        b.moved = Some(20);
        b.copied = Some(30);
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.files, 150);
        assert_eq!(g.bytes, Some(1_500));
        assert_eq!(g.moved, Some(120));
        assert_eq!(g.copied, Some(30));
        assert_eq!(g.destination.locations[0].path, "/archive/media/2016");
        assert_eq!(g.destination.locations[0].files, 150);
    }

    #[test]
    fn bytes_and_disposition_are_all_or_omitted() {
        let mut a = atom(42, 100, None, "archived", 100);
        a.destination_dirs = vec![("/archive/media", 100)];
        a.bytes = Some(1_000);
        a.moved = Some(100);
        a.copied = Some(0);
        let mut b = atom(48, 200, None, "archived", 50);
        b.destination_dirs = vec![("/archive/media", 50)];
        // Pre-vocabulary rows: bytes and disposition unknown.
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].bytes, None);
        assert_eq!(groups[0].moved, None);
        assert_eq!(groups[0].copied, None);
    }

    #[test]
    fn observed_deletions_stay_apart_from_performed_acts() {
        let mut observed = atom(70, 100, None, "deleted", 1204);
        observed.observed = true;
        let performed = atom(71, 200, Some("dupes"), "excluded", 10);
        let groups = group_acts(&[observed, performed], &[], 3);
        assert_eq!(groups.len(), 2);
        assert!(groups[0].observed);
        assert!(!groups[1].observed);
    }

    #[test]
    fn generic_transitions_flow_through_untouched() {
        let atoms = vec![atom(80, 100, Some("changed my mind"), "restored", 12)];
        let groups = group_acts(&atoms, &[], 3);
        assert_eq!(groups[0].transition, "restored");
        assert_eq!(groups[0].files, 12);
    }

    #[test]
    fn groups_order_by_earliest_decision() {
        let mut late = atom(90, 900, None, "excluded", 1);
        late.observed = false;
        let mut early = atom(10, 100, None, "deleted", 2);
        early.observed = true;
        let groups = group_acts(&[late, early], &[], 3);
        assert_eq!(groups[0].transition, "deleted");
        assert_eq!(groups[1].transition, "excluded");
    }

    #[test]
    fn decisions_within_a_group_are_oldest_first() {
        let atoms = vec![
            atom(63, 300, None, "excluded", 1),
            atom(57, 100, None, "excluded", 1),
            atom(61, 200, None, "excluded", 1),
        ];
        let groups = group_acts(&atoms, &[], 3);
        let ids: Vec<i64> = groups[0].decisions.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![57, 61, 63]);
    }
}
