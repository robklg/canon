//! Act grouping — the what/why register.
//!
//! No I/O anywhere here; callers supply everything fetched.

use std::collections::HashMap;

use super::locations::{aggregate_locations, LocationAggregate};

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
    /// How many of this slice's files still stand (present rows) — the
    /// stamp accumulator's present split. Extraction and deletion atoms
    /// contribute 0: nothing they narrate stands here. Feeds the
    /// coincidence predicate, never a rendered count.
    pub present_files: i64,
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
    /// Whether the full reason renders here. Defaults `true` (every slice
    /// states its reason); `assign_reason_sites` narrows to the decision's
    /// first emitted slice in reading order — the other slices cite the
    /// bare id.
    pub reason_here: bool,
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
    /// Present-file share of the group's slices (see `ActAtom`); the
    /// coincidence predicate's evidence, never a rendered count.
    pub present_files: i64,
    /// All-or-omitted: `Some` only when every grouped decision knew.
    pub bytes: Option<i64>,
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    pub decisions: Vec<ActDecision>,
}

/// The whys of an act group, ready to render: distinct reasons in
/// first-seen order with the decisions that gave them, the reasoned
/// decisions whose full text renders elsewhere (cited by bare id), and the
/// decisions that recorded none — by id, so "without reason" never reads
/// as "without decision" (a reasonless decision is still a real recorded
/// act). `cited` and `without_reason` never conflate — `without_reason`
/// stays an exact truth-claim about decisions with no reason anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasonSummary {
    pub reasons: Vec<(String, Vec<i64>)>,
    pub cited: Vec<i64>,
    pub without_reason: Vec<i64>,
}

impl ActGroup {
    pub fn reason_summary(&self) -> ReasonSummary {
        let mut reasons: Vec<(String, Vec<i64>)> = Vec::new();
        let mut cited = Vec::new();
        let mut without_reason = Vec::new();
        for decision in &self.decisions {
            match &decision.reason {
                Some(_) if !decision.reason_here => cited.push(decision.id),
                Some(reason) => match reasons.iter_mut().find(|(r, _)| r == reason) {
                    Some((_, ids)) => ids.push(decision.id),
                    None => reasons.push((reason.clone(), vec![decision.id])),
                },
                None => without_reason.push(decision.id),
            }
        }
        ReasonSummary {
            reasons,
            cited,
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
        present_files: i64,
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
                present_files: 0,
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
        accum.present_files += atom.present_files;
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
            reason_here: true,
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
                present_files: accum.present_files,
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
            present_files: 0,
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
        assert_eq!(summary.without_reason, vec![64], "reasonless ids carried");
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
