//! Trail domain logic — the read side of the decision trail.
//!
//! Pure logic for the `trail` command: classifying decisions into rollup
//! families, merging decisions and notes into one timeline, matching scope
//! prefixes, parsing time-lens values, and aggregating day rollups.
//!
//! No I/O. Timestamps arrive as epoch seconds; local-timezone day mapping
//! happens in the ops layer, which passes precomputed dates in.

use std::collections::HashMap;
use std::path::Path;

use chrono::{Datelike, Duration, NaiveDate, Weekday};

use super::decision::Decision;
use super::note::Note;

/// Rollup family of a decision command — the minimal *what* classification the
/// time lens needs. Deliberately small: the full operation taxonomy belongs to
/// the self-describing-receipts slice and its /vision vocabulary work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionFamily {
    /// Content given its canonical home (apply).
    Archive,
    /// Sources dismissed from consideration.
    Exclude,
    /// Exclusions lifted — a change of mind.
    Restore,
    /// Canon observing the world (scan): deletions surface here via the
    /// absent side of the decision stamp, never as Canon's own act.
    Observe,
    /// Known command with no fate contribution (intent, knowledge, fleet,
    /// housekeeping).
    Other,
    /// Command identifier from a newer Canon — rendered raw, never dropped.
    Unrecognized,
}

/// Total mapping from decision command identifiers to rollup families.
/// Identifiers are append-only and never reused (see `DecisionCommand`), so
/// each arm here is permanent history: never move an identifier between
/// families.
pub fn decision_family(command: &str) -> DecisionFamily {
    match command {
        "apply" => DecisionFamily::Archive,
        "exclude_set" | "exclude_duplicates" | "exclude_set_object" => DecisionFamily::Exclude,
        "exclude_clear" | "exclude_clear_object" => DecisionFamily::Restore,
        "scan" => DecisionFamily::Observe,
        "cluster_generate" | "cluster_refresh" | "roots_rm" | "roots_suspend"
        | "roots_unsuspend" | "import_facts" | "prune" | "facts_delete" | "note_clear" => {
            DecisionFamily::Other
        }
        _ => DecisionFamily::Unrecognized,
    }
}

/// The *what* a decision records — a content transition in registered
/// vocabulary. The trail rollup renders the terminal fates; receipts stamp the
/// terminal subset plus `restored`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Archived,
    Excluded,
    Restored,
    Deleted,
}

impl Transition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::Excluded => "excluded",
            Self::Restored => "restored",
            Self::Deleted => "deleted",
        }
    }
}

/// The presence axis of a decision's stamp: whether the stamped sources are
/// present (indexed / archived) or absent (deleted / tombstoned). The
/// discriminant command identity alone cannot supply — one scan stamps both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FateAspect {
    Present,
    Absent,
}

/// The single what-derivation, keyed on `(family, aspect)` rather than command
/// so the trail (which has the presence axis) and receipts (which supply the
/// aspect by receipt kind) share one source of truth. Wider than the command:
/// `scan` (Observe) yields `Deleted` only on its `Absent` bucket; its `Present`
/// bucket is indexing, which no receipt stamps (`None`).
pub fn fate_transition(family: DecisionFamily, aspect: FateAspect) -> Option<Transition> {
    use DecisionFamily::*;
    use FateAspect::*;
    match (family, aspect) {
        (Archive, Present) => Some(Transition::Archived),
        (Exclude, _) => Some(Transition::Excluded),
        (Restore, _) => Some(Transition::Restored),
        (Observe, Absent) => Some(Transition::Deleted),
        _ => None,
    }
}

/// The posture of a transition: Canon performing a change vs. observing one the
/// world made (registered vocabulary, orthogonal to the transition word).
///
/// The rollup renders only the transition word; posture is the derivation's
/// other half, stamped wherever a receipt records a transition. It lives here
/// beside `fate_transition` so the what-vocabulary has one home — no caller
/// renders it yet, so it is allowed to sit unused until the receipt writer.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Posture {
    Performed,
    Observed,
}

#[allow(dead_code)]
impl Posture {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Performed => "performed",
            Self::Observed => "observed",
        }
    }
}

/// The posture accompanying a transition. `Observed` exactly when Canon
/// witnessed the change rather than performing it — a scan-observed deletion;
/// every other receipt-stamped transition is `Performed`.
#[allow(dead_code)]
pub fn fate_posture(family: DecisionFamily, aspect: FateAspect) -> Posture {
    match (family, aspect) {
        (DecisionFamily::Observe, FateAspect::Absent) => Posture::Observed,
        _ => Posture::Performed,
    }
}

/// One event on the mixed timeline: an action (decision) or a thought (note).
pub enum TimelineEvent {
    Decision(Decision),
    Note(Note),
}

impl TimelineEvent {
    pub fn created_at(&self) -> i64 {
        match self {
            TimelineEvent::Decision(d) => d.created_at,
            TimelineEvent::Note(n) => n.created_at,
        }
    }

    /// Stable ordering: timestamp, then decisions before notes, then id.
    fn sort_key(&self) -> (i64, u8, i64) {
        match self {
            TimelineEvent::Decision(d) => (d.created_at, 0, d.id),
            TimelineEvent::Note(n) => (n.created_at, 1, n.id),
        }
    }
}

/// Merge decisions and notes into one ascending timeline with a stable
/// tie-break, so repeated runs render identically.
pub fn merge_events(decisions: Vec<Decision>, notes: Vec<Note>) -> Vec<TimelineEvent> {
    let mut events: Vec<TimelineEvent> = decisions
        .into_iter()
        .map(TimelineEvent::Decision)
        .chain(notes.into_iter().map(TimelineEvent::Note))
        .collect();
    events.sort_by_key(|e| e.sort_key());
    events
}

/// Bidirectional touches-scope over rel prefixes within one root: a decision
/// touches the viewed scope if its prefix is under the view *or* an ancestor
/// of it. "" is the root itself and touches everything. Segment-aware:
/// "a/bc" does not touch "a/b".
pub fn scopes_touch(view_prefix: &str, other_prefix: &str) -> bool {
    Path::new(view_prefix).starts_with(other_prefix)
        || Path::new(other_prefix).starts_with(view_prefix)
}

/// A parsed time-lens value: `--since` (from date onward) or `--on` (one day).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenValue {
    Since(NaiveDate),
    On(NaiveDate),
}

impl WhenValue {
    pub fn date(&self) -> NaiveDate {
        match self {
            WhenValue::Since(d) | WhenValue::On(d) => *d,
        }
    }
}

/// Parse a time-lens value: `today`, `yesterday`, a weekday name (most recent
/// occurrence, today included), or `YYYY-MM-DD`. Case-insensitive.
pub fn parse_when(input: &str, today: NaiveDate) -> Result<NaiveDate, String> {
    let lower = input.trim().to_lowercase();
    match lower.as_str() {
        "today" => return Ok(today),
        "yesterday" => return Ok(today - Duration::days(1)),
        _ => {}
    }
    if let Ok(weekday) = lower.parse::<Weekday>() {
        let diff = (7 + i64::from(today.weekday().num_days_from_monday())
            - i64::from(weekday.num_days_from_monday()))
            % 7;
        return Ok(today - Duration::days(diff));
    }
    NaiveDate::parse_from_str(&lower, "%Y-%m-%d").map_err(|_| {
        format!(
            "invalid time value '{input}' (expected today, yesterday, a weekday, or YYYY-MM-DD)"
        )
    })
}

/// Per-decision aggregate of stamped sources, split by the presence axis.
///
/// The split is load-bearing: one scan decision stamps both newly indexed
/// (present) and deleted (absent) sources, and object-level exclusions stamp
/// tombstones — only the presence flag disaggregates the transitions.
#[derive(Debug, Clone, Copy, Default)]
pub struct StampAgg {
    pub present_count: i64,
    pub present_bytes: i64,
    pub absent_count: i64,
    pub absent_bytes: i64,
}

/// One fate's line in a day rollup. `bytes: None` means the stamp no longer
/// supports a size — the line omits it rather than guessing.
#[derive(Debug, Clone, Copy)]
pub struct FateLine {
    pub files: i64,
    pub bytes: Option<i64>,
}

/// Aggregation of one day's decisions by fate.
#[derive(Debug, Clone, Copy)]
pub struct DayRollup {
    pub deleted: FateLine,
    pub archived: FateLine,
    pub excluded: FateLine,
    /// Decisions that contributed to no fate line (intent, knowledge, fleet,
    /// housekeeping, restores, scans that deleted nothing, unrecognized).
    pub other_actions: usize,
}

impl DayRollup {
    pub fn is_empty(&self) -> bool {
        self.deleted.files == 0
            && self.archived.files == 0
            && self.excluded.files == 0
            && self.other_actions == 0
    }
}

/// Compute a rollup over decisions using the presence-split stamp aggregates.
///
/// deleted  = absent bucket of Observe-family decisions (a scan's deletions;
///            tombstone stamps from object exclusions stay out of "deleted")
/// archived = present bucket of Archive-family decisions
/// excluded = structured completed count (stamp count as fallback); bytes only
///            when the stamp supports them
pub fn compute_rollup(decisions: &[&Decision], stamps: &HashMap<i64, StampAgg>) -> DayRollup {
    let mut deleted = FateLine {
        files: 0,
        bytes: None,
    };
    let mut archived = FateLine {
        files: 0,
        bytes: None,
    };
    let mut excluded = FateLine {
        files: 0,
        bytes: None,
    };
    let mut excluded_stamped = true;
    let mut other_actions = 0usize;

    for d in decisions {
        let agg = stamps.get(&d.id).copied().unwrap_or_default();
        let contributed = match decision_family(&d.command) {
            DecisionFamily::Observe => {
                deleted.files += agg.absent_count;
                deleted.bytes = add_bytes(deleted.bytes, agg.absent_count, agg.absent_bytes);
                agg.absent_count > 0
            }
            DecisionFamily::Archive => {
                archived.files += agg.present_count;
                archived.bytes = add_bytes(archived.bytes, agg.present_count, agg.present_bytes);
                agg.present_count > 0
            }
            DecisionFamily::Exclude => {
                let files = d.count_completed.unwrap_or(agg.present_count);
                excluded.files += files;
                if agg.present_count > 0 {
                    excluded.bytes =
                        add_bytes(excluded.bytes, agg.present_count, agg.present_bytes);
                } else if files > 0 {
                    excluded_stamped = false;
                }
                files > 0
            }
            DecisionFamily::Restore | DecisionFamily::Other | DecisionFamily::Unrecognized => false,
        };
        if !contributed {
            other_actions += 1;
        }
    }

    if !excluded_stamped {
        // At least one contributing exclusion has no stamp left — a partial
        // sum would understate silently, so omit the size entirely.
        excluded.bytes = None;
    }

    DayRollup {
        deleted,
        archived,
        excluded,
        other_actions,
    }
}

fn add_bytes(current: Option<i64>, count: i64, bytes: i64) -> Option<i64> {
    if count > 0 {
        Some(current.unwrap_or(0) + bytes)
    } else {
        current
    }
}

/// One day of the time lens: date, rollup, and the day's events in
/// chronological order.
pub struct DayGroup {
    pub date: NaiveDate,
    pub rollup: DayRollup,
    pub events: Vec<TimelineEvent>,
}

/// Group an ascending timeline into days. Dates are precomputed by the caller
/// (local-timezone mapping is not domain logic); input order is preserved, so
/// days come out oldest → newest.
pub fn group_by_day(
    dated: Vec<(NaiveDate, TimelineEvent)>,
    stamps: &HashMap<i64, StampAgg>,
) -> Vec<DayGroup> {
    let mut groups: Vec<DayGroup> = Vec::new();
    for (date, event) in dated {
        if groups.last().map(|g| g.date) != Some(date) {
            groups.push(DayGroup {
                date,
                rollup: compute_rollup(&[], stamps),
                events: Vec::new(),
            });
        }
        groups.last_mut().unwrap().events.push(event);
    }
    for group in &mut groups {
        let decisions: Vec<&Decision> = group
            .events
            .iter()
            .filter_map(|e| match e {
                TimelineEvent::Decision(d) => Some(d),
                TimelineEvent::Note(_) => None,
            })
            .collect();
        group.rollup = compute_rollup(&decisions, stamps);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::DecisionCommand;

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

    fn mk_note(id: i64, created_at: i64) -> Note {
        Note {
            id,
            root_id: 1,
            rel_path: "a".to_string(),
            text: "a thought".to_string(),
            created_at,
        }
    }

    fn date(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    // decision_family — total over every registered identifier

    #[test]
    fn decision_family_covers_all_registered_commands() {
        let expected = [
            (DecisionCommand::Scan, DecisionFamily::Observe),
            (DecisionCommand::Apply, DecisionFamily::Archive),
            (DecisionCommand::ExcludeSet, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeClear, DecisionFamily::Restore),
            (DecisionCommand::ExcludeDuplicates, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeSetObject, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeClearObject, DecisionFamily::Restore),
            (DecisionCommand::ClusterGenerate, DecisionFamily::Other),
            (DecisionCommand::ClusterRefresh, DecisionFamily::Other),
            (DecisionCommand::RootsRm, DecisionFamily::Other),
            (DecisionCommand::RootsSuspend, DecisionFamily::Other),
            (DecisionCommand::RootsUnsuspend, DecisionFamily::Other),
            (DecisionCommand::ImportFacts, DecisionFamily::Other),
            (DecisionCommand::Prune, DecisionFamily::Other),
            (DecisionCommand::FactsDelete, DecisionFamily::Other),
            (DecisionCommand::NoteClear, DecisionFamily::Other),
        ];
        for (cmd, family) in expected {
            assert_eq!(decision_family(cmd.as_str()), family, "{}", cmd.as_str());
        }
    }

    #[test]
    fn decision_family_unknown_is_unrecognized() {
        assert_eq!(
            decision_family("reduce_sweep"),
            DecisionFamily::Unrecognized
        );
        assert_eq!(decision_family(""), DecisionFamily::Unrecognized);
    }

    // fate_transition / fate_posture — the shared what-derivation

    #[test]
    fn fate_transition_covers_family_aspect_matrix() {
        use DecisionFamily::*;
        use FateAspect::*;
        use Transition::*;
        let expected = [
            ((Archive, Present), Some(Archived)),
            ((Archive, Absent), None), // apply never stamps absent
            ((Exclude, Present), Some(Excluded)),
            ((Exclude, Absent), Some(Excluded)), // object-exclusion tombstones
            ((Restore, Present), Some(Restored)),
            ((Restore, Absent), Some(Restored)),
            ((Observe, Present), None),         // indexing — no fate
            ((Observe, Absent), Some(Deleted)), // scan-observed deletion
            ((Other, Present), None),
            ((Other, Absent), None),
            ((Unrecognized, Present), None),
            ((Unrecognized, Absent), None),
        ];
        for ((family, aspect), want) in expected {
            assert_eq!(
                fate_transition(family, aspect),
                want,
                "{family:?} / {aspect:?}"
            );
        }
    }

    #[test]
    fn transition_as_str_registered_words() {
        assert_eq!(Transition::Archived.as_str(), "archived");
        assert_eq!(Transition::Excluded.as_str(), "excluded");
        assert_eq!(Transition::Restored.as_str(), "restored");
        assert_eq!(Transition::Deleted.as_str(), "deleted");
    }

    #[test]
    fn fate_posture_observed_only_for_scan_deletion() {
        use DecisionFamily::*;
        use FateAspect::*;
        // The one observed transition: a scan witnessing a loss.
        assert_eq!(fate_posture(Observe, Absent), Posture::Observed);
        // Everything else Canon performs.
        for (family, aspect) in [
            (Observe, Present),
            (Archive, Present),
            (Exclude, Present),
            (Exclude, Absent),
            (Restore, Present),
            (Other, Absent),
            (Unrecognized, Present),
        ] {
            assert_eq!(
                fate_posture(family, aspect),
                Posture::Performed,
                "{family:?} / {aspect:?}"
            );
        }
    }

    #[test]
    fn posture_as_str_registered_words() {
        assert_eq!(Posture::Performed.as_str(), "performed");
        assert_eq!(Posture::Observed.as_str(), "observed");
    }

    // parse_when

    #[test]
    fn parse_when_today_and_yesterday() {
        let today = date("2026-07-12");
        assert_eq!(parse_when("today", today).unwrap(), today);
        assert_eq!(parse_when("TODAY", today).unwrap(), today);
        assert_eq!(parse_when("yesterday", today).unwrap(), date("2026-07-11"));
    }

    #[test]
    fn parse_when_weekday_most_recent() {
        // 2026-07-12 is a Sunday.
        let today = date("2026-07-12");
        assert_eq!(parse_when("saturday", today).unwrap(), date("2026-07-11"));
        assert_eq!(parse_when("monday", today).unwrap(), date("2026-07-06"));
        // Same weekday as today resolves to today, not a week ago.
        assert_eq!(parse_when("sunday", today).unwrap(), today);
        assert_eq!(parse_when("Sat", today).unwrap(), date("2026-07-11"));
    }

    #[test]
    fn parse_when_iso_date() {
        let today = date("2026-07-12");
        assert_eq!(parse_when("2026-05-12", today).unwrap(), date("2026-05-12"));
    }

    #[test]
    fn parse_when_invalid() {
        let today = date("2026-07-12");
        assert!(parse_when("someday", today).is_err());
        assert!(parse_when("2026-13-01", today).is_err());
        assert!(parse_when("", today).is_err());
    }

    // scopes_touch

    #[test]
    fn scopes_touch_relations() {
        assert!(scopes_touch("a/b", "a/b")); // equal
        assert!(scopes_touch("a/b/c", "a/b")); // decision on ancestor
        assert!(scopes_touch("a/b", "a/b/c")); // decision on descendant
        assert!(!scopes_touch("a/b", "a/x")); // sibling
        assert!(scopes_touch("", "a/b")); // root view touches everything
        assert!(scopes_touch("a/b", "")); // root-level decision touches every view
        assert!(scopes_touch("", ""));
        assert!(!scopes_touch("a/bc", "a/b")); // segment boundary
        assert!(!scopes_touch("a/b", "a/bc"));
    }

    #[test]
    fn scopes_touch_absolute_paths() {
        // Arrival matching runs on absolute snapshot paths (view prefix vs. a
        // decision's recorded destination_path), not rel-prefixes within one
        // root — the same predicate must hold there too.
        assert!(scopes_touch("/archive/x", "/archive/x")); // equal
        assert!(scopes_touch("/archive/x/y", "/archive/x")); // view deeper than destination
        assert!(scopes_touch("/archive/x", "/archive/x/y")); // destination deeper than view
        assert!(!scopes_touch("/archive/x", "/archive/y")); // sibling
        assert!(!scopes_touch("/archive/x", "/archive/xc")); // segment boundary
        assert!(!scopes_touch("/archive/xc", "/archive/x"));
    }

    // merge_events

    #[test]
    fn merge_events_orders_and_tie_breaks() {
        let decisions = vec![mk_decision(2, "scan", 100), mk_decision(1, "apply", 50)];
        let notes = vec![mk_note(7, 100), mk_note(3, 10)];
        let events = merge_events(decisions, notes);
        let keys: Vec<(i64, u8, i64)> = events.iter().map(|e| e.sort_key()).collect();
        // Ascending; at ts=100 the decision precedes the note.
        assert_eq!(keys, vec![(10, 1, 3), (50, 0, 1), (100, 0, 2), (100, 1, 7)]);
    }

    #[test]
    fn merge_events_deterministic() {
        let make = || {
            merge_events(
                vec![mk_decision(1, "scan", 100), mk_decision(2, "scan", 100)],
                vec![mk_note(1, 100)],
            )
            .iter()
            .map(|e| e.sort_key())
            .collect::<Vec<_>>()
        };
        assert_eq!(make(), make());
    }

    // compute_rollup

    fn agg(pc: i64, pb: i64, ac: i64, ab: i64) -> StampAgg {
        StampAgg {
            present_count: pc,
            present_bytes: pb,
            absent_count: ac,
            absent_bytes: ab,
        }
    }

    #[test]
    fn rollup_scan_mixed_stamp_counts_only_absent_as_deleted() {
        // A scan that indexed 12 new files AND deleted 1,350: only the absent
        // side is "deleted" — fresh files must never count as deletions.
        let d = mk_decision(1, "scan", 100);
        let stamps = HashMap::from([(1, agg(12, 999, 1350, 35_000))]);
        let r = compute_rollup(&[&d], &stamps);
        assert_eq!(r.deleted.files, 1350);
        assert_eq!(r.deleted.bytes, Some(35_000));
        assert_eq!(r.archived.files, 0);
        assert_eq!(r.other_actions, 0);
    }

    #[test]
    fn rollup_object_exclusion_tombstones_not_deleted() {
        // Object-level exclusion stamps tombstones (absent) — they are not
        // deletions and must not surface in "deleted".
        let d = mk_decision(1, "exclude_set_object", 100);
        let mut d = d;
        d.count_completed = Some(5);
        let stamps = HashMap::from([(1, agg(3, 300, 2, 200))]);
        let r = compute_rollup(&[&d], &stamps);
        assert_eq!(r.deleted.files, 0);
        assert_eq!(r.excluded.files, 5);
        assert_eq!(r.excluded.bytes, Some(300)); // present bytes only
    }

    #[test]
    fn rollup_apply_archived_from_present_bucket() {
        let d = mk_decision(1, "apply", 100);
        let stamps = HashMap::from([(1, agg(47, 3_900, 0, 0))]);
        let r = compute_rollup(&[&d], &stamps);
        assert_eq!(r.archived.files, 47);
        assert_eq!(r.archived.bytes, Some(3_900));
    }

    #[test]
    fn rollup_excluded_without_stamp_omits_bytes() {
        // Restamped history: the count column still knows, the stamp doesn't.
        let mut d = mk_decision(1, "exclude_set", 100);
        d.count_completed = Some(210);
        let r = compute_rollup(&[&d], &HashMap::new());
        assert_eq!(r.excluded.files, 210);
        assert_eq!(r.excluded.bytes, None); // omit, never guess
    }

    #[test]
    fn rollup_partial_exclusion_stamp_omits_bytes_entirely() {
        // Two exclusions, one stamped, one not: a partial byte sum would
        // silently understate — omit.
        let mut d1 = mk_decision(1, "exclude_set", 100);
        d1.count_completed = Some(10);
        let mut d2 = mk_decision(2, "exclude_set", 101);
        d2.count_completed = Some(20);
        let stamps = HashMap::from([(1, agg(10, 1_000, 0, 0))]);
        let r = compute_rollup(&[&d1, &d2], &stamps);
        assert_eq!(r.excluded.files, 30);
        assert_eq!(r.excluded.bytes, None);
    }

    #[test]
    fn rollup_non_fate_decisions_count_as_other() {
        let d1 = mk_decision(1, "cluster_generate", 100);
        let d2 = mk_decision(2, "exclude_clear", 101);
        let d3 = mk_decision(3, "future_command", 102);
        let d4 = mk_decision(4, "scan", 103); // scan that deleted nothing
        let r = compute_rollup(&[&d1, &d2, &d3, &d4], &HashMap::new());
        assert_eq!(r.other_actions, 4);
        assert!(r.deleted.files == 0 && r.archived.files == 0 && r.excluded.files == 0);
    }

    // group_by_day

    #[test]
    fn group_by_day_splits_and_rolls_up() {
        let d1 = mk_decision(1, "apply", 100);
        let d2 = mk_decision(2, "scan", 200);
        let n1 = mk_note(1, 150);
        let stamps = HashMap::from([(1, agg(5, 500, 0, 0)), (2, agg(0, 0, 3, 300))]);
        let dated = vec![
            (date("2026-07-11"), TimelineEvent::Decision(d1)),
            (date("2026-07-11"), TimelineEvent::Note(n1)),
            (date("2026-07-12"), TimelineEvent::Decision(d2)),
        ];
        let groups = group_by_day(dated, &stamps);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].date, date("2026-07-11"));
        assert_eq!(groups[0].events.len(), 2);
        assert_eq!(groups[0].rollup.archived.files, 5);
        assert_eq!(groups[1].rollup.deleted.files, 3);
    }

    #[test]
    fn group_by_day_note_only_day_has_empty_rollup() {
        let dated = vec![(date("2026-07-11"), TimelineEvent::Note(mk_note(1, 100)))];
        let groups = group_by_day(dated, &HashMap::new());
        assert_eq!(groups.len(), 1);
        assert!(groups[0].rollup.is_empty());
    }
}
