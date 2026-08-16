//! The mixed timeline and its day/fate rollups.
//!
//! Merges decisions and notes into one ascending timeline, classifies
//! decisions into rollup families via `core::domain::fate`, and aggregates
//! by day. Timestamps arrive as epoch seconds; local-timezone day mapping
//! happens in the ops layer, which passes precomputed dates in.
//!
//! No I/O.

use std::collections::HashMap;

use chrono::{Datelike, Duration, NaiveDate, Weekday};

use crate::core::domain::decision::Decision;
use crate::core::domain::fate::{decision_family, DecisionFamily};
use crate::notes::Note;

/// One event on the mixed timeline: an action (decision) or a thought (note).
pub enum TimelineEvent {
    Decision(Box<Decision>),
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
        .map(|d| TimelineEvent::Decision(Box::new(d)))
        .chain(notes.into_iter().map(TimelineEvent::Note))
        .collect();
    events.sort_by_key(|e| e.sort_key());
    events
}

/// A parsed time-lens value: `--since` (from date onward) or `--on` (one day).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenValue {
    Since(NaiveDate),
    On(NaiveDate),
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
                TimelineEvent::Decision(d) => Some(d.as_ref()),
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
            (date("2026-07-11"), TimelineEvent::Decision(Box::new(d1))),
            (date("2026-07-11"), TimelineEvent::Note(n1)),
            (date("2026-07-12"), TimelineEvent::Decision(Box::new(d2))),
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
