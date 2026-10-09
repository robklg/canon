//! The mixed timeline and its day/fate rollups.
//!
//! Merges decisions and notes into one ascending timeline, classifies
//! decisions into rollup families via `core::domain::fate`, and aggregates
//! by day. Timestamps arrive as epoch seconds; the day each falls on, on the user's
//! clock, is read in the ops layer, which passes precomputed dates in.
//!
//! No I/O.

use std::collections::HashMap;

use jiff::civil::{Date, Weekday};
use jiff::ToSpan;

use crate::core::domain::decision::Decision;
use crate::core::domain::fate::{decision_family, fate_transition, DecisionFamily, FateAspect};
use crate::core::domain::time::reading::{read_time, TimeReadError};
use crate::core::domain::time::value::{clock_secs_as_if_utc, ClockSpan, Precision, TimeValue};
use crate::core::domain::time::zone::UserZone;
use crate::notes::Note;

/// A decision's **act**, for the timeline's act column: the registered
/// transition word where one exists, the stored command identifier
/// otherwise.
///
/// Derived, never a literal — the same one what-derivation the rollups and
/// receipt `[meta]` use (`core::domain::fate::fate_transition`). The
/// `Present` aspect is the timelines: a scope-lens row is about a place as
/// it stands, and the `Absent` half (a scan's deletions) is the day
/// rollup's, which already speaks it.
///
/// The fallback arm returns the command identifier as stored —
/// `scan`, `cluster_generate`. Those underscored identifiers are **accepted
/// residue, not a defect to fix here**: coining an act word is domain
/// vocabulary, which `/vision` owns (`DecisionFamily`'s own doc comment
/// routes the taxonomy there). This one arm is where that work attaches,
/// which is why the derivation is one function rather than a match at the
/// call site.
pub fn decision_act(command: &str) -> &str {
    fate_transition(decision_family(command), FateAspect::Present)
        .map(|t| t.as_str())
        .unwrap_or(command)
}

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

/// A parsed time-lens value: `--since` (from the period's start onward) or `--on` (within
/// the period). The period is a typed time read through the one list of spellings, or a
/// relative word's day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenValue {
    Since(TimeValue),
    On(TimeValue),
}

/// What [`parse_when`] read: the period, and whether a relative word named it (`today`,
/// `yesterday`, a weekday), which the lens label echoes as typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedWhen {
    pub value: TimeValue,
    pub relative: bool,
}

/// A weekday by its English name or three-letter abbreviation, already lower-cased — the
/// names the lens has always accepted.
fn weekday_named(name: &str) -> Option<Weekday> {
    Some(match name {
        "mon" | "monday" => Weekday::Monday,
        "tue" | "tuesday" => Weekday::Tuesday,
        "wed" | "wednesday" => Weekday::Wednesday,
        "thu" | "thursday" => Weekday::Thursday,
        "fri" | "friday" => Weekday::Friday,
        "sat" | "saturday" => Weekday::Saturday,
        "sun" | "sunday" => Weekday::Sunday,
        _ => return None,
    })
}

/// Parse a time-lens value, trimmed first: `today`, `yesterday` or a weekday name (most
/// recent occurrence, today included), in any case — each that day, a clock reading at day
/// grain; anything else through the one list of spellings (`reading::read_time`), with no
/// spelling of the trail's own. The three words are the trail's alone.
pub fn parse_when(input: &str, today: Date) -> Result<ParsedWhen, TimeReadError> {
    let trimmed = input.trim();
    let lower = trimmed.to_lowercase();
    let relative = match lower.as_str() {
        "today" => Some(today),
        "yesterday" => today.checked_sub(1.day()).ok(),
        _ => weekday_named(&lower).and_then(|weekday| {
            let diff = (7 + i64::from(today.weekday().to_monday_zero_offset())
                - i64::from(weekday.to_monday_zero_offset()))
                % 7;
            today.checked_sub(diff.days()).ok()
        }),
    };
    match relative {
        Some(day) => TimeValue::clock(day.at(0, 0, 0, 0), Precision::Day)
            .map(|value| ParsedWhen {
                value,
                relative: true,
            })
            .map_err(|_| TimeReadError::OutOfRange),
        None => read_time(trimmed).map(|value| ParsedWhen {
            value,
            relative: false,
        }),
    }
}

/// The time lens: its kind and its period, read once on the user's clock through the frame
/// rule. Selection compares each time's span with the period ([`ClockSpan::lies_inside`] for
/// `--on`, [`ClockSpan::starts_at_or_after`] for `--since`) — clock readings only, so the
/// repeated hour and the skipped one are right by construction.
#[derive(Debug, Clone, Copy)]
pub struct TimeLens {
    since: bool,
    period: ClockSpan,
}

/// A day's worth of seconds, twice: no zone's offset reaches 26 hours.
const WINDOW_MARGIN_SECS: i64 = 2 * 86_400;

impl TimeLens {
    /// The lens for `when`, its period read on `zone`'s clock (the frame rule, once).
    pub fn new(when: WhenValue, zone: &UserZone) -> TimeLens {
        let (since, value) = match when {
            WhenValue::Since(value) => (true, value),
            WhenValue::On(value) => (false, value),
        };
        TimeLens {
            since,
            period: value.on_clock(zone),
        }
    }

    /// Whether the time `ts` (seconds since 1970) falls in the lens: read on `zone`'s clock
    /// and compared with the period. A time outside the years Canon reads is not selected.
    pub fn selects(&self, ts: i64, zone: &UserZone) -> bool {
        let Ok(value) = TimeValue::from_epoch_secs(ts) else {
            return false;
        };
        let span = value.on_clock(zone);
        if self.since {
            span.starts_at_or_after(&self.period)
        } else {
            span.lies_inside(&self.period)
        }
    }

    /// The database fetch's bounds `[start, end)` in seconds since 1970 — a speed path, never
    /// a meaning: the period's clock readings counted as if they were UTC (no zone read),
    /// widened by two days on each side so every zone's reading of the period lies inside,
    /// and held at the limits of an `i64`. `--since` runs to the end. Selection is
    /// [`selects`](Self::selects), always applied after.
    pub fn fetch_window(&self) -> (i64, i64) {
        let start = clock_secs_as_if_utc(self.period.start()).saturating_sub(WINDOW_MARGIN_SECS);
        let end = if self.since {
            i64::MAX
        } else {
            (clock_secs_as_if_utc(self.period.end()) + 1).saturating_add(WINDOW_MARGIN_SECS)
        };
        (start, end)
    }

    /// How much of `date` the period covers on the clock. A period of a day or longer covers
    /// each day it selects from whole; a shorter one is named in the day's heading — from its
    /// start to the day's end for `--since`, the period alone for `--on`.
    pub fn day_part(&self, date: Date) -> DayPart {
        let Some(time) = self.period.printed_time_as_asked() else {
            return DayPart::Whole;
        };
        if self.since {
            if self.period.start() <= date.at(0, 0, 0, 0) {
                DayPart::Whole
            } else {
                DayPart::From(time)
            }
        } else {
            DayPart::Only(time)
        }
    }
}

/// How much of one day a time-lens heading covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DayPart {
    Whole,
    /// Covered from this time of day to the day's end (the first day of a `--since` shorter
    /// than a day).
    From(String),
    /// Covered only within the day (an `--on` shorter than a day).
    Only(String),
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
///            tombstone stamps from object exclusions stay out of `deleted`)
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

/// One day of the time lens: date, how much of it the period covers, rollup, and the day's
/// events in chronological order.
pub struct DayGroup {
    pub date: Date,
    pub part: DayPart,
    pub rollup: DayRollup,
    pub events: Vec<TimelineEvent>,
}

/// Group an ascending timeline into days. Dates are precomputed by the caller
/// (reading a time on the user's clock is the ops layer's); input order is preserved, so
/// days come out oldest → newest. Each day's rollup counts only the events given — the ones
/// the lens selected.
pub fn group_by_day(
    dated: Vec<(Date, TimelineEvent)>,
    stamps: &HashMap<i64, StampAgg>,
    lens: &TimeLens,
) -> Vec<DayGroup> {
    let mut groups: Vec<DayGroup> = Vec::new();
    for (date, event) in dated {
        if groups.last().map(|g| g.date) != Some(date) {
            groups.push(DayGroup {
                date,
                part: lens.day_part(date),
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
            zone: None,
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

    fn date(s: &str) -> Date {
        s.parse().unwrap()
    }

    // parse_when

    fn day_value(s: &str) -> TimeValue {
        TimeValue::clock(date(s).at(0, 0, 0, 0), Precision::Day).unwrap()
    }

    fn relative(input: &str, today: Date) -> TimeValue {
        let parsed = parse_when(input, today).unwrap();
        assert!(parsed.relative, "{input}");
        parsed.value
    }

    #[test]
    fn parse_when_today_and_yesterday() {
        let today = date("2026-07-12");
        assert_eq!(relative("today", today), day_value("2026-07-12"));
        assert_eq!(relative("TODAY", today), day_value("2026-07-12"));
        assert_eq!(relative("yesterday", today), day_value("2026-07-11"));
    }

    #[test]
    fn parse_when_weekday_most_recent() {
        // 2026-07-12 is a Sunday.
        let today = date("2026-07-12");
        assert_eq!(relative("saturday", today), day_value("2026-07-11"));
        assert_eq!(relative("monday", today), day_value("2026-07-06"));
        // Same weekday as today resolves to today, not a week ago.
        assert_eq!(relative("sunday", today), day_value("2026-07-12"));
        assert_eq!(relative("Sat", today), day_value("2026-07-11"));
    }

    #[test]
    fn parse_when_iso_date() {
        let today = date("2026-07-12");
        let parsed = parse_when("2026-05-12", today).unwrap();
        assert!(!parsed.relative);
        assert_eq!(parsed.value, day_value("2026-05-12"));
    }

    #[test]
    fn parse_when_invalid() {
        let today = date("2026-07-12");
        assert!(parse_when("someday", today).is_err());
        assert!(parse_when("2026-13-01", today).is_err());
        assert!(parse_when("", today).is_err());
    }

    /// The lens reads the one list of spellings and three words of its own — nothing else.
    /// Every row of the list is accepted at its grain; the words in any case; the input
    /// trimmed first. Refused, each with its cause: one-digit months and days and signed
    /// years (the list never held them), dates that do not exist, an offset on a date alone,
    /// and a weekday spelling the lens never took.
    #[test]
    fn the_time_lens_reads_the_one_list_and_three_words() {
        let today = date("2026-07-12");
        for (text, grain) in [
            ("2024", Precision::Year),
            ("2024-06", Precision::Month),
            ("2024-06-15", Precision::Day),
            ("2024:06:15", Precision::Day),
            ("2024-06-15T14:30", Precision::Minute),
            ("2024-06-15 14:30", Precision::Minute),
            ("2024-06-15T14:30:05", Precision::Second),
            ("2024-06-15T14:30:05.45", Precision::Fraction),
            ("2024-06-15T14:30Z", Precision::Minute),
            ("2024-06-15T14:30+02:00", Precision::Minute),
            ("2024-06-15 14:30 +02:00", Precision::Minute),
            ("2024-06-15T14:30:05-00:00", Precision::Second),
            (" 2024-06-15 ", Precision::Day),
        ] {
            let parsed = parse_when(text, today).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert!(!parsed.relative, "{text}");
            assert_eq!(parsed.value.precision(), grain, "{text}");
        }
        for word in [
            "today",
            "Yesterday",
            " TODAY ",
            "mon",
            "Monday",
            "TUE",
            "wednesday",
            "thu",
            "fri",
            "sat",
            "sunday",
        ] {
            let parsed = parse_when(word, today).unwrap_or_else(|e| panic!("{word}: {e}"));
            assert!(parsed.relative, "{word}");
            assert_eq!(parsed.value.precision(), Precision::Day, "{word}");
        }
        for (refused, cause) in [
            ("2024-1-5", TimeReadError::NotInList),
            ("-0001-12-31", TimeReadError::NotInList),
            ("+10000-01-01", TimeReadError::NotInList),
            ("2024-02-30", TimeReadError::ImpossibleDate),
            ("2023-12-31-05:00", TimeReadError::OffsetOnDateAlone),
            ("tues", TimeReadError::NotInList),
            ("", TimeReadError::NotInList),
        ] {
            assert_eq!(parse_when(refused, today), Err(cause), "{refused:?}");
        }
    }

    // the time lens

    fn lens(when: &str, since: bool, zone: &UserZone) -> TimeLens {
        let value = read_time(when).unwrap();
        let when = if since {
            WhenValue::Since(value)
        } else {
            WhenValue::On(value)
        };
        TimeLens::new(when, zone)
    }

    fn secs(text: &str) -> i64 {
        text.parse::<jiff::Timestamp>().unwrap().as_second()
    }

    /// The exactness pin. In Amsterdam the clocks went back at 03:00 on 27 October 2024, so
    /// 02:30 happened twice (00:30Z and 01:30Z): `--on 2024-10-27T02:30` selects both, and
    /// neither neighbouring minute. In New York the same instants read on 26 October, so
    /// neither is selected there.
    #[test]
    fn the_time_lens_selects_both_moments_of_the_repeated_hour() {
        let ams = UserZone::for_test("Europe/Amsterdam");
        let nyc = UserZone::for_test("America/New_York");
        let minute = lens("2024-10-27T02:30", false, &ams);
        let first = secs("2024-10-27T00:30:00Z");
        let second = secs("2024-10-27T01:30:00Z");
        assert!(minute.selects(first, &ams));
        assert!(minute.selects(second, &ams));
        assert!(!minute.selects(secs("2024-10-27T00:29:59Z"), &ams));
        assert!(!minute.selects(secs("2024-10-27T01:31:00Z"), &ams));
        let there = lens("2024-10-27T02:30", false, &nyc);
        assert!(!there.selects(first, &nyc));
        assert!(!there.selects(second, &nyc));
    }

    /// The fetch window holds every instant the lens selects, whatever the zone: two days
    /// either side of the period's readings counted as if UTC.
    #[test]
    fn the_fetch_window_holds_every_selected_instant() {
        // The first and last second of 2024-06-15 on the clocks farthest east and west of
        // UTC (+14:00, −12:00), and on one between: each is selected, and inside the window.
        for (zone, first, last) in [
            (
                "Pacific/Kiritimati",
                "2024-06-14T10:00:00Z",
                "2024-06-15T09:59:59Z",
            ),
            ("Etc/GMT+12", "2024-06-15T12:00:00Z", "2024-06-16T11:59:59Z"),
            (
                "Europe/Amsterdam",
                "2024-06-14T22:00:00Z",
                "2024-06-15T21:59:59Z",
            ),
        ] {
            let zone = UserZone::for_test(zone);
            let day = lens("2024-06-15", false, &zone);
            let (start, end) = day.fetch_window();
            for ts in [secs(first), secs(last)] {
                assert!(day.selects(ts, &zone), "{zone:?} {ts}");
                assert!(
                    start <= ts && ts < end,
                    "{zone:?} {ts} not in [{start}, {end})"
                );
            }
            assert!(!day.selects(secs(first) - 1, &zone));
            assert!(!day.selects(secs(last) + 1, &zone));
            let (_, open) = lens("2024-06-15", true, &zone).fetch_window();
            assert_eq!(open, i64::MAX);
        }
        // At the last period jiff holds, the window stays in range.
        let last = lens("9999-12-31", false, &UserZone::utc());
        let (start, end) = last.fetch_window();
        assert!(start < end);
    }

    #[test]
    fn a_heading_covers_the_day_the_period_covers() {
        let zone = UserZone::utc();
        let day = date("2024-06-15");
        let next = date("2024-06-16");
        assert_eq!(lens("2024-06", false, &zone).day_part(day), DayPart::Whole);
        assert_eq!(
            lens("2024-06-15", true, &zone).day_part(day),
            DayPart::Whole
        );
        let since = lens("2024-06-15T14:30", true, &zone);
        assert_eq!(since.day_part(day), DayPart::From("14:30".into()));
        assert_eq!(since.day_part(next), DayPart::Whole);
        assert_eq!(
            lens("2024-06-15T14:30", false, &zone).day_part(day),
            DayPart::Only("14:30".into())
        );
        // A minute at the day's end is still a period shorter than a day, named alone.
        assert_eq!(
            lens("2024-06-15T23:59", false, &zone).day_part(day),
            DayPart::Only("23:59".into())
        );
        assert_eq!(
            lens("2024-06-15T14:30:05.45", true, &zone).day_part(day),
            DayPart::From("14:30:05.45".into())
        );
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

    fn whole_days() -> TimeLens {
        lens("2026", false, &UserZone::utc())
    }

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
        let groups = group_by_day(dated, &stamps, &whole_days());
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].date, date("2026-07-11"));
        assert_eq!(groups[0].events.len(), 2);
        assert_eq!(groups[0].rollup.archived.files, 5);
        assert_eq!(groups[1].rollup.deleted.files, 3);
    }

    #[test]
    fn group_by_day_note_only_day_has_empty_rollup() {
        let dated = vec![(date("2026-07-11"), TimelineEvent::Note(mk_note(1, 100)))];
        let groups = group_by_day(dated, &HashMap::new(), &whole_days());
        assert_eq!(groups.len(), 1);
        assert!(groups[0].rollup.is_empty());
    }
}
