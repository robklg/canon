//! The time noun: one stored time, its form and its grain, and **the frame rule** that reads
//! it on the user's clock.
//!
//! A time is stored as what was written, in one of three forms named by its offset — an
//! instant with no recorded offset, an instant with a recorded offset, or a clock reading with
//! no offset — at the precision it was written (a year, a month, a day, a minute, a second, or
//! a fraction of a second). Nothing is padded to a precision nobody wrote.
//!
//! **The frame rule** is [`TimeValue::on_clock`]: an instant is read in the user's zone; a
//! clock reading is read as written. It converts only in the direction that always has one
//! answer — an instant to its reading on a clock — and never a clock reading to an instant,
//! which has two answers in the hour that repeats when clocks go back and none in the hour
//! skipped when they go forward. A recorded offset is stored and never read here. Everything
//! that prints a time reads it through this rule ([`TimeValue::printed`] is its one printed
//! spelling); the user's zone is a parameter, never read inside it.
//!
//! **The readable line**: a `TimeValue` exists only where every zone reads it in the years
//! 0000–9999 — the four digits the list of spellings holds — so a printed value can always be
//! typed back. An instant lies from `0000-01-02T01:59:59Z` (at the most negative offset a zone
//! can have, `0000-01-01T00:00:00`) up to jiff's `Timestamp::MAX` (at the most positive one,
//! the last moment of 9999); a clock reading lies in the years 0000–9999. Every constructor
//! refuses anything else.

use std::fmt::Write as _;

use jiff::civil::{self, DateTime};
use jiff::tz::Offset;
use jiff::{SignedDuration, Timestamp, ToSpan};

use super::zone::UserZone;

/// The grain a time was written at, coarsest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Precision {
    Year,
    Month,
    Day,
    Minute,
    Second,
    /// Fractions of a second, kept to the nanosecond.
    Fraction,
}

impl Precision {
    /// The grain in plain words, as a sentence says a time is known "only to the …".
    pub fn word(self) -> &'static str {
        match self {
            Precision::Year => "year",
            Precision::Month => "month",
            Precision::Day => "day",
            Precision::Minute => "minute",
            Precision::Second => "second",
            Precision::Fraction => "fraction of a second",
        }
    }
}

/// The three stored forms, named by their offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeForm {
    /// An instant whose local offset is unknown (`-00:00`, or a file's save time).
    Instant,
    /// An instant with the offset it was written with (`Z` and `+00:00` both offset 0).
    InstantWithOffset,
    /// A clock reading with no offset.
    Clock,
}

/// A stored time. Fields are private: every value passes a checked constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeValue(Repr);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Repr {
    Instant {
        at: Timestamp,
        offset: Option<Offset>,
        precision: Precision,
    },
    Clock {
        at: DateTime,
        precision: Precision,
    },
}

/// Why a constructor refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeValueError {
    /// Outside the readable line (see the module doc).
    OutOfRange,
    /// An instant written coarser than a minute: an offset on a date alone means nothing.
    InstantCoarserThanMinute,
    /// The value carries a part finer than its precision (seconds on a minute value).
    BelowGrain,
    /// A recorded offset that is not a whole number of minutes: the list of spellings writes
    /// offsets as `±HH:MM` only, so such a value could never be written back and read again.
    OffsetNotWholeMinutes,
}

/// A time outside the readable line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRange;

/// Why a time outside the readable line has no value, in the one wording both the type's
/// refusal and the list of spellings' refusal give.
pub(super) const EDGE_OF_THE_YEARS: &str =
    "the time lies at the edge of the years 0000–9999 or beyond";

impl std::fmt::Display for TimeValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TimeValueError::OutOfRange => EDGE_OF_THE_YEARS,
            TimeValueError::InstantCoarserThanMinute => "an offset needs a time of day",
            TimeValueError::BelowGrain => "the time carries a part finer than its precision",
            TimeValueError::OffsetNotWholeMinutes => "the offset is not a whole number of minutes",
        })
    }
}

impl std::error::Error for TimeValueError {}

/// The earliest instant every zone reads in year 0000: `0000-01-02T01:59:59Z`, which the most
/// negative offset jiff allows (`-25:59:59`) reads as `0000-01-01T00:00:00`.
fn first_readable_instant() -> Timestamp {
    Offset::MIN
        .to_timestamp(civil::date(0, 1, 1).at(0, 0, 0, 0))
        .expect("the first moment of year 0000 at the most negative offset is a jiff timestamp")
}

fn instant_is_readable(at: Timestamp) -> bool {
    at >= first_readable_instant() && at <= Timestamp::MAX
}

fn clock_is_readable(at: DateTime) -> bool {
    (0..=9999).contains(&at.year())
}

/// Whether `at` carries nothing finer than `precision`.
fn within_grain(at: DateTime, precision: Precision) -> bool {
    let below_day =
        at.hour() == 0 && at.minute() == 0 && at.second() == 0 && at.subsec_nanosecond() == 0;
    match precision {
        Precision::Year => at.month() == 1 && at.day() == 1 && below_day,
        Precision::Month => at.day() == 1 && below_day,
        Precision::Day => below_day,
        Precision::Minute => at.second() == 0 && at.subsec_nanosecond() == 0,
        Precision::Second => at.subsec_nanosecond() == 0,
        Precision::Fraction => true,
    }
}

/// `at` cut down to `precision` — the start of the period it lies in.
fn truncate(at: DateTime, precision: Precision) -> DateTime {
    let (y, mo, d) = (at.year(), at.month(), at.day());
    let (h, mi, s) = (at.hour(), at.minute(), at.second());
    match precision {
        Precision::Year => civil::date(y, 1, 1).at(0, 0, 0, 0),
        Precision::Month => civil::date(y, mo, 1).at(0, 0, 0, 0),
        Precision::Day => civil::date(y, mo, d).at(0, 0, 0, 0),
        Precision::Minute => civil::date(y, mo, d).at(h, mi, 0, 0),
        Precision::Second => civil::date(y, mo, d).at(h, mi, s, 0),
        Precision::Fraction => at,
    }
}

impl TimeValue {
    /// An instant, with the offset it was written with (`Some`) or none (`None`). Minute or
    /// finer; nothing below its grain as written in its own offset.
    pub fn instant(
        at: Timestamp,
        offset: Option<Offset>,
        precision: Precision,
    ) -> Result<Self, TimeValueError> {
        if precision < Precision::Minute {
            return Err(TimeValueError::InstantCoarserThanMinute);
        }
        if !instant_is_readable(at) {
            return Err(TimeValueError::OutOfRange);
        }
        if let Some(o) = offset {
            if o.seconds() % 60 != 0 {
                return Err(TimeValueError::OffsetNotWholeMinutes);
            }
        }
        let written = offset.unwrap_or(Offset::UTC).to_datetime(at);
        if !within_grain(written, precision) {
            return Err(TimeValueError::BelowGrain);
        }
        Ok(TimeValue(Repr::Instant {
            at,
            offset,
            precision,
        }))
    }

    /// A clock reading with no offset, at its precision; nothing below its grain.
    pub fn clock(at: DateTime, precision: Precision) -> Result<Self, TimeValueError> {
        if !clock_is_readable(at) {
            return Err(TimeValueError::OutOfRange);
        }
        if !within_grain(at, precision) {
            return Err(TimeValueError::BelowGrain);
        }
        Ok(TimeValue(Repr::Clock { at, precision }))
    }

    /// A file's save time: an instant with no recorded offset, at second precision.
    /// `Err` outside the readable line.
    pub fn from_epoch_secs(secs: i64) -> Result<Self, OutOfRange> {
        let at = Timestamp::from_second(secs).map_err(|_| OutOfRange)?;
        TimeValue::instant(at, None, Precision::Second).map_err(|_| OutOfRange)
    }

    /// **The frame rule.** The value's span on the user's clock: an instant read in `zone`
    /// (always one answer), a clock reading as written — never a clock reading converted to
    /// an instant, and never the recorded offset.
    pub fn on_clock(&self, zone: &UserZone) -> ClockSpan {
        match self.0 {
            Repr::Instant { at, precision, .. } => ClockSpan {
                start: truncate(zone.to_clock(at), precision),
                precision,
            },
            Repr::Clock { at, precision } => ClockSpan::as_written(at, precision),
        }
    }

    /// The one printed spelling, on the user's clock, at the value's precision:
    /// `2024`, `2024-06`, `2024-06-15`, `2024-06-15 14:30`, `2024-06-15 14:30:05`. A fraction
    /// prints to the second, truncated. Never an offset. The span's own spelling
    /// ([`ClockSpan::printed`]), read through the frame rule.
    pub fn printed(&self, zone: &UserZone) -> String {
        self.on_clock(zone).printed()
    }

    /// The one ISO 8601 spelling of what is stored: its form, its precision and its offset —
    /// `-00:00` for an instant with no recorded offset, `+00:00` (never `Z`) for a zero
    /// offset, none for a clock reading. A fraction keeps its value without trailing zeros and
    /// always at least one digit, so reading the text back gives the same value.
    pub fn iso_text(&self) -> String {
        match self.0 {
            Repr::Clock { at, precision } => written(at, precision),
            Repr::Instant {
                at,
                offset,
                precision,
            } => {
                let shown = offset.unwrap_or(Offset::UTC);
                let mut text = written(shown.to_datetime(at), precision);
                match offset {
                    None => text.push_str("-00:00"),
                    Some(o) => {
                        let total = o.seconds();
                        let sign = if total < 0 { '-' } else { '+' };
                        let abs = total.unsigned_abs();
                        let _ = write!(text, "{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60);
                    }
                }
                text
            }
        }
    }

    pub fn precision(&self) -> Precision {
        match self.0 {
            Repr::Instant { precision, .. } | Repr::Clock { precision, .. } => precision,
        }
    }

    pub fn form(&self) -> TimeForm {
        match self.0 {
            Repr::Instant { offset: None, .. } => TimeForm::Instant,
            Repr::Instant {
                offset: Some(_), ..
            } => TimeForm::InstantWithOffset,
            Repr::Clock { .. } => TimeForm::Clock,
        }
    }

    /// The recorded offset of an instant written with one; `None` otherwise. Read by storage
    /// alone — never by the frame rule.
    pub fn recorded_offset(&self) -> Option<Offset> {
        match self.0 {
            Repr::Instant { offset, .. } => offset,
            Repr::Clock { .. } => None,
        }
    }

    /// The fraction of a second, for a value at `Fraction` precision; `None` otherwise.
    /// Read by storage alone.
    pub fn fraction_nanos(&self) -> Option<i32> {
        if self.precision() != Precision::Fraction {
            return None;
        }
        Some(match self.0 {
            Repr::Instant { at, offset, .. } => offset
                .unwrap_or(Offset::UTC)
                .to_datetime(at)
                .subsec_nanosecond(),
            Repr::Clock { at, .. } => at.subsec_nanosecond(),
        })
    }

    /// The stored number in `value_time`: an instant's UTC seconds; a clock reading counted
    /// as if it were UTC; fractions cut to the second below. Storage's encoding only: no
    /// comparison reads it.
    pub fn legacy_epoch_number(&self) -> i64 {
        match self.0 {
            Repr::Instant { at, .. } => floor_seconds(at.as_second(), at.subsec_nanosecond()),
            Repr::Clock { at, .. } => clock_secs_as_if_utc(at),
        }
    }
}

/// A clock reading counted as seconds since 1970 as if it were UTC, rounded toward the past:
/// arithmetic on the reading alone, no zone and no offset read. An encoding and a bound, never
/// a reading of the user's clock: no comparison or selection may use it.
pub fn clock_secs_as_if_utc(at: DateTime) -> i64 {
    seconds_floor(at.duration_since(civil::date(1970, 1, 1).at(0, 0, 0, 0)))
}

/// Seconds rounded toward the past, whatever sign the sub-second part carries.
fn floor_seconds(secs: i64, nanos: i32) -> i64 {
    if nanos < 0 {
        secs - 1
    } else {
        secs
    }
}

fn seconds_floor(d: SignedDuration) -> i64 {
    floor_seconds(d.as_secs(), d.subsec_nanos())
}

/// `at` written in ISO 8601 at `precision`, with no offset.
fn written(at: DateTime, precision: Precision) -> String {
    let date = match precision {
        Precision::Year => return format!("{:04}", at.year()),
        Precision::Month => return format!("{:04}-{:02}", at.year(), at.month()),
        _ => format!("{:04}-{:02}-{:02}", at.year(), at.month(), at.day()),
    };
    match precision {
        Precision::Year | Precision::Month | Precision::Day => date,
        Precision::Minute => format!("{date}T{:02}:{:02}", at.hour(), at.minute()),
        Precision::Second => format!(
            "{date}T{:02}:{:02}:{:02}",
            at.hour(),
            at.minute(),
            at.second()
        ),
        Precision::Fraction => format!(
            "{date}T{:02}:{:02}:{:02}.{}",
            at.hour(),
            at.minute(),
            at.second(),
            fraction_digits(at.subsec_nanosecond())
        ),
    }
}

/// A fraction of a second's digits as written back: no trailing zeros, never rounded, and
/// always at least one digit, so the text reads back at fraction precision.
fn fraction_digits(nanos: i32) -> String {
    let digits = format!("{nanos:09}");
    let kept = digits.trim_end_matches('0');
    if kept.is_empty() {
        "0".to_string()
    } else {
        kept.to_string()
    }
}

/// The rows of the whole-period table ([`ClockSpan::satisfies`]): what a comparison asks of a
/// value's whole period against a typed one. A value coarser than the typed period is in none
/// of `<`, `=` and `>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodOp {
    /// `=` — lies inside the period.
    Inside,
    /// `>=` — starts at or after the period's start.
    StartsAtOrAfterStart,
    /// `>` — starts at or after the period's end.
    StartsAtOrAfterEnd,
    /// `<=` — ends by the period's end.
    EndsByEnd,
    /// `<` — ends by the period's start.
    EndsByStart,
}

/// The day a time of day is read on: a clock reading on this day, so the whole-period table
/// compares times of day as it compares every other time. Any day serves because nothing reads
/// it back: the modifier table lets only the hour, minute and second be read off a time of day,
/// and it is never printed.
pub const REFERENCE_DAY: civil::Date = civil::date(2000, 1, 1);

/// A value's period on the user's clock: where it starts, and its grain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSpan {
    start: DateTime,
    precision: Precision,
}

impl ClockSpan {
    /// The frame rule's clock-reading arm: a clock reading's span is the reading as written,
    /// on any clock.
    fn as_written(at: DateTime, precision: Precision) -> ClockSpan {
        ClockSpan {
            start: at,
            precision,
        }
    }

    pub fn start(&self) -> DateTime {
        self.start
    }

    /// One grain after the start, computed on the clock reading itself (no clock-change
    /// arithmetic). A fraction is a point one nanosecond long, so no value can both end by
    /// its start and start at or after its end. At the top of year 9999 the next grain is
    /// past the last clock reading jiff holds, and the end is that last reading.
    pub fn end(&self) -> DateTime {
        let grain = match self.precision {
            Precision::Year => 1.year(),
            Precision::Month => 1.month(),
            Precision::Day => 1.day(),
            Precision::Minute => 1.minute(),
            Precision::Second => 1.second(),
            Precision::Fraction => 1.nanosecond(),
        };
        self.start.checked_add(grain).unwrap_or(DateTime::MAX)
    }

    pub fn precision(&self) -> Precision {
        self.precision
    }

    /// **The whole-period table**: whether this whole period satisfies `op` against
    /// `period`. Every comparison of times — a typed period in a filter, the trail's `--on`
    /// and `--since` — reads this one table, on clock readings only: no instant is computed.
    pub fn satisfies(&self, op: PeriodOp, period: &ClockSpan) -> bool {
        match op {
            PeriodOp::Inside => self.start >= period.start && self.end() <= period.end(),
            PeriodOp::StartsAtOrAfterStart => self.start >= period.start,
            PeriodOp::StartsAtOrAfterEnd => self.start >= period.end(),
            PeriodOp::EndsByEnd => self.end() <= period.end(),
            PeriodOp::EndsByStart => self.end() <= period.start,
        }
    }

    /// Whether the typed period's boundary for `op` falls strictly inside this period, so this
    /// value is on neither side of it: `<` and `>=` test the typed start, `<=` and `>` the typed
    /// end, `=` both. The third side of the whole-period table ([`satisfies`](Self::satisfies)).
    pub fn spans(&self, op: PeriodOp, period: &ClockSpan) -> bool {
        let strictly_inside = |at: DateTime| self.start < at && at < self.end();
        match op {
            PeriodOp::EndsByStart | PeriodOp::StartsAtOrAfterStart => strictly_inside(period.start),
            PeriodOp::EndsByEnd | PeriodOp::StartsAtOrAfterEnd => strictly_inside(period.end()),
            PeriodOp::Inside => strictly_inside(period.start) || strictly_inside(period.end()),
        }
    }

    /// The one printed spelling of a time: `2024`, `2024-06`, `2024-06-15`,
    /// `2024-06-15 14:30`, `2024-06-15 14:30:05`. A fraction prints to the second, truncated,
    /// never rounded. Never an offset.
    pub fn printed(&self) -> String {
        self.spelled(false)
    }

    /// A typed period named back as it was asked: [`printed`](Self::printed), except that a
    /// period written to a fraction of a second keeps its fraction digits
    /// (`2024-06-15 14:30:05.45` — no trailing zeros, never rounded). It names a period a
    /// person asked for; a stored value always prints through `printed`.
    pub fn printed_as_asked(&self) -> String {
        self.spelled(true)
    }

    /// The period's spelling; with `fraction`, a fraction's digits are kept.
    fn spelled(&self, fraction: bool) -> String {
        let s = self.start;
        let date = match self.precision {
            Precision::Year => return format!("{:04}", s.year()),
            Precision::Month => return format!("{:04}-{:02}", s.year(), s.month()),
            _ => format!("{:04}-{:02}-{:02}", s.year(), s.month(), s.day()),
        };
        match self.time_spelled(fraction) {
            Some(time) => format!("{date} {time}"),
            None => date,
        }
    }

    /// The time of day alone, as asked: `14:30`, `14:30:05`, `14:30:05.45`; `None` for a
    /// period of a day or longer.
    pub fn printed_time_as_asked(&self) -> Option<String> {
        self.time_spelled(true)
    }

    /// The time of day alone, as [`printed`](Self::printed) writes it: `14:30`, `14:30:05`, a
    /// fraction to the second, truncated; `None` for a period of a day or longer.
    pub fn printed_time(&self) -> Option<String> {
        self.time_spelled(false)
    }

    /// The time of day at the period's grain; with `fraction`, a fraction's digits are kept.
    fn time_spelled(&self, fraction: bool) -> Option<String> {
        let s = self.start;
        let (h, m, sec) = (s.hour(), s.minute(), s.second());
        match self.precision {
            Precision::Year | Precision::Month | Precision::Day => None,
            Precision::Minute => Some(format!("{h:02}:{m:02}")),
            Precision::Second => Some(format!("{h:02}:{m:02}:{sec:02}")),
            Precision::Fraction if !fraction => Some(format!("{h:02}:{m:02}:{sec:02}")),
            Precision::Fraction => Some(format!(
                "{h:02}:{m:02}:{sec:02}.{}",
                fraction_digits(s.subsec_nanosecond())
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::reading::read_time;

    fn ams() -> UserZone {
        UserZone::for_test("Europe/Amsterdam")
    }

    fn nyc() -> UserZone {
        UserZone::for_test("America/New_York")
    }

    fn ts(text: &str) -> Timestamp {
        text.parse().expect("a test timestamp")
    }

    fn dt(text: &str) -> DateTime {
        text.parse().expect("a test clock reading")
    }

    fn save_time(text: &str) -> TimeValue {
        TimeValue::instant(ts(text), None, Precision::Second).unwrap()
    }

    /// The law's verifying test. In Amsterdam, clocks went back at 03:00 on 27 October 2024,
    /// so 02:30 happened twice — at 00:30Z and at 01:30Z — and forward at 02:00 on 31 March
    /// 2024, so 02:30 never happened. Both instants of the repeated hour read 02:30 on the
    /// user's clock, print the same and span the same; a clock reading written in the skipped
    /// hour reads, prints and spans as written.
    #[test]
    fn the_frame_rule_reads_both_clock_change_hours_on_the_users_clock() {
        let zone = ams();
        let first = save_time("2024-10-27T00:30:00Z");
        let second = save_time("2024-10-27T01:30:00Z");
        let reading = dt("2024-10-27T02:30:00");
        assert_eq!(first.on_clock(&zone).start(), reading);
        assert_eq!(second.on_clock(&zone).start(), reading);
        assert_eq!(first.printed(&zone), "2024-10-27 02:30:00");
        assert_eq!(first.printed(&zone), second.printed(&zone));
        assert_eq!(first.on_clock(&zone), second.on_clock(&zone));
        assert_eq!(first.on_clock(&zone).end(), dt("2024-10-27T02:30:01"));

        let skipped = TimeValue::clock(dt("2024-03-31T02:30:00"), Precision::Minute).unwrap();
        assert_eq!(skipped.on_clock(&zone).start(), dt("2024-03-31T02:30:00"));
        assert_eq!(skipped.on_clock(&zone).end(), dt("2024-03-31T02:31:00"));
        assert_eq!(skipped.printed(&zone), "2024-03-31 02:30");
    }

    fn span(text: &str) -> ClockSpan {
        read_time(text).unwrap().on_clock(&ams())
    }

    /// Every comparison operator of the whole-period table: `=P` lies
    /// inside P, `>=P` starts at or after P's start, `>P` starts at or after P's end, `<=P`
    /// ends by P's end, `<P` ends by P's start.
    fn holds(value: &ClockSpan, op: &str, period: &ClockSpan) -> bool {
        let op = match op {
            "=" => PeriodOp::Inside,
            ">=" => PeriodOp::StartsAtOrAfterStart,
            ">" => PeriodOp::StartsAtOrAfterEnd,
            "<=" => PeriodOp::EndsByEnd,
            "<" => PeriodOp::EndsByStart,
            other => panic!("not an operator: {other}"),
        };
        value.satisfies(op, period)
    }

    /// Which of `<`, `=`, `>` a value's whole period satisfies against a period.
    fn sides(value: &ClockSpan, period: &ClockSpan) -> [bool; 3] {
        [
            holds(value, "<", period),
            holds(value, "=", period),
            holds(value, ">", period),
        ]
    }

    /// The whole-period table, every operator at every grain, read as stated: each row's
    /// expected answer is written out, never computed.
    #[test]
    fn clock_span_comparisons_follow_the_whole_period_table() {
        // (value, operator, typed period, selected?)
        let rows = [
            // A second against each coarser grain, every operator.
            ("2024-06-15 14:30:05", "=", "2024-06-15 14:30", true),
            ("2024-06-15 14:30:05", "=", "2024-06-15", true),
            ("2024-06-15 14:30:05", "=", "2024-06", true),
            ("2024-06-15 14:30:05", "=", "2024", true),
            ("2024-06-15 14:30:05", "=", "2024-06-15 14:30:05", true),
            ("2024-06-15 14:30:05", "=", "2024-06-15 14:31", false),
            ("2024-06-15 14:30:05", ">=", "2024-06-15", true),
            ("2024-06-15 14:30:05", ">=", "2024-06-16", false),
            ("2024-06-15 14:30:05", ">", "2024-06-15", false),
            ("2024-06-15 14:30:05", ">", "2024-06-14", true),
            ("2024-06-15 14:30:05", ">", "2024-05", true),
            ("2024-06-15 14:30:05", ">", "2024-06", false),
            ("2024-06-15 14:30:05", "<=", "2024-06", true),
            ("2024-06-15 14:30:05", "<=", "2024-05", false),
            ("2024-06-15 14:30:05", "<", "2024-06", false),
            ("2024-06-15 14:30:05", "<", "2024-07", true),
            ("2024-06-15 14:30:05", "<", "2025", true),
            ("2024-06-15 14:30:05", "<", "2024", false),
            ("2024-06-15 14:30:05", ">=", "2024-06-15 14:30:05", true),
            ("2024-06-15 14:30:05", ">", "2024-06-15 14:30:05", false),
            ("2024-06-15 14:30:05", ">", "2024-06-15 14:30:04", true),
            ("2024-06-15 14:30:05", "<=", "2024-06-15 14:30:05", true),
            ("2024-06-15 14:30:05", "<", "2024-06-15 14:30:05", false),
            ("2024-06-15 14:30:05", "<", "2024-06-15 14:30:06", true),
            // The edges of a minute: one second past its end, one before its start.
            ("2024-06-15 14:31:00", "=", "2024-06-15 14:30", false),
            ("2024-06-15 14:31:00", ">", "2024-06-15 14:30", true),
            ("2024-06-15 14:29:59", "=", "2024-06-15 14:30", false),
            ("2024-06-15 14:29:59", "<", "2024-06-15 14:30", true),
            ("2024-06-15 14:30:00", ">=", "2024-06-15 14:30", true),
            ("2024-06-15 14:30:00", "<", "2024-06-15 14:30", false),
            // Whole periods: a month inside itself and its year, a month not inside a day.
            ("2024-06", "=", "2024-06", true),
            ("2024-06", "=", "2024", true),
            ("2024-06", "<=", "2024-06", true),
            ("2024-06", "<", "2024-07", true),
            ("2024-06", ">", "2024-05", true),
            ("2024-06", "=", "2024-06-15", false),
            // A value coarser than the typed period: in none of `<`, `=`, `>`, and so in
            // neither `<=` nor `>=` past the parts it shares.
            ("2024", "<", "2024-06", false),
            ("2024", "=", "2024-06", false),
            ("2024", ">", "2024-06", false),
            ("2024", "<=", "2024-06", false),
            ("2024", ">=", "2024-06", false),
            ("2024", ">=", "2024-01", true),
            ("2024", "<=", "2024-12", true),
            ("2024-06-15", "=", "2024-06-15 14:30", false),
            ("2024-06-15", "<", "2024-06-15 14:30", false),
            ("2024-06-15", ">", "2024-06-15 14:30", false),
            // A typed fraction is a point one nanosecond long: `=` is the same instant.
            (
                "2024-06-15 14:30:05.45",
                "=",
                "2024-06-15 14:30:05.45",
                true,
            ),
            (
                "2024-06-15 14:30:05.453",
                "=",
                "2024-06-15 14:30:05.45",
                false,
            ),
            (
                "2024-06-15 14:30:05.453",
                ">",
                "2024-06-15 14:30:05.45",
                true,
            ),
            (
                "2024-06-15 14:30:05.449",
                "<",
                "2024-06-15 14:30:05.45",
                true,
            ),
            (
                "2024-06-15 14:30:05.45",
                ">",
                "2024-06-15 14:30:05.45",
                false,
            ),
            (
                "2024-06-15 14:30:05.45",
                "<",
                "2024-06-15 14:30:05.45",
                false,
            ),
            (
                "2024-06-15 14:30:05.45",
                ">=",
                "2024-06-15 14:30:05.45",
                true,
            ),
            (
                "2024-06-15 14:30:05.45",
                "<=",
                "2024-06-15 14:30:05.45",
                true,
            ),
            ("2024-06-15 14:30:05.45", "=", "2024-06-15 14:30:05", true),
            ("2024-06-15 14:30:06", ">", "2024-06-15 14:30:05.45", true),
            // A second against a point inside it is coarser: none of the three.
            ("2024-06-15 14:30:05", "<", "2024-06-15 14:30:05.45", false),
            ("2024-06-15 14:30:05", "=", "2024-06-15 14:30:05.45", false),
            ("2024-06-15 14:30:05", ">", "2024-06-15 14:30:05.45", false),
            // The last periods of 9999 end at the last reading jiff holds and still compare.
            ("9999-12-31 23:59:59", "=", "9999", true),
            ("9999-12-31 23:59:59", "=", "9999-12-31", true),
            ("9999-12-31 23:59:59", ">=", "9999-12-31", true),
        ];
        for (value, op, period, expected) in rows {
            assert_eq!(
                holds(&span(value), op, &span(period)),
                expected,
                "{value} {op} {period}"
            );
        }
    }

    /// Calendar grains nest, so a value at the typed grain or finer satisfies exactly one of
    /// `<`, `=`, `>`, and `<=`/`>=` are each the union of two of them. A coarser value that
    /// holds the period satisfies none of the three (written as a text prefix of the period,
    /// since the spellings nest as the grains do); a coarser value wholly before or after it,
    /// exactly one.
    #[test]
    fn the_period_table_splits_every_finer_value_three_ways() {
        let periods = [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15 14:30",
            "2024-06-15 14:30:05",
            "2024-06-15 14:30:05.5",
        ];
        let values = [
            "2023",
            "2024",
            "2025",
            "2024-05",
            "2024-06",
            "2024-07",
            "2024-06-14",
            "2024-06-15",
            "2024-06-16",
            "2024-06-15 14:29",
            "2024-06-15 14:30",
            "2024-06-15 14:31",
            "2024-06-15 14:30:04",
            "2024-06-15 14:30:05",
            "2024-06-15 14:30:06",
            "2024-06-15 14:30:05.4",
            "2024-06-15 14:30:05.5",
            "2024-06-15 14:30:05.6",
        ];
        for p in periods {
            let period = span(p);
            for v in values {
                let value = span(v);
                let [lt, eq, gt] = sides(&value, &period);
                let count = [lt, eq, gt].iter().filter(|b| **b).count();
                let holds_the_period = value.precision() < period.precision() && p.starts_with(v);
                if holds_the_period {
                    assert_eq!(count, 0, "coarser {v} against {p}");
                } else {
                    assert_eq!(count, 1, "{v} against {p}");
                }
                if value.precision() >= period.precision() {
                    assert_eq!(holds(&value, "<=", &period), lt || eq, "{v} <= {p}");
                    assert_eq!(holds(&value, ">=", &period), gt || eq, "{v} >= {p}");
                }
            }
        }
    }

    /// `spans` is the table's third side: for `<` against `>=` and `<=` against `>`, every
    /// value satisfies one of the pair or spans the boundary they share, never two of the three;
    /// for `=`, a value spans exactly where it is coarser than the period and holds it.
    #[test]
    fn spans_is_the_third_side_of_the_period_table() {
        let periods = [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15 14:30",
            "2024-06-15 14:30:05",
            "2024-06-15 14:30:05.5",
        ];
        let values = [
            "2023",
            "2024",
            "2025",
            "2024-01",
            "2024-05",
            "2024-06",
            "2024-07",
            "2024-12",
            "2024-06-01",
            "2024-06-14",
            "2024-06-15",
            "2024-06-16",
            "2024-06-30",
            "2024-06-15 14:29",
            "2024-06-15 14:30",
            "2024-06-15 14:31",
            "2024-06-15 14:30:04",
            "2024-06-15 14:30:05",
            "2024-06-15 14:30:06",
            "2024-06-15 14:30:05.4",
            "2024-06-15 14:30:05.5",
            "2024-06-15 14:30:05.6",
        ];
        let pairs = [
            (PeriodOp::EndsByStart, PeriodOp::StartsAtOrAfterStart),
            (PeriodOp::EndsByEnd, PeriodOp::StartsAtOrAfterEnd),
        ];
        for p in periods {
            let period = span(p);
            for v in values {
                let value = span(v);
                for (op, opposite) in pairs {
                    let held = [
                        value.satisfies(op, &period),
                        value.satisfies(opposite, &period),
                        value.spans(op, &period),
                    ];
                    assert_eq!(
                        held.iter().filter(|b| **b).count(),
                        1,
                        "{v} {op:?}/{opposite:?} {p}: {held:?}"
                    );
                    assert_eq!(value.spans(op, &period), value.spans(opposite, &period));
                }
                let holds_the_period = value.precision() < period.precision() && p.starts_with(v);
                assert_eq!(
                    value.spans(PeriodOp::Inside, &period),
                    holds_the_period,
                    "{v} = {p}"
                );
            }
        }
    }

    /// The 23- and 25-hour days in Amsterdam: every instant of the repeated hour lies inside
    /// its day and its minute on the user's clock; an instant after the skipped hour lies
    /// after a clock reading written in it.
    #[test]
    fn the_period_table_reads_the_23_and_25_hour_days_on_the_users_clock() {
        let zone = ams();
        let on = |text: &str| read_time(text).unwrap().on_clock(&zone);
        let first = save_time("2024-10-27T00:30:00Z").on_clock(&zone);
        let second = save_time("2024-10-27T01:30:00Z").on_clock(&zone);
        let day_start = save_time("2024-10-26T22:00:00Z").on_clock(&zone);
        let day_end = save_time("2024-10-27T22:59:59Z").on_clock(&zone);
        for moment in [&first, &second, &day_start, &day_end] {
            assert!(holds(moment, "=", &on("2024-10-27")));
        }
        for moment in [&first, &second] {
            assert!(holds(moment, "=", &on("2024-10-27 02:30")));
            assert!(holds(moment, ">", &on("2024-10-27 02:29")));
            assert!(holds(moment, "<", &on("2024-10-27 02:31")));
        }
        assert!(holds(
            &save_time("2024-10-26T21:59:59Z").on_clock(&zone),
            "<",
            &on("2024-10-27")
        ));
        assert!(holds(
            &save_time("2024-10-27T23:00:00Z").on_clock(&zone),
            ">",
            &on("2024-10-27")
        ));

        let after_skip = save_time("2024-03-31T01:30:00Z").on_clock(&zone);
        assert!(holds(&after_skip, "=", &on("2024-03-31")));
        assert!(holds(&after_skip, "=", &on("2024-03-31 03:30")));
        assert!(holds(&after_skip, ">", &on("2024-03-31 02:30")));
        let in_skip = on("2024-03-31 02:30:00");
        assert!(holds(&in_skip, "=", &on("2024-03-31 02:30")));
        assert!(holds(&in_skip, "<", &on("2024-03-31 03:00")));
    }

    #[test]
    fn a_period_is_named_as_asked_with_its_fraction() {
        let cases = [
            ("2024", "2024", None),
            ("2024-06", "2024-06", None),
            ("2024:06:15", "2024-06-15", None),
            ("2024-06-15 14:30", "2024-06-15 14:30", Some("14:30")),
            (
                "2024-06-15 14:30:05",
                "2024-06-15 14:30:05",
                Some("14:30:05"),
            ),
            (
                "2024-06-15T14:30:05.45",
                "2024-06-15 14:30:05.45",
                Some("14:30:05.45"),
            ),
            (
                "2024-06-15T14:30:05.450",
                "2024-06-15 14:30:05.45",
                Some("14:30:05.45"),
            ),
        ];
        for (text, asked, time) in cases {
            let span = span(text);
            assert_eq!(span.printed_as_asked(), asked, "{text}");
            assert_eq!(span.printed_time_as_asked().as_deref(), time, "{text}");
        }
        // A stored value's one printed spelling still truncates the fraction.
        assert_eq!(
            span("2024-06-15T14:30:05.45").printed(),
            "2024-06-15 14:30:05"
        );
    }

    #[test]
    fn a_span_is_one_grain_long_and_a_fraction_is_a_point() {
        let zone = ams();
        let cases = [
            ("2024", "2024-01-01T00:00:00", "2025-01-01T00:00:00"),
            ("2024-02", "2024-02-01T00:00:00", "2024-03-01T00:00:00"),
            ("2024-02-29", "2024-02-29T00:00:00", "2024-03-01T00:00:00"),
            (
                "2024-12-31 23:59",
                "2024-12-31T23:59:00",
                "2025-01-01T00:00:00",
            ),
            (
                "2024-12-31 23:59:59",
                "2024-12-31T23:59:59",
                "2025-01-01T00:00:00",
            ),
            (
                "2024-06-15 14:30:05.45",
                "2024-06-15T14:30:05.45",
                "2024-06-15T14:30:05.450000001",
            ),
        ];
        for (text, start, end) in cases {
            let span = read_time(text).unwrap().on_clock(&zone);
            assert_eq!(span.start(), dt(start), "{text}");
            assert_eq!(span.end(), dt(end), "{text}");
        }
    }

    #[test]
    fn the_last_span_of_year_9999_ends_at_the_last_reading_jiff_holds() {
        let span = read_time("9999").unwrap().on_clock(&ams());
        assert_eq!(span.end(), DateTime::MAX);
    }

    #[test]
    fn an_instant_reads_in_the_users_zone_and_is_cut_to_its_grain() {
        // A minute value read in a zone whose offset has seconds (Amsterdam's local mean
        // time before 1937, +00:19:32) still spans a whole minute.
        let value = read_time("1930-06-01T12:00Z").unwrap();
        let span = value.on_clock(&ams());
        assert_eq!(span.precision(), Precision::Minute);
        assert_eq!(span.start().second(), 0);
    }

    #[test]
    fn printed_is_one_spelling_per_precision_with_no_offset() {
        let zone = ams();
        let cases = [
            ("2024", "2024"),
            ("2024-06", "2024-06"),
            ("2024-06-15", "2024-06-15"),
            ("2024-06-15 14:30", "2024-06-15 14:30"),
            ("2024-06-15 14:30:05", "2024-06-15 14:30:05"),
            ("2024-06-15 14:30:05.95", "2024-06-15 14:30:05"),
            ("2024-06-15T14:30:05.999999999+02:00", "2024-06-15 14:30:05"),
            ("2024-06-15T14:30+05:00", "2024-06-15 11:30"),
        ];
        for (text, printed) in cases {
            assert_eq!(read_time(text).unwrap().printed(&zone), printed, "{text}");
        }
    }

    #[test]
    fn iso_text_writes_the_stored_form_precision_and_offset() {
        let cases = [
            ("2023-12-31T21:00:00-05:00", "2023-12-31T21:00:00-05:00"),
            ("2023-12-31T20:00:00-00:00", "2023-12-31T20:00:00-00:00"),
            ("2023-12-31T21:00:00", "2023-12-31T21:00:00"),
            ("2024", "2024"),
            ("2024-06", "2024-06"),
            ("2024-06-15", "2024-06-15"),
            ("2024-06-15T14:30", "2024-06-15T14:30"),
            (
                "2024-06-15T14:30:05.45+02:00",
                "2024-06-15T14:30:05.45+02:00",
            ),
            (
                "2024-06-15T14:30:05.450+02:00",
                "2024-06-15T14:30:05.45+02:00",
            ),
            ("2023-12-31T23:30:00Z", "2023-12-31T23:30:00+00:00"),
            ("2023-12-31T23:30:00+00:00", "2023-12-31T23:30:00+00:00"),
            ("2024:07:23 11:06:32", "2024-07-23T11:06:32"),
        ];
        for (text, iso) in cases {
            assert_eq!(read_time(text).unwrap().iso_text(), iso, "{text}");
        }
        assert_eq!(
            save_time("2023-12-31T20:00:00Z").iso_text(),
            "2023-12-31T20:00:00-00:00"
        );
    }

    #[test]
    fn a_zero_fraction_keeps_one_digit_so_it_reads_back_at_its_precision() {
        let value = read_time("2024-06-15T14:30:05.00+02:00").unwrap();
        assert_eq!(value.precision(), Precision::Fraction);
        assert_eq!(value.iso_text(), "2024-06-15T14:30:05.0+02:00");
        let back = read_time(&value.iso_text()).unwrap();
        assert_eq!(back, value);
        assert_eq!(back.precision(), Precision::Fraction);
    }

    #[test]
    fn every_value_reads_back_from_its_iso_text() {
        for text in [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "2024-06-15T14:30:05.123456789",
            "2023-12-31T21:00:00-05:00",
            "2023-12-31T20:00:00-00:00",
            "2023-12-31T20:00:00Z",
            "2024-06-15T14:30+13:45",
        ] {
            let value = read_time(text).unwrap();
            assert_eq!(read_time(&value.iso_text()).unwrap(), value, "{text}");
        }
    }

    /// The New Year check for display: printed, `|year` and `|date` read from the
    /// span (the modifiers themselves are pinned in the facility).
    fn year_and_date(value: &TimeValue, zone: &UserZone) -> (String, String) {
        let span = value.on_clock(zone);
        let year = format!("{:04}", span.start().year());
        let date = if span.precision() >= Precision::Day {
            span.start().date().to_string()
        } else {
            "no value".to_string()
        };
        (year, date)
    }

    const NEW_YEAR_ROWS: [&str; 7] = [
        "2023-12-31T21:00:00-05:00",
        "2023-12-31T23:30:00Z",
        "2023-12-31T23:30:00-00:00",
        "2023-07-14T21:00:00+02:00",
        "2023:12:31 21:00:00",
        "2024",
        "2024-06-15T14:30:05.45+02:00",
    ];

    #[test]
    fn the_new_year_display_table_reads_on_amsterdams_clock() {
        let zone = ams();
        let expected = [
            ("2024-01-01 03:00:00", "2024", "2024-01-01"),
            ("2024-01-01 00:30:00", "2024", "2024-01-01"),
            ("2024-01-01 00:30:00", "2024", "2024-01-01"),
            ("2023-07-14 21:00:00", "2023", "2023-07-14"),
            ("2023-12-31 21:00:00", "2023", "2023-12-31"),
            ("2024", "2024", "no value"),
            ("2024-06-15 14:30:05", "2024", "2024-06-15"),
        ];
        for (text, (printed, year, date)) in NEW_YEAR_ROWS.iter().zip(expected) {
            let value = read_time(text).unwrap();
            assert_eq!(value.printed(&zone), printed, "{text}");
            assert_eq!(
                year_and_date(&value, &zone),
                (year.into(), date.into()),
                "{text}"
            );
        }
        // The second and third rows and a file saved at 00:30 on 1 January print alike.
        let saved = save_time("2023-12-31T23:30:00Z");
        assert_eq!(saved.printed(&zone), "2024-01-01 00:30:00");
    }

    #[test]
    fn the_new_year_display_table_reads_on_new_yorks_clock() {
        // Derived from the rule: an instant read at New York's offset (-05:00 in winter,
        // -04:00 in summer); a clock reading as written.
        let zone = nyc();
        let expected = [
            ("2023-12-31 21:00:00", "2023", "2023-12-31"),
            ("2023-12-31 18:30:00", "2023", "2023-12-31"),
            ("2023-12-31 18:30:00", "2023", "2023-12-31"),
            ("2023-07-14 15:00:00", "2023", "2023-07-14"),
            ("2023-12-31 21:00:00", "2023", "2023-12-31"),
            ("2024", "2024", "no value"),
            ("2024-06-15 08:30:05", "2024", "2024-06-15"),
        ];
        for (text, (printed, year, date)) in NEW_YEAR_ROWS.iter().zip(expected) {
            let value = read_time(text).unwrap();
            assert_eq!(value.printed(&zone), printed, "{text}");
            assert_eq!(
                year_and_date(&value, &zone),
                (year.into(), date.into()),
                "{text}"
            );
        }
    }

    #[test]
    fn an_instant_coarser_than_a_minute_is_refused() {
        let at = ts("2024-06-15T00:00:00Z");
        assert_eq!(
            TimeValue::instant(at, None, Precision::Day),
            Err(TimeValueError::InstantCoarserThanMinute)
        );
    }

    #[test]
    fn a_value_with_a_part_below_its_grain_is_refused() {
        assert_eq!(
            TimeValue::clock(dt("2024-06-15T14:30:05"), Precision::Minute),
            Err(TimeValueError::BelowGrain)
        );
        assert_eq!(
            TimeValue::clock(dt("2024-06-02T00:00:00"), Precision::Month),
            Err(TimeValueError::BelowGrain)
        );
        assert_eq!(
            TimeValue::instant(ts("2024-06-15T14:30:05Z"), None, Precision::Minute),
            Err(TimeValueError::BelowGrain)
        );
    }

    #[test]
    fn an_offset_must_be_whole_minutes() {
        let odd = Offset::from_seconds(3601).unwrap();
        assert_eq!(
            TimeValue::instant(ts("2024-06-15T14:30:00Z"), Some(odd), Precision::Fraction),
            Err(TimeValueError::OffsetNotWholeMinutes)
        );
    }

    #[test]
    fn the_readable_line_holds_at_both_edges() {
        let first = first_readable_instant();
        assert_eq!(first, ts("0000-01-02T01:59:59Z"));
        let first_secs = first.as_second();
        let last_secs = Timestamp::MAX.as_second();
        assert_eq!(last_secs, 253_402_207_200);
        assert!(TimeValue::from_epoch_secs(first_secs).is_ok());
        assert_eq!(TimeValue::from_epoch_secs(first_secs - 1), Err(OutOfRange));
        assert!(TimeValue::from_epoch_secs(last_secs).is_ok());
        assert_eq!(TimeValue::from_epoch_secs(last_secs + 1), Err(OutOfRange));
        // The conventional far-future sentinel, 9999-12-31 23:59:59 UTC, lies past the line.
        assert_eq!(TimeValue::from_epoch_secs(253_402_300_799), Err(OutOfRange));

        // At each edge, read at the most extreme offsets a zone can have, the value prints a
        // four-digit year that the list of spellings reads back: printed = typed at the line.
        let extremes = [Offset::MIN, Offset::MAX, Offset::UTC];
        for secs in [first_secs, last_secs] {
            let value = TimeValue::from_epoch_secs(secs).unwrap();
            for offset in extremes {
                let zone = UserZone::for_test_offset(offset);
                let printed = value.printed(&zone);
                let back = read_time(&printed).unwrap_or_else(|e| panic!("{printed}: {e}"));
                assert_eq!(back.printed(&zone), printed);
            }
        }
    }

    #[test]
    fn a_clock_reading_outside_the_four_digit_years_is_refused() {
        assert_eq!(
            TimeValue::clock(civil::date(-1, 12, 31).at(0, 0, 0, 0), Precision::Second),
            Err(TimeValueError::OutOfRange)
        );
        assert!(TimeValue::clock(dt("0000-01-01T00:00:00"), Precision::Year).is_ok());
    }

    #[test]
    fn the_legacy_number_is_what_the_old_parser_stored() {
        // Values the old import parser produced for each form it accepted (RFC 3339 → the UTC
        // instant; naive ISO and EXIF → as if UTC; a four-digit year → its first second UTC).
        let cases: [(&str, i64); 6] = [
            ("2020-01-15T10:30:00", 1_579_084_200),
            ("2020-01-15T10:30:00+00:00", 1_579_084_200),
            ("2020-01-15T12:30:00+02:00", 1_579_084_200),
            ("2020:07:23 11:06:32", 1_595_502_392),
            ("2005", 1_104_537_600),
            ("2020-01-15T10:30:00.75Z", 1_579_084_200),
        ];
        for (text, number) in cases {
            assert_eq!(
                read_time(text).unwrap().legacy_epoch_number(),
                number,
                "{text}"
            );
        }
        assert_eq!(save_time("1969-12-31T23:59:59Z").legacy_epoch_number(), -1);
        let before_1970 = read_time("1969-12-31T23:59:59.5Z").unwrap();
        assert_eq!(before_1970.legacy_epoch_number(), -1);
        let clock_before_1970 = read_time("1969-12-31T23:59:59.5").unwrap();
        assert_eq!(clock_before_1970.legacy_epoch_number(), -1);
    }

    #[test]
    fn form_and_storage_parts_follow_the_written_offset() {
        let with = read_time("2024-06-15T14:30:05.45+02:00").unwrap();
        assert_eq!(with.form(), TimeForm::InstantWithOffset);
        assert_eq!(
            with.recorded_offset(),
            Some(Offset::from_seconds(7200).unwrap())
        );
        assert_eq!(with.fraction_nanos(), Some(450_000_000));
        let without = read_time("2024-06-15T14:30:05-00:00").unwrap();
        assert_eq!(without.form(), TimeForm::Instant);
        assert_eq!(without.recorded_offset(), None);
        assert_eq!(without.fraction_nanos(), None);
        let clock = read_time("2024-06-15").unwrap();
        assert_eq!(clock.form(), TimeForm::Clock);
        assert_eq!(clock.precision(), Precision::Day);
    }
}
