//! **The one list of spellings**: how Canon reads text as a time. This module is the code's one
//! written statement of it; `import-facts.md` is the user's.
//!
//! | Shape | Precision | Form |
//! |---|---|---|
//! | `YYYY` | year | clock reading |
//! | `YYYY-MM` | month | clock reading |
//! | `YYYY-MM-DD`, `YYYY:MM:DD` | day | clock reading |
//! | a date + `T`, `t` or one space + `HH:MM` | minute | clock reading |
//! | … + `:SS` | second | clock reading |
//! | … + `.` and 1–9 digits | fraction | clock reading |
//! | any of the last three + `Z`/`z` or `±HH:MM`, directly or after one space | as above | an instant with that offset (`Z` and `+00:00` both offset 0) |
//! | any of the last three + `-00:00` | as above | an instant with no recorded offset |
//!
//! Refused: a date alone with an offset ([`TimeReadError::OffsetOnDateAlone`]), a date or time
//! that does not exist ([`TimeReadError::ImpossibleDate`]), an instant outside the years every
//! zone reads in 0000–9999 ([`TimeReadError::OutOfRange`]), and anything else — `+0200`,
//! trailing text — ([`TimeReadError::NotInList`]). Nothing is ever cut off and dropped.
//!
//! The form is decided by the offset alone, as RFC 3339 writes it. The shape decides the
//! precision (whether seconds were written is something only the text can say); jiff decides
//! validity, the instant and the offset, reading the text in the RFC 3339 shape this module
//! writes it into. A number used as a time is a year, and only a whole number from 1900 to 2100
//! ([`read_year_number`]); a number never means seconds since 1970.
//!
//! Two callers: import now, and the conversion of a key to time later.

use jiff::civil;
use jiff::fmt::temporal::{Pieces, PiecesOffset};
use jiff::tz::Offset;

use super::value::{Precision, TimeValue, TimeValueError, REFERENCE_DAY};

/// The list's forms as a person is shown them — one per grain a date is usually asked at —
/// so a refusal that names the forms shows the ones this list reads, never a copy written
/// beside it. Each reads through [`read_time`] (pinned).
pub const EXAMPLE_FORMS: [&str; 4] = ["2024", "2024-06", "2024-06-15", "2024-06-15 14:30"];

/// A time with an offset as a person is shown it — for the refusal of an offset on a date
/// alone. Reads through [`read_time`] (pinned).
pub const EXAMPLE_WITH_OFFSET: &str = "2024-06-15 14:30 +02:00";

/// Why text did not read as a time. `Display` is the user-facing sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeReadError {
    NotInList,
    ImpossibleDate,
    OffsetOnDateAlone,
    YearOutOfRange,
    OutOfRange,
}

impl std::fmt::Display for TimeReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TimeReadError::NotInList => {
                "not written in a form Canon recognizes as a time (a date such as 2024-06-15, \
                 2024:06:15, 2024-06 or 2024, optionally a time such as 14:30:05 and an \
                 offset such as +02:00 or Z — see \"How Canon interprets a time\" in the facts \
                 documentation)"
            }
            TimeReadError::ImpossibleDate => "not a date or time that exists",
            TimeReadError::OffsetOnDateAlone => "an offset needs a time of day to belong to",
            TimeReadError::YearOutOfRange => {
                "a number is a time only when it is a whole year from 1900 to 2100"
            }
            TimeReadError::OutOfRange => super::value::EDGE_OF_THE_YEARS,
        })
    }
}

impl std::error::Error for TimeReadError {}

/// Read text through the list of spellings.
pub fn read_time(text: &str) -> Result<TimeValue, TimeReadError> {
    let shape = match_shape(text.as_bytes())?;
    build(&shape)
}

/// Read text as a time of day: `HH:MM` (minute), `HH:MM:SS` (second), `HH:MM:SS.` and 1–9
/// digits (fraction) — a clock reading on [`REFERENCE_DAY`] at the precision written, so the
/// whole-period table compares times of day as it compares every other time. Two digits each;
/// an hour past 23 or a minute or second past 59 does not exist.
pub fn read_time_of_day(text: &str) -> Result<TimeValue, TimeReadError> {
    let bytes = text.as_bytes();
    let mut c = Cursor { bytes, at: 0 };
    let not_in_list = TimeReadError::NotInList;
    let hh = c.digits::<2>().ok_or(not_in_list)?;
    if !c.eat(b':') {
        return Err(not_in_list);
    }
    let mm = c.digits::<2>().ok_or(not_in_list)?;
    let mut ss = None;
    let mut nanos = 0;
    let mut precision = Precision::Minute;
    if c.eat(b':') {
        ss = Some(c.digits::<2>().ok_or(not_in_list)?);
        precision = Precision::Second;
        if c.eat(b'.') {
            let start = c.at;
            while c.peek().is_some_and(|b| b.is_ascii_digit()) {
                c.at += 1;
            }
            let digits = bytes.get(start..c.at).unwrap_or_default();
            if !(1..=9).contains(&digits.len()) {
                return Err(not_in_list);
            }
            nanos = digits
                .iter()
                .chain(std::iter::repeat_n(&b'0', 9 - digits.len()))
                .fold(0i32, |acc, d| acc * 10 + i32::from(d - b'0'));
            precision = Precision::Fraction;
        }
    }
    if !c.done() {
        return Err(not_in_list);
    }
    let (h, m, sec) = (number(&hh), number(&mm), ss.map_or(0, |s| number(&s)));
    if h > 23 || m > 59 || sec > 59 {
        return Err(TimeReadError::ImpossibleDate);
    }
    let at = REFERENCE_DAY.at(h as i8, m as i8, sec as i8, nanos);
    TimeValue::clock(at, precision).map_err(value_error)
}

/// Read a number given the time hint: a whole year from 1900 to 2100 (`2019.0` counts), a clock
/// reading at year precision. Anything else is refused — never seconds since 1970.
pub fn read_year_number(n: f64) -> Result<TimeValue, TimeReadError> {
    if n.fract() != 0.0 || !(1900.0..=2100.0).contains(&n) {
        return Err(TimeReadError::YearOutOfRange);
    }
    let year = n as i16;
    TimeValue::clock(civil::date(year, 1, 1).at(0, 0, 0, 0), Precision::Year)
        .map_err(|_| TimeReadError::YearOutOfRange)
}

// ---------------------------------------------------------------------------
// The shape: Canon's table, matched byte by byte (ASCII only, so a multi-byte character
// anywhere is simply not in the list — never a cut inside one).
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum WrittenOffset {
    None,
    /// `Z` or `z`.
    Zulu,
    /// `±HH:MM`, sign kept so `-00:00` stays apart from `+00:00`.
    Numeric {
        negative: bool,
        hh: [u8; 2],
        mm: [u8; 2],
    },
}

#[derive(Debug)]
struct Shape {
    year: [u8; 4],
    month: Option<[u8; 2]>,
    day: Option<[u8; 2]>,
    /// `HH:MM`, then `:SS`, then the fraction digits.
    time: Option<TimeShape>,
    offset: WrittenOffset,
}

#[derive(Debug)]
struct TimeShape {
    hh: [u8; 2],
    mm: [u8; 2],
    ss: Option<[u8; 2]>,
    fraction: Option<Vec<u8>>,
}

impl Shape {
    fn precision(&self) -> Precision {
        match &self.time {
            None if self.month.is_none() => Precision::Year,
            None if self.day.is_none() => Precision::Month,
            None => Precision::Day,
            Some(t) if t.ss.is_none() => Precision::Minute,
            Some(t) if t.fraction.is_none() => Precision::Second,
            Some(_) => Precision::Fraction,
        }
    }
}

/// A cursor over the bytes.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn digits<const N: usize>(&mut self) -> Option<[u8; N]> {
        let mut out = [0u8; N];
        for slot in &mut out {
            let b = self.peek().filter(u8::is_ascii_digit)?;
            *slot = b;
            self.at += 1;
        }
        Some(out)
    }

    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }

    /// An offset at the cursor, directly or after one space; `None` (cursor unmoved) if there
    /// is none.
    fn offset(&mut self) -> Option<WrittenOffset> {
        let start = self.at;
        self.eat(b' ');
        let found = match self.peek() {
            Some(b'Z' | b'z') => {
                self.at += 1;
                Some(WrittenOffset::Zulu)
            }
            Some(sign @ (b'+' | b'-')) => {
                self.at += 1;
                let hh = self.digits::<2>();
                let colon = self.eat(b':');
                let mm = self.digits::<2>();
                match (hh, colon, mm) {
                    (Some(hh), true, Some(mm)) => Some(WrittenOffset::Numeric {
                        negative: sign == b'-',
                        hh,
                        mm,
                    }),
                    _ => None,
                }
            }
            _ => None,
        };
        if found.is_none() {
            self.at = start;
        }
        found
    }
}

fn match_shape(bytes: &[u8]) -> Result<Shape, TimeReadError> {
    let mut c = Cursor { bytes, at: 0 };
    let not_in_list = TimeReadError::NotInList;
    let year = c.digits::<4>().ok_or(not_in_list)?;
    let mut shape = Shape {
        year,
        month: None,
        day: None,
        time: None,
        offset: WrittenOffset::None,
    };
    if c.done() {
        return Ok(shape);
    }
    // The date separator: `-` (year-month or a full date) or `:` (the EXIF date, full only).
    let sep = match c.peek() {
        Some(b @ (b'-' | b':')) => b,
        _ => return Err(refusal_after_date(&mut c)),
    };
    c.at += 1;
    shape.month = Some(c.digits::<2>().ok_or(not_in_list)?);
    if c.done() {
        return if sep == b'-' {
            Ok(shape)
        } else {
            Err(not_in_list)
        };
    }
    if !c.eat(sep) {
        return Err(refusal_after_date(&mut c));
    }
    shape.day = Some(c.digits::<2>().ok_or(not_in_list)?);
    if c.done() {
        return Ok(shape);
    }
    // A time of day after `T`, `t` or one space.
    let at_time = c.at;
    if matches!(c.peek(), Some(b'T' | b't' | b' ')) {
        c.at += 1;
        if let Some(hh) = c.digits::<2>() {
            if !c.eat(b':') {
                return Err(not_in_list);
            }
            let mm = c.digits::<2>().ok_or(not_in_list)?;
            let mut time = TimeShape {
                hh,
                mm,
                ss: None,
                fraction: None,
            };
            if c.eat(b':') {
                time.ss = Some(c.digits::<2>().ok_or(not_in_list)?);
                if c.eat(b'.') {
                    let start = c.at;
                    while c.peek().is_some_and(|b| b.is_ascii_digit()) {
                        c.at += 1;
                    }
                    let count = c.at - start;
                    if !(1..=9).contains(&count) {
                        return Err(not_in_list);
                    }
                    time.fraction = bytes.get(start..c.at).map(<[u8]>::to_vec);
                }
            }
            shape.time = Some(time);
            if let Some(offset) = c.offset() {
                shape.offset = offset;
            }
            return if c.done() {
                Ok(shape)
            } else {
                Err(not_in_list)
            };
        }
        c.at = at_time;
    }
    Err(refusal_after_date(&mut c))
}

/// What follows a date that is not a time of day: an offset alone is its own refusal; anything
/// else is not in the list.
fn refusal_after_date(c: &mut Cursor<'_>) -> TimeReadError {
    if c.offset().is_some() && c.done() {
        TimeReadError::OffsetOnDateAlone
    } else {
        TimeReadError::NotInList
    }
}

fn number(digits: &[u8]) -> i32 {
    digits
        .iter()
        .fold(0i32, |acc, d| acc * 10 + i32::from(d - b'0'))
}

/// The shape written as the RFC 3339 text jiff reads, then read: jiff decides whether the date
/// and time exist, and the instant an offset names.
fn build(shape: &Shape) -> Result<TimeValue, TimeReadError> {
    let precision = shape.precision();
    let year = number(&shape.year);
    let Some(time) = &shape.time else {
        // A date alone: a clock reading at its grain.
        let month = shape.month.map_or(1, |m| number(&m));
        let day = shape.day.map_or(1, |d| number(&d));
        let date = civil::Date::new(year as i16, month as i8, day as i8)
            .map_err(|_| TimeReadError::ImpossibleDate)?;
        return TimeValue::clock(date.at(0, 0, 0, 0), precision).map_err(value_error);
    };
    let month = shape.month.expect("a time follows a full date");
    let day = shape.day.expect("a time follows a full date");
    let mut text = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}",
        year,
        number(&month),
        number(&day),
        number(&time.hh),
        number(&time.mm),
    );
    if let Some(ss) = time.ss {
        text.push_str(&format!(":{:02}", number(&ss)));
    }
    if let Some(fraction) = &time.fraction {
        text.push('.');
        text.extend(fraction.iter().map(|&b| char::from(b)));
    }
    match &shape.offset {
        WrittenOffset::None => {}
        WrittenOffset::Zulu => text.push('Z'),
        WrittenOffset::Numeric { negative, hh, mm } => text.push_str(&format!(
            "{}{:02}:{:02}",
            if *negative { '-' } else { '+' },
            number(hh),
            number(mm)
        )),
    }
    let pieces = Pieces::parse(&text).map_err(|_| TimeReadError::ImpossibleDate)?;
    let clock = pieces
        .date()
        .to_datetime(pieces.time().unwrap_or(civil::Time::midnight()));
    let offset = match pieces.offset() {
        None => return TimeValue::clock(clock, precision).map_err(value_error),
        Some(PiecesOffset::Zulu) => Some(Offset::UTC),
        Some(PiecesOffset::Numeric(n)) if n.is_negative() && n.offset() == Offset::UTC => None,
        Some(PiecesOffset::Numeric(n)) => Some(n.offset()),
        Some(_) => return Err(TimeReadError::NotInList),
    };
    let at = offset
        .unwrap_or(Offset::UTC)
        .to_timestamp(clock)
        .map_err(|_| TimeReadError::OutOfRange)?;
    TimeValue::instant(at, offset, precision).map_err(value_error)
}

fn value_error(e: TimeValueError) -> TimeReadError {
    match e {
        TimeValueError::OutOfRange => TimeReadError::OutOfRange,
        // The shape writes nothing below its grain, nothing coarser than a minute with an
        // offset, and offsets in whole minutes; reaching these is a defect in the table.
        TimeValueError::BelowGrain
        | TimeValueError::InstantCoarserThanMinute
        | TimeValueError::OffsetNotWholeMinutes => TimeReadError::NotInList,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_time_of_day_reads_on_the_reference_day_at_the_precision_written() {
        let day = REFERENCE_DAY;
        let cases = [
            ("14:30", day.at(14, 30, 0, 0), Precision::Minute),
            ("00:00", day.at(0, 0, 0, 0), Precision::Minute),
            ("23:59:59", day.at(23, 59, 59, 0), Precision::Second),
            ("14:30:05", day.at(14, 30, 5, 0), Precision::Second),
            (
                "14:30:05.45",
                day.at(14, 30, 5, 450_000_000),
                Precision::Fraction,
            ),
            (
                "14:30:05.000000001",
                day.at(14, 30, 5, 1),
                Precision::Fraction,
            ),
        ];
        for (text, at, precision) in cases {
            let value = read_time_of_day(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(value, TimeValue::clock(at, precision).unwrap(), "{text}");
        }
        let refused = [
            ("24:00", TimeReadError::ImpossibleDate),
            ("14:60", TimeReadError::ImpossibleDate),
            ("14:30:60", TimeReadError::ImpossibleDate),
            ("1:30", TimeReadError::NotInList),
            ("14", TimeReadError::NotInList),
            ("14:3", TimeReadError::NotInList),
            ("14:30:", TimeReadError::NotInList),
            ("14:30:05.", TimeReadError::NotInList),
            ("14:30:05.1234567890", TimeReadError::NotInList),
            ("14:30 ", TimeReadError::NotInList),
            ("14:30Z", TimeReadError::NotInList),
            ("14.30", TimeReadError::NotInList),
            ("", TimeReadError::NotInList),
        ];
        for (text, error) in refused {
            assert_eq!(read_time_of_day(text), Err(error), "{text:?}");
        }
    }
    use crate::core::domain::time::value::TimeForm;

    fn read(text: &str) -> TimeValue {
        read_time(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    fn shape_of(text: &str) -> (Precision, TimeForm) {
        let v = read(text);
        (v.precision(), v.form())
    }

    #[test]
    fn a_year_alone_is_a_clock_reading_at_year_precision() {
        assert_eq!(shape_of("2024"), (Precision::Year, TimeForm::Clock));
    }

    #[test]
    fn a_year_and_month_is_a_clock_reading_at_month_precision() {
        assert_eq!(shape_of("2024-06"), (Precision::Month, TimeForm::Clock));
    }

    #[test]
    fn a_date_is_a_clock_reading_at_day_precision_in_both_spellings() {
        assert_eq!(shape_of("2024-06-15"), (Precision::Day, TimeForm::Clock));
        assert_eq!(shape_of("2024:06:15"), (Precision::Day, TimeForm::Clock));
        assert_eq!(read("2024:06:15"), read("2024-06-15"));
    }

    #[test]
    fn a_time_of_day_to_the_minute_after_t_or_a_space() {
        for text in [
            "2024-06-15T14:30",
            "2024-06-15t14:30",
            "2024-06-15 14:30",
            "2024:06:15 14:30",
        ] {
            assert_eq!(
                shape_of(text),
                (Precision::Minute, TimeForm::Clock),
                "{text}"
            );
        }
    }

    #[test]
    fn a_time_of_day_to_the_second() {
        assert_eq!(
            shape_of("2024-06-15T14:30:05"),
            (Precision::Second, TimeForm::Clock)
        );
    }

    #[test]
    fn a_time_of_day_with_a_fraction_keeps_it() {
        let v = read("2024-06-15T14:30:05.45");
        assert_eq!(
            (v.precision(), v.form()),
            (Precision::Fraction, TimeForm::Clock)
        );
        assert_eq!(v.fraction_nanos(), Some(450_000_000));
        assert_eq!(
            read("2024-06-15T14:30:05.123456789").fraction_nanos(),
            Some(123_456_789)
        );
    }

    #[test]
    fn z_plus_zero_and_minus_zero_are_three_stored_outcomes() {
        let z = read("2023-12-31T23:30:00Z");
        let plus = read("2023-12-31T23:30:00+00:00");
        let minus = read("2023-12-31T23:30:00-00:00");
        assert_eq!(z.form(), TimeForm::InstantWithOffset);
        assert_eq!(z.recorded_offset(), Some(Offset::UTC));
        assert_eq!(plus, z, "Z and +00:00 are both offset 0");
        assert_eq!(minus.form(), TimeForm::Instant);
        assert_eq!(minus.recorded_offset(), None);
        assert_eq!(minus.legacy_epoch_number(), z.legacy_epoch_number());
        assert_eq!(read("2023-12-31T23:30Z").precision(), Precision::Minute);
    }

    #[test]
    fn an_offset_is_kept_at_every_precision_with_a_time() {
        for (text, precision) in [
            ("2023-12-31T21:00-05:00", Precision::Minute),
            ("2023-12-31T21:00:00-05:00", Precision::Second),
            ("2024:07:23 11:06:32.45+02:00", Precision::Fraction),
        ] {
            let v = read(text);
            assert_eq!(v.precision(), precision, "{text}");
            assert_eq!(v.form(), TimeForm::InstantWithOffset, "{text}");
        }
        let exif = read("2024:07:23 11:06:32.45+02:00");
        assert_eq!(exif.recorded_offset(), Some(Offset::from_hours(2).unwrap()));
        assert_eq!(exif.iso_text(), "2024-07-23T11:06:32.45+02:00");
    }

    #[test]
    fn an_offset_may_follow_one_space() {
        assert_eq!(
            read("2024-06-15 14:30:05 +02:00"),
            read("2024-06-15T14:30:05+02:00")
        );
        assert_eq!(read("2024:06:15 14:30:05 Z"), read("2024-06-15T14:30:05Z"));
        assert!(
            read_time("2024-06-15 14:30:05  +02:00").is_err(),
            "one space, not two"
        );
    }

    #[test]
    fn lower_case_t_and_z_read_as_upper_case() {
        assert_eq!(read("2024-06-15t14:30:05z"), read("2024-06-15T14:30:05Z"));
    }

    #[test]
    fn a_date_alone_with_an_offset_is_refused_by_name() {
        for text in [
            "2023-12-31-05:00",
            "2023-12-31Z",
            "2023-12-31 +02:00",
            "2024-06Z",
            "2024+01:00",
        ] {
            assert_eq!(
                read_time(text),
                Err(TimeReadError::OffsetOnDateAlone),
                "{text}"
            );
        }
    }

    #[test]
    fn a_date_or_time_that_does_not_exist_is_refused_by_name() {
        for text in [
            "2024-02-30",
            "2023-02-29",
            "2024-13",
            "2024-00",
            "2024-06-15T25:00",
            "2024-06-15T14:61",
        ] {
            assert_eq!(
                read_time(text),
                Err(TimeReadError::ImpossibleDate),
                "{text}"
            );
        }
        assert!(read_time("2024-02-29").is_ok());
    }

    #[test]
    fn an_instant_beyond_the_readable_line_is_refused_out_of_range() {
        assert_eq!(
            read_time("9999-12-31T23:00:00-05:00"),
            Err(TimeReadError::OutOfRange)
        );
        assert_eq!(
            read_time("9999-12-31T23:59:59Z"),
            Err(TimeReadError::OutOfRange)
        );
        assert_eq!(
            read_time("0000-01-01T00:00:00Z"),
            Err(TimeReadError::OutOfRange)
        );
        // A clock reading in the same years is in the line.
        assert!(read_time("9999-12-31T23:59:59").is_ok());
    }

    /// The forms a refusal shows are forms this list reads, at the grains they claim.
    #[test]
    fn the_example_forms_read_through_the_one_list() {
        let grains = [
            Precision::Year,
            Precision::Month,
            Precision::Day,
            Precision::Minute,
        ];
        assert_eq!(
            EXAMPLE_FORMS.len(),
            grains.len(),
            "every shown form has its grain"
        );
        for (text, grain) in EXAMPLE_FORMS.iter().zip(grains) {
            let value = read_time(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(value.precision(), grain, "{text}");
            assert_eq!(value.form(), TimeForm::Clock, "{text}");
        }
        let offset = read_time(EXAMPLE_WITH_OFFSET).unwrap();
        assert_eq!(offset.form(), TimeForm::InstantWithOffset);
        assert_eq!(offset.precision(), Precision::Minute);
    }

    #[test]
    fn anything_else_is_not_in_the_list() {
        for text in [
            "",
            "24",
            "20240615",
            "2024-6-15",
            "2024:06",
            "2024-06:15",
            "2024-06-15T14",
            "2024-06-15T14:30:05+0200",
            "2024-06-15T14:30:05+02",
            "2024-06-15T14:30:05.",
            "2024-06-15T14:30:05.1234567890",
            "2024-06-15T14:30.5",
            "2024-06-15 14:30:05 PM",
            "2020:07:23 11:06:32é",
            "2024-06-15T14:30:05Z trailing",
            " 2024",
            "2024 ",
            "+2024-06-15",
            "2024-06-15T14:30:05[Europe/Amsterdam]",
            "1.5e9",
        ] {
            assert_eq!(read_time(text), Err(TimeReadError::NotInList), "{text:?}");
        }
    }

    #[test]
    fn a_multi_byte_character_at_every_byte_position_never_panics() {
        // The class the old 19-character cut once had: a cut inside a character panics.
        let base = "2024:07:23 11:06:32.45+02:00";
        for i in 0..=base.len() {
            let (head, tail) = base.split_at(i);
            let text = format!("{head}é{tail}");
            assert!(read_time(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn a_year_number_is_a_whole_year_from_1900_to_2100() {
        assert_eq!(read_year_number(1899.0), Err(TimeReadError::YearOutOfRange));
        assert_eq!(read_year_number(2101.0), Err(TimeReadError::YearOutOfRange));
        assert_eq!(read_year_number(2019.5), Err(TimeReadError::YearOutOfRange));
        assert_eq!(read_year_number(1.5e9), Err(TimeReadError::YearOutOfRange));
        assert_eq!(
            read_year_number(f64::NAN),
            Err(TimeReadError::YearOutOfRange)
        );
        for n in [1900.0, 2100.0, 2019.0] {
            let v = read_year_number(n).unwrap();
            assert_eq!(
                (v.precision(), v.form()),
                (Precision::Year, TimeForm::Clock)
            );
            assert_eq!(v.iso_text(), format!("{n}"));
        }
        assert_eq!(read_year_number(2019.0).unwrap(), read("2019"));
    }

    #[test]
    fn the_refusal_sentences_read_as_written() {
        assert_eq!(
            TimeReadError::NotInList.to_string(),
            "not written in a form Canon recognizes as a time (a date such as 2024-06-15, \
             2024:06:15, 2024-06 or 2024, optionally a time such as 14:30:05 and an \
             offset such as +02:00 or Z — see \"How Canon interprets a time\" in the facts \
             documentation)"
        );
        assert_eq!(
            TimeReadError::OutOfRange.to_string(),
            "the time lies at the edge of the years 0000–9999 or beyond"
        );
        // Neither says the text is not a time.
        for e in [TimeReadError::NotInList, TimeReadError::OutOfRange] {
            assert!(!e.to_string().contains("not a time"), "{e}");
        }
    }
}
