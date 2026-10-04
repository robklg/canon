//! Value transforms: path accessors and modifiers.
//!
//! A transform turns one fact value into another — `[-1]` takes a path's last
//! segment, `|year` reads a year out of a timestamp, `|bucket(60,300)` puts a
//! number in a range. Both halves of the expression language use them, so they
//! live apart from the key vocabulary that names values and the patterns that
//! arrange them.

use anyhow::{bail, Result};
use jiff::civil::Weekday;

use crate::core::domain::fact::FactValue;
use crate::core::domain::time::value::{Precision, TimeValue};
use crate::core::domain::time::zone::UserZone;

/// A modifier with optional arguments
#[derive(Debug, Clone, PartialEq)]
pub struct ModifierCall {
    pub modifier: Modifier,
    pub args: Vec<f64>, // empty for no args
}

/// Path segment accessor for indexing into path-type values
#[derive(Debug, Clone, PartialEq)]
pub enum PathAccessor {
    /// Single index: [2] or [-1]
    Index(i32),
    /// Slice: [1:3] or [-3:-1] or [1:] or [:3]
    Slice {
        start: Option<i32>,
        end: Option<i32>,
    },
}

/// Modifier category for grouping
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ModifierCategory {
    Time,
    String,
    Numeric,
}

/// Modifiers that transform values
#[derive(Debug, Clone, Copy, PartialEq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "lowercase")]
pub enum Modifier {
    // Time modifiers (for time-type facts)
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Date, // YYYY-MM-DD
    Time, // HH:MM:SS
    #[strum(serialize = "datetime")]
    DateTime, // YYYY-MM-DDTHH:MM:SS
    #[strum(serialize = "yearmonth")]
    YearMonth, // YYYY-MM
    Week,
    Weekday,
    Quarter,
    // String modifiers
    Stem,       // filename without extension
    Ext,        // file extension
    Short,      // first 8 chars (for hashes)
    Lowercase,  // convert to lowercase
    Uppercase,  // convert to uppercase
    Capitalize, // capitalize first letter, lowercase rest
    // Numeric modifiers
    Bucket, // magnitude-based or threshold-based ranges
}

impl Modifier {
    /// Get the category of this modifier
    pub const fn category(&self) -> ModifierCategory {
        match self {
            Modifier::Year
            | Modifier::Month
            | Modifier::Day
            | Modifier::Hour
            | Modifier::Minute
            | Modifier::Second
            | Modifier::Date
            | Modifier::Time
            | Modifier::DateTime
            | Modifier::YearMonth
            | Modifier::Week
            | Modifier::Weekday
            | Modifier::Quarter => ModifierCategory::Time,
            Modifier::Stem
            | Modifier::Ext
            | Modifier::Short
            | Modifier::Lowercase
            | Modifier::Uppercase
            | Modifier::Capitalize => ModifierCategory::String,
            Modifier::Bucket => ModifierCategory::Numeric,
        }
    }
}

/// Parse a modifier with optional arguments: "year" or "bucket(60,300,600)"
pub fn parse_modifier(s: &str) -> Result<ModifierCall> {
    use strum::IntoEnumIterator;

    // Check for function syntax: modifier(arg1,arg2,...)
    let (name, args) = if let Some(paren_pos) = s.find('(') {
        if !s.ends_with(')') {
            bail!("Unclosed '(' in modifier: '{s}'");
        }
        #[expect(clippy::string_slice, reason = "a `find` position")]
        let name = &s[..paren_pos];
        #[expect(
            clippy::string_slice,
            reason = "inside a found `(` and a trailing `)`, both ASCII"
        )]
        let args_str = &s[paren_pos + 1..s.len() - 1];

        let args: Vec<f64> = if args_str.is_empty() {
            vec![]
        } else {
            args_str
                .split(',')
                .map(|a| {
                    a.trim()
                        .parse::<f64>()
                        .map_err(|_| anyhow::anyhow!("Invalid modifier argument: '{}'", a.trim()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        (name, args)
    } else {
        (s, vec![])
    };

    let lower = name.to_lowercase();
    for m in Modifier::iter() {
        let mname: &'static str = m.into();
        if mname == lower {
            return Ok(ModifierCall { modifier: m, args });
        }
    }

    // Build list of available modifiers for error message
    let available: Vec<&'static str> = Modifier::iter().map(|m| m.into()).collect();
    bail!(
        "Unknown modifier: '{}'. Available: {}",
        name,
        available.join(", ")
    )
}

/// Apply a path accessor to a value
pub fn apply_accessor(value: &FactValue, accessor: &PathAccessor, key: &str) -> Result<FactValue> {
    let path_str = match value {
        FactValue::Path(p) => p,
        FactValue::Text(t) => t,
        _ => bail!(
            "Path accessor can only be applied to path or text values, but '{}' is {}",
            key,
            value_type_name(value)
        ),
    };

    let segments: Vec<&str> = path_str.split('/').filter(|s| !s.is_empty()).collect();
    let len = segments.len() as i32;

    match accessor {
        PathAccessor::Index(idx) => {
            let actual_idx = normalize_index(*idx, len);
            if actual_idx < 0 || actual_idx >= len {
                bail!(
                    "Index {} out of bounds for path '{}' with {} segment{}",
                    idx,
                    path_str,
                    len,
                    if len == 1 { "" } else { "s" }
                );
            }
            Ok(FactValue::Text(segments[actual_idx as usize].to_string()))
        }
        PathAccessor::Slice { start, end } => {
            let start_idx = start.map(|s| normalize_index(s, len)).unwrap_or(0);
            let end_idx = end.map(|e| normalize_index(e, len)).unwrap_or(len);

            // Check bounds for explicit indices
            if let Some(s) = start {
                let normalized = normalize_index(*s, len);
                if normalized < 0 || normalized > len {
                    bail!(
                        "Slice start {} out of bounds for path '{}' with {} segment{}",
                        s,
                        path_str,
                        len,
                        if len == 1 { "" } else { "s" }
                    );
                }
            }
            if let Some(e) = end {
                let normalized = normalize_index(*e, len);
                if normalized < 0 || normalized > len {
                    bail!(
                        "Slice end {} out of bounds for path '{}' with {} segment{}",
                        e,
                        path_str,
                        len,
                        if len == 1 { "" } else { "s" }
                    );
                }
            }

            let start_idx = start_idx.max(0) as usize;
            let end_idx = end_idx.max(0) as usize;

            if start_idx >= end_idx || start_idx >= segments.len() {
                return Ok(FactValue::Path(String::new()));
            }

            let sliced: Vec<&str> = segments[start_idx..end_idx.min(segments.len())].to_vec();
            Ok(FactValue::Path(sliced.join("/")))
        }
    }
}

/// Normalize a Python-style index (negative wraps around)
fn normalize_index(idx: i32, len: i32) -> i32 {
    if idx < 0 {
        len + idx
    } else {
        idx
    }
}

/// What a modifier gave: a value, or no value and why.
///
/// No value is a result, never an error: a time known only to `2024` has no month, which is
/// a fact about the value and not a mistake in the expression. Each caller reads it as it
/// reads a missing fact.
#[derive(Debug, Clone)]
pub enum Modified {
    Value(FactValue),
    NoValue(NoValueCause),
}

/// Why a modifier gave no value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NoValueCause {
    /// The time was written coarser than the modifier reads: `|month` on a value known only
    /// to the year. A value is never padded to a precision nobody wrote.
    TooCoarse {
        modifier: Modifier,
        precision: Precision,
    },
}

impl std::fmt::Display for NoValueCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoValueCause::TooCoarse { precision, .. } => {
                write!(
                    f,
                    "the time is known only to the {}",
                    precision_word(*precision)
                )
            }
        }
    }
}

fn precision_word(precision: Precision) -> &'static str {
    match precision {
        Precision::Year => "year",
        Precision::Month => "month",
        Precision::Day => "day",
        Precision::Minute => "minute",
        Precision::Second => "second",
        Precision::Fraction => "fraction of a second",
    }
}

/// Apply a modifier to a value
///
/// The `for_display` parameter controls formatting context:
/// - `true`: Human-readable format (e.g., `<60`, `>600`)
/// - `false`: Path-safe format (e.g., `-Inf-60`, `600-Inf`)
///
/// A time modifier reads the value on the user's clock (`zone`), through the frame rule.
pub fn apply_modifier(
    value: &FactValue,
    call: &ModifierCall,
    key: &str,
    for_display: bool,
    zone: &UserZone,
) -> Result<Modified> {
    let text = |s: String| Ok(Modified::Value(FactValue::Text(s)));
    match call.modifier {
        // Time modifiers
        Modifier::Year
        | Modifier::Month
        | Modifier::Day
        | Modifier::Hour
        | Modifier::Minute
        | Modifier::Second
        | Modifier::Date
        | Modifier::Time
        | Modifier::DateTime
        | Modifier::YearMonth
        | Modifier::Week
        | Modifier::Weekday
        | Modifier::Quarter => {
            let tv = match value {
                FactValue::Time(tv) => *tv,
                // AUDIT: a number given to a time modifier is read as seconds since 1970, until
                // time modifiers on a number are refused by name.
                FactValue::Num(n) => TimeValue::from_epoch_secs(*n as i64)
                    .map_err(|_| anyhow::anyhow!("Invalid timestamp: {n}"))?,
                _ => {
                    let name: &'static str = call.modifier.into();
                    bail!(
                        "Time modifier '{}' requires a time-type fact, but '{}' is {}.",
                        name,
                        key,
                        value_type_name(value)
                    )
                }
            };
            Ok(apply_time_modifier(&tv, call.modifier, zone))
        }

        // String modifiers
        Modifier::Stem => {
            let s = fact_value_to_string(value, zone);
            let path = std::path::Path::new(&s);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(&s);
            text(stem.to_string())
        }
        Modifier::Ext => {
            let s = fact_value_to_string(value, zone);
            let path = std::path::Path::new(&s);
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            text(ext.to_string())
        }
        Modifier::Short => text(fact_value_to_string(value, zone).chars().take(8).collect()),
        Modifier::Lowercase => text(fact_value_to_string(value, zone).to_lowercase()),
        Modifier::Uppercase => text(fact_value_to_string(value, zone).to_uppercase()),
        Modifier::Capitalize => {
            let s = fact_value_to_string(value, zone);
            let mut chars = s.chars();
            text(match chars.next() {
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
                None => String::new(),
            })
        }

        // Numeric modifiers
        Modifier::Bucket => {
            let n = match value {
                FactValue::Num(n) => *n,
                _ => bail!(
                    "Bucket modifier requires numeric fact, '{}' is {}",
                    key,
                    value_type_name(value)
                ),
            };

            if call.args.is_empty() {
                text(format_magnitude_bucket(n))
            } else {
                text(format_threshold_bucket(n, &call.args, for_display))
            }
        }
    }
}

/// Apply an accessor, then a modifier chain, the way every reader of a key does. The chain
/// stops at the first no-value: nothing follows a value that is not there.
pub fn apply_chain(
    value: FactValue,
    accessor: Option<&PathAccessor>,
    modifiers: &[ModifierCall],
    key: &str,
    for_display: bool,
    zone: &UserZone,
) -> Result<Modified> {
    let mut value = match accessor {
        Some(acc) => apply_accessor(&value, acc, key)?,
        None => value,
    };
    for call in modifiers {
        match apply_modifier(&value, call, key, for_display, zone)? {
            Modified::Value(next) => value = next,
            none @ Modified::NoValue(_) => return Ok(none),
        }
    }
    Ok(Modified::Value(value))
}

/// The precision a time modifier reads: a value written coarser has no value for it.
/// `|datetime` reads any precision — it is the printed form.
fn precision_needed(modifier: Modifier) -> Precision {
    match modifier {
        Modifier::Year | Modifier::DateTime => Precision::Year,
        Modifier::YearMonth | Modifier::Month | Modifier::Quarter => Precision::Month,
        Modifier::Date | Modifier::Day | Modifier::Week | Modifier::Weekday => Precision::Day,
        Modifier::Hour | Modifier::Minute | Modifier::Time => Precision::Minute,
        Modifier::Second => Precision::Second,
        Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize
        | Modifier::Bucket => unreachable!("not a time modifier"),
    }
}

/// A time modifier, read off the value's span on the user's clock. No output carries an
/// offset.
fn apply_time_modifier(tv: &TimeValue, modifier: Modifier, zone: &UserZone) -> Modified {
    let span = tv.on_clock(zone);
    if span.precision() < precision_needed(modifier) {
        return Modified::NoValue(NoValueCause::TooCoarse {
            modifier,
            precision: span.precision(),
        });
    }
    let s = span.start();
    let result = match modifier {
        Modifier::Year => format!("{:04}", s.year()),
        Modifier::Month => format!("{:02}", s.month()),
        Modifier::Day => format!("{:02}", s.day()),
        Modifier::Hour => format!("{:02}", s.hour()),
        Modifier::Minute => format!("{:02}", s.minute()),
        Modifier::Second => format!("{:02}", s.second()),
        Modifier::Date => format!("{:04}-{:02}-{:02}", s.year(), s.month(), s.day()),
        // At the value's own grain: `14:30` for a minute, `14:30:05` finer.
        Modifier::Time => match span.precision() {
            Precision::Minute => format!("{:02}:{:02}", s.hour(), s.minute()),
            _ => format!("{:02}:{:02}:{:02}", s.hour(), s.minute(), s.second()),
        },
        Modifier::DateTime => tv.printed(zone),
        Modifier::YearMonth => format!("{:04}-{:02}", s.year(), s.month()),
        Modifier::Week => format!("{:02}", s.date().iso_week_date().week()), // ISO week number
        Modifier::Weekday => weekday_name(s.weekday()).to_string(),
        Modifier::Quarter => format!("Q{}", (s.month() - 1) / 3 + 1),
        Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize
        | Modifier::Bucket => unreachable!("not a time modifier"),
    };
    Modified::Value(FactValue::Text(result))
}

fn weekday_name(day: Weekday) -> &'static str {
    match day {
        Weekday::Monday => "Monday",
        Weekday::Tuesday => "Tuesday",
        Weekday::Wednesday => "Wednesday",
        Weekday::Thursday => "Thursday",
        Weekday::Friday => "Friday",
        Weekday::Saturday => "Saturday",
        Weekday::Sunday => "Sunday",
    }
}

/// Format bucket using magnitude (powers of 10)
fn format_magnitude_bucket(n: f64) -> String {
    if n == 0.0 {
        return "0".to_string();
    }

    let abs_n = n.abs();
    let sign = if n < 0.0 { "-" } else { "" };
    let log = abs_n.log10().floor() as i32;
    let lower = 10_f64.powi(log);
    let upper = 10_f64.powi(log + 1);

    format!(
        "{}{}-{}",
        sign,
        format_bucket_num(lower),
        format_bucket_num(upper)
    )
}

/// Format bucket using custom thresholds
/// Uses exact threshold values as specified by user (no SI suffix conversion)
fn format_threshold_bucket(n: f64, thresholds: &[f64], for_display: bool) -> String {
    for (i, &t) in thresholds.iter().enumerate() {
        if n < t {
            if i == 0 {
                // Below first threshold
                return if for_display {
                    format!("<{}", format_threshold_num(t))
                } else {
                    format!("-Inf-{}", format_threshold_num(t))
                };
            } else {
                return format!(
                    "{}-{}",
                    format_threshold_num(thresholds[i - 1]),
                    format_threshold_num(t)
                );
            }
        }
    }
    // Value >= last threshold
    let last = format_threshold_num(*thresholds.last().unwrap());
    if for_display {
        format!(">{last}")
    } else {
        format!("{last}-Inf")
    }
}

/// Format a user-specified threshold value (preserves exact value, no SI suffixes)
fn format_threshold_num(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Format number with SI suffixes for bucket labels
fn format_bucket_num(v: f64) -> String {
    if v >= 1_000_000_000.0 {
        format!("{}G", (v / 1_000_000_000.0) as i64)
    } else if v >= 1_000_000.0 {
        format!("{}M", (v / 1_000_000.0) as i64)
    } else if v >= 1_000.0 {
        format!("{}K", (v / 1_000.0) as i64)
    } else if v >= 1.0 {
        format!("{}", v as i64)
    } else if v == 0.0 {
        "0".to_string()
    } else {
        // For sub-1 values, trim trailing zeros
        format!("{v:.3}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

/// Convert a FactValue to string: a time is its printed form on the user's clock.
pub(super) fn fact_value_to_string(value: &FactValue, zone: &UserZone) -> String {
    match value {
        FactValue::Text(s) => s.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{n}")
            }
        }
        FactValue::Time(tv) => tv.printed(zone),
    }
}

/// Get a human-readable type name for a FactValue
fn value_type_name(value: &FactValue) -> &'static str {
    match value {
        FactValue::Text(_) => "text",
        FactValue::Path(_) => "path",
        FactValue::Num(_) => "number",
        FactValue::Time(_) => "time",
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::domain::time::reading::read_time;

    fn amsterdam() -> UserZone {
        UserZone::for_test("Europe/Amsterdam")
    }

    fn call(modifier: Modifier) -> ModifierCall {
        ModifierCall {
            modifier,
            args: vec![],
        }
    }

    /// The modifier's output as text, or `None` for no value.
    fn modified(value: &FactValue, modifier: Modifier, zone: &UserZone) -> Option<String> {
        match apply_modifier(value, &call(modifier), "k", true, zone).unwrap() {
            Modified::Value(FactValue::Text(t)) => Some(t),
            Modified::Value(other) => panic!("not text: {other:?}"),
            Modified::NoValue(_) => None,
        }
    }

    const TIME_MODIFIERS: [Modifier; 13] = [
        Modifier::Year,
        Modifier::Month,
        Modifier::Day,
        Modifier::Hour,
        Modifier::Minute,
        Modifier::Second,
        Modifier::Date,
        Modifier::Time,
        Modifier::DateTime,
        Modifier::YearMonth,
        Modifier::Week,
        Modifier::Weekday,
        Modifier::Quarter,
    ];

    /// A save time at 23:30 UTC on New Year's Eve is 00:30 on 1 January in Amsterdam: every
    /// time modifier reads the user's clock, and none carries an offset.
    #[test]
    fn every_time_modifier_reads_a_save_time_on_the_users_clock() {
        // 2023-12-31T23:30:00Z
        let saved = FactValue::Time(TimeValue::from_epoch_secs(1704065400).unwrap());
        let expected = [
            "2024",
            "01",
            "01",
            "00",
            "30",
            "00",
            "2024-01-01",
            "00:30:00",
            "2024-01-01 00:30:00",
            "2024-01",
            "01",
            "Monday",
            "Q1",
        ];
        for (modifier, want) in TIME_MODIFIERS.iter().zip(expected) {
            assert_eq!(
                modified(&saved, *modifier, &amsterdam()).as_deref(),
                Some(want),
                "{modifier:?}"
            );
        }
    }

    /// A clock reading reads as written, whatever the user's zone.
    #[test]
    fn every_time_modifier_reads_a_clock_reading_as_written() {
        let reading = FactValue::Time(read_time("2023:12:31 23:30:00").unwrap());
        let expected = [
            "2023",
            "12",
            "31",
            "23",
            "30",
            "00",
            "2023-12-31",
            "23:30:00",
            "2023-12-31 23:30:00",
            "2023-12",
            "52",
            "Sunday",
            "Q4",
        ];
        for zone in [amsterdam(), UserZone::for_test("America/New_York")] {
            for (modifier, want) in TIME_MODIFIERS.iter().zip(expected) {
                assert_eq!(
                    modified(&reading, *modifier, &zone).as_deref(),
                    Some(want),
                    "{modifier:?}"
                );
            }
        }
    }

    /// Each modifier reads a grain; a value written coarser has no value for it — never a
    /// value padded to a precision nobody wrote. `|datetime` reads every precision.
    #[test]
    fn a_modifier_finer_than_the_value_was_written_gives_no_value() {
        let needs = |m: Modifier| match m {
            Modifier::Year | Modifier::DateTime => Precision::Year,
            Modifier::YearMonth | Modifier::Month | Modifier::Quarter => Precision::Month,
            Modifier::Date | Modifier::Day | Modifier::Week | Modifier::Weekday => Precision::Day,
            Modifier::Hour | Modifier::Minute | Modifier::Time => Precision::Minute,
            Modifier::Second => Precision::Second,
            _ => unreachable!(),
        };
        for written in [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "2024-06-15T14:30:05.45",
        ] {
            let tv = read_time(written).unwrap();
            let value = FactValue::Time(tv);
            for modifier in TIME_MODIFIERS {
                let got = apply_modifier(&value, &call(modifier), "k", true, &amsterdam()).unwrap();
                if tv.precision() >= needs(modifier) {
                    assert!(matches!(got, Modified::Value(_)), "{written} {modifier:?}");
                } else {
                    match got {
                        Modified::NoValue(NoValueCause::TooCoarse {
                            modifier: m,
                            precision,
                        }) => {
                            assert_eq!(m, modifier);
                            assert_eq!(precision, tv.precision());
                        }
                        Modified::Value(v) => panic!("{written} {modifier:?} gave {v:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn datetime_is_the_printed_form_at_every_precision() {
        for written in [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "2024-06-15T14:30:05.45+02:00",
        ] {
            let tv = read_time(written).unwrap();
            assert_eq!(
                modified(&FactValue::Time(tv), Modifier::DateTime, &amsterdam()),
                Some(tv.printed(&amsterdam())),
                "{written}"
            );
        }
    }

    #[test]
    fn time_prints_at_the_values_own_grain() {
        let minute = FactValue::Time(read_time("2024-06-15T14:30").unwrap());
        assert_eq!(
            modified(&minute, Modifier::Time, &amsterdam()).as_deref(),
            Some("14:30")
        );
        let fraction = FactValue::Time(read_time("2024-06-15T14:30:05.45").unwrap());
        assert_eq!(
            modified(&fraction, Modifier::Time, &amsterdam()).as_deref(),
            Some("14:30:05")
        );
    }

    #[test]
    fn a_time_modifier_on_text_is_refused() {
        let err = apply_modifier(
            &FactValue::Text("x".into()),
            &call(Modifier::Year),
            "content.Make",
            true,
            &amsterdam(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Time modifier 'year' requires a time-type fact, but 'content.Make' is text."
        );
    }

    #[test]
    fn test_bucket_magnitude() {
        // Test format_magnitude_bucket directly
        assert_eq!(format_magnitude_bucket(0.0), "0");
        assert_eq!(format_magnitude_bucket(5.0), "1-10");
        assert_eq!(format_magnitude_bucket(50.0), "10-100");
        assert_eq!(format_magnitude_bucket(500.0), "100-1K");
        assert_eq!(format_magnitude_bucket(5000.0), "1K-10K");
        assert_eq!(format_magnitude_bucket(50000.0), "10K-100K");
        assert_eq!(format_magnitude_bucket(5000000.0), "1M-10M");
        assert_eq!(format_magnitude_bucket(5000000000.0), "1G-10G");
        // Negative numbers
        assert_eq!(format_magnitude_bucket(-50.0), "-10-100");
    }

    #[test]
    fn test_bucket_threshold_display() {
        // Test format_threshold_bucket with for_display=true
        // Uses exact values as specified (no SI suffix conversion)
        let thresholds = vec![60.0, 3600.0, 7200.0];
        assert_eq!(format_threshold_bucket(30.0, &thresholds, true), "<60");
        assert_eq!(format_threshold_bucket(100.0, &thresholds, true), "60-3600");
        assert_eq!(
            format_threshold_bucket(5000.0, &thresholds, true),
            "3600-7200"
        );
        assert_eq!(format_threshold_bucket(10000.0, &thresholds, true), ">7200");
    }

    #[test]
    fn test_bucket_threshold_path() {
        // Test format_threshold_bucket with for_display=false (path-safe)
        let thresholds = vec![60.0, 3600.0, 7200.0];
        assert_eq!(format_threshold_bucket(30.0, &thresholds, false), "-Inf-60");
        assert_eq!(
            format_threshold_bucket(100.0, &thresholds, false),
            "60-3600"
        );
        assert_eq!(
            format_threshold_bucket(5000.0, &thresholds, false),
            "3600-7200"
        );
        assert_eq!(
            format_threshold_bucket(10000.0, &thresholds, false),
            "7200-Inf"
        );
    }

    #[test]
    fn test_parse_bucket_with_args() {
        // Test parsing bucket modifier with arguments
        let call = parse_modifier("bucket(60,300,600)").unwrap();
        assert_eq!(call.modifier, Modifier::Bucket);
        assert_eq!(call.args, vec![60.0, 300.0, 600.0]);

        // Without args
        let call = parse_modifier("bucket").unwrap();
        assert_eq!(call.modifier, Modifier::Bucket);
        assert!(call.args.is_empty());
    }

    #[test]
    fn test_bucket_num_format() {
        // Test format_bucket_num with SI suffixes
        assert_eq!(format_bucket_num(0.0), "0");
        assert_eq!(format_bucket_num(1.0), "1");
        assert_eq!(format_bucket_num(100.0), "100");
        assert_eq!(format_bucket_num(1000.0), "1K");
        assert_eq!(format_bucket_num(10000.0), "10K");
        assert_eq!(format_bucket_num(1000000.0), "1M");
        assert_eq!(format_bucket_num(1000000000.0), "1G");
        assert_eq!(format_bucket_num(0.5), "0.5");
        assert_eq!(format_bucket_num(0.123), "0.123");
    }
}
