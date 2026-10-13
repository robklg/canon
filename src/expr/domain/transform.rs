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
use crate::core::domain::number::{read_number, spelled};
use crate::core::domain::time::reading::read_time;
use crate::core::domain::time::value::{Precision, TimeForm, TimeValue, REFERENCE_DAY};
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

/// Modifier category for grouping, in the order lists of modifiers name them.
#[derive(Debug, Clone, Copy, PartialEq, strum::EnumIter)]
pub enum ModifierCategory {
    Time,
    Numeric,
    String,
}

/// Modifiers that transform values
#[derive(Debug, Clone, Copy, PartialEq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "lowercase")]
pub enum Modifier {
    // Time modifiers (for times, and text read as one)
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
    Number, // text read as a number; a number is itself
}

impl Modifier {
    /// **Whether the modifier reads a time of day** (`|time`'s result): its hour, minute and
    /// second, or itself. A time of day is read on a reference day, so every other time
    /// modifier would read a date nobody wrote. The one rule the binding table and the chain
    /// both read.
    pub(super) const fn reads_a_time_of_day(&self) -> bool {
        matches!(
            self,
            Modifier::Hour | Modifier::Minute | Modifier::Second | Modifier::Time
        )
    }

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
            Modifier::Bucket | Modifier::Number => ModifierCategory::Numeric,
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
                    read_number(a.trim())
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

/// Apply a path accessor to a value: a path, or text read as one. An accessor that finds no
/// segment — an index or a slice bound past the end — gives no value.
fn apply_accessor(value: &FactValue, accessor: &PathAccessor, key: &str) -> Result<Modified> {
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
    let no_segment = Ok(Modified::NoValue(NoValueCause::NoSegment));
    let found = |value: FactValue| Ok(Modified::Value(value));

    match accessor {
        PathAccessor::Index(idx) => {
            let actual_idx = normalize_index(*idx, len);
            if actual_idx < 0 || actual_idx >= len {
                return no_segment;
            }
            found(FactValue::Text(segments[actual_idx as usize].to_string()))
        }
        PathAccessor::Slice { start, end } => {
            let out_of_bounds = |bound: &Option<i32>| {
                bound.is_some_and(|b| {
                    let normalized = normalize_index(b, len);
                    normalized < 0 || normalized > len
                })
            };
            if out_of_bounds(start) || out_of_bounds(end) {
                return no_segment;
            }
            let start_idx = start.map(|s| normalize_index(s, len)).unwrap_or(0);
            let end_idx = end.map(|e| normalize_index(e, len)).unwrap_or(len);
            let start_idx = start_idx.max(0) as usize;
            let end_idx = end_idx.max(0) as usize;

            if start_idx >= end_idx || start_idx >= segments.len() {
                return found(FactValue::Path(String::new()));
            }

            let sliced: Vec<&str> = segments[start_idx..end_idx.min(segments.len())].to_vec();
            found(FactValue::Path(sliced.join("/")))
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
    /// Text given to a time modifier that is not a time (`|year` on `misc`).
    NotATime { modifier: Modifier },
    /// Text given to `|number` that is not a number.
    NotANumber,
    /// An accessor found no segment: an index or a slice bound past the end.
    NoSegment,
}

impl std::fmt::Display for NoValueCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoValueCause::TooCoarse { precision, .. } => {
                write!(f, "the time is known only to the {}", precision.word())
            }
            NoValueCause::NotATime { .. } => f.write_str("the text is not a time"),
            NoValueCause::NotANumber => f.write_str("the text is not a number"),
            NoValueCause::NoSegment => f.write_str("the path has no such segment"),
        }
    }
}

/// Apply one modifier to a value. `produced_by` is the modifier that gave the value, if one
/// did: a text modifier after another modifier reads that modifier's printed form
/// ([`rendered`]), and one given a stored number or time directly refuses it — the user
/// says which printed form they mean (`|number`, `|datetime`).
///
/// The `for_display` parameter controls formatting context:
/// - `true`: Human-readable format (e.g., `<60`, `>600`)
/// - `false`: Path-safe format (e.g., `-Inf-60`, `600-Inf`)
///
/// A time modifier reads a time on the user's clock (`zone`), through the frame rule, and
/// text through the one time reading; text that is not a time has no value.
fn apply_modifier(
    value: &FactValue,
    produced_by: Option<Modifier>,
    call: &ModifierCall,
    key: &str,
    for_display: bool,
    zone: &UserZone,
) -> Result<Modified> {
    let name: &'static str = call.modifier.into();
    let text = |s: String| Ok(Modified::Value(FactValue::Text(s)));
    // The text a string modifier works on.
    let text_in = || -> Result<String> {
        match (produced_by, value) {
            (Some(producer), _) => Ok(rendered(Some(producer), value, zone)),
            (None, FactValue::Text(s) | FactValue::Path(s)) => Ok(s.clone()),
            (None, FactValue::Num(_) | FactValue::Time(_)) => bail!(
                "String modifier '{}' requires a text or path value, but '{}' is {}.",
                name,
                key,
                value_type_name(value)
            ),
        }
    };
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
            // A time of day has no date to read.
            if produced_by == Some(Modifier::Time) && !call.modifier.reads_a_time_of_day() {
                bail!("Time modifier '{name}' needs a date, and a time of day (|time) has none.");
            }
            let tv = match value {
                FactValue::Time(tv) => *tv,
                FactValue::Text(t) => match read_time(t) {
                    Ok(tv) => tv,
                    Err(_) => {
                        return Ok(Modified::NoValue(NoValueCause::NotATime {
                            modifier: call.modifier,
                        }))
                    }
                },
                FactValue::Num(_) | FactValue::Path(_) => bail!(
                    "Time modifier '{}' requires a time or text value, but '{}' is {}.",
                    name,
                    key,
                    value_type_name(value)
                ),
            };
            apply_time_modifier(&tv, call.modifier, zone)
        }

        // String modifiers
        Modifier::Stem => {
            let s = text_in()?;
            let path = std::path::Path::new(&s);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(&s);
            text(stem.to_string())
        }
        Modifier::Ext => {
            let s = text_in()?;
            let path = std::path::Path::new(&s);
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            text(ext.to_string())
        }
        Modifier::Short => text(text_in()?.chars().take(8).collect()),
        Modifier::Lowercase => text(text_in()?.to_lowercase()),
        Modifier::Uppercase => text(text_in()?.to_uppercase()),
        Modifier::Capitalize => {
            let s = text_in()?;
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
        Modifier::Number => match value {
            FactValue::Num(n) => Ok(Modified::Value(FactValue::Num(*n))),
            FactValue::Text(t) => Ok(match read_number(t) {
                Ok(n) => Modified::Value(FactValue::Num(n)),
                Err(_) => Modified::NoValue(NoValueCause::NotANumber),
            }),
            FactValue::Time(_) | FactValue::Path(_) => bail!(
                "Modifier 'number' requires a text or number value, but '{}' is {}.",
                key,
                value_type_name(value)
            ),
        },
    }
}

/// Apply an accessor, then a modifier chain, the way every reader of a key does. The chain
/// stops at the first no-value: nothing follows a value that is not there. It carries the
/// modifier that gave the current value, so a text modifier reads that modifier's printed
/// form; a reader printing the chain's result prints it through [`rendered`] with the
/// chain's last modifier.
pub fn apply_chain(
    value: FactValue,
    accessor: Option<&PathAccessor>,
    modifiers: &[ModifierCall],
    key: &str,
    for_display: bool,
    zone: &UserZone,
) -> Result<Modified> {
    let mut value = match accessor {
        Some(acc) => match apply_accessor(&value, acc, key)? {
            Modified::Value(found) => found,
            none @ Modified::NoValue(_) => return Ok(none),
        },
        None => value,
    };
    let mut produced_by = None;
    for call in modifiers {
        match apply_modifier(&value, produced_by, call, key, for_display, zone)? {
            Modified::Value(next) => {
                value = next;
                produced_by = Some(call.modifier);
            }
            none @ Modified::NoValue(_) => return Ok(none),
        }
    }
    Ok(Modified::Value(value))
}

/// **The one printed form of a value**, given the modifier that produced it (`last`; `None`
/// for a value no modifier gave). A number part prints as Canon has always placed it —
/// `|year` `2024`, `|month`/`|day`/`|hour`/`|minute`/`|second`/`|week` two digits,
/// `|weekday` `Monday`, `|quarter` `Q3`; `|time` the time of day at the value's grain, to
/// the second; every other value its own spelling: a number through the one number spelling,
/// a time through the frame rule's printed form (`|date` `2024-07-23`, `|yearmonth`
/// `2024-07`, `|datetime` `2024-07-23 14:30:45`), text and paths as they are. Patterns place
/// this, `facts` reports it, and a text modifier after a modifier reads it.
pub fn rendered(last: Option<Modifier>, value: &FactValue, zone: &UserZone) -> String {
    let part = |spell: &dyn Fn(i64) -> String| match value {
        FactValue::Num(n) => spell(*n as i64),
        other => printed_plain(other, zone),
    };
    let Some(modifier) = last else {
        return printed_plain(value, zone);
    };
    match modifier {
        Modifier::Year => part(&|n| format!("{n:04}")),
        Modifier::Month
        | Modifier::Day
        | Modifier::Hour
        | Modifier::Minute
        | Modifier::Second
        | Modifier::Week => part(&|n| format!("{n:02}")),
        Modifier::Weekday => part(
            &|n| match i8::try_from(n).map(Weekday::from_monday_one_offset) {
                Ok(Ok(day)) => weekday_name(day).to_string(),
                _ => spelled(n as f64),
            },
        ),
        Modifier::Quarter => part(&|n| format!("Q{n}")),
        Modifier::Time => match value {
            FactValue::Time(tv) => tv
                .on_clock(zone)
                .printed_time()
                .unwrap_or_else(|| tv.printed(zone)),
            other => printed_plain(other, zone),
        },
        Modifier::Date
        | Modifier::YearMonth
        | Modifier::DateTime
        | Modifier::Number
        | Modifier::Bucket
        | Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize => printed_plain(value, zone),
    }
}

/// A value's own spelling: text and paths as they are, a number through the one number
/// spelling, a time through the frame rule's printed form on the user's clock.
fn printed_plain(value: &FactValue, zone: &UserZone) -> String {
    match value {
        FactValue::Text(s) => s.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => spelled(*n),
        FactValue::Time(tv) => tv.printed(zone),
    }
}

/// **Whether evaluating `value` through an accessor and `modifiers` reads an instant on the
/// clock** — the one answer to "did this run's clock matter?", read by the archive's
/// statements about the zone a manifest's run used.
///
/// An instant is a time whose form is not a clock reading, every save time among them; a
/// clock reading reads as written on any clock, so it never counts. A stored instant reads
/// the clock compared or printed bare, and through a time modifier; text reads one when a
/// time modifier reads it as an instant. The first transform is the only one that sees the
/// stored value — a time modifier's result is already a clock reading, and whatever follows
/// reads that. An accessor refuses a time, and `|bucket`, `|number` and the text modifiers
/// refuse one; none of them reads a clock. A number never reads one.
pub(crate) fn reads_an_instant(
    value: &FactValue,
    accessor: Option<&PathAccessor>,
    modifiers: &[ModifierCall],
) -> bool {
    if accessor.is_some() {
        return false;
    }
    let first = modifiers.first().map(|call| call.modifier);
    let time_modifier = first.is_some_and(|m| m.category() == ModifierCategory::Time);
    match value {
        FactValue::Time(tv) => tv.form() != TimeForm::Clock && (first.is_none() || time_modifier),
        FactValue::Text(t) => {
            time_modifier && read_time(t).is_ok_and(|tv| tv.form() != TimeForm::Clock)
        }
        FactValue::Num(_) | FactValue::Path(_) => false,
    }
}

/// The record a run keeps of [`reads_an_instant`]: set by every evaluation site through the
/// rule, read once the run is done. Held by the run's own context (`FactCache` for filters,
/// `EvalContext` for patterns), so no evaluation signature carries it.
#[derive(Debug, Default)]
pub(crate) struct InstantRecord(std::cell::Cell<bool>);

impl InstantRecord {
    /// Note one evaluation, through the rule. Once the run has read an instant, nothing
    /// further changes the answer, so the rule is not asked again.
    pub(crate) fn note(
        &self,
        value: &FactValue,
        accessor: Option<&PathAccessor>,
        modifiers: &[ModifierCall],
    ) {
        if !self.0.get() && reads_an_instant(value, accessor, modifiers) {
            self.0.set(true);
        }
    }

    /// Whether any evaluation noted so far read an instant on the clock.
    pub(crate) fn read(&self) -> bool {
        self.0.get()
    }
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
        | Modifier::Bucket
        | Modifier::Number => unreachable!("not a time modifier"),
    }
}

/// A time modifier, read off the value's span on the user's clock. A number part is a whole
/// number (`|weekday` 1–7 from Monday, `|quarter` 1–4, `|week` the ISO week); every other
/// result is a clock reading of what was read — `|date` at day grain, `|yearmonth` at month
/// grain, `|datetime` at the value's own, `|time` on the reference day at the value's own —
/// so no result has an offset or reads a clock again.
fn apply_time_modifier(tv: &TimeValue, modifier: Modifier, zone: &UserZone) -> Result<Modified> {
    let span = tv.on_clock(zone);
    if span.precision() < precision_needed(modifier) {
        return Ok(Modified::NoValue(NoValueCause::TooCoarse {
            modifier,
            precision: span.precision(),
        }));
    }
    let s = span.start();
    let part = |n: i64| Ok(Modified::Value(FactValue::Num(n as f64)));
    let clock = |at: jiff::civil::DateTime, precision: Precision| -> Result<Modified> {
        Ok(Modified::Value(FactValue::Time(TimeValue::clock(
            at, precision,
        )?)))
    };
    match modifier {
        Modifier::Year => part(s.year().into()),
        Modifier::Month => part(s.month().into()),
        Modifier::Day => part(s.day().into()),
        Modifier::Hour => part(s.hour().into()),
        Modifier::Minute => part(s.minute().into()),
        Modifier::Second => part(s.second().into()),
        Modifier::Week => part(s.date().iso_week_date().week().into()), // ISO week number
        Modifier::Weekday => part(s.weekday().to_monday_one_offset().into()),
        Modifier::Quarter => part(((s.month() - 1) / 3 + 1).into()),
        Modifier::Date => clock(s.date().at(0, 0, 0, 0), Precision::Day),
        Modifier::YearMonth => clock(s.date().first_of_month().at(0, 0, 0, 0), Precision::Month),
        Modifier::DateTime => clock(s, span.precision()),
        Modifier::Time => clock(REFERENCE_DAY.to_datetime(s.time()), span.precision()),
        Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize
        | Modifier::Bucket
        | Modifier::Number => unreachable!("not a time modifier"),
    }
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

/// Every label `|bucket(a,b,…)` gives as `facts` and filters print it — read off the bucket
/// function itself, one number below the first threshold and one at each threshold, so the
/// list cannot drift from what a value is put in.
pub(super) fn threshold_bucket_labels(thresholds: &[f64]) -> Vec<String> {
    let mut labels: Vec<String> = std::iter::once(f64::NEG_INFINITY)
        .chain(thresholds.iter().copied())
        .map(|n| format_threshold_bucket(n, thresholds, true))
        .collect();
    labels.dedup();
    labels
}

/// Whether `label` is one plain `|bucket` gives: its lower edge read back through the bucket
/// function gives the same label.
pub(super) fn is_magnitude_bucket_label(label: &str) -> bool {
    if label == "0" {
        return true;
    }
    let (sign, unsigned) = match label.strip_prefix('-') {
        Some(rest) => (-1.0, rest),
        None => (1.0, label),
    };
    let Some((lower, _)) = unsigned.split_once('-') else {
        return false;
    };
    let (digits, scale) = match lower.char_indices().last() {
        Some((at, 'K')) => (lower.get(..at), 1e3),
        Some((at, 'M')) => (lower.get(..at), 1e6),
        Some((at, 'G')) => (lower.get(..at), 1e9),
        _ => (Some(lower), 1.0),
    };
    digits
        .and_then(|d| read_number(d).ok())
        .is_some_and(|n| n > 0.0 && format_magnitude_bucket(sign * n * scale) == label)
}

/// Format a user-specified threshold value: the one number spelling, no SI suffixes.
fn format_threshold_num(v: f64) -> String {
    spelled(v)
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

    /// The modifier's output in its printed form, or `None` for no value.
    fn modified(value: &FactValue, modifier: Modifier, zone: &UserZone) -> Option<String> {
        match apply_modifier(value, None, &call(modifier), "k", true, zone).unwrap() {
            Modified::Value(v) => Some(rendered(Some(modifier), &v, zone)),
            Modified::NoValue(_) => None,
        }
    }

    /// A chain's output in its printed form, or `None` for no value.
    fn chained(
        value: &FactValue,
        modifiers: &[Modifier],
        zone: &UserZone,
    ) -> Result<Option<String>> {
        let calls: Vec<ModifierCall> = modifiers.iter().map(|m| call(*m)).collect();
        Ok(
            match apply_chain(value.clone(), None, &calls, "k", true, zone)? {
                Modified::Value(v) => Some(rendered(modifiers.last().copied(), &v, zone)),
                Modified::NoValue(_) => None,
            },
        )
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
    /// time modifier reads the user's clock, and none has an offset.
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
                let got =
                    apply_modifier(&value, None, &call(modifier), "k", true, &amsterdam()).unwrap();
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
                        Modified::NoValue(other) => panic!("{written} {modifier:?}: {other}"),
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

    /// Every time modifier gives a typed value — a number part a whole number, the others a
    /// clock reading — and each has one printed form, the one Canon has always placed.
    #[test]
    fn every_modifier_gives_a_typed_value_with_one_printed_form() {
        let zone = amsterdam();
        let reading = FactValue::Time(read_time("2024-07-23 14:30:45").unwrap());
        let clock = |text: &str| FactValue::Time(read_time(text).unwrap());
        let cases = [
            (Modifier::Year, FactValue::Num(2024.0), "2024"),
            (Modifier::Month, FactValue::Num(7.0), "07"),
            (Modifier::Day, FactValue::Num(23.0), "23"),
            (Modifier::Hour, FactValue::Num(14.0), "14"),
            (Modifier::Minute, FactValue::Num(30.0), "30"),
            (Modifier::Second, FactValue::Num(45.0), "45"),
            (Modifier::Week, FactValue::Num(30.0), "30"),
            (Modifier::Weekday, FactValue::Num(2.0), "Tuesday"),
            (Modifier::Quarter, FactValue::Num(3.0), "Q3"),
            (Modifier::Date, clock("2024-07-23"), "2024-07-23"),
            (Modifier::YearMonth, clock("2024-07"), "2024-07"),
            (
                Modifier::DateTime,
                clock("2024-07-23 14:30:45"),
                "2024-07-23 14:30:45",
            ),
            (Modifier::Time, clock("2000-01-01 14:30:45"), "14:30:45"),
        ];
        for (modifier, typed, printed) in cases {
            match apply_modifier(&reading, None, &call(modifier), "k", true, &zone).unwrap() {
                Modified::Value(v) => {
                    assert_eq!(format!("{v:?}"), format!("{typed:?}"), "{modifier:?}");
                    assert_eq!(rendered(Some(modifier), &v, &zone), printed, "{modifier:?}");
                }
                Modified::NoValue(cause) => panic!("{modifier:?}: {cause}"),
            }
        }
        // A part below ten prints with its leading zero, a weekday as its word.
        let early = FactValue::Time(read_time("2024-01-05 04:03:02").unwrap());
        for (modifier, printed) in [
            (Modifier::Month, "01"),
            (Modifier::Day, "05"),
            (Modifier::Hour, "04"),
            (Modifier::Minute, "03"),
            (Modifier::Second, "02"),
            (Modifier::Week, "01"),
            (Modifier::Weekday, "Friday"),
            (Modifier::Quarter, "Q1"),
        ] {
            assert_eq!(modified(&early, modifier, &zone).as_deref(), Some(printed));
        }
        // `|weekday` counts from Monday 1 to Sunday 7.
        for (day, n) in [("2024-07-22", 1.0), ("2024-07-28", 7.0)] {
            let v = apply_modifier(
                &clock(day),
                None,
                &call(Modifier::Weekday),
                "k",
                true,
                &zone,
            );
            assert!(matches!(v.unwrap(), Modified::Value(FactValue::Num(got)) if got == n));
        }
    }

    /// The modifiers that read a time off a save time read it on the user's clock, and what
    /// they give is a clock reading: it reads as written on any clock afterwards.
    #[test]
    fn a_time_modifiers_result_is_a_clock_reading_of_the_users_clock() {
        // 2023-12-31T23:30:00Z: 00:30 on New Year's Day in Amsterdam.
        let saved = FactValue::Time(TimeValue::from_epoch_secs(1704065400).unwrap());
        for (modifier, printed) in [
            (Modifier::Date, "2024-01-01"),
            (Modifier::YearMonth, "2024-01"),
            (Modifier::DateTime, "2024-01-01 00:30:00"),
            (Modifier::Time, "00:30:00"),
        ] {
            let Modified::Value(FactValue::Time(tv)) =
                apply_modifier(&saved, None, &call(modifier), "k", true, &amsterdam()).unwrap()
            else {
                panic!("{modifier:?} gave no time");
            };
            assert_eq!(tv.form(), TimeForm::Clock, "{modifier:?}");
            for zone in [amsterdam(), UserZone::for_test("America/New_York")] {
                assert_eq!(
                    rendered(Some(modifier), &FactValue::Time(tv), &zone),
                    printed
                );
            }
        }
    }

    /// `|number` reads text through the one number reading — no value where it does not
    /// read — and gives a number as itself.
    #[test]
    fn number_reads_text_as_a_number_and_a_number_as_itself() {
        let zone = amsterdam();
        let number = |value: FactValue| {
            apply_modifier(&value, None, &call(Modifier::Number), "k", true, &zone)
        };
        for (text, n) in [
            ("07", 7.0),
            ("0500", 500.0),
            ("-2.5", -2.5),
            ("1e3", 1000.0),
        ] {
            assert!(
                matches!(number(FactValue::Text(text.into())).unwrap(), Modified::Value(FactValue::Num(got)) if got == n),
                "{text}"
            );
        }
        for text in ["misc", "", " 5", "+5", "inf"] {
            assert!(
                matches!(
                    number(FactValue::Text(text.into())).unwrap(),
                    Modified::NoValue(NoValueCause::NotANumber)
                ),
                "{text:?}"
            );
        }
        assert!(matches!(
            number(FactValue::Num(5.5)).unwrap(),
            Modified::Value(FactValue::Num(n)) if n == 5.5
        ));
        assert!(number(FactValue::Time(read_time("2024").unwrap())).is_err());
        assert!(number(FactValue::Path("a/b".into())).is_err());
        assert_eq!(
            rendered(Some(Modifier::Number), &FactValue::Num(500.0), &zone),
            "500"
        );
    }

    /// A time modifier reads text through the one time reading — EXIF's spelling among
    /// them — and text it cannot read as a time has no value.
    #[test]
    fn a_time_modifier_reads_text_as_a_time() {
        let zone = amsterdam();
        let exif = FactValue::Text("2024:06:15 14:30:05".into());
        assert_eq!(
            modified(&exif, Modifier::Year, &zone).as_deref(),
            Some("2024")
        );
        assert_eq!(
            modified(&exif, Modifier::Month, &zone).as_deref(),
            Some("06")
        );
        assert_eq!(
            modified(&exif, Modifier::DateTime, &zone).as_deref(),
            Some("2024-06-15 14:30:05")
        );
        // Read as a time first, so a coarse text is too coarse like any coarse time.
        let year = FactValue::Text("2024".into());
        assert!(matches!(
            apply_modifier(&year, None, &call(Modifier::Month), "k", true, &zone).unwrap(),
            Modified::NoValue(NoValueCause::TooCoarse { .. })
        ));
        for text in ["misc", "", "Monday"] {
            assert!(
                matches!(
                    apply_modifier(
                        &FactValue::Text(text.into()),
                        None,
                        &call(Modifier::Year),
                        "k",
                        true,
                        &zone
                    )
                    .unwrap(),
                    Modified::NoValue(NoValueCause::NotATime {
                        modifier: Modifier::Year
                    })
                ),
                "{text:?}"
            );
        }
    }

    /// A number never means seconds since 1970: a time modifier refuses it, and a path.
    #[test]
    fn a_time_modifier_on_a_number_or_a_path_is_refused() {
        for value in [
            FactValue::Num(1_718_452_800.0),
            FactValue::Path("a/b".into()),
        ] {
            let err = apply_modifier(
                &value,
                None,
                &call(Modifier::Year),
                "content.X",
                true,
                &amsterdam(),
            )
            .unwrap_err();
            assert!(
                err.to_string().starts_with(
                    "Time modifier 'year' requires a time or text value, but 'content.X' is"
                ),
                "{err}"
            );
        }
    }

    /// A time of day carries no date: after `|time` only the hour, minute and second are
    /// read, and a date-reading modifier is refused rather than reading the reference day.
    #[test]
    fn a_time_of_day_gives_no_date() {
        let saved = FactValue::Time(TimeValue::from_epoch_secs(1_718_409_600 + 52_245).unwrap());
        let utc = UserZone::utc();
        assert_eq!(
            chained(&saved, &[Modifier::Time, Modifier::Hour], &utc).unwrap(),
            Some("14".into())
        );
        for date_part in [
            Modifier::Year,
            Modifier::Weekday,
            Modifier::Date,
            Modifier::DateTime,
        ] {
            assert!(
                chained(&saved, &[Modifier::Time, date_part], &utc).is_err(),
                "{date_part:?}"
            );
        }
    }

    /// An accessor that finds no segment gives no value, never an error.
    #[test]
    fn an_accessor_that_finds_no_segment_gives_no_value() {
        let path = FactValue::Text("a/b/c".into());
        let misses = [
            PathAccessor::Index(3),
            PathAccessor::Index(-4),
            PathAccessor::Slice {
                start: Some(5),
                end: None,
            },
            PathAccessor::Slice {
                start: None,
                end: Some(-5),
            },
        ];
        for accessor in misses {
            assert!(
                matches!(
                    apply_chain(path.clone(), Some(&accessor), &[], "k", true, &amsterdam())
                        .unwrap(),
                    Modified::NoValue(NoValueCause::NoSegment)
                ),
                "{accessor:?}"
            );
        }
        let last = apply_chain(
            path,
            Some(&PathAccessor::Index(-1)),
            &[],
            "k",
            true,
            &amsterdam(),
        );
        assert!(matches!(last.unwrap(), Modified::Value(FactValue::Text(t)) if t == "c"));
    }

    /// A text modifier after another modifier reads that modifier's printed form; one given a
    /// stored number or time directly is refused — the user says which printed form they
    /// mean, `|number` and `|datetime` included.
    #[test]
    fn a_text_modifier_after_a_modifier_reads_its_printed_form() {
        let zone = amsterdam();
        let reading = FactValue::Time(read_time("2024-07-22 14:30:45").unwrap());
        let cases: [(&[Modifier], &str); 7] = [
            (&[Modifier::Weekday, Modifier::Lowercase], "monday"),
            (&[Modifier::Quarter, Modifier::Lowercase], "q3"),
            (&[Modifier::Date, Modifier::Uppercase], "2024-07-22"),
            (&[Modifier::Month, Modifier::Short], "07"),
            (&[Modifier::Weekday, Modifier::Uppercase], "MONDAY"),
            (
                &[Modifier::DateTime, Modifier::Lowercase],
                "2024-07-22 14:30:45",
            ),
            (&[Modifier::Time, Modifier::Short], "14:30:45"),
        ];
        for (chain, printed) in cases {
            assert_eq!(
                chained(&reading, chain, &zone).unwrap().as_deref(),
                Some(printed),
                "{chain:?}"
            );
        }
        let fnumber = FactValue::Num(2.8);
        assert_eq!(
            chained(&fnumber, &[Modifier::Number, Modifier::Lowercase], &zone)
                .unwrap()
                .as_deref(),
            Some("2.8")
        );
        for (value, modifier) in [
            (fnumber.clone(), Modifier::Lowercase),
            (fnumber, Modifier::Stem),
            (reading.clone(), Modifier::Lowercase),
            (reading, Modifier::Short),
        ] {
            let err = chained(&value, &[modifier], &zone).unwrap_err();
            assert!(err.to_string().starts_with("String modifier"), "{err}");
        }
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

    /// The rule's edges: a stored instant reads the clock compared or printed bare and
    /// through a time modifier; text reads one only where a time modifier reads it as an
    /// instant; an accessor, `|bucket`, `|number` and the text modifiers refuse a time; a clock
    /// reading and a number never read one.
    #[test]
    fn the_instant_rule_reads_only_what_reads_a_clock() {
        let calls = |ms: &[Modifier]| ms.iter().map(|m| call(*m)).collect::<Vec<_>>();
        let instant = FactValue::Time(TimeValue::from_epoch_secs(1_718_452_800).unwrap());
        let clock = FactValue::Time(read_time("2024-06-15 14:30:05").unwrap());
        let index = PathAccessor::Index(0);
        let reads = |value: &FactValue, ms: &[Modifier]| reads_an_instant(value, None, &calls(ms));

        assert!(reads(&instant, &[]), "compared or printed bare");
        assert!(reads(&instant, &[Modifier::Year]));
        assert!(reads(&instant, &[Modifier::Date, Modifier::Lowercase]));
        for refusing in [Modifier::Bucket, Modifier::Number, Modifier::Lowercase] {
            assert!(!reads(&instant, &[refusing]), "{refusing:?}");
        }
        assert!(!reads_an_instant(
            &instant,
            Some(&index),
            &calls(&[Modifier::Year])
        ));
        assert!(!reads(&clock, &[]));
        assert!(!reads(&clock, &[Modifier::Year]));

        let text = |t: &str| FactValue::Text(t.into());
        assert!(reads(&text("2024-06-15T14:30:05Z"), &[Modifier::Year]));
        assert!(reads(&text("2024-06-15T14:30:05-00:00"), &[Modifier::Date]));
        assert!(
            !reads(&text("2024-06-15T14:30:05Z"), &[]),
            "text compared as text"
        );
        assert!(
            !reads(&text("2024:06:15 14:30:05"), &[Modifier::Year]),
            "a clock reading"
        );
        assert!(!reads(&text("misc"), &[Modifier::Year]));
        assert!(!reads(&FactValue::Num(1.0e9), &[Modifier::Year]));
        assert!(!reads(&FactValue::Num(1.0e9), &[]));
    }

    // =========================================================================
    // rendered with no modifier: a value's own spelling
    // =========================================================================

    #[test]
    fn display_integer_num() {
        assert_eq!(
            rendered(None, &FactValue::Num(42.0), &UserZone::utc()),
            "42"
        );
    }

    #[test]
    fn display_float_num() {
        assert_eq!(
            rendered(None, &FactValue::Num(12.375), &UserZone::utc()),
            "12.375"
        );
    }

    #[test]
    fn display_time() {
        // 2024-06-15 12:00:00 UTC
        assert_eq!(
            rendered(
                None,
                &FactValue::Time(TimeValue::from_epoch_secs(1718452800).unwrap()),
                &UserZone::utc()
            ),
            "2024-06-15 12:00:00"
        );
    }
}
