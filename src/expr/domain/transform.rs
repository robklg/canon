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

use super::print::bucket_label;
use crate::core::domain::time::reading::{read_time, read_time_of_day};
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

impl ModifierCall {
    /// **The modifier's one spelling**: `|name`, and `|bucket`'s numbers in parentheses, each
    /// through the one number spelling, with no spaces (`|bucket(60,300)`). Read back as a key's
    /// modifier, it is the same call.
    pub(in crate::expr) fn spelled(&self) -> String {
        let name: &'static str = self.modifier.into();
        if self.args.is_empty() {
            format!("|{name}")
        } else {
            let args: Vec<String> = self.args.iter().map(|&a| spelled(a)).collect();
            format!("|{name}({})", args.join(","))
        }
    }
}

impl PathAccessor {
    /// **The accessor's one spelling**: `[n]`, `[a:b]`, a bound left out where it was
    /// (`[:-1]`, `[1:]`). Read back after a key, it is the same accessor.
    pub(in crate::expr) fn spelled(&self) -> String {
        let bound = |b: &Option<i32>| b.map(|n| n.to_string()).unwrap_or_default();
        match self {
            PathAccessor::Index(n) => format!("[{n}]"),
            PathAccessor::Slice { start, end } => format!("[{}:{}]", bound(start), bound(end)),
        }
    }
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
/// **A path's segments**: the names between `/`, a leading `/` and an empty name between `//`
/// not counted. What an accessor counts and what a pattern makes a directory of.
pub(in crate::expr) fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

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

    let segments = path_segments(path_str);
    let len = segments.len() as i32;
    let found = |value: FactValue| Ok(Modified::Value(value));

    match accessor {
        PathAccessor::Index(idx) => {
            let actual_idx = normalize_index(*idx, len);
            if actual_idx < 0 || actual_idx >= len {
                return Ok(Modified::NoValue(NoValueCause::NoSegment));
            }
            found(FactValue::Text(segments[actual_idx as usize].to_string()))
        }
        // A slice always has a value, as in Python: its bounds are clamped to the segments,
        // and one that selects none is the path with no segments. Taken from the first
        // segment of an absolute path, what it selects keeps the leading `/`.
        PathAccessor::Slice { start, end } => {
            let clamp = |bound: i32| normalize_index(bound, len).clamp(0, len) as usize;
            let start_idx = start.map_or(0, clamp);
            let end_idx = end.map_or(len as usize, clamp);
            if start_idx >= end_idx {
                return found(FactValue::Path(String::new()));
            }
            let sliced = segments[start_idx..end_idx].join("/");
            let absolute = path_str.starts_with('/') && start_idx == 0;
            found(FactValue::Path(if absolute {
                format!("/{sliced}")
            } else {
                sliced
            }))
        }
    }
}

/// **A name's extension**: what follows its last dot, where that is neither its first character
/// nor its last — none for `README`, `.bashrc` and `trail.`, the one rule for the extension a
/// source fact gives and `|ext` reads.
pub(in crate::expr) fn extension(name: &str) -> Option<&str> {
    std::path::Path::new(name)
        .extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| !ext.is_empty())
}

/// A name with its first character upper case and the rest lower.
fn capitalized(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
        None => String::new(),
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
    /// An index found no segment: it is past the end.
    NoSegment,
    /// A name with no extension, or one ending in a dot: `|ext` of `README`, `.bashrc`, `trail.`.
    NoExtension,
    /// Plain `|bucket` given a number below zero: its ranges are powers of ten from 0 up.
    BelowZero,
}

impl NoValueCause {
    /// Why there is no value, where Canon has a reason to say; a name with no extension needs
    /// none.
    pub fn reason(&self) -> Option<String> {
        match self {
            NoValueCause::TooCoarse { precision, .. } => Some(format!(
                "the time is known only to the {}",
                precision.word()
            )),
            NoValueCause::NotATime { .. } => Some("the text is not a time".to_string()),
            NoValueCause::NotANumber => Some("the text is not a number".to_string()),
            NoValueCause::NoSegment => Some("the path has no such segment".to_string()),
            NoValueCause::BelowZero => Some("|bucket groups numbers from 0 up".to_string()),
            NoValueCause::NoExtension => None,
        }
    }
}

/// **The no-value sentence**: `{subject} has no value`, and its reason after a colon where the
/// cause has one.
pub(in crate::expr) fn no_value_sentence(subject: &str, cause: NoValueCause) -> String {
    match cause.reason() {
        Some(reason) => format!("{subject} has no value: {reason}"),
        None => format!("{subject} has no value"),
    }
}

/// Apply one modifier to a value. `produced_by` is the modifier that gave the value, if one
/// did: a text modifier after another modifier reads that modifier's printed form
/// ([`rendered`]), and one given a stored number or time directly refuses it — the user
/// says which printed form they mean (`|number`, `|datetime`).
///
/// `|bucket` gives its range's label ([`bucket_label`]), the one printed form a report, a
/// rejection and a directory share.
///
/// A time modifier reads a time on the user's clock (`zone`), through the frame rule, and
/// text through the one time reading; text that is not a time has no value.
fn apply_modifier(
    value: &FactValue,
    produced_by: Option<Modifier>,
    call: &ModifierCall,
    key: &str,
    zone: &UserZone,
) -> Result<Modified> {
    let name: &'static str = call.modifier.into();
    let text = |s: String| Ok(Modified::Value(FactValue::Text(s)));
    let cased = |s: String| {
        Ok(Modified::Value(match value {
            FactValue::Path(_) => FactValue::Path(s),
            FactValue::Text(_) | FactValue::Num(_) | FactValue::Time(_) => FactValue::Text(s),
        }))
    };
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
                // A time of day written as text is read as one by the modifiers that read a
                // time of day, as its printed form is.
                FactValue::Text(t) => match read_time(t).or_else(|error| {
                    if call.modifier.reads_a_time_of_day() {
                        read_time_of_day(t)
                    } else {
                        Err(error)
                    }
                }) {
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
            Ok(match extension(&s) {
                Some(ext) => Modified::Value(FactValue::Text(ext.to_string())),
                None => Modified::NoValue(NoValueCause::NoExtension),
            })
        }
        Modifier::Short => text(text_in()?.chars().take(8).collect()),
        // A case modifier keeps a path a path, and capitalizes each of its names.
        Modifier::Lowercase => cased(text_in()?.to_lowercase()),
        Modifier::Uppercase => cased(text_in()?.to_uppercase()),
        Modifier::Capitalize => {
            let s = text_in()?;
            cased(match value {
                FactValue::Path(_) => s.split('/').map(capitalized).collect::<Vec<_>>().join("/"),
                FactValue::Text(_) | FactValue::Num(_) | FactValue::Time(_) => capitalized(&s),
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

            match bucket_range(n, &call.args) {
                Some(range) => text(bucket_label(&range)),
                None => Ok(Modified::NoValue(NoValueCause::BelowZero)),
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
    zone: &UserZone,
) -> Result<Chained> {
    // A value read through no transform is compared or printed as it is: on the clock, where
    // it is an instant.
    let mut read_an_instant =
        accessor.is_none() && modifiers.is_empty() && is_stored_instant(&value);
    let chained = |value: Modified, read_an_instant: bool| {
        Ok(Chained {
            value,
            read_an_instant,
        })
    };
    let mut value = match accessor {
        Some(acc) => match apply_accessor(&value, acc, key)? {
            Modified::Value(found) => found,
            none @ Modified::NoValue(_) => return chained(none, read_an_instant),
        },
        None => value,
    };
    let mut produced_by = None;
    for call in modifiers {
        // **The instant rule, at the step that reads**: a time modifier given an instant — a
        // stored one, or text that reads as one — reads it on the clock.
        if call.modifier.category() == ModifierCategory::Time && reads_as_an_instant(&value) {
            read_an_instant = true;
        }
        match apply_modifier(&value, produced_by, call, key, zone)? {
            Modified::Value(next) => {
                value = next;
                produced_by = Some(call.modifier);
            }
            none @ Modified::NoValue(_) => return chained(none, read_an_instant),
        }
    }
    chained(Modified::Value(value), read_an_instant)
}

/// What a value went through its accessor and modifiers to, and whether a step of it read an
/// instant on the user's clock.
#[derive(Debug, Clone)]
pub struct Chained {
    pub value: Modified,
    pub read_an_instant: bool,
}

/// A stored time that is an instant, every save time among them; a clock reading reads as
/// written on any clock.
fn is_stored_instant(value: &FactValue) -> bool {
    matches!(value, FactValue::Time(tv) if tv.form() != TimeForm::Clock)
}

/// A value a time modifier reads as an instant: a stored instant, or text the one time reading
/// reads as one.
fn reads_as_an_instant(value: &FactValue) -> bool {
    match value {
        FactValue::Text(t) => read_time(t).is_ok_and(|tv| tv.form() != TimeForm::Clock),
        FactValue::Time(_) | FactValue::Num(_) | FactValue::Path(_) => is_stored_instant(value),
    }
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

/// The record a run keeps of whether it read an instant on the clock: set from each chain's
/// own answer ([`Chained::read_an_instant`]), read once the run is done — the one answer to
/// "did this run's clock matter?", read by the archive's statements about the zone a
/// manifest's run used. Held by the run's own context (`FactCache` for filters, `EvalContext`
/// for patterns).
#[derive(Debug, Default)]
pub(crate) struct InstantRecord(std::cell::Cell<bool>);

impl InstantRecord {
    /// Note one evaluation's answer.
    pub(crate) fn note(&self, read_an_instant: bool) {
        if read_an_instant {
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

/// **A bucket's range**: what `|bucket` puts a number in. Plain `|bucket` groups by powers of
/// ten from 0 up — `0` alone, then `10^e` to `10^(e+1)` — and has no range for a number below
/// zero; `|bucket(a,b,…)` groups any number, below the first threshold, between two, or from
/// the last up. The printing rule spells each range ([`bucket_label`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum BucketRange {
    Zero,
    PowerOfTen(i32),
    Below(f64),
    Between(f64, f64),
    From(f64),
}

impl BucketRange {
    /// The range's lower edge, which it includes: below the first threshold has none.
    pub(in crate::expr) fn lower_edge(&self) -> f64 {
        match *self {
            BucketRange::Zero => 0.0,
            BucketRange::PowerOfTen(e) => 10f64.powi(e),
            BucketRange::Below(_) => f64::NEG_INFINITY,
            BucketRange::Between(lower, _) | BucketRange::From(lower) => lower,
        }
    }
}

/// The range `n` falls in, for `|bucket` with `thresholds` (none for plain `|bucket`); `None`
/// for a number below zero under plain `|bucket`.
pub(in crate::expr) fn bucket_range(n: f64, thresholds: &[f64]) -> Option<BucketRange> {
    let Some(&first) = thresholds.first() else {
        return if n == 0.0 {
            Some(BucketRange::Zero)
        } else if n < 0.0 {
            None
        } else {
            Some(BucketRange::PowerOfTen(power_of_ten(n)))
        };
    };
    if n < first {
        return Some(BucketRange::Below(first));
    }
    let range = thresholds
        .windows(2)
        .find(|pair| n < pair[1])
        .map(|pair| BucketRange::Between(pair[0], pair[1]));
    Some(range.unwrap_or(BucketRange::From(
        thresholds.last().copied().unwrap_or(first),
    )))
}

/// The power of ten a positive number's leading digit stands at, read off Rust's shortest
/// exponent form (`5e-5` is -5), which is exact at a power of ten where `log10` is not.
fn power_of_ten(n: f64) -> i32 {
    let written = format!("{n:e}");
    written
        .split_once('e')
        .and_then(|(_, exponent)| exponent.parse().ok())
        .unwrap_or(0)
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
        match apply_modifier(value, None, &call(modifier), "k", zone).unwrap() {
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
            match apply_chain(value.clone(), None, &calls, "k", zone)?.value {
                Modified::Value(v) => Some(rendered(modifiers.last().copied(), &v, zone)),
                Modified::NoValue(_) => None,
            },
        )
    }

    /// A time of day written as text is read by `|hour`, `|minute`, `|second` and `|time`, as
    /// a time of day's printed form is; a modifier needing a date has no value for it.
    #[test]
    fn an_hour_reads_a_time_of_day_stored_as_text() {
        let zone = amsterdam();
        let text = FactValue::Text("14:30:05".into());
        assert_eq!(
            modified(&text, Modifier::Hour, &zone).as_deref(),
            Some("14")
        );
        assert_eq!(
            modified(&text, Modifier::Minute, &zone).as_deref(),
            Some("30")
        );
        assert_eq!(
            modified(&text, Modifier::Second, &zone).as_deref(),
            Some("05")
        );
        assert_eq!(
            modified(&text, Modifier::Time, &zone).as_deref(),
            Some("14:30:05")
        );
        assert_eq!(modified(&text, Modifier::Year, &zone), None);
        assert_eq!(modified(&text, Modifier::Date, &zone), None);
        // The same as `|time`'s printed form read again.
        let printed = chained(
            &FactValue::Text("14:30".into()),
            &[Modifier::Time, Modifier::Lowercase, Modifier::Hour],
            &zone,
        )
        .unwrap();
        assert_eq!(printed.as_deref(), Some("14"));
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
                let got = apply_modifier(&value, None, &call(modifier), "k", &amsterdam()).unwrap();
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
                        Modified::NoValue(other) => panic!("{written} {modifier:?}: {other:?}"),
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
            match apply_modifier(&reading, None, &call(modifier), "k", &zone).unwrap() {
                Modified::Value(v) => {
                    assert_eq!(format!("{v:?}"), format!("{typed:?}"), "{modifier:?}");
                    assert_eq!(rendered(Some(modifier), &v, &zone), printed, "{modifier:?}");
                }
                Modified::NoValue(cause) => panic!("{modifier:?}: {cause:?}"),
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
            let v = apply_modifier(&clock(day), None, &call(Modifier::Weekday), "k", &zone);
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
                apply_modifier(&saved, None, &call(modifier), "k", &amsterdam()).unwrap()
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
        let number =
            |value: FactValue| apply_modifier(&value, None, &call(Modifier::Number), "k", &zone);
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
            apply_modifier(&year, None, &call(Modifier::Month), "k", &zone).unwrap(),
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

    /// A slice of `path`, as its value's debug text (a value has no `==`).
    fn sliced(path: FactValue, start: Option<i32>, end: Option<i32>) -> String {
        let accessor = PathAccessor::Slice { start, end };
        match apply_chain(path, Some(&accessor), &[], "k", &amsterdam())
            .unwrap()
            .value
        {
            Modified::Value(value) => format!("{value:?}"),
            Modified::NoValue(cause) => panic!("{accessor:?}: {cause:?}"),
        }
    }

    /// **A slice always has a value**, as in Python: its bounds are clamped to the segments,
    /// and one that selects none is the path with no segments.
    #[test]
    fn a_slice_always_has_a_value() {
        let abc = || FactValue::Path("a/b/c".into());
        let path = |p: &str| format!("{:?}", FactValue::Path(p.into()));
        for (start, end, expected) in [
            (Some(5), None, ""),
            (None, Some(-5), ""),
            (Some(2), Some(4), "c"),
            (Some(-10), None, "a/b/c"),
            (None, Some(10), "a/b/c"),
            (Some(2), Some(1), ""),
            (None, Some(-1), "a/b"),
            (Some(1), Some(-1), "b"),
        ] {
            assert_eq!(
                sliced(abc(), start, end),
                path(expected),
                "[{start:?}:{end:?}]"
            );
        }
        // A file at the top of its root: `[:-1]` is the path with no segments.
        assert_eq!(
            sliced(FactValue::Path("photo.jpg".into()), None, Some(-1)),
            path("")
        );
        assert_eq!(sliced(FactValue::Text("".into()), None, None), path(""));
    }

    /// **A slice of an absolute path from its first segment keeps the leading `/`**, text
    /// read as a path included; one starting later, or selecting no segment, has none.
    #[test]
    fn a_slice_from_the_first_segment_keeps_the_slash() {
        let absolute = || FactValue::Path("/private/tmp/claude-501/a.jpg".into());
        let path = |p: &str| format!("{:?}", FactValue::Path(p.into()));
        assert_eq!(
            sliced(absolute(), None, Some(3)),
            path("/private/tmp/claude-501")
        );
        assert_eq!(sliced(absolute(), Some(0), Some(2)), path("/private/tmp"));
        assert_eq!(sliced(absolute(), Some(-10), Some(1)), path("/private"));
        assert_eq!(sliced(absolute(), None, None), format!("{:?}", absolute()));
        assert_eq!(sliced(absolute(), Some(1), Some(3)), path("tmp/claude-501"));
        assert_eq!(sliced(absolute(), Some(0), Some(0)), path(""));
        assert_eq!(
            sliced(FactValue::Text("/a/b".into()), None, Some(1)),
            path("/a")
        );
        assert_eq!(
            sliced(FactValue::Path("a/b".into()), None, Some(1)),
            path("a")
        );
    }

    /// **A case modifier keeps a path a path**, and `|capitalize` capitalizes each of its
    /// names; on text it capitalizes the whole value once, and `|stem`, `|ext` and `|short`
    /// give text.
    #[test]
    fn case_modifiers_keep_a_path() {
        let zone = amsterdam();
        let on = |value: FactValue, modifier: Modifier| match apply_modifier(
            &value,
            None,
            &call(modifier),
            "k",
            &zone,
        )
        .unwrap()
        {
            Modified::Value(value) => value,
            Modified::NoValue(cause) => panic!("{modifier:?}: {cause:?}"),
        };
        let shown = |value: FactValue| format!("{value:?}");
        let path = || FactValue::Path("my PHOTOS/IMG_01.JPG".into());
        assert_eq!(
            shown(on(path(), Modifier::Lowercase)),
            shown(FactValue::Path("my photos/img_01.jpg".into()))
        );
        assert_eq!(
            shown(on(path(), Modifier::Uppercase)),
            shown(FactValue::Path("MY PHOTOS/IMG_01.JPG".into()))
        );
        assert_eq!(
            shown(on(path(), Modifier::Capitalize)),
            shown(FactValue::Path("My photos/Img_01.jpg".into()))
        );
        assert_eq!(
            shown(on(
                FactValue::Text("my PHOTOS/IMG".into()),
                Modifier::Capitalize
            )),
            shown(FactValue::Text("My photos/img".into()))
        );
        for modifier in [Modifier::Stem, Modifier::Short] {
            assert!(
                matches!(on(path(), modifier), FactValue::Text(_)),
                "{modifier:?}"
            );
        }
        assert_eq!(
            shown(on(path(), Modifier::Ext)),
            shown(FactValue::Text("JPG".into()))
        );
        // Through the chain, a case modifier after another keeps the path.
        let calls = [call(Modifier::Lowercase), call(Modifier::Capitalize)];
        assert!(matches!(
            apply_chain(path(), None, &calls, "k", &zone).unwrap().value,
            Modified::Value(FactValue::Path(p)) if p == "My photos/Img_01.jpg"
        ));
    }

    /// **A worked-out empty value has no value**: `|ext` of a name with no extension — none, a
    /// leading dot alone, a trailing dot, empty text — has none, with no reason to state; a
    /// stored empty text keeps its value through a text modifier, and has none for a modifier
    /// reading it as a number or a time.
    #[test]
    fn a_worked_out_empty_value_has_no_value() {
        let zone = amsterdam();
        let on = |text: &str, modifier: Modifier| {
            apply_modifier(
                &FactValue::Text(text.into()),
                None,
                &call(modifier),
                "k",
                &zone,
            )
            .unwrap()
        };
        for name in ["README", ".bashrc", "trail.", "", "dir/README"] {
            assert!(
                matches!(
                    on(name, Modifier::Ext),
                    Modified::NoValue(NoValueCause::NoExtension)
                ),
                "{name:?}"
            );
        }
        assert_eq!(
            no_value_sentence("source.ext", NoValueCause::NoExtension),
            "source.ext has no value"
        );
        assert!(
            matches!(on("README", Modifier::Stem), Modified::Value(FactValue::Text(t)) if t == "README")
        );
        for modifier in [
            Modifier::Lowercase,
            Modifier::Uppercase,
            Modifier::Short,
            Modifier::Stem,
        ] {
            assert!(
                matches!(on("", modifier), Modified::Value(FactValue::Text(t)) if t.is_empty()),
                "{modifier:?}"
            );
        }
        assert!(matches!(
            on("", Modifier::Number),
            Modified::NoValue(NoValueCause::NotANumber)
        ));
        assert!(matches!(
            on("", Modifier::Year),
            Modified::NoValue(NoValueCause::NotATime { .. })
        ));
    }

    /// An index that finds no segment gives no value, never an error.
    #[test]
    fn an_accessor_that_finds_no_segment_gives_no_value() {
        let path = FactValue::Text("a/b/c".into());
        let misses = [PathAccessor::Index(3), PathAccessor::Index(-4)];
        for accessor in misses {
            assert!(
                matches!(
                    apply_chain(path.clone(), Some(&accessor), &[], "k", &amsterdam())
                        .unwrap()
                        .value,
                    Modified::NoValue(NoValueCause::NoSegment)
                ),
                "{accessor:?}"
            );
        }
        let last = apply_chain(path, Some(&PathAccessor::Index(-1)), &[], "k", &amsterdam());
        assert!(matches!(last.unwrap().value, Modified::Value(FactValue::Text(t)) if t == "c"));
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

    /// What `|bucket` gives a number, through the modifier: its range's label.
    fn bucketed(n: f64, args: &[f64]) -> Option<String> {
        let call = ModifierCall {
            modifier: Modifier::Bucket,
            args: args.to_vec(),
        };
        match apply_modifier(&FactValue::Num(n), None, &call, "k", &amsterdam()).unwrap() {
            Modified::Value(v) => Some(rendered(Some(Modifier::Bucket), &v, &amsterdam())),
            Modified::NoValue(_) => None,
        }
    }

    #[test]
    fn test_bucket_magnitude() {
        let cases = [
            (0.0, "0"),
            (5.0, "1-to-10"),
            (1.0, "1-to-10"),
            (10.0, "10-to-100"),
            (50.0, "10-to-100"),
            (500.0, "100-to-1K"),
            (999.0, "100-to-1K"),
            (1000.0, "1K-to-10K"),
            (5000.0, "1K-to-10K"),
            (50000.0, "10K-to-100K"),
            (5_000_000.0, "1M-to-10M"),
            (5_000_000_000.0, "1G-to-10G"),
            (0.5, "0.1-to-1"),
            (0.1, "0.1-to-1"),
        ];
        for (n, label) in cases {
            assert_eq!(bucketed(n, &[]).as_deref(), Some(label), "{n}");
        }
    }

    #[test]
    fn test_bucket_threshold_display() {
        // Thresholds are spelled as written, with no suffix.
        let thresholds = [60.0, 3600.0, 7200.0];
        for (n, label) in [
            (30.0, "under-60"),
            (60.0, "60-to-3600"),
            (100.0, "60-to-3600"),
            (5000.0, "3600-to-7200"),
            (7200.0, "7200-and-up"),
            (10000.0, "7200-and-up"),
        ] {
            assert_eq!(bucketed(n, &thresholds).as_deref(), Some(label), "{n}");
        }
    }

    /// Plain `|bucket` groups from 0 up: a number below zero has no range, and says why.
    #[test]
    fn a_negative_number_under_plain_bucket_has_no_value_and_says_why() {
        let call = ModifierCall {
            modifier: Modifier::Bucket,
            args: Vec::new(),
        };
        let none = apply_modifier(&FactValue::Num(-50.0), None, &call, "k", &amsterdam()).unwrap();
        let Modified::NoValue(cause) = none else {
            panic!("{none:?}")
        };
        assert_eq!(cause, NoValueCause::BelowZero);
        assert_eq!(
            no_value_sentence("source.size|bucket", cause),
            "source.size|bucket has no value: |bucket groups numbers from 0 up"
        );
        // With thresholds, any number has a range.
        assert_eq!(bucketed(-50.0, &[0.0]).as_deref(), Some("under-0"));
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

    /// **The instant rule, asked of the chain**: a step reads an instant on the clock where a
    /// stored instant is compared or printed with no transform, or a time modifier is given an
    /// instant — a stored one, or text that reads as one, after an accessor or a text modifier
    /// too. A clock reading reads as written on any clock, and a modifier that refuses a time
    /// reads none.
    #[test]
    fn the_instant_rule_reads_only_what_reads_a_clock() {
        let calls = |ms: &[Modifier]| ms.iter().map(|m| call(*m)).collect::<Vec<_>>();
        let zone = amsterdam();
        let read = |value: &FactValue, accessor: Option<&PathAccessor>, ms: &[Modifier]| {
            apply_chain(value.clone(), accessor, &calls(ms), "k", &zone)
                .is_ok_and(|chained| chained.read_an_instant)
        };
        let reads = |value: &FactValue, ms: &[Modifier]| read(value, None, ms);
        let instant = FactValue::Time(TimeValue::from_epoch_secs(1_718_452_800).unwrap());
        let clock = FactValue::Time(read_time("2024-06-15 14:30:05").unwrap());

        assert!(reads(&instant, &[]), "compared or printed bare");
        assert!(reads(&instant, &[Modifier::Year]));
        assert!(reads(&instant, &[Modifier::Date, Modifier::Lowercase]));
        // A modifier that is no time modifier reads no clock, given text that reads as an
        // instant: each of these runs, so only the rule can answer.
        let instant_text = || FactValue::Text("2024-06-15T14:30:05Z".into());
        for other in [
            Modifier::Number,
            Modifier::Lowercase,
            Modifier::Short,
            Modifier::Stem,
        ] {
            assert!(
                apply_chain(instant_text(), None, &calls(&[other]), "k", &zone).is_ok(),
                "{other:?} runs"
            );
            assert!(!reads(&instant_text(), &[other]), "{other:?}");
        }
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

    /// **A time read through an accessor or after a text modifier is recorded**: the step that
    /// reads it is the one that answers, wherever it stands in the chain.
    #[test]
    fn a_time_read_through_an_accessor_or_after_a_text_modifier_is_recorded() {
        let calls = |ms: &[Modifier]| ms.iter().map(|m| call(*m)).collect::<Vec<_>>();
        let zone = amsterdam();
        let read = |value: FactValue, accessor: Option<&PathAccessor>, ms: &[Modifier]| {
            apply_chain(value, accessor, &calls(ms), "k", &zone)
                .unwrap()
                .read_an_instant
        };
        let first = PathAccessor::Index(0);
        let instant_text = || FactValue::Text("2024-06-15T14:30:05Z/a.jpg".into());
        assert!(read(instant_text(), Some(&first), &[Modifier::Year]));
        assert!(read(
            FactValue::Text("2024-06-15T14:30:05Z".into()),
            None,
            &[Modifier::Uppercase, Modifier::Year]
        ));
        // `|short` cuts the text to `2024-06-`, which reads as no time: nothing is read.
        assert!(!read(
            FactValue::Text("2024-06-15T14:30:05Z".into()),
            None,
            &[Modifier::Short, Modifier::Date]
        ));
        // A clock reading through the same steps reads none, and neither does the accessor
        // on its own.
        let clock_text = || FactValue::Text("2024-06-15 14:30:05/a.jpg".into());
        assert!(!read(clock_text(), Some(&first), &[Modifier::Year]));
        assert!(!read(instant_text(), Some(&first), &[]));
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
