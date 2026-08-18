//! Value transforms: path accessors and modifiers.
//!
//! A transform turns one fact value into another — `[-1]` takes a path's last
//! segment, `|year` reads a year out of a timestamp, `|bucket(60,300)` puts a
//! number in a range. Both halves of the expression language use them, so they
//! live apart from the key vocabulary that names values and the patterns that
//! arrange them.

use anyhow::{bail, Result};

use crate::core::domain::fact::FactValue;

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
        let name = &s[..paren_pos];
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

/// Apply a modifier to a value
///
/// The `for_display` parameter controls formatting context:
/// - `true`: Human-readable format (e.g., `<60`, `>600`)
/// - `false`: Path-safe format (e.g., `-Inf-60`, `600-Inf`)
pub fn apply_modifier(
    value: &FactValue,
    call: &ModifierCall,
    key: &str,
    for_display: bool,
) -> Result<FactValue> {
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
            let timestamp = match value {
                FactValue::Time(ts) => *ts,
                FactValue::Num(n) => *n as i64,
                _ => {
                    let name: &'static str = call.modifier.into();
                    bail!(
                        "Time modifier '{}' requires a time-type fact, but '{}' is {}. \
                         Time modifiers work with facts stored as value_time in the database.",
                        name,
                        key,
                        value_type_name(value)
                    )
                }
            };
            apply_time_modifier(timestamp, call.modifier)
        }

        // String modifiers
        Modifier::Stem => {
            let s = fact_value_to_string(value);
            let path = std::path::Path::new(&s);
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(&s);
            Ok(FactValue::Text(stem.to_string()))
        }
        Modifier::Ext => {
            let s = fact_value_to_string(value);
            let path = std::path::Path::new(&s);
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            Ok(FactValue::Text(ext.to_string()))
        }
        Modifier::Short => {
            let s = fact_value_to_string(value);
            Ok(FactValue::Text(s.chars().take(8).collect()))
        }
        Modifier::Lowercase => {
            let s = fact_value_to_string(value);
            Ok(FactValue::Text(s.to_lowercase()))
        }
        Modifier::Uppercase => {
            let s = fact_value_to_string(value);
            Ok(FactValue::Text(s.to_uppercase()))
        }
        Modifier::Capitalize => {
            let s = fact_value_to_string(value);
            let mut chars = s.chars();
            let result = match chars.next() {
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
                None => String::new(),
            };
            Ok(FactValue::Text(result))
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
                Ok(FactValue::Text(format_magnitude_bucket(n)))
            } else {
                Ok(FactValue::Text(format_threshold_bucket(
                    n,
                    &call.args,
                    for_display,
                )))
            }
        }
    }
}

/// Apply a time modifier to a timestamp
fn apply_time_modifier(timestamp: i64, modifier: Modifier) -> Result<FactValue> {
    use chrono::Datelike;

    let dt = chrono::DateTime::from_timestamp(timestamp, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {timestamp}"))?;

    let result = match modifier {
        Modifier::Year => dt.format("%Y").to_string(),
        Modifier::Month => dt.format("%m").to_string(),
        Modifier::Day => dt.format("%d").to_string(),
        Modifier::Hour => dt.format("%H").to_string(),
        Modifier::Minute => dt.format("%M").to_string(),
        Modifier::Second => dt.format("%S").to_string(),
        Modifier::Date => dt.format("%Y-%m-%d").to_string(),
        Modifier::Time => dt.format("%H:%M:%S").to_string(),
        Modifier::DateTime => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Modifier::YearMonth => dt.format("%Y-%m").to_string(),
        Modifier::Week => dt.format("%V").to_string(), // ISO week number
        Modifier::Weekday => dt.format("%A").to_string(), // Full weekday name
        Modifier::Quarter => {
            let q = (dt.month() - 1) / 3 + 1;
            format!("Q{q}")
        }
        _ => unreachable!(),
    };

    Ok(FactValue::Text(result))
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

/// Convert a FactValue to string
pub(super) fn fact_value_to_string(value: &FactValue) -> String {
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
        FactValue::Time(ts) => ts.to_string(),
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
