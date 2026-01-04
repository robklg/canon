//! Expression parsing and evaluation for output patterns.
//!
//! This module handles pattern expansion for output paths in manifests.
//! Patterns consist of literal text and placeholders like `{fact.key}`,
//! `{source.rel_path[-1]}`, or `{content.DateTimeOriginal|year}`.

use anyhow::{bail, Result};
use std::collections::HashMap;

// ============================================================================
// Types
// ============================================================================

/// A parsed pattern consisting of segments
#[derive(Debug, Clone)]
pub struct Pattern {
    pub segments: Vec<PatternSegment>,
}

/// A segment in a pattern - either literal text or an expression
#[derive(Debug, Clone)]
pub enum PatternSegment {
    Literal(String),
    Expr(Expr),
}

/// A parsed expression from within `{...}`
#[derive(Debug, Clone)]
pub struct Expr {
    pub key: String,
    pub accessor: Option<PathAccessor>,
    pub modifiers: Vec<Modifier>,
}

/// Path segment accessor for indexing into path-type values
#[derive(Debug, Clone, PartialEq)]
pub enum PathAccessor {
    /// Single index: [2] or [-1]
    Index(i32),
    /// Slice: [1:3] or [-3:-1] or [1:] or [:3]
    Slice { start: Option<i32>, end: Option<i32> },
}

/// Modifiers that transform values
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Modifier {
    // Time modifiers (for time-type facts)
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Date,      // YYYY-MM-DD
    Time,      // HH:MM:SS
    DateTime,  // YYYY-MM-DDTHH:MM:SS
    YearMonth, // YYYY-MM
    Week,
    Weekday,
    Quarter,
    // String modifiers
    Stem,  // filename without extension
    Ext,   // file extension
    Short, // first 8 chars (for hashes)
}

/// Fact value types for evaluation.
/// Matches the typed columns in the facts table: value_text, value_num, value_time.
#[derive(Debug, Clone)]
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),    // Unix timestamp
    Path(String), // Path that supports segment indexing (for derived path facts)
}

/// Context for pattern evaluation - provides fact values and source info
pub struct EvalContext {
    /// Fact values by key (properly typed from database)
    facts: HashMap<String, FactValue>,
    /// Source root path (for path derivation)
    source_root: Option<String>,
    /// Source relative path (for path derivation)
    source_rel_path: Option<String>,
    /// Scope prefix for scope.rel_path derivation
    scope_prefix: Option<String>,
}

impl EvalContext {
    pub fn new() -> Self {
        EvalContext {
            facts: HashMap::new(),
            source_root: None,
            source_rel_path: None,
            scope_prefix: None,
        }
    }

    /// Set a fact value (should be properly typed from database lookup)
    pub fn set_fact(&mut self, key: &str, value: FactValue) {
        self.facts.insert(key.to_string(), value);
    }

    /// Set source root path (for deriving source.root, source.path)
    pub fn set_source_root(&mut self, root: String) {
        self.source_root = Some(root);
    }

    /// Set source relative path (for deriving source.rel_path, filename, etc.)
    pub fn set_source_rel_path(&mut self, rel_path: String) {
        self.source_rel_path = Some(rel_path);
    }

    /// Set scope prefix for deriving scope.rel_path
    pub fn set_scope_prefix(&mut self, prefix: Option<String>) {
        self.scope_prefix = prefix;
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Alias Expansion
// ============================================================================

/// Known aliases that expand to expressions
fn expand_alias(name: &str) -> Option<&'static str> {
    match name {
        "filename" => Some("source.rel_path[-1]"),
        "stem" => Some("source.rel_path[-1]|stem"),
        "ext" => Some("source.rel_path[-1]|ext"),
        "hash" => Some("object.hash"),
        "hash_short" => Some("object.hash|short"),
        "id" => Some("source.id"),
        _ => None,
    }
}

// ============================================================================
// Pattern Parsing
// ============================================================================

/// Parse a pattern string into a Pattern struct
pub fn parse_pattern(pattern: &str) -> Result<Pattern> {
    let mut segments = Vec::new();
    let mut chars = pattern.chars().peekable();
    let mut literal = String::new();

    while let Some(c) = chars.next() {
        if c == '{' {
            // Save any accumulated literal
            if !literal.is_empty() {
                segments.push(PatternSegment::Literal(std::mem::take(&mut literal)));
            }

            // Parse expression until closing brace
            let mut expr_str = String::new();
            let mut depth = 1;
            while let Some(c) = chars.next() {
                if c == '{' {
                    depth += 1;
                    expr_str.push(c);
                } else if c == '}' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                    expr_str.push(c);
                } else {
                    expr_str.push(c);
                }
            }
            if depth != 0 {
                bail!("Unclosed '{{' in pattern");
            }

            // Check for alias expansion
            let expr_str = if let Some(expanded) = expand_alias(&expr_str) {
                expanded.to_string()
            } else {
                expr_str
            };

            let expr = parse_expr(&expr_str)?;
            segments.push(PatternSegment::Expr(expr));
        } else {
            literal.push(c);
        }
    }

    // Save any remaining literal
    if !literal.is_empty() {
        segments.push(PatternSegment::Literal(literal));
    }

    Ok(Pattern { segments })
}

/// Parse an expression string (the content within `{...}`)
fn parse_expr(s: &str) -> Result<Expr> {
    let s = s.trim();
    if s.is_empty() {
        bail!("Empty expression");
    }

    // Split by pipe to get modifiers
    let parts: Vec<&str> = s.split('|').collect();
    let key_part = parts[0].trim();
    let modifier_parts = &parts[1..];

    // Parse key and accessor from key_part
    let (key, accessor) = parse_key_and_accessor(key_part)?;

    // Parse modifiers
    let mut modifiers = Vec::new();
    for mod_str in modifier_parts {
        let modifier = parse_modifier(mod_str.trim())?;
        modifiers.push(modifier);
    }

    Ok(Expr {
        key,
        accessor,
        modifiers,
    })
}

/// Parse key with optional accessor: `source.rel_path[-1]` or `source.rel_path[1:3]`
fn parse_key_and_accessor(s: &str) -> Result<(String, Option<PathAccessor>)> {
    if let Some(bracket_start) = s.find('[') {
        let key = s[..bracket_start].to_string();
        let rest = &s[bracket_start..];

        if !rest.ends_with(']') {
            bail!("Unclosed '[' in accessor");
        }
        let inner = &rest[1..rest.len() - 1];

        let accessor = parse_accessor(inner)?;
        Ok((key, Some(accessor)))
    } else {
        Ok((s.to_string(), None))
    }
}

/// Parse accessor content: `-1`, `2`, `1:3`, `-3:-1`, `:3`, `1:`
fn parse_accessor(s: &str) -> Result<PathAccessor> {
    if s.contains(':') {
        // Slice
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        let start = if parts[0].is_empty() {
            None
        } else {
            Some(
                parts[0]
                    .parse::<i32>()
                    .map_err(|_| anyhow::anyhow!("Invalid slice start: '{}'", parts[0]))?,
            )
        };
        let end = if parts[1].is_empty() {
            None
        } else {
            Some(
                parts[1]
                    .parse::<i32>()
                    .map_err(|_| anyhow::anyhow!("Invalid slice end: '{}'", parts[1]))?,
            )
        };
        Ok(PathAccessor::Slice { start, end })
    } else {
        // Single index
        let index = s
            .parse::<i32>()
            .map_err(|_| anyhow::anyhow!("Invalid index: '{}'", s))?;
        Ok(PathAccessor::Index(index))
    }
}

/// Parse a modifier name
fn parse_modifier(s: &str) -> Result<Modifier> {
    match s.to_lowercase().as_str() {
        "year" => Ok(Modifier::Year),
        "month" => Ok(Modifier::Month),
        "day" => Ok(Modifier::Day),
        "hour" => Ok(Modifier::Hour),
        "minute" => Ok(Modifier::Minute),
        "second" => Ok(Modifier::Second),
        "date" => Ok(Modifier::Date),
        "time" => Ok(Modifier::Time),
        "datetime" => Ok(Modifier::DateTime),
        "yearmonth" => Ok(Modifier::YearMonth),
        "week" => Ok(Modifier::Week),
        "weekday" => Ok(Modifier::Weekday),
        "quarter" => Ok(Modifier::Quarter),
        "stem" => Ok(Modifier::Stem),
        "ext" => Ok(Modifier::Ext),
        "short" => Ok(Modifier::Short),
        _ => bail!(
            "Unknown modifier: '{}'. Available: year, month, day, hour, minute, second, \
             date, time, datetime, yearmonth, week, weekday, quarter, stem, ext, short",
            s
        ),
    }
}

/// Extract all fact keys referenced by a pattern (for prefetching from DB)
pub fn extract_fact_keys(pattern: &Pattern) -> Vec<String> {
    let mut keys = Vec::new();
    for segment in &pattern.segments {
        if let PatternSegment::Expr(expr) = segment {
            keys.push(expr.key.clone());
        }
    }
    keys
}

// ============================================================================
// Pattern Evaluation
// ============================================================================

/// Evaluate a parsed pattern with the given context
pub fn evaluate(pattern: &Pattern, ctx: &EvalContext) -> Result<String> {
    let mut result = String::new();

    for segment in &pattern.segments {
        match segment {
            PatternSegment::Literal(s) => result.push_str(s),
            PatternSegment::Expr(expr) => {
                let value = evaluate_expr(expr, ctx)?;
                result.push_str(&value);
            }
        }
    }

    // Sanitize path (remove potentially dangerous characters)
    let result = result.replace("..", "_").replace('\0', "_");

    Ok(result)
}

/// Evaluate a single expression
fn evaluate_expr(expr: &Expr, ctx: &EvalContext) -> Result<String> {
    // Get the base value
    let value = get_value(&expr.key, ctx)?;

    // Apply accessor if present
    let value = if let Some(ref accessor) = expr.accessor {
        apply_accessor(&value, accessor, &expr.key)?
    } else {
        value
    };

    // Apply modifiers in order
    let mut result = value;
    for modifier in &expr.modifiers {
        result = apply_modifier(&result, *modifier, &expr.key)?;
    }

    // Convert to string
    Ok(fact_value_to_string(&result))
}

/// Get a fact value by key, handling derived facts
fn get_value(key: &str, ctx: &EvalContext) -> Result<FactValue> {
    // Handle derived/built-in facts first
    match key {
        "source.rel_path" => {
            if let Some(ref rel_path) = ctx.source_rel_path {
                return Ok(FactValue::Path(rel_path.clone()));
            }
            bail!("source.rel_path not available");
        }
        "source.root" => {
            if let Some(ref root) = ctx.source_root {
                return Ok(FactValue::Path(root.clone()));
            }
            bail!("source.root not available");
        }
        "source.path" => {
            // Derived: root + "/" + rel_path
            match (&ctx.source_root, &ctx.source_rel_path) {
                (Some(root), Some(rel_path)) => {
                    let full = if rel_path.is_empty() {
                        root.clone()
                    } else {
                        format!("{}/{}", root, rel_path)
                    };
                    return Ok(FactValue::Path(full));
                }
                _ => bail!("source.path not available (requires root and rel_path)"),
            }
        }
        "scope.rel_path" => {
            // Derived: strip scope prefix from full path
            match (&ctx.scope_prefix, &ctx.source_root, &ctx.source_rel_path) {
                (Some(scope), Some(root), Some(rel_path)) => {
                    let full_path = if rel_path.is_empty() {
                        root.clone()
                    } else {
                        format!("{}/{}", root, rel_path)
                    };
                    // Strip scope prefix
                    let scope_rel = if full_path.starts_with(scope) {
                        let stripped = &full_path[scope.len()..];
                        stripped.trim_start_matches('/').to_string()
                    } else {
                        // If scope doesn't match, return full rel_path
                        rel_path.clone()
                    };
                    return Ok(FactValue::Path(scope_rel));
                }
                (None, _, _) => bail!("scope.rel_path not available (no scope was specified during manifest generation)"),
                _ => bail!("scope.rel_path not available"),
            }
        }
        _ => {}
    }

    // Look up in facts
    if let Some(value) = ctx.facts.get(key) {
        return Ok(value.clone());
    }

    // Build list of available facts for error message
    let mut available: Vec<&str> = ctx.facts.keys().map(|s| s.as_str()).collect();
    // Add derived facts that are available
    if ctx.source_rel_path.is_some() {
        available.push("source.rel_path");
    }
    if ctx.source_root.is_some() {
        available.push("source.root");
    }
    if ctx.source_root.is_some() && ctx.source_rel_path.is_some() {
        available.push("source.path");
    }
    if ctx.scope_prefix.is_some() && ctx.source_root.is_some() && ctx.source_rel_path.is_some() {
        available.push("scope.rel_path");
    }
    available.sort();

    bail!(
        "Unknown fact '{}'. Available facts: {}",
        key,
        available.join(", ")
    );
}

/// Apply a path accessor to a value
fn apply_accessor(value: &FactValue, accessor: &PathAccessor, key: &str) -> Result<FactValue> {
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
pub fn apply_modifier(value: &FactValue, modifier: Modifier, key: &str) -> Result<FactValue> {
    match modifier {
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
                _ => bail!(
                    "Time modifier '{}' requires a time-type fact, but '{}' is {}. \
                     Time modifiers work with facts stored as value_time in the database.",
                    modifier_name(modifier),
                    key,
                    value_type_name(value)
                ),
            };
            apply_time_modifier(timestamp, modifier)
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
    }
}

/// Apply a time modifier to a timestamp
fn apply_time_modifier(timestamp: i64, modifier: Modifier) -> Result<FactValue> {
    use chrono::Datelike;

    let dt = chrono::DateTime::from_timestamp(timestamp, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", timestamp))?;

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
            format!("Q{}", q)
        }
        _ => unreachable!(),
    };

    Ok(FactValue::Text(result))
}

/// Convert a FactValue to string
fn fact_value_to_string(value: &FactValue) -> String {
    match value {
        FactValue::Text(s) => s.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
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

/// Get modifier name for error messages
fn modifier_name(modifier: Modifier) -> &'static str {
    match modifier {
        Modifier::Year => "year",
        Modifier::Month => "month",
        Modifier::Day => "day",
        Modifier::Hour => "hour",
        Modifier::Minute => "minute",
        Modifier::Second => "second",
        Modifier::Date => "date",
        Modifier::Time => "time",
        Modifier::DateTime => "datetime",
        Modifier::YearMonth => "yearmonth",
        Modifier::Week => "week",
        Modifier::Weekday => "weekday",
        Modifier::Quarter => "quarter",
        Modifier::Stem => "stem",
        Modifier::Ext => "ext",
        Modifier::Short => "short",
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_literal() {
        let pattern = parse_pattern("foo/bar").unwrap();
        assert_eq!(pattern.segments.len(), 1);
        assert!(matches!(&pattern.segments[0], PatternSegment::Literal(s) if s == "foo/bar"));
    }

    #[test]
    fn test_parse_simple_expr() {
        let pattern = parse_pattern("{filename}").unwrap();
        assert_eq!(pattern.segments.len(), 1);
        // filename is an alias, so it should expand
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "source.rel_path");
            assert!(matches!(e.accessor, Some(PathAccessor::Index(-1))));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_mixed() {
        let pattern = parse_pattern("photos/{filename}").unwrap();
        assert_eq!(pattern.segments.len(), 2);
        assert!(matches!(&pattern.segments[0], PatternSegment::Literal(s) if s == "photos/"));
    }

    #[test]
    fn test_parse_with_modifier() {
        let pattern = parse_pattern("{content.DateTimeOriginal|year}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "content.DateTimeOriginal");
            assert_eq!(e.modifiers.len(), 1);
            assert_eq!(e.modifiers[0], Modifier::Year);
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_with_accessor() {
        let pattern = parse_pattern("{source.rel_path[-2]}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "source.rel_path");
            assert!(matches!(e.accessor, Some(PathAccessor::Index(-2))));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_slice_accessor() {
        let pattern = parse_pattern("{source.rel_path[1:3]}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert!(matches!(
                e.accessor,
                Some(PathAccessor::Slice {
                    start: Some(1),
                    end: Some(3)
                })
            ));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_evaluate_simple() {
        let pattern = parse_pattern("{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image.jpg");
    }

    #[test]
    fn test_evaluate_path_index() {
        let pattern = parse_pattern("{source.rel_path[-2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/vacation/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation");
    }

    #[test]
    fn test_evaluate_path_slice() {
        let pattern = parse_pattern("{source.rel_path[0:2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/vacation/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "photos/2024");
    }

    #[test]
    fn test_evaluate_time_modifier() {
        let pattern = parse_pattern("{source.mtime|year}").unwrap();
        let mut ctx = EvalContext::new();
        // 2024-06-15 12:00:00 UTC
        ctx.set_fact("source.mtime", FactValue::Time(1718452800));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024");
    }

    #[test]
    fn test_evaluate_stem_modifier() {
        let pattern = parse_pattern("{stem}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image");
    }

    #[test]
    fn test_evaluate_ext_modifier() {
        let pattern = parse_pattern("{ext}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "jpg");
    }

    #[test]
    fn test_out_of_bounds_error() {
        let pattern = parse_pattern("{source.rel_path[10]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn test_scope_rel_path() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/Photos".to_string());
        ctx.set_source_rel_path("Home/2024/vacation/image.jpg".to_string());
        ctx.set_scope_prefix(Some("/Photos/Home".to_string()));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024/vacation/image.jpg");
    }

    #[test]
    fn test_extract_fact_keys() {
        let pattern = parse_pattern("{content.DateTimeOriginal|year}/{filename}").unwrap();
        let keys = extract_fact_keys(&pattern);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"content.DateTimeOriginal".to_string()));
        assert!(keys.contains(&"source.rel_path".to_string()));
    }

    #[test]
    fn test_hash_short_alias() {
        let pattern = parse_pattern("{hash_short}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_fact("object.hash", FactValue::Text("abcdef1234567890".to_string()));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "abcdef12");
    }

    #[test]
    fn test_complex_pattern() {
        let pattern = parse_pattern("{source.rel_path[0]}/{source.mtime|year}/{stem}_{hash_short}.{ext}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("vacation/photos/IMG_001.jpg".to_string());
        ctx.set_fact("source.mtime", FactValue::Time(1718452800));
        ctx.set_fact("object.hash", FactValue::Text("abcdef1234567890".to_string()));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation/2024/IMG_001_abcdef12.jpg");
    }
}
