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
    pub modifiers: Vec<ModifierCall>,
}

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

/// Fact type classification (without the actual value).
/// Matches the typed columns in the facts table plus Path for derived facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactType {
    Text,
    Num,
    Time,
    Path, // Derived path facts that support segment indexing
}

impl FactType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FactType::Text => "text",
            FactType::Num => "num",
            FactType::Time => "time",
            FactType::Path => "path",
        }
    }
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
// Built-in Keys
// ============================================================================

/// Visibility of a built-in key in `canon facts` output
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BuiltinKeyVisibility {
    /// Shown by default in facts listing
    Default,
    /// Only shown with --all flag
    Hidden,
    /// Not shown in facts listing (alias-only or special)
    NotListed,
}

/// Category of a built-in key for display purposes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BuiltinKeyCategory {
    /// Computed directly from source columns
    BuiltIn,
    /// Derived/computed from other data
    Derived,
    /// Stored in facts table (not computed)
    Stored,
}

/// Built-in keys - derived from source/object columns or always-valid fact keys.
/// These are recognized in filters without checking the facts table.
/// Some keys also serve as pattern aliases with expansions.
#[derive(Debug, Clone, Copy, PartialEq, strum::IntoStaticStr, strum::EnumIter)]
pub enum BuiltinKey {
    // Source fields (derived from source columns)
    #[strum(serialize = "source.ext")]
    SourceExt,
    #[strum(serialize = "source.size")]
    SourceSize,
    #[strum(serialize = "source.mtime")]
    SourceMtime,
    #[strum(serialize = "source.path")]
    SourcePath,
    #[strum(serialize = "source.root")]
    SourceRoot,
    #[strum(serialize = "source.rel_path")]
    SourceRelPath,
    #[strum(serialize = "source.id")]
    SourceId,
    #[strum(serialize = "source.device")]
    SourceDevice,
    #[strum(serialize = "source.inode")]
    SourceInode,

    // Aliases (also valid in filters, with pattern expansions)
    #[strum(serialize = "filename")]
    Filename,
    #[strum(serialize = "stem")]
    Stem,
    #[strum(serialize = "ext")]
    Ext,
    #[strum(serialize = "hash")]
    Hash,
    #[strum(serialize = "hash_short")]
    HashShort,
    #[strum(serialize = "id")]
    Id,

    // Legacy shortcuts
    #[strum(serialize = "size")]
    Size,
    #[strum(serialize = "mtime")]
    Mtime,
    #[strum(serialize = "root_id")]
    RootId,

    // Well-known content fact (valid even before any hashing is done)
    #[strum(serialize = "content.hash.sha256")]
    ContentHashSha256,
}

impl BuiltinKey {
    /// Get the pattern expansion for this key (if it's an alias).
    /// Used when expanding `{key}` in manifest patterns.
    pub fn expansion(&self) -> Option<&'static str> {
        match self {
            BuiltinKey::Filename => Some("source.rel_path[-1]"),
            BuiltinKey::Stem => Some("source.rel_path[-1]|stem"),
            BuiltinKey::Ext => Some("source.rel_path[-1]|ext"),
            BuiltinKey::Hash => Some("object.hash"),
            BuiltinKey::HashShort => Some("object.hash|short"),
            BuiltinKey::Id => Some("source.id"),
            _ => None,
        }
    }

    /// Get the visibility of this key in `canon facts` output
    pub fn visibility(&self) -> BuiltinKeyVisibility {
        match self {
            // Default visible
            BuiltinKey::SourceExt
            | BuiltinKey::SourceSize
            | BuiltinKey::SourceMtime
            | BuiltinKey::SourcePath
            | BuiltinKey::SourceRelPath
            | BuiltinKey::Filename => BuiltinKeyVisibility::Default,

            // Hidden
            BuiltinKey::SourceRoot
            | BuiltinKey::SourceId
            | BuiltinKey::SourceDevice
            | BuiltinKey::SourceInode => BuiltinKeyVisibility::Hidden,

            // Not listed (aliases, legacy shortcuts, special keys)
            BuiltinKey::Stem
            | BuiltinKey::Ext
            | BuiltinKey::Hash
            | BuiltinKey::HashShort
            | BuiltinKey::Id
            | BuiltinKey::Size
            | BuiltinKey::Mtime
            | BuiltinKey::RootId
            | BuiltinKey::ContentHashSha256 => BuiltinKeyVisibility::NotListed,
        }
    }

    /// Get the category of this key for display purposes
    pub fn category(&self) -> BuiltinKeyCategory {
        match self {
            // Built-in: computed directly from source columns
            BuiltinKey::SourceExt
            | BuiltinKey::SourceSize
            | BuiltinKey::SourceMtime
            | BuiltinKey::SourceRelPath
            | BuiltinKey::SourceId
            | BuiltinKey::SourceDevice
            | BuiltinKey::SourceInode
            | BuiltinKey::Size
            | BuiltinKey::Mtime
            | BuiltinKey::RootId
            | BuiltinKey::Id => BuiltinKeyCategory::BuiltIn,

            // Derived: computed from other data
            BuiltinKey::SourcePath
            | BuiltinKey::SourceRoot
            | BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Ext => BuiltinKeyCategory::Derived,

            // Stored: lives in facts table
            BuiltinKey::Hash | BuiltinKey::HashShort | BuiltinKey::ContentHashSha256 => {
                BuiltinKeyCategory::Stored
            }
        }
    }

    /// Get a human-readable description for this key
    pub fn description(&self) -> Option<&'static str> {
        match self {
            BuiltinKey::SourceExt => Some("File extension"),
            BuiltinKey::SourceSize | BuiltinKey::Size => Some("File size in bytes"),
            BuiltinKey::SourceMtime | BuiltinKey::Mtime => Some("File modification time"),
            BuiltinKey::SourcePath => Some("Full absolute path"),
            BuiltinKey::SourceRoot => Some("Root path"),
            BuiltinKey::SourceRelPath => Some("Relative path from root"),
            BuiltinKey::SourceId => Some("Source database ID"),
            BuiltinKey::SourceDevice => Some("Device ID"),
            BuiltinKey::SourceInode => Some("Inode number"),
            BuiltinKey::Filename => Some("Filename (last path component)"),
            BuiltinKey::Stem => Some("Filename without extension"),
            BuiltinKey::Ext => Some("File extension"),
            BuiltinKey::Hash => Some("Content hash"),
            BuiltinKey::HashShort => Some("Content hash (short)"),
            BuiltinKey::Id => Some("Source ID"),
            BuiltinKey::RootId => Some("Root ID"),
            BuiltinKey::ContentHashSha256 => Some("SHA-256 content hash"),
        }
    }

    /// Get the fact type for this key
    pub fn fact_type(&self) -> FactType {
        match self {
            BuiltinKey::SourceExt
            | BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Ext
            | BuiltinKey::Hash
            | BuiltinKey::HashShort
            | BuiltinKey::ContentHashSha256 => FactType::Text,

            BuiltinKey::SourceSize
            | BuiltinKey::Size
            | BuiltinKey::SourceId
            | BuiltinKey::SourceDevice
            | BuiltinKey::SourceInode
            | BuiltinKey::Id
            | BuiltinKey::RootId => FactType::Num,

            BuiltinKey::SourceMtime | BuiltinKey::Mtime => FactType::Time,

            BuiltinKey::SourcePath | BuiltinKey::SourceRoot | BuiltinKey::SourceRelPath => {
                FactType::Path
            }
        }
    }

    /// Try to parse a string as a built-in key
    pub fn from_str(s: &str) -> Option<BuiltinKey> {
        use strum::IntoEnumIterator;
        for key in BuiltinKey::iter() {
            let name: &'static str = key.into();
            if name == s {
                return Some(key);
            }
        }
        None
    }
}

/// Check if a key is a built-in (recognized without checking facts table)
pub fn is_builtin_key(key: &str) -> bool {
    BuiltinKey::from_str(key).is_some()
}

// ============================================================================
// ParsedFactKey
// ============================================================================

/// A parsed fact key with optional accessor and modifiers.
///
/// Wraps the result of `parse_key_with_modifiers()` into a reusable struct.
/// Used when the same parsed key needs to be applied to multiple sources,
/// such as in grouped distribution or manifest generation.
///
/// ## Example
///
/// ```ignore
/// use crate::expr::ParsedFactKey;
///
/// let key = ParsedFactKey::parse("source.mtime|year")?;
/// assert_eq!(key.base_key, "source.mtime");
/// assert!(key.is_builtin());
/// assert!(key.has_transforms());
/// ```
#[derive(Debug, Clone)]
pub struct ParsedFactKey {
    /// Original key string for display (e.g., "source.mtime|year")
    pub raw: String,
    /// Base fact key after normalization (e.g., "source.mtime")
    pub base_key: String,
    /// Optional path accessor (e.g., [-1] for last segment)
    pub accessor: Option<PathAccessor>,
    /// Modifiers to apply (e.g., [Year])
    pub modifiers: Vec<ModifierCall>,
}

impl ParsedFactKey {
    /// Parse a key string into its components.
    ///
    /// Keys without a namespace prefix are normalized to `content.*`
    /// (e.g., "Make" becomes "content.Make"). Built-in keys (source.*, filename, etc.)
    /// are not modified.
    pub fn parse(key: &str) -> Result<Self> {
        let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;
        Ok(Self {
            raw: key.to_string(),
            base_key,
            accessor,
            modifiers,
        })
    }

    /// Check if this key refers to a built-in fact.
    pub fn is_builtin(&self) -> bool {
        BuiltinKey::from_str(&self.base_key).is_some()
    }

    /// Check if this key has transforms (accessor or modifiers).
    pub fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }
}

/// Get pattern expansion for an alias (if it exists)
fn expand_alias(name: &str) -> Option<&'static str> {
    BuiltinKey::from_str(name).and_then(|k| k.expansion())
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
            for c in chars.by_ref() {
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
pub fn parse_key_and_accessor(s: &str) -> Result<(String, Option<PathAccessor>)> {
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
            .map_err(|_| anyhow::anyhow!("Invalid index: '{s}'"))?;
        Ok(PathAccessor::Index(index))
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

/// Known namespace prefixes for fact keys
const KNOWN_PREFIXES: &[&str] = &["source.", "content.", "policy.", "object."];

/// Normalize a base fact key by adding `content.` prefix if needed.
/// Returns the key unchanged if it's a built-in key or already has a namespace prefix.
pub fn normalize_fact_key(key: &str) -> String {
    // Check if it's a built-in key (source.ext, filename, etc.)
    if BuiltinKey::from_str(key).is_some() {
        return key.to_string();
    }

    // Check if it already has a known namespace prefix
    for prefix in KNOWN_PREFIXES {
        if key.starts_with(prefix) {
            return key.to_string();
        }
    }

    // Add content. prefix
    format!("content.{key}")
}

/// Normalize a full key string that may contain accessors and modifiers.
/// E.g., "Make|year" becomes "content.Make|year", but "source.mtime|year" stays unchanged.
pub fn normalize_key_string(key: &str) -> String {
    // Split off modifiers first
    let parts: Vec<&str> = key.split('|').collect();
    let key_part = parts[0];

    // Split off accessor if present (e.g., "key[-1]" -> "key", "[-1]")
    let (base_part, accessor_part) = if let Some(bracket_pos) = key_part.find('[') {
        (&key_part[..bracket_pos], &key_part[bracket_pos..])
    } else {
        (key_part, "")
    };

    // Normalize the base part
    let normalized_base = normalize_fact_key(base_part);

    // Reconstruct with accessor and modifiers
    let mut result = normalized_base;
    result.push_str(accessor_part);
    for modifier in &parts[1..] {
        result.push('|');
        result.push_str(modifier);
    }
    result
}

/// Parse a key string that may contain accessors and modifiers: "source.rel_path[-1]|stem"
/// Returns (base_key, accessor, modifiers)
///
/// Keys without a namespace prefix are normalized to `content.*` (e.g., "Make" becomes "content.Make").
/// Built-in keys (source.*, filename, etc.) are not modified.
pub fn parse_key_with_modifiers(
    key: &str,
) -> Result<(String, Option<PathAccessor>, Vec<ModifierCall>)> {
    // Split by | first to separate modifiers
    let parts: Vec<&str> = key.split('|').collect();
    let key_part = parts[0];

    // Parse accessor from the key part
    let (base_key, accessor) = parse_key_and_accessor(key_part)?;

    // Normalize the base key (add content. prefix if needed)
    let normalized_key = normalize_fact_key(&base_key);

    // Parse modifiers
    let mut modifiers = Vec::new();
    for mod_str in &parts[1..] {
        let modifier_call = parse_modifier(mod_str.trim())?;
        modifiers.push(modifier_call);
    }

    Ok((normalized_key, accessor, modifiers))
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

    // Normalize to a clean relative path (strip leading '/', collapse '//', remove '.' segments)
    let result = normalize_pattern_result(&result);

    Ok(result)
}

/// Normalize a pattern result to a clean relative path.
/// Strips leading '/', collapses '//' to '/', removes '.' components.
/// The existing '..' sanitization (replaced with '_') runs before this.
fn normalize_pattern_result(path: &str) -> String {
    path.split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect::<Vec<_>>()
        .join("/")
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

    // Apply modifiers in order (for_display: false since patterns are used for paths)
    let mut result = value;
    for modifier_call in &expr.modifiers {
        result = apply_modifier(&result, modifier_call, &expr.key, false)?;
    }

    // Convert to string
    Ok(fact_value_to_string(&result))
}

/// Get a fact value by key, handling derived facts
fn get_value(key: &str, ctx: &EvalContext) -> Result<FactValue> {
    // Handle scope.rel_path specially (not a BuiltinKey)
    if key == "scope.rel_path" {
        // Derived: strip scope prefix from full path
        match (&ctx.scope_prefix, &ctx.source_root, &ctx.source_rel_path) {
            (Some(scope), Some(root), Some(rel_path)) => {
                let full_path = if rel_path.is_empty() {
                    root.clone()
                } else {
                    format!("{root}/{rel_path}")
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
            (None, _, _) => bail!(
                "scope.rel_path not available (no scope was specified during manifest generation)"
            ),
            _ => bail!("scope.rel_path not available"),
        }
    }

    // Handle built-in keys via enum
    if let Some(builtin) = BuiltinKey::from_str(key) {
        match builtin {
            BuiltinKey::SourceRelPath => {
                if let Some(ref rel_path) = ctx.source_rel_path {
                    return Ok(FactValue::Path(rel_path.clone()));
                }
                bail!("source.rel_path not available");
            }
            BuiltinKey::SourceRoot => {
                if let Some(ref root) = ctx.source_root {
                    return Ok(FactValue::Path(root.clone()));
                }
                bail!("source.root not available");
            }
            BuiltinKey::SourcePath => {
                // Derived: root + "/" + rel_path
                match (&ctx.source_root, &ctx.source_rel_path) {
                    (Some(root), Some(rel_path)) => {
                        let full = if rel_path.is_empty() {
                            root.clone()
                        } else {
                            format!("{root}/{rel_path}")
                        };
                        return Ok(FactValue::Path(full));
                    }
                    _ => bail!("source.path not available (requires root and rel_path)"),
                }
            }
            // Other builtin keys are looked up in facts or not available in patterns
            _ => {}
        }
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
fn fact_value_to_string(value: &FactValue) -> String {
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
            assert_eq!(e.modifiers[0].modifier, Modifier::Year);
            assert!(e.modifiers[0].args.is_empty());
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
        ctx.set_fact(
            "object.hash",
            FactValue::Text("abcdef1234567890".to_string()),
        );
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "abcdef12");
    }

    #[test]
    fn test_complex_pattern() {
        let pattern =
            parse_pattern("{source.rel_path[0]}/{source.mtime|year}/{stem}_{hash_short}.{ext}")
                .unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("vacation/photos/IMG_001.jpg".to_string());
        ctx.set_fact("source.mtime", FactValue::Time(1718452800));
        ctx.set_fact(
            "object.hash",
            FactValue::Text("abcdef1234567890".to_string()),
        );
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation/2024/IMG_001_abcdef12.jpg");
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

    // =========================================================================
    // ParsedFactKey tests
    // =========================================================================

    #[test]
    fn parsed_key_simple() {
        let key = ParsedFactKey::parse("source.ext").unwrap();
        assert_eq!(key.raw, "source.ext");
        assert_eq!(key.base_key, "source.ext");
        assert!(key.accessor.is_none());
        assert!(key.modifiers.is_empty());
        assert!(key.is_builtin());
        assert!(!key.has_transforms());
    }

    #[test]
    fn parsed_key_with_accessor() {
        let key = ParsedFactKey::parse("source.rel_path[-1]").unwrap();
        assert_eq!(key.raw, "source.rel_path[-1]");
        assert_eq!(key.base_key, "source.rel_path");
        assert!(matches!(key.accessor, Some(PathAccessor::Index(-1))));
        assert!(key.modifiers.is_empty());
        assert!(key.is_builtin());
        assert!(key.has_transforms());
    }

    #[test]
    fn parsed_key_with_modifier() {
        let key = ParsedFactKey::parse("source.mtime|year").unwrap();
        assert_eq!(key.raw, "source.mtime|year");
        assert_eq!(key.base_key, "source.mtime");
        assert!(key.accessor.is_none());
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].modifier, Modifier::Year);
        assert!(key.is_builtin());
        assert!(key.has_transforms());
    }

    #[test]
    fn parsed_key_complex() {
        let key = ParsedFactKey::parse("source.rel_path[-1]|stem").unwrap();
        assert_eq!(key.raw, "source.rel_path[-1]|stem");
        assert_eq!(key.base_key, "source.rel_path");
        assert!(matches!(key.accessor, Some(PathAccessor::Index(-1))));
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].modifier, Modifier::Stem);
        assert!(key.is_builtin());
        assert!(key.has_transforms());
    }

    #[test]
    fn parsed_key_stored_fact_normalized() {
        // A stored fact key without prefix gets content. added
        let key = ParsedFactKey::parse("Make").unwrap();
        assert_eq!(key.raw, "Make");
        assert_eq!(key.base_key, "content.Make");
        assert!(!key.is_builtin());
        assert!(!key.has_transforms());
    }

    #[test]
    fn parsed_key_stored_fact_with_modifier() {
        let key = ParsedFactKey::parse("DateTimeOriginal|yearmonth").unwrap();
        assert_eq!(key.raw, "DateTimeOriginal|yearmonth");
        assert_eq!(key.base_key, "content.DateTimeOriginal");
        assert!(key.accessor.is_none());
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].modifier, Modifier::YearMonth);
        assert!(!key.is_builtin());
        assert!(key.has_transforms());
    }

    // =========================================================================
    // normalize_pattern_result tests
    // =========================================================================

    #[test]
    fn normalize_pattern_result_normal_filename() {
        assert_eq!(normalize_pattern_result("5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_normal_subdir() {
        assert_eq!(
            normalize_pattern_result("subdir/file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_leading_slash() {
        assert_eq!(normalize_pattern_result("/5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_double_leading_slash() {
        assert_eq!(normalize_pattern_result("//5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_interior_double_slash() {
        assert_eq!(
            normalize_pattern_result("subdir//file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_leading_dot_slash() {
        assert_eq!(
            normalize_pattern_result("./subdir/file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_interior_dot() {
        assert_eq!(
            normalize_pattern_result("subdir/./file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_empty() {
        assert_eq!(normalize_pattern_result(""), "");
    }

    #[test]
    fn normalize_pattern_result_lone_slash() {
        assert_eq!(normalize_pattern_result("/"), "");
    }

    #[test]
    fn normalize_pattern_result_triple_slash() {
        assert_eq!(normalize_pattern_result("///"), "");
    }

    #[test]
    fn normalize_pattern_result_multi_level() {
        assert_eq!(normalize_pattern_result("a/b/c"), "a/b/c");
    }

    // =========================================================================
    // Pattern normalization integration tests
    // =========================================================================

    #[test]
    fn test_pattern_flat_file_no_absolute_path() {
        // Source with single-component rel_path (flat file, no subdirectory)
        // Pattern: {source.rel_path[:-1]}/{filename}
        // rel_path = "5.avi", rel_path[:-1] = "" (empty)
        // Raw concatenation: "/5.avi" -> Normalized: "5.avi"
        let pattern = parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("5.avi".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "5.avi");
    }

    #[test]
    fn test_pattern_subdirectory_file_unchanged() {
        // Source with multi-component rel_path — normalization should not change valid paths
        // Pattern: {source.rel_path[:-1]}/{filename}
        // rel_path = "subdir/file.jpg", rel_path[:-1] = "subdir"
        // Result: "subdir/file.jpg" — unchanged by normalization
        let pattern = parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("subdir/file.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "subdir/file.jpg");
    }
}
