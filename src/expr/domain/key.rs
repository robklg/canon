//! The key vocabulary: what a fact key may name, and how a written key is read.
//!
//! A key is how the language points at a value — `source.mtime`, `filename`,
//! `Make`. Built-in keys are answered from a source's own columns and are valid
//! before any enrichment has run; everything else is looked up in the facts
//! table, under a `content.` prefix the writer may leave off. Parsing a key
//! splits it into that base name, an optional path accessor, and a modifier
//! chain — the same reading for both halves of the language.

use anyhow::{bail, Result};

use super::transform::{parse_modifier, ModifierCall, PathAccessor};
use crate::core::domain::fact::FactType;

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
pub(super) fn expand_alias(name: &str) -> Option<&'static str> {
    BuiltinKey::from_str(name).and_then(|k| k.expansion())
}

// ============================================================================
// Key Parsing
// ============================================================================

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::transform::Modifier;
    use super::*;

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

    #[test]
    fn every_builtin_key_is_reachable_under_a_distinct_name() {
        // A built-in key's serialized string is the word a user types, so the
        // set of them is the vocabulary Canon promises. Two keys answering to
        // one name makes the second unreachable, and editing any one of these
        // strings retires a word from the language — pinned here so that is a
        // decision someone makes, not a rename that happens to compile.
        use strum::IntoEnumIterator;

        let mut names: Vec<&'static str> = Vec::new();
        for key in BuiltinKey::iter() {
            let name: &'static str = key.into();
            assert_eq!(
                BuiltinKey::from_str(name),
                Some(key),
                "'{name}' does not resolve back to the key it names"
            );
            assert!(
                !names.contains(&name),
                "two built-in keys answer to '{name}'"
            );
            names.push(name);
        }

        names.sort_unstable();
        assert_eq!(
            names,
            [
                "content.hash.sha256",
                "ext",
                "filename",
                "hash",
                "hash_short",
                "id",
                "mtime",
                "root_id",
                "size",
                "source.device",
                "source.ext",
                "source.id",
                "source.inode",
                "source.mtime",
                "source.path",
                "source.rel_path",
                "source.root",
                "source.size",
                "stem",
            ]
        );
    }
}
