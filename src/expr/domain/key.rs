//! The key vocabulary: what a fact key may name, and how a written key is read.
//!
//! A key is how the language points at a value — `source.mtime`, `filename`,
//! `Make`. Built-in keys are answered from a source's own columns and are valid
//! before any enrichment has run; everything else is looked up in the facts
//! table, under a `content.` prefix the writer may leave off. Parsing a key
//! splits it into that base name, an optional path accessor, and a modifier
//! chain — the same reading for both halves of the language.

use anyhow::{bail, Result};

use super::bind::{check_read_as, ReadAs, Refused};
use super::transform::{apply_chain, Modified, ModifierCall, PathAccessor};
use crate::core::domain::fact::{FactType, FactValue};
use crate::core::domain::time::zone::UserZone;

// ============================================================================
// The pattern-only key
// ============================================================================

/// The one spelling of the pattern-only key `scope.rel_path`.
///
/// Not a `BuiltinKey`: it is not a fact and never appears in a filter. It is
/// derived at pattern-evaluation time from the scope the manifest records,
/// which a filter has no access to — so admitting it to the built-in
/// vocabulary would widen what a `--where` clause accepts, for a key that
/// could not mean anything there.
pub const SCOPE_REL_PATH: &str = "scope.rel_path";

/// The spelling of the pattern-only key `object.hash`, for every site that
/// writes the key itself.
///
/// Not a `BuiltinKey` either, and for a different reason than
/// `scope.rel_path`: it names content identity, which no source column
/// answers. A pattern gets it from the lock entry the manifest recorded; a
/// filter asks `content.hash.sha256` instead. It has a name here because
/// several places wrote the literal before this const existed.
///
/// One literal survives, below: `HashShort`'s expansion is `"object.hash|short"`,
/// a composed *expression* rather than a naming of the key, and a
/// `&'static str` return cannot be built from a const and a suffix. Stated
/// rather than left for a reader to notice — a const claiming to be the only
/// spelling while one sits eighty lines under it is the overstatement this
/// facility exists to avoid.
pub const OBJECT_HASH: &str = "object.hash";

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
            BuiltinKey::Hash => Some(OBJECT_HASH),
            BuiltinKey::HashShort => Some("object.hash|short"),
            BuiltinKey::Id => Some("source.id"),
            _ => None,
        }
    }

    /// The stored key, and the modifier applied before any the writer added,
    /// that this key is read as outside patterns — where it is an alias of a
    /// stored fact rather than a name for itself.
    ///
    /// The two hash aliases name content identity, which no source column
    /// answers. A pattern reads them from the lock entry (`expansion`, above);
    /// a filter or a `facts` report reads the stored content hash. Without
    /// this the alias reached the stored-fact lookup under its own name, where
    /// no fact is ever named `hash`: every comparison was false, and `NOT`
    /// turned it into every source.
    pub(super) fn reading(&self) -> Option<(&'static str, Option<&'static str>)> {
        match self {
            BuiltinKey::Hash => Some((BuiltinKey::ContentHashSha256.into(), None)),
            BuiltinKey::HashShort => Some((BuiltinKey::ContentHashSha256.into(), Some("short"))),

            // Exhaustive, like `is_computed`: a new built-in says whether it
            // is read as something else. A target here must have no reading
            // of its own — the filter applies this once at parse and again
            // at evaluation (`a_reading_is_applied_once_however_often_the_key_is_read`).
            BuiltinKey::SourceExt
            | BuiltinKey::SourceSize
            | BuiltinKey::SourceMtime
            | BuiltinKey::SourcePath
            | BuiltinKey::SourceRoot
            | BuiltinKey::SourceRelPath
            | BuiltinKey::SourceId
            | BuiltinKey::SourceDevice
            | BuiltinKey::SourceInode
            | BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Ext
            | BuiltinKey::Id
            | BuiltinKey::Size
            | BuiltinKey::Mtime
            | BuiltinKey::RootId
            | BuiltinKey::ContentHashSha256 => None,
        }
    }

    /// Whether the source itself answers this key, or the facts table does.
    ///
    /// This is the conjugation the context-supplied law reads: a key the
    /// evaluation context answers is never fetched from the facts table, so a
    /// stored fact cannot reach evaluation wearing a built-in's name and move
    /// where files land.
    ///
    /// Exhaustive on purpose — no `_` arm. A new built-in must say which side
    /// it falls on or the build refuses it, because the alternative is a new
    /// key defaulting into a classification nobody chose. The classification
    /// is not free to drift from the resolver either:
    /// `a_key_is_computed_exactly_when_the_resolver_answers_it` asserts this
    /// function agrees with `get_builtin_value` across the whole enum.
    pub fn is_computed(&self) -> bool {
        match self {
            BuiltinKey::SourceExt
            | BuiltinKey::SourceSize
            | BuiltinKey::SourceMtime
            | BuiltinKey::SourcePath
            | BuiltinKey::SourceRoot
            | BuiltinKey::SourceRelPath
            | BuiltinKey::SourceId
            | BuiltinKey::SourceDevice
            | BuiltinKey::SourceInode
            | BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Ext
            | BuiltinKey::Id
            | BuiltinKey::Size
            | BuiltinKey::Mtime
            | BuiltinKey::RootId => true,

            // Need an object lookup, not derivable from the source alone.
            BuiltinKey::Hash | BuiltinKey::HashShort | BuiltinKey::ContentHashSha256 => false,
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

// ============================================================================
// ParsedFactKey
// ============================================================================

/// A parsed fact key with optional accessor and modifiers.
///
/// What `expr::read_report` gives a report for each key: the key as written, read through the one key
/// reader (`filter::WrittenKey::read`) and checked against its type, then applied to every
/// source a distribution or a grouping counts.
///
/// ## Example
///
/// ```ignore
/// use crate::expr::read_report;
///
/// let key = read_report(conn, Some("source.mtime|year"), &[], &[])?.key.unwrap();
/// assert_eq!(key.base_key, "source.mtime");
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
    /// What a value read through this key's chain may be, as the key stood when the report
    /// read it.
    pub(in crate::expr) read_as: ReadAs,
}

impl ParsedFactKey {
    /// Parse a key string into its components.
    ///
    /// Keys without a kept prefix (`source.`, `content.`) are normalized to `content.*`
    /// (e.g., "Make" becomes "content.Make"). Built-in keys (source.*, filename, etc.)
    /// are not modified, except the hash aliases, which read as the stored
    /// content hash (`BuiltinKey::reading`).
    #[cfg(test)]
    pub fn parse(key: &str) -> Result<Self> {
        let read = super::filter::WrittenKey::read(key)?;
        Ok(Self {
            raw: key.to_string(),
            base_key: read.base,
            accessor: read.accessor,
            modifiers: read.modifiers,
            read_as: ReadAs::Any,
        })
    }

    /// A stored value through this key's accessor and modifiers, as a report counts it. The
    /// value's type is checked first ([`Self::check_type`]), so a key whose type changed while
    /// the report ran stops it as that answer, never as a modifier failing.
    pub fn report_value(&self, value: FactValue, zone: &UserZone) -> Result<Modified> {
        self.check_type(&value)?;
        apply_chain(
            value,
            self.accessor.as_ref(),
            &self.modifiers,
            &self.raw,
            true,
            zone,
        )
    }

    /// Whether a stored value is what the key was read as ([`check_read_as`]): a key with an
    /// accessor or modifiers was read as its one type, or as having no facts; a bare key reads
    /// every value it meets, of whatever type.
    pub(in crate::expr) fn check_type(&self, value: &FactValue) -> Result<()> {
        check_read_as(self.read_as, &self.base_key, value, Refused::Key)
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
        #[expect(clippy::string_slice, reason = "a `find` position")]
        let key = s[..bracket_start].to_string();
        #[expect(clippy::string_slice, reason = "a `find` position")]
        let rest = &s[bracket_start..];

        if !rest.ends_with(']') {
            bail!("Unclosed '[' in accessor");
        }
        #[expect(
            clippy::string_slice,
            reason = "inside a leading `[` and a trailing `]`, both ASCII"
        )]
        let inner = &rest[1..rest.len() - 1];

        let accessor = parse_accessor(inner)?;
        Ok((key, Some(accessor)))
    } else {
        Ok((s.to_string(), None))
    }
}

/// Parse accessor content: `-1`, `2`, `1:3`, `-3:-1`, `:3`, `1:`
pub(super) fn parse_accessor(s: &str) -> Result<PathAccessor> {
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

/// The prefixes a written key keeps as written: the ones a stored key can
/// carry. Import stores every key under `content.` and refuses `source.`,
/// which the source row answers — so any other prefix is read the way import
/// stored it, under `content.`. `object.` names a key only in patterns
/// (`OBJECT_HASH`), which never reach this reader. A prefix listed here that
/// no writer produces makes a filter look for a name no fact has, and select
/// nothing in silence — which `policy.` did once its one fact moved to the
/// exclusion columns.
const KNOWN_PREFIXES: &[&str] = &["source.", "content."];

/// Normalize a base fact key by adding `content.` prefix if needed.
/// Returns the key unchanged if it's a built-in key or carries a kept prefix (`KNOWN_PREFIXES`).
pub fn normalize_fact_key(key: &str) -> String {
    // Check if it's a built-in key (source.ext, filename, etc.)
    if BuiltinKey::from_str(key).is_some() {
        return key.to_string();
    }

    // Check if it already carries a kept prefix
    for prefix in KNOWN_PREFIXES {
        if key.starts_with(prefix) {
            return key.to_string();
        }
    }

    // Add content. prefix
    format!("content.{key}")
}

/// Read a written base key as the key the asking half evaluates: an alias of
/// a stored fact becomes that fact's key plus the modifier it implies
/// (`BuiltinKey::reading`), and any other key is normalized. The one place
/// both key readers below take the alias from, so the filter's text form and
/// the parsed form cannot disagree.
pub(super) fn read_base_key(key: &str) -> (String, Option<&'static str>) {
    match BuiltinKey::from_str(key).and_then(|k| k.reading()) {
        Some((stored, leading_modifier)) => (stored.to_string(), leading_modifier),
        None => (normalize_fact_key(key), None),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::transform::Modifier;
    use super::*;

    /// The kept prefixes are the ones a stored key can carry: `content.`,
    /// which import writes, and `source.`, which the source row answers and
    /// import refuses. This is a change detector, not a derivation: any edit
    /// to the list fails here, and the editor owes the check that import
    /// stores keys under the new prefix as written — the agreement itself is
    /// pinned beside import (`facts/domain.rs`) over sampled prefixes only.
    #[test]
    fn the_kept_prefixes_are_the_ones_a_stored_key_can_carry() {
        assert_eq!(KNOWN_PREFIXES, &["source.", "content."]);
    }

    /// A reading turns an alias into its stored key once, through the one key reader
    /// (`WrittenKey::read`) that `--where`, `--key` and `--group-by` share. It holds because a target has no reading of its own; a doubled `short` would
    /// hide, since cutting a cut hash changes nothing, so the modifier count is what is
    /// asserted.
    #[test]
    fn a_reading_is_applied_once_however_often_the_key_is_read() {
        use strum::IntoEnumIterator;

        for builtin in BuiltinKey::iter() {
            let Some((target, leading)) = builtin.reading() else {
                continue;
            };
            let name: &'static str = builtin.into();
            assert!(
                BuiltinKey::from_str(target).is_some_and(|t| t.reading().is_none()),
                "{name} reads as {target}, which has a reading of its own"
            );
            let filter = super::super::filter::WrittenKey::read(name).unwrap();
            assert_eq!(filter.base, target, "{name}");
            assert_eq!(
                filter.modifiers.len(),
                usize::from(leading.is_some()),
                "{name}"
            );
        }
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
        assert!(BuiltinKey::from_str(&key.base_key).is_some());
        assert!(!key.has_transforms());
    }

    #[test]
    fn parsed_key_with_accessor() {
        let key = ParsedFactKey::parse("source.rel_path[-1]").unwrap();
        assert_eq!(key.raw, "source.rel_path[-1]");
        assert_eq!(key.base_key, "source.rel_path");
        assert!(matches!(key.accessor, Some(PathAccessor::Index(-1))));
        assert!(key.modifiers.is_empty());
        assert!(BuiltinKey::from_str(&key.base_key).is_some());
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
        assert!(BuiltinKey::from_str(&key.base_key).is_some());
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
        assert!(BuiltinKey::from_str(&key.base_key).is_some());
        assert!(key.has_transforms());
    }

    #[test]
    fn parsed_key_stored_fact_normalized() {
        // A stored fact key without prefix gets content. added
        let key = ParsedFactKey::parse("Make").unwrap();
        assert_eq!(key.raw, "Make");
        assert_eq!(key.base_key, "content.Make");
        assert!(BuiltinKey::from_str(&key.base_key).is_none());
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
        assert!(BuiltinKey::from_str(&key.base_key).is_none());
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
