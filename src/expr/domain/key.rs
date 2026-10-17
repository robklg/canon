//! The key vocabulary: what a key's name may name, and how a written key is read.
//!
//! A key is how the language points at a value — `source.mtime`, `filename`,
//! `Make`. There are three kinds. A **source fact** (`source.*`) is read from
//! what the scanner records: a fixed set, the same for every source, never
//! imported. A **content fact** (`content.*`) is imported, or recorded when
//! Canon hashes, and is looked up in the facts table under a `content.` prefix
//! the writer may leave off. A **built-in key** is neither: a short name for a
//! value people use often, which may have the same value as another key
//! (`size` is `source.size`'s value, `hash` the stored content hash).
//!
//! `read_key_name` is the one reader of a key's name, and each command applies
//! its own rule to the readings it gets. Parsing a written key splits it into
//! that name, an optional path accessor, and a modifier chain — the same
//! reading for both halves of the language.

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
/// Not a `Key`: it is not a fact and never appears in a filter. It is
/// derived at pattern-evaluation time from the scope the manifest records,
/// which a filter has no access to — so admitting it to the key vocabulary
/// would widen what a `--where` clause accepts, for a key that could not mean
/// anything there.
pub const SCOPE_REL_PATH: &str = "scope.rel_path";

/// The spelling of the pattern-only key `object.hash`, for every site that
/// writes the key itself.
///
/// Not a `Key` either, and for a different reason than `scope.rel_path`: it
/// names content identity, which no source column answers. A pattern gets it
/// from the lock entry the manifest recorded; a filter asks
/// `content.hash.sha256` instead. It has a name here because several places
/// wrote the literal before this const existed.
///
/// One literal survives, below: `HashShort`'s expansion is `"object.hash|short"`,
/// a composed *expression* rather than a naming of the key, and a
/// `&'static str` return cannot be built from a const and a suffix. Stated
/// rather than left for a reader to notice — a const claiming to be the only
/// spelling while one sits eighty lines under it is the overstatement this
/// facility exists to avoid.
pub const OBJECT_HASH: &str = "object.hash";

// ============================================================================
// The three kinds of key
// ============================================================================

/// The content hash's key: a content fact, recorded when Canon hashes a source,
/// whose type (text) is known before any fact of it exists.
pub const CONTENT_HASH: &str = "content.hash.sha256";

/// A source fact: a fact about a source itself, read from what the scanner
/// records. A fixed set, the same for every source; never imported, never
/// deleted on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
pub enum SourceFact {
    #[strum(serialize = "source.ext")]
    Ext,
    #[strum(serialize = "source.size")]
    Size,
    #[strum(serialize = "source.mtime")]
    Mtime,
    #[strum(serialize = "source.path")]
    Path,
    #[strum(serialize = "source.root")]
    Root,
    #[strum(serialize = "source.rel_path")]
    RelPath,
    #[strum(serialize = "source.id")]
    Id,
    #[strum(serialize = "source.device")]
    Device,
    #[strum(serialize = "source.inode")]
    Inode,
}

impl SourceFact {
    fn fact_type(self) -> FactType {
        match self {
            SourceFact::Ext => FactType::Text,
            SourceFact::Size | SourceFact::Id | SourceFact::Device | SourceFact::Inode => {
                FactType::Num
            }
            SourceFact::Mtime => FactType::Time,
            SourceFact::Path | SourceFact::Root | SourceFact::RelPath => FactType::Path,
        }
    }

    fn description(self) -> &'static str {
        match self {
            SourceFact::Ext => "File extension",
            SourceFact::Size => "File size in bytes",
            SourceFact::Mtime => "File modification time",
            SourceFact::Path => "Full absolute path",
            SourceFact::Root => "Root path",
            SourceFact::RelPath => "Relative path from root",
            SourceFact::Id => "Source database ID",
            SourceFact::Device => "Device ID",
            SourceFact::Inode => "Inode number",
        }
    }
}

/// A built-in key: a short name for a value people use often, neither a source
/// fact nor a content fact. It may have the same value as another key.
///
/// In patterns the first six are also aliases with expansions (`expansion`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
pub enum BuiltinKey {
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
    #[strum(serialize = "size")]
    Size,
    #[strum(serialize = "mtime")]
    Mtime,
    #[strum(serialize = "root_id")]
    RootId,
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
            BuiltinKey::Size | BuiltinKey::Mtime | BuiltinKey::RootId => None,
        }
    }

    /// The source fact this built-in key has the value of, where it is a short
    /// name for one (`size` is `source.size`). The one place the pairing is
    /// spoken: every reader of the pairing reads it. Exhaustive, so a new
    /// built-in key says whether it is one.
    pub fn source_fact(self) -> Option<SourceFact> {
        match self {
            BuiltinKey::Ext => Some(SourceFact::Ext),
            BuiltinKey::Size => Some(SourceFact::Size),
            BuiltinKey::Mtime => Some(SourceFact::Mtime),
            BuiltinKey::Id => Some(SourceFact::Id),
            BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Hash
            | BuiltinKey::HashShort
            | BuiltinKey::RootId => None,
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
            BuiltinKey::Hash => Some((CONTENT_HASH, None)),
            BuiltinKey::HashShort => Some((CONTENT_HASH, Some("short"))),

            // Exhaustive, like `Key::is_computed`: a new built-in key says
            // whether it is read as something else. A target here must have no
            // reading of its own — the filter applies this once at parse and
            // again at evaluation
            // (`a_reading_is_applied_once_however_often_the_key_is_read`).
            BuiltinKey::Filename
            | BuiltinKey::Stem
            | BuiltinKey::Ext
            | BuiltinKey::Id
            | BuiltinKey::Size
            | BuiltinKey::Mtime
            | BuiltinKey::RootId => None,
        }
    }

    /// A key that is a short name for a source fact has that source fact's
    /// type (`source_fact`); the others name their own.
    fn fact_type(self) -> FactType {
        if let Some(fact) = self.source_fact() {
            return fact.fact_type();
        }
        match self {
            BuiltinKey::Filename | BuiltinKey::Stem | BuiltinKey::Hash | BuiltinKey::HashShort => {
                FactType::Text
            }
            BuiltinKey::RootId => FactType::Num,
            BuiltinKey::Ext | BuiltinKey::Id | BuiltinKey::Size | BuiltinKey::Mtime => {
                unreachable!("typed through its source fact")
            }
        }
    }

    fn description(self) -> &'static str {
        match self {
            BuiltinKey::Filename => "Filename (last path component)",
            BuiltinKey::Stem => "Filename without extension",
            BuiltinKey::Ext => "File extension",
            BuiltinKey::Hash => "Content hash",
            BuiltinKey::HashShort => "Content hash (short)",
            BuiltinKey::Id => "Source ID",
            BuiltinKey::Size => "File size in bytes",
            BuiltinKey::Mtime => "File modification time",
            BuiltinKey::RootId => "Root ID",
        }
    }
}

// ============================================================================
// The name reader
// ============================================================================

/// What a key's name can be read as: the source fact it names, the built-in key
/// it names, the content fact it names. A name may have more than one reading
/// (`filename` is a built-in key and, read under `content.`, a content fact), or
/// none (`source.extension`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyName {
    /// A `source.` name that is one of the source facts.
    pub source_fact: Option<SourceFact>,
    /// One of the built-in keys.
    pub builtin: Option<BuiltinKey>,
    /// The name read under `content.` (`Make` → `content.Make`, `content.Make`
    /// as it is); `None` for any `source.` name.
    pub content_key: Option<String>,
}

/// **The key-name law**: every command that takes a key's name reads it here,
/// and each applies its own rule to the readings it gets — an expression and
/// `--emit` take [`KeyName::in_expression`], import and `facts delete` the
/// content reading alone. Everything given is the name: no quoting, accessor,
/// modifier or alias is read.
pub fn read_key_name(name: &str) -> KeyName {
    use strum::IntoEnumIterator;

    let source_fact = SourceFact::iter().find(|fact| <&'static str>::from(*fact) == name);
    let builtin = BuiltinKey::iter().find(|key| <&'static str>::from(*key) == name);
    let content_key = if name.starts_with("source.") {
        None
    } else if name.starts_with("content.") {
        Some(name.to_string())
    } else {
        Some(format!("content.{name}"))
    };
    KeyName {
        source_fact,
        builtin,
        content_key,
    }
}

impl KeyName {
    /// The reading in an expression and at `--emit`: the source fact, else the
    /// built-in key, else the content fact; `None` for a `source.` name that is
    /// no source fact.
    pub fn in_expression(self) -> Option<Key> {
        let content = self.content_key.clone();
        self.in_pattern().or(content.map(Key::ContentFact))
    }

    /// The reading in a pattern: the source fact, else the built-in key, never
    /// the content fact — a pattern reads a stored key as written, with no
    /// `content.` default.
    pub(in crate::expr) fn in_pattern(self) -> Option<Key> {
        self.source_fact
            .map(Key::SourceFact)
            .or(self.builtin.map(Key::BuiltinKey))
    }
}

/// A key of one of the three kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    SourceFact(SourceFact),
    BuiltinKey(BuiltinKey),
    /// A content fact, by its full `content.` name.
    ContentFact(String),
}

/// Which of the three kinds a key is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    SourceFact,
    BuiltinKey,
    ContentFact,
}

/// Whether `canon facts` lists a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listing {
    /// Listed by default.
    Shown,
    /// Listed only with `--all`.
    Hidden,
    /// Never listed.
    NotListed,
}

impl Key {
    /// The key a name reads as in an expression —
    /// `read_key_name(name).in_expression()`. A key's own name reads back as
    /// the key, so a base key already read can be asked again here.
    pub fn read(name: &str) -> Option<Key> {
        read_key_name(name).in_expression()
    }

    /// The nine source facts, then the nine built-in keys.
    pub fn all() -> impl Iterator<Item = Key> {
        Key::all_typed().map(|(key, _, _)| key)
    }

    /// `Key::all` with each key's type and description, which every source fact
    /// and built-in key has without reading any facts — so a consumer listing
    /// the vocabulary needs no case for a key with none.
    pub fn all_typed() -> impl Iterator<Item = (Key, FactType, &'static str)> {
        use strum::IntoEnumIterator;

        SourceFact::iter()
            .map(|fact| (Key::SourceFact(fact), fact.fact_type(), fact.description()))
            .chain(
                BuiltinKey::iter()
                    .map(|key| (Key::BuiltinKey(key), key.fact_type(), key.description())),
            )
    }

    /// The source fact whose value this key has: a source fact itself, or a
    /// built-in key that is a short name for one (`BuiltinKey::source_fact`).
    pub fn as_source_fact(&self) -> Option<SourceFact> {
        match self {
            Key::SourceFact(fact) => Some(*fact),
            Key::BuiltinKey(key) => key.source_fact(),
            Key::ContentFact(_) => None,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Key::SourceFact(fact) => fact.into(),
            Key::BuiltinKey(key) => key.into(),
            Key::ContentFact(name) => name,
        }
    }

    pub fn kind(&self) -> KeyKind {
        match self {
            Key::SourceFact(_) => KeyKind::SourceFact,
            Key::BuiltinKey(_) => KeyKind::BuiltinKey,
            Key::ContentFact(_) => KeyKind::ContentFact,
        }
    }

    /// Whether the source row answers this key, or the facts table does.
    ///
    /// This is the conjugation the context-supplied law reads: a key the
    /// evaluation context answers is never fetched from the facts table, so a
    /// stored fact cannot reach evaluation wearing a built-in key's name and
    /// move where files land.
    ///
    /// Exhaustive on purpose — no `_` arm among the built-in keys. A new one
    /// must say which side it falls on or the build refuses it, because the
    /// alternative is a new key defaulting into a classification nobody chose.
    /// Every source fact is read from the source row, by what a source fact is.
    /// The classification is not free to drift from the resolver either:
    /// `a_key_is_computed_exactly_when_the_resolver_answers_it` asserts this
    /// function agrees with `source_row_value` over every key.
    pub fn is_computed(&self) -> bool {
        match self {
            Key::SourceFact(_) => true,
            Key::BuiltinKey(key) => match key {
                BuiltinKey::Filename
                | BuiltinKey::Stem
                | BuiltinKey::Ext
                | BuiltinKey::Id
                | BuiltinKey::Size
                | BuiltinKey::Mtime
                | BuiltinKey::RootId => true,

                // Read as the stored content hash (`BuiltinKey::reading`).
                BuiltinKey::Hash | BuiltinKey::HashShort => false,
            },
            Key::ContentFact(_) => false,
        }
    }

    /// The key's type where it is known without reading any of its facts: a
    /// source fact's and a built-in key's own, and the content hash's (text).
    /// `None`: read it from the key's facts.
    pub fn known_type(&self) -> Option<FactType> {
        match self {
            Key::SourceFact(fact) => Some(fact.fact_type()),
            Key::BuiltinKey(key) => Some(key.fact_type()),
            Key::ContentFact(name) => (name == CONTENT_HASH).then_some(FactType::Text),
        }
    }

    /// Whether `canon facts` lists this key. A content fact is listed where it
    /// has facts.
    pub fn listing(&self) -> Listing {
        match self {
            Key::SourceFact(fact) => match fact {
                SourceFact::Ext
                | SourceFact::Size
                | SourceFact::Mtime
                | SourceFact::Path
                | SourceFact::RelPath => Listing::Shown,
                SourceFact::Root | SourceFact::Id | SourceFact::Device | SourceFact::Inode => {
                    Listing::Hidden
                }
            },
            Key::BuiltinKey(key) => match key {
                BuiltinKey::Filename => Listing::Shown,
                BuiltinKey::Stem
                | BuiltinKey::Ext
                | BuiltinKey::Hash
                | BuiltinKey::HashShort
                | BuiltinKey::Id
                | BuiltinKey::Size
                | BuiltinKey::Mtime
                | BuiltinKey::RootId => Listing::NotListed,
            },
            Key::ContentFact(_) => Listing::Shown,
        }
    }

    /// A human-readable description: every source fact and built-in key has
    /// one, and of the content facts only the content hash.
    pub fn description(&self) -> Option<&'static str> {
        match self {
            Key::SourceFact(fact) => Some(fact.description()),
            Key::BuiltinKey(key) => Some(key.description()),
            Key::ContentFact(name) => (name == CONTENT_HASH).then_some("SHA-256 content hash"),
        }
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
    /// Base fact key after reading (e.g., "source.mtime")
    pub base_key: String,
    /// Optional path accessor (e.g., [-1] for last segment)
    pub accessor: Option<PathAccessor>,
    /// Modifiers to apply (e.g., [Year])
    pub modifiers: Vec<ModifierCall>,
    /// The key the name as written reads as (`hash` is the built-in key, though its
    /// `base_key` is the content hash). A consumer that decides on a key reads this — never
    /// `raw`, which is for display: quoting changes how a key is read, never which key.
    pub key: Option<Key>,
    /// What a value read through this key's chain may be, as the key stood when the report
    /// read it.
    pub(in crate::expr) read_as: ReadAs,
}

impl ParsedFactKey {
    /// Parse a key string into its components.
    ///
    /// The name is read in the expression's order (`KeyName::in_expression`): a
    /// source fact or built-in key stays its own name — except the hash keys,
    /// which read as the stored content hash (`BuiltinKey::reading`) — and any
    /// other name is read under `content.` (`Make` becomes `content.Make`).
    #[cfg(test)]
    pub fn parse(key: &str) -> Result<Self> {
        let read = super::filter::WrittenKey::read(key)?;
        Ok(Self {
            raw: key.to_string(),
            base_key: read.base,
            accessor: read.accessor,
            modifiers: read.modifiers,
            key: read.key,
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
    read_key_name(name).builtin.and_then(|k| k.expansion())
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

/// Read a written base key as the key the asking half evaluates, in the
/// expression's order (`KeyName::in_expression`): a built-in key with a reading
/// becomes that stored key plus the modifier it implies (`BuiltinKey::reading`),
/// a source fact or built-in key its own name, a content fact its `content.`
/// name. A name with no reading stays as given. The one place the key readers
/// take the alias from, so the filter's text form, the parsed form and
/// `--emit`'s value (`ops/value.rs`) cannot disagree.
pub(in crate::expr) fn read_base_key(key: &str) -> (String, Option<&'static str>) {
    match Key::read(key) {
        Some(Key::BuiltinKey(builtin)) => match builtin.reading() {
            Some((stored, leading_modifier)) => (stored.to_string(), leading_modifier),
            None => (builtin_name(builtin).to_string(), None),
        },
        Some(read) => (read.name().to_string(), None),
        None => (key.to_string(), None),
    }
}

fn builtin_name(key: BuiltinKey) -> &'static str {
    key.into()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::transform::Modifier;
    use super::*;

    /// Every key of the vocabulary, the content hash with them.
    fn every_key() -> impl Iterator<Item = Key> {
        Key::all().chain([Key::ContentFact(CONTENT_HASH.to_string())])
    }

    /// The three readings of a name, from the vocabulary: a plain name is read
    /// under `content.`; a built-in key's name also has that content reading; a
    /// `source.` name has none, and is a source fact or nothing.
    #[test]
    fn a_name_has_up_to_three_readings() {
        let content = |name: &str| Some(name.to_string());
        let no_source_fact: Option<SourceFact> = None;
        let no_builtin: Option<BuiltinKey> = None;
        let cases = [
            ("Make", no_source_fact, no_builtin, content("content.Make")),
            ("content.Make", None, None, content("content.Make")),
            (
                "filename",
                None,
                Some(BuiltinKey::Filename),
                content("content.filename"),
            ),
            (
                "hash",
                None,
                Some(BuiltinKey::Hash),
                content("content.hash"),
            ),
            (
                "size",
                None,
                Some(BuiltinKey::Size),
                content("content.size"),
            ),
            (
                "mtime",
                None,
                Some(BuiltinKey::Mtime),
                content("content.mtime"),
            ),
            (
                "root_id",
                None,
                Some(BuiltinKey::RootId),
                content("content.root_id"),
            ),
            ("source.size", Some(SourceFact::Size), None, None),
            ("source.extension", None, None, None),
            ("content.hash.sha256", None, None, content(CONTENT_HASH)),
        ];
        for (name, source_fact, builtin, content_key) in cases {
            assert_eq!(
                read_key_name(name),
                KeyName {
                    source_fact,
                    builtin,
                    content_key
                },
                "{name}"
            );
        }
    }

    /// The content reading, which import stores a key under: a plain name gains
    /// `content.`, a `content.` name is kept as given. Any other prefix is part
    /// of the plain name — a reader keeping `policy.` as written would look for
    /// a name no import stores, and both sides of the key-name law's verifier
    /// read through this one reader, so the rule is pinned here.
    #[test]
    fn a_plain_name_is_read_under_content() {
        for (name, want) in [
            ("Make", "content.Make"),
            ("hash.sha256", "content.hash.sha256"),
            ("DateTimeOriginal", "content.DateTimeOriginal"),
            ("policy.reviewed", "content.policy.reviewed"),
            ("object.hash", "content.object.hash"),
            ("Content.Make", "content.Content.Make"),
            ("content.Make", "content.Make"),
            ("content.hash.sha256", "content.hash.sha256"),
        ] {
            assert_eq!(
                read_key_name(name).content_key.as_deref(),
                Some(want),
                "{name}"
            );
        }
    }

    /// No `source.` name has a content reading — a source fact or not — so
    /// import and `facts delete` never reach a fact under such a name.
    #[test]
    fn a_source_name_has_no_content_reading() {
        for name in [
            "source.size",
            "source.mtime",
            "source.ext",
            "source.extension",
        ] {
            assert_eq!(read_key_name(name).content_key, None, "{name}");
        }
    }

    /// An expression reads the source fact, else the built-in key, else the
    /// content fact: `filename` is the built-in key, never `content.filename`.
    #[test]
    fn an_expression_reads_the_source_fact_then_the_built_in_key_then_the_content_fact() {
        assert_eq!(
            Key::read("source.ext"),
            Some(Key::SourceFact(SourceFact::Ext))
        );
        assert_eq!(Key::read("ext"), Some(Key::BuiltinKey(BuiltinKey::Ext)));
        assert_eq!(
            Key::read("filename"),
            Some(Key::BuiltinKey(BuiltinKey::Filename))
        );
        assert_eq!(
            Key::read("Make"),
            Some(Key::ContentFact("content.Make".to_string()))
        );
        assert_eq!(Key::read("source.extension"), None);
    }

    /// The two lists are the vocabulary's, pinned by name: editing one retires
    /// a word from the language or moves it to another kind, which is a
    /// decision someone makes, not a rename that happens to compile.
    #[test]
    fn the_source_facts_and_the_built_in_keys_are_the_vocabulary_s() {
        let names = |kind: KeyKind| -> Vec<String> {
            Key::all()
                .filter(|key| key.kind() == kind)
                .map(|key| key.name().to_string())
                .collect()
        };
        assert_eq!(
            names(KeyKind::SourceFact),
            [
                "source.ext",
                "source.size",
                "source.mtime",
                "source.path",
                "source.root",
                "source.rel_path",
                "source.id",
                "source.device",
                "source.inode",
            ]
        );
        assert_eq!(
            names(KeyKind::BuiltinKey),
            [
                "filename",
                "stem",
                "ext",
                "hash",
                "hash_short",
                "id",
                "size",
                "mtime",
                "root_id"
            ]
        );
        assert!(names(KeyKind::ContentFact).is_empty());
    }

    /// A reading turns an alias into its stored key once, through the one key reader
    /// (`WrittenKey::read`) that `--where`, `--key` and `--group-by` share. It holds because a
    /// target is a content fact, which has no reading; a doubled `short` would hide, since
    /// cutting a cut hash changes nothing, so the modifier count is what is asserted.
    #[test]
    fn a_reading_is_applied_once_however_often_the_key_is_read() {
        use strum::IntoEnumIterator;

        for builtin in BuiltinKey::iter() {
            let Some((target, leading)) = builtin.reading() else {
                continue;
            };
            let name = builtin_name(builtin);
            assert!(
                matches!(Key::read(target), Some(Key::ContentFact(_))),
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

    fn is_source_fact(base: &str) -> bool {
        matches!(Key::read(base), Some(Key::SourceFact(_)))
    }

    fn is_content_fact(base: &str) -> bool {
        matches!(Key::read(base), Some(Key::ContentFact(_)))
    }

    #[test]
    fn parsed_key_simple() {
        let key = ParsedFactKey::parse("source.ext").unwrap();
        assert_eq!(key.raw, "source.ext");
        assert_eq!(key.base_key, "source.ext");
        assert!(key.accessor.is_none());
        assert!(key.modifiers.is_empty());
        assert!(is_source_fact(&key.base_key));
        assert!(!key.has_transforms());
    }

    #[test]
    fn parsed_key_with_accessor() {
        let key = ParsedFactKey::parse("source.rel_path[-1]").unwrap();
        assert_eq!(key.raw, "source.rel_path[-1]");
        assert_eq!(key.base_key, "source.rel_path");
        assert!(matches!(key.accessor, Some(PathAccessor::Index(-1))));
        assert!(key.modifiers.is_empty());
        assert!(is_source_fact(&key.base_key));
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
        assert!(is_source_fact(&key.base_key));
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
        assert!(is_source_fact(&key.base_key));
        assert!(key.has_transforms());
    }

    #[test]
    fn parsed_key_stored_fact_normalized() {
        // A stored fact key without prefix gets content. added
        let key = ParsedFactKey::parse("Make").unwrap();
        assert_eq!(key.raw, "Make");
        assert_eq!(key.base_key, "content.Make");
        assert!(is_content_fact(&key.base_key));
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
        assert!(is_content_fact(&key.base_key));
        assert!(key.has_transforms());
    }

    #[test]
    fn every_builtin_key_is_reachable_under_a_distinct_name() {
        // A key's name is the word a user types, so the set of them is the
        // vocabulary Canon promises. Two keys answering to one name makes the
        // second unreachable, and editing any one of these strings retires a
        // word from the language — pinned here so that is a decision someone
        // makes, not a rename that happens to compile. Every name reads back,
        // through the one reader, as the key it names.
        let mut names: Vec<String> = Vec::new();
        for key in every_key() {
            let name = key.name().to_string();
            assert_eq!(
                Key::read(&name).as_ref(),
                Some(&key),
                "'{name}' does not resolve back to the key it names"
            );
            assert!(!names.contains(&name), "two keys answer to '{name}'");
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
