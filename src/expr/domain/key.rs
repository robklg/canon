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
//! its own rule to the readings it gets. Reading a written key splits it into
//! that name, an optional path accessor, and a modifier chain: one grammar,
//! `read_key`, wherever a key stands — a filter, a report key, a pattern's `{…}`
//! (it reads a built-in key's own modifier with `parse_modifier`).

use anyhow::Result;

use super::bind::{check_read_as, result_of, Gives, ReadAs, Refused, Rows};
use super::print::{printed_name, printed_name_read, quoted};
use super::problem::{cut, Fix, Place};
use super::read::{is_space, read_quoted, skip_end, skip_to, Cursor, Grammar};
use super::transform::{parse_modifier, Modifier, ModifierCall, PathAccessor};
use crate::core::domain::fact::{FactType, FactValue, KeyTypes};
use crate::core::domain::number::number_form;

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
    /// that this key is read as — where it is a short name for a stored fact
    /// rather than a name for itself.
    ///
    /// The two hash keys name content identity, which no source column
    /// answers: a filter or a `facts` report reads the stored content hash, a
    /// pattern the hash the lock entry recorded under the same key (the
    /// context-supplied law). Without this the key reached the stored-fact
    /// lookup under its own name, where no fact is ever named `hash`: every
    /// comparison was false, and `NOT` turned it into every source.
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

    /// What the key means, in one line — the one meaning every surface naming a
    /// built-in key prints: `canon facts`' list of built-in keys and the manifest's
    /// comment block.
    fn description(self) -> &'static str {
        match self {
            BuiltinKey::Filename => "last path component",
            BuiltinKey::Stem => "file name without its extension",
            BuiltinKey::Ext => "same as source.ext",
            BuiltinKey::Hash => "content hash, content.hash.sha256",
            BuiltinKey::HashShort => "first 8 characters of the content hash",
            BuiltinKey::Id => "same as source.id",
            BuiltinKey::Size => "same as source.size",
            BuiltinKey::Mtime => "same as source.mtime",
            BuiltinKey::RootId => "ID of the source's root",
        }
    }
}

// ============================================================================
// The name reader
// ============================================================================

/// **How a key's name is read**, which every caller of [`read_key_name`] declares, so no
/// command takes a reading by default. Named by what the reading does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameReading {
    /// A name a person typed as a whole argument, perhaps a key copied from Canon's output:
    /// Canon's printed quoting is read off first (the printing rule's inverse), so
    /// `"content.File Size"` names `content.File Size`. Nothing expression-like is read.
    QuotingReadOff,
    /// The name as written: a tool's name, which import stores as the tool wrote it, or a name
    /// Canon has already read. Never read off twice: `"a"` read again stays `"a"`.
    AsWritten,
}

/// What a key's name can be read as: the source fact it names, the built-in key
/// it names, the content fact it names. A name may have more than one reading
/// (`filename` is a built-in key and, read under `content.`, a content fact), or
/// none (`source.extension`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyName {
    /// The name as read, which a command names in its answers.
    pub name: String,
    /// A `source.` name that is one of the source facts.
    pub source_fact: Option<SourceFact>,
    /// One of the built-in keys.
    pub builtin: Option<BuiltinKey>,
    /// The name read under `content.` (`Make` → `content.Make`, `content.Make`
    /// as it is); `None` for any `source.` name.
    pub content_key: Option<String>,
}

/// **The key-name law**: every command that takes a key's name reads it here, declaring
/// how ([`NameReading`]) — a name a person typed with Canon's printed quoting read off, a
/// tool's or an already-read name as written — and each applies its own rule to the readings
/// it gets: an expression and `--emit` take [`KeyName::in_expression`], import and
/// `facts delete` the content reading alone. No accessor, modifier or alias is read.
pub fn read_key_name(given: &str, reading: NameReading) -> KeyName {
    use strum::IntoEnumIterator;

    let name = match reading {
        NameReading::QuotingReadOff => printed_name_read(given),
        NameReading::AsWritten => given.to_string(),
    };
    let name = name.as_str();
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
        name: name.to_string(),
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
        self.source_fact
            .map(Key::SourceFact)
            .or(self.builtin.map(Key::BuiltinKey))
            .or(self.content_key.map(Key::ContentFact))
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
    /// The key a name reads as in an expression — `read_key_name(name, AsWritten)
    /// .in_expression()`. A key's own name reads back as the key, so a base key already read
    /// can be asked again here. Never a command's reading of a name a person typed, which
    /// declares its reading at [`read_key_name`].
    pub fn read(name: &str) -> Option<Key> {
        read_key_name(name, NameReading::AsWritten).in_expression()
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

    /// Whether this is a built-in key read as the content hash (`hash`,
    /// `hash_short`) — which a pattern reads off the hash the lock entry
    /// records (the context-supplied law). Read off `BuiltinKey::reading`, the
    /// one place that pairing is spoken.
    pub fn reads_the_content_hash(&self) -> bool {
        matches!(self, Key::BuiltinKey(key)
            if matches!(key.reading(), Some((CONTENT_HASH, _))))
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
/// What `expr::read_report` gives a report for each key: the key as typed, read by the key grammar
/// ([`read_key`]) and checked against its type, then applied to every source a distribution or
/// a grouping counts.
///
/// ## Example
///
/// ```ignore
/// use crate::expr::read_report;
///
/// let key = read_report(conn, Some("source.mtime|year"), &[], &filters)?.key.unwrap();
/// assert_eq!(key.base_key, "source.mtime");
/// assert!(key.has_transforms());
/// ```
#[derive(Debug, Clone)]
pub struct ParsedFactKey {
    /// The key's text as the reader read it, for display: as typed, quotes kept, the spaces
    /// around it dropped (e.g., "source.mtime|year")
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
    /// What the key's rows are: what its chain gives, or the types of a bare key of mixed type.
    pub(in crate::expr) rows: Rows,
    /// The key as the user spelled it, which a report's header prints.
    pub(in crate::expr) spelled: Spelled,
}

/// **A key as the user spelled it**: the name — a key alias's `@name` as typed, any other name
/// as the printing rule writes it — and which of the key's transforms were typed after it. A
/// built-in key's own reading (`hash_short` read through `|short`) and a key alias's value are
/// not typed, and come before every transform that is.
#[derive(Debug, Clone)]
pub(in crate::expr) struct Spelled {
    pub(in crate::expr) name: String,
    pub(in crate::expr) accessor_typed: bool,
    /// The index of the first modifier typed after the name.
    pub(in crate::expr) typed_from: usize,
}

impl Spelled {
    /// How `key` was spelled: a transform was typed where its place is in the text the key was
    /// typed in, and is not the name's own place (a built-in key's reading sits there).
    pub(in crate::expr) fn of(key: &InputKey) -> Spelled {
        let typed = |place: &Place| place.text == key.place.text && *place != key.name_place;
        let name = match &key.written {
            Some(_) => key.input_name.clone(),
            None => printed_name(&key.name_text()),
        };
        Spelled {
            name,
            accessor_typed: key.accessor.as_ref().is_some_and(|(_, at)| typed(at)),
            typed_from: key
                .modifiers
                .iter()
                .position(|(_, at)| typed(at))
                .unwrap_or(key.modifiers.len()),
        }
    }
}

impl ParsedFactKey {
    /// Parse a key string into its components.
    ///
    /// The name is read in the expression's order (`KeyName::in_expression`): a
    /// source fact or built-in key stays its own name — except the hash keys,
    /// which read as the stored content hash (`BuiltinKey::reading`) — and any
    /// other name is read under `content.` (`Make` becomes `content.Make`). Its rows are what
    /// its chain gives from the base key's known type, a content fact's read as text — or text,
    /// where that chain does not bind.
    #[cfg(test)]
    pub fn parse(key: &str) -> Result<Self> {
        let known = super::bind::known_type(&read_one(key)?.base);
        Self::parse_as(key, known.unwrap_or(FactType::Text))
            .or_else(|_| Self::read_with(key, Rows::One(Gives::Text)))
    }

    /// [`Self::parse`] for a key whose base holds facts of type `base_type`, read as a report
    /// reads it: an error where its chain does not bind.
    #[cfg(test)]
    pub fn parse_as(key: &str, base_type: FactType) -> Result<Self> {
        let read = read_one(key)?;
        let types = std::collections::HashMap::from([(read.base.clone(), KeyTypes::of(base_type))]);
        match super::bind::bind_key(&read, &read.input, super::bind::Reads::Report, &types) {
            Ok((_, chain)) => Self::read_with(key, chain.rows()),
            Err(cause) => anyhow::bail!("{key} does not bind: {cause:?}"),
        }
    }

    /// A key read with the rows a test says it has.
    #[cfg(test)]
    fn read_with(key: &str, rows: Rows) -> Result<Self> {
        let read = read_one(key)?;
        Ok(Self {
            raw: read.input.clone(),
            base_key: read.base.clone(),
            accessor: read.accessor.as_ref().map(|(accessor, _)| accessor.clone()),
            modifiers: read
                .modifiers
                .iter()
                .map(|(call, _)| call.clone())
                .collect(),
            key: read.key.clone(),
            read_as: ReadAs::Any,
            rows,
            spelled: Spelled::of(&read),
        })
    }

    /// **The expression a report counted**, as its header prints it: the name as the user spelled
    /// it ([`Spelled`]), then the accessor and the modifiers typed after it — any default a
    /// report added among them ([`Self::with_modifier`]) — each in its one spelling. Typed back
    /// as a key, it reads as this key.
    pub fn printed(&self) -> String {
        let accessor = match &self.accessor {
            Some(accessor) if self.spelled.accessor_typed => accessor.spelled(),
            _ => String::new(),
        };
        let modifiers: String = self.modifiers[self.spelled.typed_from..]
            .iter()
            .map(ModifierCall::spelled)
            .collect();
        format!("{}{accessor}{modifiers}", self.spelled.name)
    }

    /// **A default, as an expression**: this key with `modifier` added after its own, its rows
    /// walked on through the modifier table from what they were, and read as its base's one type,
    /// as a key typed with that modifier is. An error where the table refuses the modifier, or the
    /// key is of mixed type — a defect in the caller's policy, never something the user typed.
    pub fn with_modifier(mut self, modifier: Modifier) -> Result<ParsedFactKey> {
        let Rows::One(gives) = &self.rows else {
            anyhow::bail!("{} is of mixed type; it takes no modifier", self.base_key);
        };
        let call = ModifierCall {
            modifier,
            args: Vec::new(),
        };
        let Some(next) = result_of(gives, !self.modifiers.is_empty(), &call) else {
            anyhow::bail!("{} cannot take |{}", self.printed(), <&str>::from(modifier));
        };
        self.read_as = match (self.read_as, gives) {
            (ReadAs::One(t), _) => ReadAs::One(t),
            (ReadAs::Any, Gives::Text) => ReadAs::One(FactType::Text),
            (ReadAs::Any, Gives::Path) => ReadAs::One(FactType::Path),
            (ReadAs::Any, Gives::Number) => ReadAs::One(FactType::Num),
            (ReadAs::Any, Gives::Time) => ReadAs::One(FactType::Time),
            (read_as, gives) => {
                anyhow::bail!("{} read as {read_as:?} gives {gives:?}", self.base_key)
            }
        };
        self.rows = Rows::One(next);
        self.modifiers.push(call);
        Ok(self)
    }

    /// **The rows' type, in the words a report's header names it**: the key list's words, and
    /// `range` for a bucket's labels, `time of day` for `|time`'s, `mixed` for a bare key of
    /// mixed type. A number part (`|year` … `|quarter`) is a number, which a filter compares it
    /// as.
    pub fn rows_word(&self) -> &'static str {
        match &self.rows {
            Rows::One(gives) => match gives {
                Gives::Text => "text",
                Gives::Path => "path",
                Gives::Number => "number",
                Gives::Time | Gives::TimeAt(_) => "time",
                Gives::TimeOfDay => "time of day",
                Gives::Part(_) => "number",
                Gives::Bucket(_) => "range",
            },
            Rows::Mixed(_) => "mixed",
        }
    }

    /// The types of a bare key of mixed type; `None` for any other key.
    pub fn mixed_types(&self) -> Option<&KeyTypes> {
        match &self.rows {
            Rows::Mixed(types) => Some(types),
            Rows::One(_) => None,
        }
    }

    /// Whether a stored value is what the key was read as ([`check_read_as`]): a key with an
    /// accessor or modifiers was read as its one type; a bare key reads every value it meets, of
    /// whatever type.
    pub(in crate::expr) fn check_type(&self, value: &FactValue) -> Result<()> {
        check_read_as(self.read_as, &self.base_key, value, Refused::Key)
    }

    /// Check if this key has transforms (accessor or modifiers).
    pub fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }
}

/// One key read as `--key` reads it, for a test: no problem, exactly one key.
#[cfg(test)]
fn read_one(key: &str) -> Result<InputKey> {
    let reading = super::read::read_keys(key, 0, Grammar::Key, super::read::AliasMode::NotLoaded);
    match (reading.problems.is_empty(), reading.value) {
        (true, Some(mut keys)) if keys.len() == 1 => Ok(keys.remove(0)),
        (_, _) => anyhow::bail!("not one key: {key}: {:?}", reading.problems),
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
// The key grammar
// ============================================================================

/// A key as typed, and what it reads as: the whole key and its name part as typed (quotes
/// included), where each sits, the key its name reads as (`read_key_name(..).in_expression()`;
/// `None` for a `source.` name that is no source fact), the base key a value is read from, its
/// accessor and its modifiers, each with its place. A key alias's key is typed as `@name`: its
/// `input` and `input_name` are what was typed, its own name, accessor and modifiers sit in the
/// alias's value, and `written` is how that value writes it.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct InputKey {
    pub input: String,
    pub input_name: String,
    pub place: Place,
    pub name_place: Place,
    pub key: Option<Key>,
    pub base: String,
    pub accessor: Option<(PathAccessor, Place)>,
    pub modifiers: Vec<(ModifierCall, Place)>,
    pub written: Option<Written>,
}

/// A key alias's key as its value writes it: the name, and the whole key from the name on.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Written {
    pub name: String,
    pub input: String,
}

impl InputKey {
    /// **The name a sentence uses follows where its cause sits**: inside a key alias's value —
    /// a text other than the one the key was typed in — the name as written there; anywhere
    /// else, the name as typed.
    pub(in crate::expr) fn name_for(&self, place: Place) -> &str {
        match &self.written {
            Some(written) if place.text != self.place.text => &written.name,
            _ => &self.input_name,
        }
    }

    /// **The key as typed up to a problem at `place`**, which a sentence names as its subject:
    /// the key's text before the problem — inside a key alias's value, as the value writes it,
    /// following [`InputKey::name_for`].
    pub(in crate::expr) fn up_to(&self, place: Place) -> &str {
        match &self.written {
            Some(written) if place.text != self.place.text => cut(
                &written.input,
                0,
                place.start.saturating_sub(self.name_place.start),
            ),
            _ => cut(&self.input, 0, place.start.saturating_sub(self.place.start)),
        }
        // Spaces before a key's first modifier or accessor are not the key's.
        .trim_end_matches(is_space)
    }

    /// Every place the key holds, for a copy that stands elsewhere.
    pub(in crate::expr) fn each_place(&mut self, f: &mut impl FnMut(&mut Place)) {
        f(&mut self.place);
        f(&mut self.name_place);
        if let Some((_, place)) = &mut self.accessor {
            f(place);
        }
        for (_, place) in &mut self.modifiers {
            f(place);
        }
    }

    /// Whether the key reads its base through an accessor or a modifier.
    pub(in crate::expr) fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }

    fn is_quoted(&self) -> bool {
        self.input_name.starts_with(['"', '\''])
    }

    /// Whether the name is `word`, typed bare — the one spelling a keyword or status word has;
    /// a quoted word is never one.
    pub(in crate::expr) fn is_bare_name_spelled(&self, word: &str) -> bool {
        !self.is_quoted() && self.input_name == word
    }

    /// Whether the name, bare or quoted, is `name`.
    pub(in crate::expr) fn names(&self, name: &str) -> bool {
        self.input_name == name || (self.is_quoted() && self.name_text() == name)
    }

    /// The name inside its quotes, an inner quote undoubled.
    pub(in crate::expr) fn name_text(&self) -> String {
        let mut cursor = Cursor::new(&self.input_name, 0);
        if self.is_quoted() {
            read_quoted(&mut cursor).unwrap_or_default()
        } else {
            self.input_name.clone()
        }
    }

    /// **The quoted name a filter's refusal may offer**: the name quoted, where the key is a bare
    /// name and the character straight after it (byte `end` of `text`) is one a name cannot
    /// contain and that cannot start anything that may follow a key in a filter. The quoted name
    /// runs to where what follows the key may start. Only a filter needs it: where a key stands
    /// alone a bare name already runs to what may follow it ([`ends_a_bare_name`]). Whether it is
    /// shown is binding's (a key with facts).
    pub(in crate::expr) fn quoted_name_fix(&self, text: &str, end: usize) -> Option<String> {
        // A key ending past its name has an accessor or a modifier typed after it; a built-in
        // key's own reading (`hash_short` as `|short`) is no such text.
        if self.is_quoted() || self.written.is_some() || end != self.name_place.end {
            return None;
        }
        let rest = cut(text, end, text.len());
        if rest.is_empty() || follows_a_key_in_a_filter(rest) {
            return None;
        }
        // The name runs to where what may follow it starts, read as the reader skips: a `)`
        // closing a `(` the name opened belongs to it (`content.Foo(5)`), and quoted text where a
        // part starts is stepped over whole.
        let extent = skip_end(text, end, |at, _| follows_a_key_in_a_filter(at.rest())) - end;
        let name = format!(
            "{}{}",
            self.input_name,
            cut(rest, 0, extent).trim_end_matches(is_space)
        );
        Some(quoted(&name))
    }
}

/// Whether `rest` starts with what may follow a key in a filter: a space, `[`, `|`, an
/// operator (`?` among them), `)` or `,`.
pub(in crate::expr) fn follows_a_key_in_a_filter(rest: &str) -> bool {
    let mut chars = rest.chars();
    match chars.next() {
        Some(c) if is_space(c) => true,
        Some('[' | '|' | '=' | '<' | '>' | '~' | '?' | ')' | ',') => true,
        Some('!') => matches!(chars.next(), Some('=' | '~')),
        _ => false,
    }
}

/// Whether a not-a-key text ends before `rest`: at what may follow a key in this position — in
/// a filter an operator, `?`, a space, `)` or `,`; in a key list a `,`; for one key only the
/// end.
fn ends_not_a_key(rest: &str, grammar: Grammar) -> bool {
    let mut chars = rest.chars();
    match grammar {
        Grammar::Filter => match chars.next() {
            Some(c) if is_space(c) => true,
            Some('=' | '<' | '>' | '~' | '?' | ')' | ',') => true,
            Some('!') => matches!(chars.next(), Some('=' | '~')),
            _ => false,
        },
        Grammar::KeyList => chars.next() == Some(','),
        // A key and a pattern's key are read from a text that ends where they must.
        Grammar::Key | Grammar::Pattern => false,
    }
}

/// A key's name in `'…'`, an inner `'` doubled: the quoting a pattern uses, since a manifest
/// holds it inside `pattern = "…"`.
pub(in crate::expr) fn single_quoted(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

/// **Where a key stands alone a space can mean nothing else**, so a `|` after spaces still
/// starts the key's modifiers, and a `[` its accessor where one may follow (`accessor`) — after a
/// bare name, a quoted one and a key alias alike: the bytes of spaces at the start of `rest`
/// before one, or `None`.
pub(in crate::expr) fn spaces_before_transforms(rest: &str, accessor: bool) -> Option<usize> {
    let after = rest.trim_start_matches(is_space);
    (after.starts_with('|') || (accessor && after.starts_with('[')))
        .then(|| rest.len() - after.len())
}

/// Step the cursor over spaces to a `|`, or to a `[` where an accessor may follow
/// ([`spaces_before_transforms`]).
pub(in crate::expr) fn step_to_transforms(cursor: &mut Cursor, accessor: bool) {
    if let Some(spaces) = spaces_before_transforms(cursor.rest(), accessor) {
        cursor.set_at(cursor.at() + spaces);
    }
}

/// Whether `c` continues a bare name: letters and digits of any language, `_ . : / -`.
pub(in crate::expr) fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '.' | ':' | '/' | '-')
}

/// **What ends a bare name**, by position: in a filter, where an operator may follow the key,
/// the first character that is not a name character (a space among them); where a key stands
/// alone only what may follow it there — `[` or `|`, and in a key list `,`. A pattern's key is
/// read from the inside of its `{…}`, so its `}` is the end of the text.
fn ends_a_bare_name(c: char, grammar: Grammar) -> bool {
    match grammar {
        Grammar::Filter => !is_name_char(c),
        Grammar::Key | Grammar::Pattern => matches!(c, '[' | '|'),
        Grammar::KeyList => matches!(c, '[' | '|' | ','),
    }
}

/// The rule line under every `|bucket` number sentence.
const BUCKET_RULE: &str = "|bucket takes numbers: |bucket(10,100)";

/// **The key grammar**: a name — a bare name, a letter or `_` first, or a quoted name; then one
/// `[…]` after it; then modifiers `|name`, the name in exact lower case, only `|bucket` taking
/// numbers in parentheses. In a filter the `[` or first `|` stands straight after the name; where a
/// key stands alone, after any spaces, after a bare name and a quoted one alike. What ends a bare
/// name is the position's ([`ends_a_bare_name`]): in a filter the first character that is not a
/// name character, where a key stands alone only what may follow it there, so a space inside is the
/// name's and spaces at its end are not. The key ends at the first character that does not fit, and
/// the caller says what may stand there. Problems inside the key are recorded on the cursor; `None`
/// where nothing can be read as a key — a text that is not one, an empty quoted name — or reading
/// stopped.
pub(in crate::expr) fn read_key(cursor: &mut Cursor, grammar: Grammar) -> Option<InputKey> {
    let start = cursor.at();
    let first = cursor.peek()?;
    let name = if matches!(first, '"' | '\'') {
        let name = read_quoted(cursor)?;
        if name.is_empty() {
            let typed = cursor.slice(start, cursor.at()).to_string();
            cursor.problem(
                start,
                cursor.at(),
                format!("`{typed}` is not a key"),
                None,
                Fix::None,
            );
            return None;
        }
        name
    } else if first.is_alphabetic() || first == '_' {
        // The name runs to what ends it here, read as the reader skips: quoted text where a part
        // starts stepped over, a `(` paired, a quote that never closes named. Spaces at its end
        // are not its own.
        skip_to(cursor, |_, c| ends_a_bare_name(c, grammar));
        if cursor.stopped() {
            return None;
        }
        let name = cursor.slice(start, cursor.at()).trim_end_matches(is_space);
        cursor.set_at(start + name.len());
        name.to_string()
    } else {
        // The text runs to what may follow a key here, read as the reader skips; it is named
        // before it is read through, so a quote in it that never closes is named after it.
        // A `(` it starts with is paired like any other; any other first character is its own.
        let ends = |at: &Cursor, _| ends_not_a_key(at.rest(), grammar);
        let from = if first == '(' {
            start
        } else {
            start + first.len_utf8()
        };
        let run_end = skip_end(cursor.text(), from, ends);
        let typed = cursor.slice(start, run_end).trim_end_matches(is_space);
        let end = start + typed.len();
        let sentence = format!("`{typed}` is not a key");
        cursor.problem(start, end, sentence, None, Fix::None);
        cursor.set_at(from);
        skip_to(cursor, ends);
        return None;
    };
    let name_end = cursor.at();
    let alone = match grammar {
        Grammar::Filter => false,
        Grammar::Key | Grammar::KeyList | Grammar::Pattern => true,
    };
    if alone {
        step_to_transforms(cursor, true);
    }
    let name_place = cursor.place(start, name_end);
    let (base, leading) = read_base_key(&name);
    let mut modifiers = Vec::new();
    if let Some(leading) = leading {
        let call = parse_modifier(leading).expect("a built-in key's reading names a modifier");
        modifiers.push((call, name_place));
    }
    let mut key = InputKey {
        input: cursor.slice(start, name_end).to_string(),
        input_name: cursor.slice(start, name_end).to_string(),
        place: name_place,
        name_place,
        key: Key::read(&name),
        base,
        accessor: None,
        modifiers,
        written: None,
    };
    continue_key(cursor, start, &mut key, true)?;
    Some(key)
}

/// **The key grammar after a name**, from the cursor: one `[…]` where the cursor stands, where
/// `accessor_may_follow` (nothing but the name read so far), then modifiers; `key`'s text and
/// place then run from byte `start` to where the key ends. A key alias's key continues here,
/// after its `@name`. `None` where reading stopped.
pub(in crate::expr) fn continue_key(
    cursor: &mut Cursor,
    start: usize,
    key: &mut InputKey,
    accessor_may_follow: bool,
) -> Option<()> {
    if accessor_may_follow && cursor.peek() == Some('[') {
        key.accessor = read_accessor(cursor)?;
    }
    while cursor.peek() == Some('|') {
        if let Some(modifier) = read_modifier(cursor) {
            key.modifiers.push(modifier);
        }
    }
    let end = cursor.at();
    key.input = cursor.slice(start, end).to_string();
    key.place = cursor.place(start, end);
    Some(())
}

/// `[n]` or `[a:b]`, from the `[`: whole numbers, an optional leading `-`, no `+`, no spaces,
/// either bound of a slice left out at will. `Some(None)` where a position cannot be read (its
/// problem recorded), `None` where the `[` never closes (reading stopped).
fn read_accessor(cursor: &mut Cursor) -> Option<Option<(PathAccessor, Place)>> {
    let open = cursor.at();
    cursor.bump();
    let inner_start = cursor.at();
    cursor.bump_while(|c| c != ']');
    if cursor.at_end() {
        cursor.problem(open, open + 1, "this `[` is unclosed", None, Fix::None);
        cursor.stop();
        return None;
    }
    let inner_end = cursor.at();
    cursor.bump();
    let place = cursor.place(open, cursor.at());
    let inner = cursor.slice(inner_start, inner_end);
    if inner.is_empty() {
        let sentence = "a position is missing after `[`";
        cursor.problem(inner_end, inner_end + 1, sentence, None, Fix::None);
        return Some(None);
    }
    let parts: Vec<(usize, &str)> = match inner.find(':') {
        None => vec![(inner_start, inner)],
        Some(colon) => vec![
            (inner_start, cut(inner, 0, colon)),
            (inner_start + colon + 1, cut(inner, colon + 1, inner.len())),
        ],
    };
    let mut bounds = Vec::new();
    for &(at, part) in &parts {
        if part.is_empty() && parts.len() == 2 {
            bounds.push(None);
            continue;
        }
        match whole_number(part) {
            Some(n) => bounds.push(Some(n)),
            None => cursor.problem(
                at,
                at + part.len(),
                format!(
                    "`{part}` is not a position — positions are whole numbers: [0], [-1], [1:3]"
                ),
                None,
                Fix::None,
            ),
        }
    }
    if bounds.len() != parts.len() {
        return Some(None);
    }
    let accessor = match bounds.as_slice() {
        [Some(index)] => PathAccessor::Index(*index),
        [start, end] => PathAccessor::Slice {
            start: *start,
            end: *end,
        },
        _ => unreachable!("one bound per part, every part read"),
    };
    Some(Some((accessor, place)))
}

/// A whole number as a position writes it: digits, an optional leading `-`.
fn whole_number(text: &str) -> Option<i32> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// One modifier, from its `|`: the name in exact lower case, and for `|bucket` the numbers in
/// parentheses after it. `None` where it cannot be read (its problem recorded). Any other
/// modifier followed by `(` ends the key there.
fn read_modifier(cursor: &mut Cursor) -> Option<(ModifierCall, Place)> {
    use strum::IntoEnumIterator;

    let bar = cursor.at();
    cursor.bump();
    let name_start = cursor.at();
    cursor.bump_while(|c| c.is_alphanumeric() || c == '_');
    let name = cursor.slice(name_start, cursor.at());
    if name.is_empty() {
        let at = cursor.at();
        let end = at + cursor.peek().map_or(0, char::len_utf8);
        cursor.problem(at, end, "a modifier is missing after `|`", None, Fix::None);
        return None;
    }
    let Some(modifier) = Modifier::iter().find(|m| <&'static str>::from(*m) == name) else {
        let all: Vec<&'static str> = Modifier::iter().map(<&'static str>::from).collect();
        let sentence = format!(
            "`|{name}` is not a modifier — Modifiers: {}",
            all.join(", ")
        );
        let end = cursor.at();
        cursor.problem(bar, end, sentence, None, Fix::None);
        return None;
    };
    let args = if modifier == Modifier::Bucket && cursor.peek() == Some('(') {
        read_bucket_numbers(cursor)?
    } else {
        Vec::new()
    };
    let place = cursor.place(bar, cursor.at());
    Some((ModifierCall { modifier, args }, place))
}

/// `|bucket`'s numbers, from the `(`: one or more, separated by commas, each larger than the
/// one before, spaces allowed inside the parentheses. `None` where they cannot be read (each
/// problem recorded).
fn read_bucket_numbers(cursor: &mut Cursor) -> Option<Vec<f64>> {
    let open = cursor.at();
    cursor.bump();
    let before = cursor.problem_count();
    let mut numbers: Vec<f64> = Vec::new();
    let mut first = true;
    loop {
        cursor.skip_spaces();
        let item_start = cursor.at();
        cursor.bump_while(|c| c != ',' && c != ')');
        if cursor.at_end() {
            // Everything after the `(` was read as its numbers.
            cursor.problem(open, open + 1, "this `(` is unclosed", None, Fix::None);
            cursor.stop();
            return None;
        }
        let item = cursor
            .slice(item_start, cursor.at())
            .trim_end_matches(is_space);
        let item_end = item_start + item.len();
        if item.is_empty() {
            let sentence = if first {
                "a number is missing after `(`"
            } else {
                "a number is missing after `,`"
            };
            let at = cursor.at();
            cursor.problem(at, at + 1, sentence, Some(BUCKET_RULE), Fix::None);
        } else {
            match number_form(item).map(|form| form.exact()) {
                None => cursor.problem(
                    item_start,
                    item_end,
                    format!("`{item}` is not a number"),
                    Some(BUCKET_RULE),
                    Fix::None,
                ),
                Some(Err(_)) => cursor.problem(
                    item_start,
                    item_end,
                    format!("`{item}` cannot be represented exactly"),
                    None,
                    Fix::None,
                ),
                Some(Ok(n)) if numbers.last().is_some_and(|&last| n <= last) => cursor.problem(
                    item_start,
                    item_end,
                    "each number of |bucket must be larger than the one before",
                    Some(BUCKET_RULE),
                    Fix::None,
                ),
                Some(Ok(n)) => numbers.push(n),
            }
        }
        first = false;
        if cursor.bump() == Some(')') {
            break;
        }
    }
    (cursor.problem_count() == before).then_some(numbers)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::read::AliasMode;
    use super::super::transform::Modifier;
    use super::*;

    /// Every key of the vocabulary, the content hash with them.
    fn every_key() -> impl Iterator<Item = Key> {
        Key::all().chain([Key::ContentFact(CONTENT_HASH.to_string())])
    }

    /// The keys read as the content hash are the two hash keys, and every built-in key is
    /// either one of them or computed by the resolver, never both and never neither.
    #[test]
    fn the_hash_keys_alone_read_the_content_hash() {
        let reading: Vec<String> = every_key()
            .filter(Key::reads_the_content_hash)
            .map(|key| key.name().to_string())
            .collect();
        assert_eq!(reading, ["hash", "hash_short"]);
        // Every built-in key is answered by exactly one of the two, so a reader asking both
        // meets no built-in key it cannot place.
        for key in every_key().filter(|key| key.kind() == KeyKind::BuiltinKey) {
            assert!(
                key.reads_the_content_hash() != key.is_computed(),
                "{}",
                key.name()
            );
        }
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
                read_key_name(name, NameReading::AsWritten),
                KeyName {
                    name: name.to_string(),
                    source_fact,
                    builtin,
                    content_key
                },
                "{name}"
            );
        }
    }

    /// **A name is read as its caller declares**: typed by a person, the key as Canon prints it
    /// reads back as that key, its name the key itself; as written, a name is the name, quotes
    /// and all — so a name already read is never read off twice.
    #[test]
    fn a_name_is_read_as_its_caller_declares() {
        use super::super::print::printed_key;
        for key in [
            "content.Make",
            "content.File Size",
            r#"content.a"b"#,
            "content.it's",
            "content.a,b",
            "content.Größe",
        ] {
            let typed = read_key_name(&printed_key(key), NameReading::QuotingReadOff);
            assert_eq!(
                (typed.name.as_str(), typed.content_key.as_deref()),
                (key, Some(key)),
                "{key}"
            );
            let written = read_key_name(key, NameReading::AsWritten);
            assert_eq!(written.name, key);
        }
        // `"""a"""` typed is the name `"a"`; read again as written it stays `"a"`.
        let typed = read_key_name(r#""""a""""#, NameReading::QuotingReadOff);
        assert_eq!(typed.name, r#""a""#);
        let again = read_key_name(&typed.name, NameReading::AsWritten);
        assert_eq!(again.content_key.as_deref(), Some(r#"content."a""#));
        // A tool's name is stored as written, quotes included, and `Key::read` — the re-read of
        // a name already read, as the grammar hands it a quoted name's text — reads it so too.
        let tool = read_key_name(r#""Foo""#, NameReading::AsWritten);
        assert_eq!(tool.content_key.as_deref(), Some(r#"content."Foo""#));
        assert_eq!(
            Key::read(r#""Foo""#),
            Some(Key::ContentFact(r#"content."Foo""#.to_string()))
        );
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
                read_key_name(name, NameReading::AsWritten)
                    .content_key
                    .as_deref(),
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
            assert_eq!(
                read_key_name(name, NameReading::AsWritten).content_key,
                None,
                "{name}"
            );
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

    /// A reading turns an alias into its stored key once, through the key grammar ([`read_key`])
    /// that `--where`, `--key` and `--group-by` share. It holds because a
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
            let filter = super::super::read::read_keys(name, 0, Grammar::Key, AliasMode::NotLoaded)
                .value
                .unwrap()
                .remove(0);
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

    // =========================================================================
    // The key grammar
    // =========================================================================

    use super::super::problem::Problem;
    use super::super::read::read_keys;

    type Said = (usize, String, Option<String>, Fix);

    fn said(problems: &[Problem]) -> Vec<Said> {
        problems
            .iter()
            .map(|p| {
                (
                    p.place.start,
                    p.sentence.clone(),
                    p.rule_line.as_deref().map(str::to_string),
                    p.fix.clone(),
                )
            })
            .collect()
    }

    /// Each key read without a problem, as typed, and every problem.
    fn read(text: &str, grammar: Grammar) -> (Vec<String>, Vec<Said>) {
        let reading = read_keys(text, 0, grammar, AliasMode::NotLoaded);
        let keys = reading.value.unwrap_or_default();
        (
            keys.into_iter().map(|k| k.input).collect(),
            said(&reading.problems),
        )
    }

    fn one(at: usize, sentence: &str) -> Vec<Said> {
        vec![(at, sentence.to_string(), None, Fix::None)]
    }

    fn with_rule(at: usize, sentence: &str, rule: &str) -> Vec<Said> {
        vec![(at, sentence.to_string(), Some(rule.to_string()), Fix::None)]
    }

    /// Where a filter's key ends and where the cursor stands after it.
    fn key_in_filter(text: &str) -> (InputKey, usize) {
        let mut cursor = Cursor::new(text, 0);
        let key = read_key(&mut cursor, Grammar::Filter).expect(text);
        assert_eq!(cursor.problem_count(), 0, "{text}");
        (key, cursor.at())
    }

    #[test]
    fn a_key_ends_at_the_first_character_that_does_not_fit() {
        for (text, key, end) in [
            ("source.rel_path|stem[0]=a", "source.rel_path|stem", 20),
            ("content.Foo@Bar=1", "content.Foo", 11),
            ("source.mtime|year|stem(1)", "source.mtime|year|stem", 22),
            ("Make IN (a)", "Make", 4),
            ("source.rel_path[-1]|stem?", "source.rel_path[-1]|stem", 24),
        ] {
            let (read, at) = key_in_filter(text);
            assert_eq!((read.input.as_str(), at), (key, end), "{text}");
        }
        let (key, _) = key_in_filter("source.rel_path[-1]|stem?");
        assert_eq!(key.input_name, "source.rel_path");
        assert_eq!(
            key.accessor.map(|(a, p)| (a, p.start, p.end)),
            Some((PathAccessor::Index(-1), 15, 19))
        );
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].0.modifier, Modifier::Stem);

        // Where only the end may follow, what follows is named as it stands.
        assert_eq!(
            read("source.rel_path|stem(5)", Grammar::Key).1,
            one(20, "`(5)` cannot follow `source.rel_path|stem`")
        );
    }

    #[test]
    fn only_bucket_takes_numbers() {
        const RULE: &str = "|bucket takes numbers: |bucket(10,100)";
        for text in [
            "source.size|bucket(60,300)",
            "source.size|bucket( 60 , 300 )",
        ] {
            let reading = read_keys(text, 0, Grammar::Key, AliasMode::NotLoaded);
            assert!(reading.problems.is_empty(), "{text}");
            let keys = reading.value.unwrap();
            assert_eq!(keys[0].modifiers[0].0.args, vec![60.0, 300.0], "{text}");
        }
        for (text, at, sentence) in [
            ("source.size|bucket()", 19, "a number is missing after `(`"),
            ("source.size|bucket(a)", 19, "`a` is not a number"),
            (
                "source.size|bucket(300,60)",
                23,
                "each number of |bucket must be larger than the one before",
            ),
            (
                "source.size|bucket(1,1)",
                21,
                "each number of |bucket must be larger than the one before",
            ),
            (
                "source.size|bucket(60,)",
                22,
                "a number is missing after `,`",
            ),
        ] {
            assert_eq!(
                read(text, Grammar::Key),
                (vec![], with_rule(at, sentence, RULE)),
                "{text}"
            );
        }
        // Any other modifier takes nothing: the key ends at its `(`.
        assert_eq!(
            read("source.rel_path|stem(1)", Grammar::Key).1,
            one(20, "`(1)` cannot follow `source.rel_path|stem`")
        );
    }

    #[test]
    fn a_comma_inside_bucket_or_a_quoted_name_belongs_to_the_key() {
        assert_eq!(
            read("Make,source.size|bucket(60,300)", Grammar::KeyList),
            (
                vec!["Make".to_string(), "source.size|bucket(60,300)".to_string()],
                vec![]
            )
        );
        let reading = read_keys(r#""a,b",Make"#, 0, Grammar::KeyList, AliasMode::NotLoaded);
        assert!(reading.problems.is_empty());
        let keys = reading.value.unwrap();
        assert_eq!(keys[0].input, r#""a,b""#);
        assert_eq!(keys[0].base, "content.a,b");
        assert_eq!(keys[1].base, "content.Make");
    }

    #[test]
    fn spaces_around_a_comma_in_a_key_list() {
        for text in ["Make , source.ext", " Make, source.ext ", "Make,source.ext"] {
            assert_eq!(
                read(text, Grammar::KeyList),
                (vec!["Make".to_string(), "source.ext".to_string()], vec![]),
                "{text}"
            );
        }
        assert_eq!(
            read(" Make ", Grammar::Key),
            (vec!["Make".to_string()], vec![])
        );
    }

    #[test]
    fn the_empty_key_sentences() {
        for (text, grammar) in [
            ("", Grammar::Key),
            ("  ", Grammar::Key),
            ("", Grammar::KeyList),
        ] {
            assert_eq!(
                read(text, grammar),
                (vec![], one(0, "the key is empty")),
                "{text:?}"
            );
        }
        let reading = read_keys("", 0, Grammar::Key, AliasMode::NotLoaded);
        assert_eq!(reading.problems[0].render("", "  "), ["  the key is empty"]);
        assert_eq!(
            read("Make,", Grammar::KeyList),
            (
                vec!["Make".to_string()],
                one(5, "a key is missing after `,`")
            )
        );
        assert_eq!(
            read("Make,,Model", Grammar::KeyList),
            (
                vec!["Make".to_string(), "Model".to_string()],
                one(5, "a key is missing after `,`")
            )
        );
        assert_eq!(
            read(",Make", Grammar::KeyList),
            (
                vec!["Make".to_string()],
                one(0, "a key is missing before `,`")
            )
        );
    }

    #[test]
    fn a_name_with_a_dash_reads_bare() {
        let reading = read_keys(
            "content.XMP-dc:Subject",
            0,
            Grammar::Key,
            AliasMode::NotLoaded,
        );
        assert!(reading.problems.is_empty());
        let key = &reading.value.unwrap()[0];
        assert_eq!(key.base, "content.XMP-dc:Subject");
        assert_eq!(
            key.key,
            Some(Key::ContentFact("content.XMP-dc:Subject".to_string()))
        );
    }

    #[test]
    fn modifier_names_are_exact_lower_case() {
        assert_eq!(
            read("source.mtime|YEAR", Grammar::Key),
            (
                vec![],
                one(
                    12,
                    "`|YEAR` is not a modifier — Modifiers: year, month, day, hour, minute, second, \
                     date, time, datetime, yearmonth, week, weekday, quarter, stem, ext, short, \
                     lowercase, uppercase, capitalize, bucket, number"
                )
            )
        );
        let (keys, problems) = read("source.mtime|year", Grammar::Key);
        assert_eq!((keys.len(), problems.len()), (1, 0));
    }

    /// The rest of the key grammar's sentences, one input each.
    #[test]
    fn every_sentence_of_the_key_grammar() {
        const POSITIONS: &str = "positions are whole numbers: [0], [-1], [1:3]";
        let position = |part: &str| format!("`{part}` is not a position — {POSITIONS}");
        for (text, at, sentence) in [
            ("source.rel_path[x]", 16, position("x")),
            ("source.rel_path[+0]", 16, position("+0")),
            ("source.rel_path[1:x]", 18, position("x")),
            ("source.rel_path[1:2:3]", 18, position("2:3")),
            ("source.rel_path[ 1]", 16, position(" 1")),
            (
                "source.rel_path[]",
                16,
                "a position is missing after `[`".to_string(),
            ),
            ("source.rel_path[0", 15, "this `[` is unclosed".to_string()),
            (
                "source.mtime|",
                13,
                "a modifier is missing after `|`".to_string(),
            ),
            ("%x", 0, "`%x` is not a key".to_string()),
            (r#""""#, 0, r#"`""` is not a key"#.to_string()),
            ("'Make", 0, "this quote is unclosed".to_string()),
            (
                "scope.rel_path",
                0,
                "`scope.rel_path` can only be used in a pattern".to_string(),
            ),
        ] {
            assert_eq!(
                read(text, Grammar::Key),
                (vec![], one(at, &sentence)),
                "{text}"
            );
        }
        for slice in [
            "source.rel_path[1:]",
            "source.rel_path[:-1]",
            "source.rel_path[:]",
        ] {
            let (keys, problems) = read(slice, Grammar::Key);
            assert_eq!((keys.len(), problems.len()), (1, 0), "{slice}");
        }
        // A status word is not a key; quoted, it names the stored fact.
        assert_eq!(
            read("archived", Grammar::Key),
            (
                vec![],
                with_rule(
                    0,
                    "`archived` is not a key",
                    "a stored fact of that name is `content.archived`"
                )
            )
        );
        let reading = read_keys(r#""archived""#, 0, Grammar::Key, AliasMode::NotLoaded);
        assert!(reading.problems.is_empty());
        assert_eq!(reading.value.unwrap()[0].base, "content.archived");
        // A built-in key's reading is a leading modifier.
        let reading = read_keys("hash_short", 0, Grammar::Key, AliasMode::NotLoaded);
        let key = &reading.value.unwrap()[0];
        assert_eq!(key.base, CONTENT_HASH);
        assert_eq!(key.modifiers[0].0.modifier, Modifier::Short);
    }

    /// **What ends a bare name is the position's**: in a filter the first character that is not
    /// a name character; where a key stands alone only what may follow it there — `[` or `|`,
    /// and in a key list `,` — so a space inside is the name's and spaces at its ends are not.
    #[test]
    fn a_bare_name_ends_where_its_position_says() {
        let bases = |text: &str, grammar: Grammar| {
            let reading = read_keys(text, 0, grammar, AliasMode::NotLoaded);
            assert!(
                reading.problems.is_empty(),
                "{text}: {:?}",
                reading.problems
            );
            let keys = reading.value.unwrap();
            keys.into_iter()
                .map(|k| (k.input, k.base))
                .collect::<Vec<_>>()
        };
        let pair = |input: &str, base: &str| (input.to_string(), base.to_string());
        // A space inside the name is the name's where a key stands alone.
        assert_eq!(
            bases("content.File Size", Grammar::Key),
            [pair("content.File Size", "content.File Size")]
        );
        assert_eq!(
            bases("content.File Size, Make", Grammar::KeyList),
            [
                pair("content.File Size", "content.File Size"),
                pair("Make", "content.Make")
            ]
        );
        let mut cursor = Cursor::new("content.File Size", 0);
        let key = read_key(&mut cursor, Grammar::Pattern).unwrap();
        assert_eq!((key.base.as_str(), cursor.at()), ("content.File Size", 17));
        // In a filter a space still ends it, an operator after it.
        let (key, at) = key_in_filter("content.File Size=1");
        assert_eq!((key.base.as_str(), at), ("content.File", 12));
        // Every character up to what may follow the name is the name's.
        for (text, grammar, want) in [
            (
                "content.Foo@Bar",
                Grammar::Key,
                vec![pair("content.Foo@Bar", "content.Foo@Bar")],
            ),
            (
                "hash_short@x",
                Grammar::Key,
                vec![pair("hash_short@x", "content.hash_short@x")],
            ),
            (
                "content.Foo(5)",
                Grammar::Key,
                vec![pair("content.Foo(5)", "content.Foo(5)")],
            ),
            (
                "Make's,Model",
                Grammar::KeyList,
                vec![
                    pair("Make's", "content.Make's"),
                    pair("Model", "content.Model"),
                ],
            ),
            // Quoted text where a part starts is stepped over, so its `,` is the name's.
            (
                "Make x='a,b',Model",
                Grammar::KeyList,
                vec![
                    pair("Make x='a,b'", "content.Make x='a,b'"),
                    pair("Model", "content.Model"),
                ],
            ),
            // A `(` that never closes hides no `,`.
            (
                "Make x(a, Model",
                Grammar::KeyList,
                vec![
                    pair("Make x(a", "content.Make x(a"),
                    pair("Model", "content.Model"),
                ],
            ),
        ] {
            assert_eq!(bases(text, grammar), want, "{text}");
        }
        // `|` and `[` still start a modifier and an accessor, after spaces too.
        for (text, base, accessor, modifiers) in [
            ("content.File Size|year", "content.File Size", false, 1),
            ("source.rel_path[-1]|stem", "source.rel_path", true, 1),
            ("content.File Size |year", "content.File Size", false, 1),
            ("source.rel_path [0]", "source.rel_path", true, 0),
        ] {
            let reading = read_keys(text, 0, Grammar::Key, AliasMode::NotLoaded);
            assert!(reading.problems.is_empty(), "{text}");
            let key = &reading.value.unwrap()[0];
            assert_eq!(
                (
                    key.base.as_str(),
                    key.accessor.is_some(),
                    key.modifiers.len()
                ),
                (base, accessor, modifiers),
                "{text}"
            );
        }
        // Spaces at the ends are not the name's, and its place does not cover them.
        let reading = read_keys(
            "  content.File Size  ",
            0,
            Grammar::Key,
            AliasMode::NotLoaded,
        );
        let key = &reading.value.unwrap()[0];
        assert_eq!(
            (key.base.as_str(), key.name_place.start, key.name_place.end),
            ("content.File Size", 2, 19)
        );
        // What follows a key's modifiers, or a quoted name, still cannot follow it.
        assert_eq!(
            read("Make|year x", Grammar::Key),
            (vec![], one(10, "`x` cannot follow `Make|year`"))
        );
        assert_eq!(
            read(r#""a"b"#, Grammar::Key),
            (vec![], one(3, r#"`b` cannot follow `"a"`"#))
        );
        // Text that is not a key pairs the `(` it starts with.
        assert_eq!(
            read("(a,b), Model", Grammar::KeyList),
            (vec!["Model".to_string()], one(0, "`(a,b)` is not a key"))
        );
        // A quote that never closes inside the name is named, and reading stops.
        assert_eq!(
            read("Make 'x", Grammar::Key),
            (vec![], one(5, "this quote is unclosed"))
        );
    }

    /// The quoted name a filter offers where the character straight after a bare name can start
    /// nothing that may follow a key there: a space or an operator may follow a name; `@` or `(`
    /// cannot.
    #[test]
    fn a_filter_offers_a_quoted_name_only_where_nothing_may_follow_the_name() {
        for (text, want) in [
            ("content.Foo@Bar=1", Some(r#""content.Foo@Bar""#)),
            ("content.Foo(5)=1", Some(r#""content.Foo(5)""#)),
            ("content.Foo@Bar|year=1", Some(r#""content.Foo@Bar""#)),
            ("content.Foo =1", None),
            ("content.Foo!=1", None),
            ("content.Foo?", None),
        ] {
            let (key, at) = key_in_filter(text);
            assert_eq!(key.quoted_name_fix(text, at).as_deref(), want, "{text}");
        }
    }

    // =========================================================================
    // A report key's header and rows
    // =========================================================================

    /// **The rows' type word**, one per kind a key's chain can give and `mixed` — the match is
    /// exhaustive, so a new kind fails to compile until it has a word.
    #[test]
    fn the_rows_word_names_every_kind_a_chain_gives() {
        for (text, base, word) in [
            ("content.S", FactType::Text, "text"),
            ("source.rel_path", FactType::Path, "path"),
            ("content.N", FactType::Num, "number"),
            ("content.T", FactType::Time, "time"),
            ("content.T|datetime", FactType::Time, "time"),
            ("content.T|date", FactType::Time, "time"),
            ("content.T|yearmonth", FactType::Time, "time"),
            ("content.T|time", FactType::Time, "time of day"),
            ("content.T|year", FactType::Time, "number"),
            ("content.T|weekday", FactType::Time, "number"),
            ("content.T|quarter", FactType::Time, "number"),
            ("content.N|bucket", FactType::Num, "range"),
            ("content.N|bucket(60,300)", FactType::Num, "range"),
            ("source.rel_path[-1]", FactType::Path, "text"),
            ("source.rel_path[:-1]", FactType::Path, "path"),
        ] {
            let key = ParsedFactKey::parse_as(text, base).unwrap();
            assert_eq!(key.rows_word(), word, "{text}");
            assert!(key.mixed_types().is_none(), "{text}");
        }
        let mut mixed = ParsedFactKey::parse("content.M").unwrap();
        mixed.rows = Rows::Mixed(KeyTypes::of(FactType::Text));
        assert_eq!(mixed.rows_word(), "mixed");
        assert!(mixed.mixed_types().is_some());
    }

    /// **The header expression**: the name as the user spelled it — bare where it reads back as
    /// the same key, otherwise in Canon's quotes, never swapped for the full stored key — then the
    /// accessor and modifiers typed after it, each in its one spelling; a built-in key's own
    /// reading is not typed, and a quoted name keeps reading as what it read as.
    #[test]
    fn a_key_prints_as_the_user_spelled_it() {
        for (typed, printed) in [
            ("content.File Size", "\"content.File Size\""),
            ("\"content.File Size\"", "\"content.File Size\""),
            ("File Size", "\"File Size\""),
            ("mtime", "mtime"),
            ("\"mtime\"", "mtime"),
            ("Model", "Model"),
            ("hash_short", "hash_short"),
            ("hash_short|uppercase", "hash_short|uppercase"),
            ("source.rel_path[:-1]", "source.rel_path[:-1]"),
            ("source.rel_path [-1]|stem", "source.rel_path[-1]|stem"),
            (
                "content.N|bucket( 60 ,300.50 )",
                "content.N|bucket(60,300.5)",
            ),
            ("\"archived\"", "\"archived\""),
            ("\"content.a|b\"", "\"content.a|b\""),
        ] {
            let key = ParsedFactKey::parse(typed).unwrap();
            assert_eq!(key.printed(), printed, "{typed}");
            let again = ParsedFactKey::parse(printed).unwrap();
            assert_eq!(
                (again.key.clone(), again.base_key.clone()),
                (key.key.clone(), key.base_key.clone()),
                "{printed} reads back as {typed}"
            );
        }
    }

    /// **Every modifier and accessor, spelled, reads back as itself**: the same call, the same
    /// accessor.
    #[test]
    fn spelled_transforms_read_back_as_themselves() {
        use strum::IntoEnumIterator;
        let mut calls: Vec<ModifierCall> = Modifier::iter()
            .map(|modifier| ModifierCall {
                modifier,
                args: Vec::new(),
            })
            .collect();
        calls.push(ModifierCall {
            modifier: Modifier::Bucket,
            args: vec![-5.0, 0.25, 60.0, 1e6],
        });
        for call in calls {
            let text = format!("content.K{}", call.spelled());
            let read = read_one(&text).unwrap();
            assert_eq!(read.modifiers.len(), 1, "{text}");
            assert_eq!(read.modifiers[0].0, call, "{text}");
        }
        for accessor in [
            PathAccessor::Index(0),
            PathAccessor::Index(-1),
            PathAccessor::Slice {
                start: Some(1),
                end: Some(3),
            },
            PathAccessor::Slice {
                start: None,
                end: Some(-1),
            },
            PathAccessor::Slice {
                start: Some(-2),
                end: None,
            },
            PathAccessor::Slice {
                start: None,
                end: None,
            },
        ] {
            let text = format!("source.rel_path{}", accessor.spelled());
            let read = read_one(&text).unwrap();
            assert_eq!(
                read.accessor.map(|(read, _)| read),
                Some(accessor),
                "{text}"
            );
        }
    }

    /// **A default, as an expression**: the modifier added after the key's own, the rows walked
    /// on, the key read as its one type — a modification time by year, a size by range, an alias
    /// for one as its key would; a modifier the table refuses, or a key of mixed type, is an
    /// error.
    #[test]
    fn a_default_is_added_as_a_modifier() {
        let year = ParsedFactKey::parse("source.mtime")
            .unwrap()
            .with_modifier(Modifier::Year)
            .unwrap();
        assert_eq!(year.printed(), "source.mtime|year");
        assert!(matches!(year.rows, Rows::One(Gives::Part(Modifier::Year))));
        assert_eq!(year.read_as, ReadAs::One(FactType::Time));

        let size = ParsedFactKey::parse("size")
            .unwrap()
            .with_modifier(Modifier::Bucket)
            .unwrap();
        assert_eq!(size.printed(), "size|bucket");
        assert_eq!(size.rows_word(), "range");

        let refused = ParsedFactKey::parse("source.ext")
            .unwrap()
            .with_modifier(Modifier::Bucket);
        assert!(refused.is_err());
        let mut mixed = ParsedFactKey::parse("content.M").unwrap();
        mixed.rows = Rows::Mixed(KeyTypes::of(FactType::Text));
        assert!(mixed.with_modifier(Modifier::Year).is_err());
    }
}
