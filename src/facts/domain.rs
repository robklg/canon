//! Import-vocabulary half of the fact domain model: the record of a content
//! fact stored on the source, which promotion reads, key classification, and
//! the keys an import stores and `facts delete` acts on.
//! A key's name is read by the expression facility's one reader
//! (`expr::read_key_name`). A value's kind is the fact noun's own
//! (`core::domain::fact::FactType`).

pub(super) mod convert;

use std::collections::{BTreeSet, HashMap};

use serde_json::value::RawValue;

use crate::core::domain::conversion::{convert, Converted, Loss, NotRead};
use crate::core::domain::fact::{Count, FactType, FactValue, KeyTypes};
use crate::core::domain::format_count;
use crate::core::domain::number::{number_form, read_number, NotExact};
use crate::core::domain::time::reading::{read_time, read_year_number};
use crate::expr::{read_key_name, NameReading, CONTENT_HASH};

/// `n` and a noun in its form for `n`: `1 fact`, `4 facts`.
pub fn counted(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", format_count(n), plural(n, one, many))
}

/// The noun's form for `n`, by the fact noun's one/many rule.
pub fn plural<'a>(n: u64, one: &'a str, many: &'a str) -> &'a str {
    match Count::of(n) {
        Count::One => one,
        Count::Many => many,
    }
}

/// A content fact stored on the source, moved to the content when an import
/// links the source to it.
///
/// An import that arrives before a source's content is known stores its facts
/// on the source; once an import links the source to its object, promotion
/// moves them to the object. This struct holds what that move reads.
#[derive(Debug, Clone)]
pub struct ContentFactOnSource {
    /// The fact ID in the database
    pub id: i64,
    /// The fact key (e.g., "content.Make")
    pub key: String,
    /// The stored value
    pub value: FactValue,
    /// When this fact was observed
    pub observed_at: i64,
}

/// The key `facts delete` acts on: the name's content reading (`Make` →
/// `content.Make`), Canon's printed quoting read off it first, so a key copied
/// from Canon's output names that key — or a refusal for a name with none — any
/// name starting with `source.`, a source fact or not. Read once, before anything
/// else the command does, so no empty selection hides the refusal.
pub fn delete_key(given: &str) -> anyhow::Result<String> {
    let reading = read_key_name(given, NameReading::QuotingReadOff);
    let key = reading.content_key.ok_or_else(|| {
        anyhow::anyhow!(
            "Cannot delete '{}': names starting with 'source.' cannot be deleted.",
            reading.name
        )
    })?;
    if is_the_content_hash(&key) {
        return Err(DeleteRefused::ContentHash.into());
    }
    Ok(key)
}

/// Which facts of a key a deletion acts on: those stored on the matching sources, or those
/// stored on their content (`--on`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactSide {
    Source,
    Object,
}

impl FactSide {
    /// The words `--on` takes.
    pub fn parse(word: &str) -> Option<FactSide> {
        match word {
            "source" => Some(FactSide::Source),
            "object" => Some(FactSide::Object),
            _ => None,
        }
    }

    /// The noun the side's facts are counted across: `source`/`sources`, `object`/`objects`.
    pub fn noun(self, n: u64) -> &'static str {
        match self {
            FactSide::Source => plural(n, "source", "sources"),
            FactSide::Object => plural(n, "object", "objects"),
        }
    }

    pub fn other(self) -> FactSide {
        match self {
            FactSide::Source => FactSide::Object,
            FactSide::Object => FactSide::Source,
        }
    }

    /// The flag that names the side: `--on source`.
    pub fn flag(self) -> &'static str {
        match self {
            FactSide::Source => "--on source",
            FactSide::Object => "--on object",
        }
    }

    /// The `entity_type` a fact stored on this side carries.
    pub fn as_stored(self) -> &'static str {
        match self {
            FactSide::Source => "source",
            FactSide::Object => "object",
        }
    }
}

/// Whether a stored key is the content hash, which `facts delete` and `facts convert` refuse:
/// decided by the key a name resolves to, never by its spelling (the key-name law).
pub fn is_the_content_hash(key: &str) -> bool {
    key == CONTENT_HASH
}

/// Why `facts delete` and `facts convert` refuse the content hash, and the way to correct one:
/// `[reason, way out]`. Each command ends the reason with its own consequence.
pub fn content_hash_answer(consequence: &str) -> [String; 2] {
    [
        format!(
            "The content hash ({CONTENT_HASH}) is Canon's own record of content identity, \
             {consequence}"
        ),
        "to correct a hash a tool imported wrongly, import the right one with \
         `canon import-facts`, which moves the source to its content"
            .to_string(),
    ]
}

/// A type word as typed: text, number, time, or `duration`, which the command taking it
/// answers — `facts convert --to` with its routes, `facts delete --value-type` with where
/// durations are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeWord {
    Type(FactType),
    Duration,
}

impl TypeWord {
    /// The type words (and quietly `num`, `datetime`), and `duration`.
    pub fn parse_word(word: &str) -> Option<TypeWord> {
        match word {
            "duration" => Some(TypeWord::Duration),
            other => FactType::parse_word(other).map(TypeWord::Type),
        }
    }
}

/// **A deletion refused** before anything is read, carried out to `main`: both are answers,
/// printed without `Error:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteRefused {
    /// The name resolves to the content hash.
    ContentHash,
    /// `--value-type duration`: Canon has no duration type.
    Duration,
}

impl DeleteRefused {
    /// Whether the refusal is an answer: both are.
    pub fn is_answer(&self) -> bool {
        match self {
            DeleteRefused::ContentHash | DeleteRefused::Duration => true,
        }
    }

    fn lines(&self) -> Vec<String> {
        match self {
            DeleteRefused::ContentHash => {
                content_hash_answer("and its facts are not deleted").into()
            }
            // Canon knows a value is a duration only where its tool gave the `duration` hint, and
            // an import stores every such value as a number (`decide`). A later conversion of the
            // key to text changes that, which the sentence does not follow.
            DeleteRefused::Duration => vec![format!(
                "Canon has no duration type: durations are stored as {}",
                FactType::Num.noun(Count::Many)
            )],
        }
    }
}

impl std::fmt::Display for DeleteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.lines().join("\n"))
    }
}

impl std::error::Error for DeleteRefused {}

/// The stored type a deletion is narrowed to, or the answer for `duration`.
pub fn delete_value_type(word: Option<TypeWord>) -> Result<Option<FactType>, DeleteRefused> {
    match word {
        None => Ok(None),
        Some(TypeWord::Type(t)) => Ok(Some(t)),
        Some(TypeWord::Duration) => Err(DeleteRefused::Duration),
    }
}

/// The key an import stores a fact under: the name's content reading (`Make` →
/// `content.Make`), or `None` for a name with none — any name starting with
/// `source.`, which the import skips. A tool's name is stored as the tool wrote it,
/// quotes included: no quoting is read off it.
pub fn import_key(name: &str) -> Option<String> {
    read_key_name(name, NameReading::AsWritten).content_key
}

/// Check if a key is a content fact (starts with "content.").
///
/// Content facts are stored on objects when available, and are eligible
/// for promotion from source to object when the source gets linked.
pub fn is_content_fact(key: &str) -> bool {
    key.starts_with("content.")
}

// ============================================================================
// What arrived, and the decision
// ============================================================================

/// A fact value as an import record carries it, before the key decides what it is. A JSON
/// number keeps the text the tool wrote, so no number passes through a float on its way in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonFact {
    Text(String),
    /// A JSON number, in the spelling the tool wrote (`1.10`, `1e5`, `-0`).
    Number(String),
    Bool(bool),
    Null,
    /// A list, or an object that is no type hint, as compact JSON text.
    Structure(String),
    /// `{"value": …, "type": "<hint>"}`: a value with the type its tool declares, and that
    /// value as the tool wrote it.
    Hinted {
        hint: Hint,
        value: Box<JsonFact>,
        written: String,
    },
}

/// The type hints Canon reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KnownHint {
    Datetime,
    Duration,
    Number,
}

/// A type hint as written: one Canon reads, or another word.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hint {
    Known(KnownHint),
    Unknown(String),
}

impl Hint {
    fn from_word(word: String) -> Hint {
        match word.as_str() {
            "datetime" => Hint::Known(KnownHint::Datetime),
            "duration" => Hint::Known(KnownHint::Duration),
            "number" => Hint::Known(KnownHint::Number),
            _ => Hint::Unknown(word),
        }
    }
}

/// What an import does with one arriving value.
#[derive(Debug, Clone)]
pub enum Disposition {
    Store(FactValue),
    Skip(SkipCause),
}

/// The type a value was read as and did not read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Reading {
    Number,
    Time,
}

/// A hinted value that reads as its hint, for a key that holds another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Misfit {
    DatetimeForText,
    DatetimeForNumber,
    DurationForText,
    DurationForTime,
}

/// Why an arriving value is not stored. Ordered as declared, the order a run's lines print in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SkipCause {
    /// `null`: the tool says there is no value.
    NoValue,
    /// The key holds two types; it takes no value until converted.
    MixedKey,
    /// A key that two names in one record read to (`Model` and `content.Model`), each with a
    /// value: neither is stored, since nothing says which one the tool meant. A name whose value
    /// is `null` names no key.
    NamedTwice,
    /// A string that does not read as the key's type.
    DoesNotRead(Reading),
    /// A string whose number reading would lose what the text carried.
    WouldLose(Loss),
    /// A number for a key that holds times: a JSON number, or a value with the `number` hint.
    NumberForTime,
    /// A number Canon cannot hold: written as an integer it cannot hold exactly, or too large to
    /// hold.
    NotExact,
    /// `true` or `false` for a key that holds times.
    BoolForTime,
    /// A list or an object for a key that holds numbers or times.
    Structure(Reading),
    HintMisfit(Misfit),
    HintDoesNotRead(KnownHint),
    UnknownHint(String),
    /// A source fact passed back, such as `worklist --emit` writes: Canon records its own.
    SourceFact,
    /// Any other name starting with `source.`.
    SourceName,
}

impl JsonFact {
    /// Whether the tool says there is no value: a bare `null`, the one value `decide` counts as
    /// a null rather than a value with a cause to fix. A hinted `null` is a value.
    pub fn is_no_value(&self) -> bool {
        matches!(self, JsonFact::Null)
    }

    /// The value as the tool wrote it, given its raw JSON: a hint's own value, else the whole.
    pub fn written<'a>(&'a self, raw: &'a RawValue) -> &'a str {
        match self {
            JsonFact::Hinted { written, .. } => written,
            JsonFact::Text(_)
            | JsonFact::Number(_)
            | JsonFact::Bool(_)
            | JsonFact::Null
            | JsonFact::Structure(_) => raw.get().trim(),
        }
    }

    /// Read an arriving value by its first character: a string, a number kept as written, a
    /// boolean, `null`, a list, or an object — a type hint where it holds a `value` and a
    /// string `type`, other keys ignored, else a structure. A list or a non-hint object is
    /// written back as compact JSON.
    pub fn from_raw(raw: &RawValue) -> anyhow::Result<JsonFact> {
        let text = raw.get().trim();
        Ok(match text.chars().next() {
            Some('"') => JsonFact::Text(serde_json::from_str(text)?),
            Some('-' | '0'..='9') => JsonFact::Number(text.to_string()),
            Some('t' | 'f') => JsonFact::Bool(serde_json::from_str(text)?),
            Some('n') => JsonFact::Null,
            Some('{') => {
                let mut object: HashMap<String, Box<RawValue>> = serde_json::from_str(text)?;
                let hint = object
                    .get("type")
                    .and_then(|t| serde_json::from_str::<String>(t.get()).ok());
                match (hint, object.remove("value")) {
                    (Some(hint), Some(value)) => JsonFact::Hinted {
                        hint: Hint::from_word(hint),
                        value: Box::new(JsonFact::from_raw(&value)?),
                        written: value.get().trim().to_string(),
                    },
                    (Some(_) | None, Some(_) | None) => JsonFact::Structure(compact(text)?),
                }
            }
            Some(_) | None => JsonFact::Structure(compact(text)?),
        })
    }
}

/// JSON text written back compactly.
fn compact(text: &str) -> anyhow::Result<String> {
    Ok(serde_json::from_str::<serde_json::Value>(text)?.to_string())
}

/// What the key holds when a value arrives.
enum KeyState {
    New,
    Holds(FactType),
    Mixed,
}

/// **The key decides**: what an import does with one arriving value, given the types the key's
/// facts hold (`None` for a key with no facts). The value is read as the key's type; a key
/// with no type takes its type from the value — the hint, or the JSON value's own (a number
/// is a number, a string text). Nothing is stored lossy or as a second type.
pub fn decide(key: Option<&KeyTypes>, fact: &JsonFact) -> Disposition {
    let state = match key.map(KeyTypes::single) {
        None => KeyState::New,
        Some(Some(t)) => KeyState::Holds(t),
        Some(None) => KeyState::Mixed,
    };
    if fact.is_no_value() {
        return Disposition::Skip(SkipCause::NoValue);
    }
    let held = match state {
        KeyState::Mixed => return Disposition::Skip(SkipCause::MixedKey),
        KeyState::New => None,
        KeyState::Holds(t) => Some(t),
    };
    match fact {
        JsonFact::Null => Disposition::Skip(SkipCause::NoValue),
        JsonFact::Hinted { hint, value, .. } => decide_hinted(held, hint, value),
        JsonFact::Text(text) => decide_text(held, text),
        JsonFact::Number(raw) => decide_number(held, raw),
        JsonFact::Bool(b) => match held {
            Some(FactType::Text) => Disposition::Store(FactValue::Text(b.to_string())),
            None | Some(FactType::Num) => {
                Disposition::Store(FactValue::Num(if *b { 1.0 } else { 0.0 }))
            }
            Some(FactType::Time) => Disposition::Skip(SkipCause::BoolForTime),
            Some(FactType::Path) => no_stored_path(),
        },
        JsonFact::Structure(json) => match held {
            None | Some(FactType::Text) => Disposition::Store(FactValue::Text(json.clone())),
            Some(FactType::Num) => Disposition::Skip(SkipCause::Structure(Reading::Number)),
            Some(FactType::Time) => Disposition::Skip(SkipCause::Structure(Reading::Time)),
            Some(FactType::Path) => no_stored_path(),
        },
    }
}

/// A key whose type is a path: none is stored, since a path is derived (`key_types` reads text,
/// number or time), so this answers only for the match to be whole.
fn no_stored_path() -> Disposition {
    Disposition::Skip(SkipCause::DoesNotRead(Reading::Number))
}

/// A string: text for a new or text key; read as the key's type by the conversion table
/// otherwise, skipped where it does not read or would lose what it carried.
fn decide_text(held: Option<FactType>, text: &str) -> Disposition {
    let (to, reading) = match held {
        None | Some(FactType::Text) => {
            return Disposition::Store(FactValue::Text(text.to_string()))
        }
        Some(FactType::Num) => (FactType::Num, Reading::Number),
        Some(FactType::Time) => (FactType::Time, Reading::Time),
        Some(FactType::Path) => return no_stored_path(),
    };
    match convert(&FactValue::Text(text.to_string()), to) {
        Ok(Converted::Unchanged(value)) => Disposition::Store(value),
        Ok(Converted::Lossy(_, loss)) => Disposition::Skip(SkipCause::WouldLose(loss)),
        Ok(Converted::DoesNotRead(NotRead::Number(_) | NotRead::Time(_))) | Err(_) => {
            Disposition::Skip(SkipCause::DoesNotRead(reading))
        }
    }
}

/// A JSON number in the spelling the tool wrote: that text for a text key; skipped for a time
/// key; for a number or new key the number Canon holds for it ([`held_number`]).
fn decide_number(held: Option<FactType>, raw: &str) -> Disposition {
    match held {
        Some(FactType::Text) => Disposition::Store(FactValue::Text(raw.to_string())),
        Some(FactType::Time) => Disposition::Skip(SkipCause::NumberForTime),
        Some(FactType::Path) => no_stored_path(),
        None | Some(FactType::Num) => match held_number(raw) {
            Ok(n) => Disposition::Store(FactValue::Num(n)),
            Err(NotExact) => Disposition::Skip(SkipCause::NotExact),
        },
    }
}

/// The number Canon holds for a JSON number, unhinted or with the `number` hint. Not held, so
/// skipped rather than changed: one written as an integer Canon cannot hold exactly, and one too
/// large to hold. A fraction lands as the nearest number held.
fn held_number(raw: &str) -> Result<f64, NotExact> {
    let form = number_form(raw).ok_or(NotExact)?;
    let n = read_number(raw).map_err(|_| NotExact)?;
    if form.written_as_integer() {
        form.exact()
    } else {
        Ok(n)
    }
}

/// A hinted value: read as its hint, then fitted to the key. A `number` hint's value is decided
/// as a JSON number with that spelling — the tool said it is a number, so its spelling is not
/// judged for loss; whether it is written as a number is the grammar's answer.
fn decide_hinted(held: Option<FactType>, hint: &Hint, value: &JsonFact) -> Disposition {
    let known = match hint {
        Hint::Known(known) => *known,
        Hint::Unknown(word) => return Disposition::Skip(SkipCause::UnknownHint(word.clone())),
    };
    let does_not_read = Disposition::Skip(SkipCause::HintDoesNotRead(known));
    let reads_as = match known {
        KnownHint::Number => {
            return match value {
                JsonFact::Text(s) | JsonFact::Number(s) if number_form(s).is_some() => {
                    decide_number(held, s)
                }
                JsonFact::Text(_)
                | JsonFact::Number(_)
                | JsonFact::Bool(_)
                | JsonFact::Null
                | JsonFact::Structure(_)
                | JsonFact::Hinted { .. } => does_not_read,
            }
        }
        KnownHint::Datetime => ReadsAs::Time,
        KnownHint::Duration => ReadsAs::Duration,
    };
    let read = match (reads_as, value) {
        (ReadsAs::Time, JsonFact::Text(s)) => read_time(s).ok().map(FactValue::Time),
        (ReadsAs::Time, JsonFact::Number(s)) => read_number(s)
            .ok()
            .and_then(|n| read_year_number(n).ok())
            .map(FactValue::Time),
        (ReadsAs::Duration, JsonFact::Text(s)) => try_parse_duration(s).map(FactValue::Num),
        (ReadsAs::Duration, JsonFact::Number(s)) => read_number(s).ok().map(FactValue::Num),
        (
            ReadsAs::Time | ReadsAs::Duration,
            JsonFact::Bool(_) | JsonFact::Null | JsonFact::Structure(_) | JsonFact::Hinted { .. },
        ) => None,
    };
    let Some(read) = read else {
        return does_not_read;
    };
    let misfit = match (reads_as, held) {
        (ReadsAs::Time, None | Some(FactType::Time))
        | (ReadsAs::Duration, None | Some(FactType::Num)) => return Disposition::Store(read),
        (ReadsAs::Time, Some(FactType::Text)) => Misfit::DatetimeForText,
        (ReadsAs::Time, Some(FactType::Num)) => Misfit::DatetimeForNumber,
        (ReadsAs::Duration, Some(FactType::Text)) => Misfit::DurationForText,
        (ReadsAs::Duration, Some(FactType::Time)) => Misfit::DurationForTime,
        (ReadsAs::Time | ReadsAs::Duration, Some(FactType::Path)) => return no_stored_path(),
    };
    Disposition::Skip(SkipCause::HintMisfit(misfit))
}

/// What a `datetime` or `duration` hint reads its value as.
#[derive(Clone, Copy)]
enum ReadsAs {
    Time,
    /// A number of seconds.
    Duration,
}

/// A duration written `H:MM:SS`, `M:SS` or seconds, as seconds. Each part is read through the
/// one reading of a number, so `inf`, `NaN` and a leading `+` are refused as they are everywhere,
/// and a total too large to hold has no value.
fn try_parse_duration(text: &str) -> Option<f64> {
    let text = text.trim();
    let number = |part: &str| read_number(part).ok();
    let parts: Vec<&str> = text.split(':').collect();
    let seconds = match parts.as_slice() {
        [h, m, s] => number(h)? * 3600.0 + number(m)? * 60.0 + number(s)?,
        [m, s] => number(m)? * 60.0 + number(s)?,
        [s] => number(s)?,
        _ => return None,
    };
    seconds.is_finite().then_some(seconds)
}

/// The keys two or more of a record's values read to (`Model` and `content.Model`), given the
/// key each name reads to and its value. Neither value of such a key is stored: nothing says
/// which one the tool meant, and storing both would keep whichever the names' order puts last.
/// A `null` is no value, so it names no key: the other value is decided alone.
pub fn keys_named_twice<'a>(
    values: impl IntoIterator<Item = (&'a str, &'a JsonFact)>,
) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut twice = BTreeSet::new();
    for (key, fact) in values {
        if fact.is_no_value() {
            continue;
        }
        if !seen.insert(key) {
            twice.insert(key.to_string());
        }
    }
    twice
}

/// Why a name an import reads as no content fact is skipped: a source fact Canon records
/// itself, passed back (`worklist --emit` writes these), or another name starting with
/// `source.`. Decided by the name's reading, never its text.
pub fn reserved_name_cause(name: &str) -> SkipCause {
    match read_key_name(name, NameReading::AsWritten).source_fact {
        Some(_) => SkipCause::SourceFact,
        None => SkipCause::SourceName,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **The key-name law's verifier**: every command reads a key's name
    /// through one reader (`expr::read_key_name`), and each applies its own rule
    /// to the readings. For a name that is not a built-in key, import's reading,
    /// the filter's, `facts delete`'s and `facts convert`'s reach the same content
    /// fact, so a filter finds, a delete removes and a conversion converts a fact by
    /// the name it was imported under. The
    /// defect it names: the asking half kept `policy.`/`object.` keys as written
    /// while import stored them under `content.`, and `--where policy.reviewed?`
    /// selected nothing. Sampled prefixes. The key as Canon prints it, copied back,
    /// reaches it too in the delete and the conversion, which read the printed quoting
    /// off a name first; import does not, so a tool's quoted name keeps its quotes.
    ///
    /// For a built-in key's name they differ, by design, and that half is
    /// pinned too: the filter reads the built-in key (`size` is the source's
    /// size), import, the delete and the conversion the content reading
    /// (`content.size`).
    #[test]
    fn a_key_is_read_by_the_name_import_stored_it_under() {
        use crate::expr::{printed_key, ParsedFactKey};

        let prefixes = [
            "",
            "content.",
            "policy.",
            "object.",
            "tag.",
            "a.b.",
            "Content.",
            "contents.",
        ];
        for prefix in prefixes {
            for name in ["Make", "reviewed", "x.y", "File Size", "it's"] {
                let written = format!("{prefix}{name}");
                let imported = import_key(&written).unwrap();
                let filtered = ParsedFactKey::parse(&written).unwrap().base_key;
                assert_eq!(filtered, imported, "{written}");
                assert_eq!(delete_key(&written).unwrap(), imported, "{written}");
                assert_eq!(
                    super::convert::convert_key(&written).unwrap(),
                    imported,
                    "{written}"
                );
                // The key as Canon prints it, copied back: the delete and the conversion read
                // the printed quoting off it first.
                let printed = printed_key(&imported);
                assert_eq!(delete_key(&printed).unwrap(), imported, "{printed}");
                let (name, key) = super::convert::read_convert_name(&printed);
                assert_eq!(key, Ok(imported.clone()), "{printed}");
                assert_eq!(name, imported, "{printed}");
            }
        }
        // A typed name whose read form starts with a quote — a stored key `content."Foo"`, typed
        // with its quotes doubled — is read off once and never again: the delete and the
        // conversion's re-read reach the key import stored, and no built-in key is read into it.
        let doubled = r#""""Foo""""#;
        assert_eq!(delete_key(doubled).unwrap(), r#"content."Foo""#);
        let (name, key) = super::convert::read_convert_name(doubled);
        assert_eq!(
            (name.as_str(), key),
            (r#""Foo""#, Ok(r#"content."Foo""#.to_string()))
        );
        assert_eq!(super::convert::fixed_type_answer(r#""hash""#), None);
        // A quoted name at import is a name with quotes: a tool's key is stored as written.
        assert_eq!(import_key(r#""Foo""#).as_deref(), Some(r#"content."Foo""#));

        // One built-in name, a deliberate difference: each command's own rule.
        assert_eq!(ParsedFactKey::parse("size").unwrap().base_key, "size");
        assert_eq!(import_key("size").as_deref(), Some("content.size"));
        assert_eq!(delete_key("size").unwrap(), "content.size");
        assert_eq!(super::convert::convert_key("size").unwrap(), "content.size");
    }

    /// `facts delete` acts on the name's content reading, with no protected
    /// namespace beyond `source.`; a name starting with `source.` is refused
    /// with one sentence, whether or not it is a source fact.
    #[test]
    fn a_delete_reads_the_name_under_content_and_refuses_a_source_name() {
        for (name, key) in [
            ("Make", "content.Make"),
            ("content.Make", "content.Make"),
            ("policy.x", "content.policy.x"),
            ("Make|lowercase", "content.Make|lowercase"),
        ] {
            assert_eq!(delete_key(name).unwrap(), key, "{name}");
        }
        for name in ["source.size", "source.extension"] {
            assert_eq!(
                delete_key(name).unwrap_err().to_string(),
                format!("Cannot delete '{name}': names starting with 'source.' cannot be deleted.")
            );
        }
    }

    /// **The content hash is refused by the key a name reads as, never its spelling**: the full
    /// key, the short name, either in Canon's quotes; `hash` reads as `content.hash` and is not
    /// the content hash, nor is a longer key that starts with it.
    #[test]
    fn the_content_hash_is_refused_under_every_spelling_that_reads_as_it() {
        for given in [
            "content.hash.sha256",
            "hash.sha256",
            r#""content.hash.sha256""#,
            r#""hash.sha256""#,
            "'content.hash.sha256'",
        ] {
            let refused = delete_key(given).expect_err(given);
            assert_eq!(
                refused.downcast_ref::<DeleteRefused>(),
                Some(&DeleteRefused::ContentHash),
                "{given}"
            );
        }
        assert_eq!(delete_key("hash").unwrap(), "content.hash");
        assert_eq!(
            delete_key("content.hash.sha256x").unwrap(),
            "content.hash.sha256x"
        );
        assert!(DeleteRefused::ContentHash.is_answer());
    }

    /// **The two commands give the content hash one reason and one way out**: the reason up to
    /// each command's own consequence, the way out byte for byte. Convert's lines are master's,
    /// written out here rather than captured.
    #[test]
    fn delete_and_convert_give_the_content_hash_one_reason() {
        let reason = "The content hash (content.hash.sha256) is Canon's own record of content \
                      identity, ";
        let way_out = "to correct a hash a tool imported wrongly, import the right one with \
                       `canon import-facts`, which moves the source to its content";
        let convert = convert::ConvertRefused::Key(convert::KeyRefusal::ContentHash).to_string();
        assert_eq!(
            convert,
            format!("{reason}and its type is fixed\n{way_out}"),
            "convert's bytes are unchanged"
        );
        assert_eq!(
            DeleteRefused::ContentHash.to_string(),
            format!("{reason}and its facts are not deleted\n{way_out}")
        );
    }

    /// **`--value-type duration` is answered, not read as a type**: one sentence, the same
    /// whatever the key holds, since every value Canon knows as a duration is stored as a
    /// number. The type words give their stored type.
    #[test]
    fn value_type_duration_is_answered() {
        let refused = delete_value_type(Some(TypeWord::Duration)).unwrap_err();
        assert_eq!(refused, DeleteRefused::Duration);
        assert!(refused.is_answer());
        assert_eq!(
            refused.to_string(),
            "Canon has no duration type: durations are stored as numbers"
        );
        for t in [FactType::Text, FactType::Num, FactType::Time] {
            assert_eq!(delete_value_type(Some(TypeWord::Type(t))), Ok(Some(t)));
        }
        assert_eq!(delete_value_type(None), Ok(None));
    }

    // =========================================================================
    // is_content_fact tests
    // =========================================================================

    #[test]
    fn is_content_fact_returns_true_for_content_keys() {
        assert!(is_content_fact("content.Make"));
        assert!(is_content_fact("content.hash.sha256"));
        assert!(is_content_fact("content.DateTimeOriginal"));
    }

    #[test]
    fn is_content_fact_returns_false_for_other_keys() {
        assert!(!is_content_fact("source.size"));
        assert!(!is_content_fact("Make")); // not normalized
        assert!(!is_content_fact("policy.reviewed"));
    }

    // =========================================================================
    // ContentFactOnSource tests
    // =========================================================================

    #[test]
    fn content_fact_on_source_construction() {
        let fact = ContentFactOnSource {
            id: 1,
            key: "content.Make".to_string(),
            value: FactValue::Text("Canon".to_string()),
            observed_at: 1704067200,
        };

        assert_eq!(fact.id, 1);
        assert_eq!(fact.key, "content.Make");
        assert!(matches!(fact.value, FactValue::Text(ref t) if t == "Canon"));
        assert_eq!(fact.observed_at, 1704067200);
    }

    #[test]
    fn content_fact_on_source_with_num_value() {
        let fact = ContentFactOnSource {
            id: 2,
            key: "content.Duration".to_string(),
            value: FactValue::Num(120.5),
            observed_at: 1704067200,
        };

        assert!(matches!(fact.value, FactValue::Num(n) if n == 120.5));
    }

    #[test]
    fn content_fact_on_source_with_time_value() {
        let time = crate::core::domain::time::reading::read_time("2024-01-01T00:00:00").unwrap();
        let fact = ContentFactOnSource {
            id: 3,
            key: "content.DateTimeOriginal".to_string(),
            value: FactValue::Time(time),
            observed_at: 1704067200,
        };

        assert!(matches!(fact.value, FactValue::Time(t) if t == time));
    }

    // =========================================================================
    // What arrived, and the decision
    // =========================================================================

    use crate::core::domain::time::value::{Precision, TimeForm, TimeValue};

    /// A decision in a form a test can compare: the stored value (a time by its ISO text), or
    /// the cause it is skipped for.
    #[derive(Debug, PartialEq)]
    enum Out {
        Text(String),
        Num(f64),
        Time(String),
        Skip(SkipCause),
    }

    use Out::Skip;

    fn text(s: &str) -> Out {
        Out::Text(s.to_string())
    }

    fn time(s: &str) -> Out {
        Out::Time(s.to_string())
    }

    /// The key's state when a value arrives.
    #[derive(Debug, Clone, Copy)]
    enum State {
        New,
        Holds(FactType),
        Mixed,
    }

    const NEW: State = State::New;
    const TEXT: State = State::Holds(FactType::Text);
    const NUM: State = State::Holds(FactType::Num);
    const TIME: State = State::Holds(FactType::Time);
    const MIXED: State = State::Mixed;

    fn key_types(state: State) -> Option<KeyTypes> {
        match state {
            State::New => None,
            State::Holds(t) => KeyTypes::from_counts([(t, 3)]),
            State::Mixed => KeyTypes::from_counts([(FactType::Text, 2), (FactType::Num, 1)]),
        }
    }

    fn arrived(raw: &str) -> JsonFact {
        let raw = RawValue::from_string(raw.to_string()).unwrap_or_else(|e| panic!("{raw}: {e}"));
        JsonFact::from_raw(&raw).unwrap_or_else(|e| panic!("{raw}: {e}"))
    }

    fn decided(state: State, raw: &str) -> Out {
        match decide(key_types(state).as_ref(), &arrived(raw)) {
            Disposition::Store(FactValue::Text(t)) => Out::Text(t),
            Disposition::Store(FactValue::Num(n)) => Out::Num(n),
            Disposition::Store(FactValue::Time(t)) => Out::Time(t.iso_text()),
            Disposition::Store(FactValue::Path(p)) => panic!("{raw}: stored a path {p}"),
            Disposition::Skip(cause) => Out::Skip(cause),
        }
    }

    fn check(rows: &[(State, &str, Out)]) {
        for (state, raw, expected) in rows {
            assert_eq!(&decided(*state, raw), expected, "{state:?} {raw}");
        }
    }

    /// The premise the import's record type rests on: serde_json takes a map of raw values
    /// under `deny_unknown_fields`, reading each value's text as the tool wrote it.
    #[test]
    fn a_raw_value_map_deserializes_under_deny_unknown_fields() {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Record {
            #[allow(dead_code)]
            source_id: i64,
            facts: HashMap<String, Box<RawValue>>,
        }
        let record: Record = serde_json::from_str(
            r#"{"source_id": 1, "facts": {"a": 1.10, "b": 9007199254740993, "c": {"value": -0, "type": "number"}}}"#,
        )
        .unwrap();
        assert_eq!(record.facts["a"].get(), "1.10");
        assert_eq!(record.facts["b"].get(), "9007199254740993");
        assert_eq!(
            record.facts["c"].get(),
            r#"{"value": -0, "type": "number"}"#
        );
        assert!(
            serde_json::from_str::<Record>(r#"{"source_id": 1, "facts": {}, "x": 2}"#).is_err()
        );
    }

    /// Each arriving shape by its first character, spaces around it allowed.
    #[test]
    fn from_raw_reads_each_shape_by_its_first_character() {
        assert_eq!(
            arrived(r#" "a \"b\" é" "#),
            JsonFact::Text("a \"b\" é".into())
        );
        assert_eq!(arrived(" 1.10 "), JsonFact::Number("1.10".into()));
        assert_eq!(arrived("-0"), JsonFact::Number("-0".into()));
        assert_eq!(arrived("true"), JsonFact::Bool(true));
        assert_eq!(arrived("false"), JsonFact::Bool(false));
        assert_eq!(arrived("null"), JsonFact::Null);
        assert_eq!(
            arrived("[1, \"x\"]"),
            JsonFact::Structure("[1,\"x\"]".into())
        );
        assert_eq!(
            arrived(r#"{"a": {"value": 1}}"#),
            JsonFact::Structure(r#"{"a":{"value":1}}"#.into())
        );
        // An object with a `value` but a `type` that is no string is no hint.
        assert_eq!(
            arrived(r#"{"value": 1, "type": 2}"#),
            JsonFact::Structure(r#"{"type":2,"value":1}"#.into())
        );
        // A hint with other keys: they are ignored.
        assert_eq!(
            arrived(r#"{"type": "datetime", "value": "2019", "source": "exif"}"#),
            JsonFact::Hinted {
                hint: Hint::Known(KnownHint::Datetime),
                value: Box::new(JsonFact::Text("2019".into())),
                written: "\"2019\"".into(),
            }
        );
        assert_eq!(
            arrived(r#"{"value": "x", "type": "bogus"}"#),
            JsonFact::Hinted {
                hint: Hint::Unknown("bogus".into()),
                value: Box::new(JsonFact::Text("x".into())),
                written: "\"x\"".into(),
            }
        );
    }

    /// **The key decides**: one row per key state and arriving shape. Each expected answer is
    /// the functional rule's: an untyped string is read as the key's type, and as text for a
    /// new key; a JSON number is a number, and its own spelling for a text key; `true`/`false`
    /// are 1/0 for a number or new key, the words for a text key, skipped for a time key;
    /// `null` is no value; a list or object is its JSON text for a text or new key, skipped
    /// otherwise; a hint reads its value and must fit the key; a key of two types takes nothing.
    #[test]
    fn decide_follows_the_key_for_every_arriving_shape() {
        use KnownHint as H;
        use Misfit as M;
        use SkipCause as C;
        let dt = r#"{"value": "2019", "type": "datetime"}"#;
        let dur = r#"{"value": "3:30", "type": "duration"}"#;
        let num = r#"{"value": "12", "type": "number"}"#;
        let bogus = r#"{"value": "x", "type": "bogus"}"#;
        check(&[
            (NEW, r#""Canon""#, text("Canon")),
            (NEW, r#""12""#, text("12")),
            (NEW, r#""2024:07:23 11:06:32""#, text("2024:07:23 11:06:32")),
            (NEW, "12", Out::Num(12.0)),
            (NEW, "1.10", Out::Num(1.1)),
            (NEW, "true", Out::Num(1.0)),
            (NEW, "false", Out::Num(0.0)),
            (NEW, "null", Skip(C::NoValue)),
            (NEW, "[1, 2]", text("[1,2]")),
            (NEW, r#"{"a": 1}"#, text(r#"{"a":1}"#)),
            (NEW, dt, time("2019")),
            (NEW, dur, Out::Num(210.0)),
            (NEW, num, Out::Num(12.0)),
            (NEW, bogus, Skip(C::UnknownHint("bogus".into()))),
            (TEXT, r#""Canon""#, text("Canon")),
            (TEXT, r#""12""#, text("12")),
            (
                TEXT,
                r#""2024:07:23 11:06:32""#,
                text("2024:07:23 11:06:32"),
            ),
            (TEXT, "12", text("12")),
            (TEXT, "1.10", text("1.10")),
            (TEXT, "true", text("true")),
            (TEXT, "false", text("false")),
            (TEXT, "null", Skip(C::NoValue)),
            (TEXT, "[1, 2]", text("[1,2]")),
            (TEXT, r#"{"a": 1}"#, text(r#"{"a":1}"#)),
            (TEXT, dt, Skip(C::HintMisfit(M::DatetimeForText))),
            (TEXT, dur, Skip(C::HintMisfit(M::DurationForText))),
            (TEXT, num, text("12")),
            (TEXT, bogus, Skip(C::UnknownHint("bogus".into()))),
            (NUM, r#""Canon""#, Skip(C::DoesNotRead(Reading::Number))),
            (NUM, r#""12""#, Out::Num(12.0)),
            (NUM, r#""01234""#, Skip(C::WouldLose(Loss::LeadingZero))),
            (NUM, r#""2.50""#, Skip(C::WouldLose(Loss::TrailingZero))),
            (
                NUM,
                r#""9007199254740993""#,
                Skip(C::WouldLose(Loss::NotExact)),
            ),
            (
                NUM,
                r#""2024:07:23 11:06:32""#,
                Skip(C::DoesNotRead(Reading::Number)),
            ),
            (NUM, "12", Out::Num(12.0)),
            (NUM, "1.10", Out::Num(1.1)),
            (NUM, "true", Out::Num(1.0)),
            (NUM, "null", Skip(C::NoValue)),
            (NUM, "[1, 2]", Skip(C::Structure(Reading::Number))),
            (NUM, r#"{"a": 1}"#, Skip(C::Structure(Reading::Number))),
            (NUM, dt, Skip(C::HintMisfit(M::DatetimeForNumber))),
            (NUM, dur, Out::Num(210.0)),
            (NUM, num, Out::Num(12.0)),
            (NUM, bogus, Skip(C::UnknownHint("bogus".into()))),
            (TIME, r#""Canon""#, Skip(C::DoesNotRead(Reading::Time))),
            (TIME, r#""2019""#, time("2019")),
            (
                TIME,
                r#""2024:07:23 11:06:32""#,
                time("2024-07-23T11:06:32"),
            ),
            (
                TIME,
                r#""0000:00:00 00:00:00""#,
                Skip(C::DoesNotRead(Reading::Time)),
            ),
            (TIME, "12", Skip(C::NumberForTime)),
            (TIME, "1.10", Skip(C::NumberForTime)),
            (TIME, "true", Skip(C::BoolForTime)),
            (TIME, "null", Skip(C::NoValue)),
            (TIME, "[1, 2]", Skip(C::Structure(Reading::Time))),
            (TIME, r#"{"a": 1}"#, Skip(C::Structure(Reading::Time))),
            (TIME, dt, time("2019")),
            (TIME, dur, Skip(C::HintMisfit(M::DurationForTime))),
            (TIME, num, Skip(C::NumberForTime)),
            (TIME, bogus, Skip(C::UnknownHint("bogus".into()))),
        ]);
        for raw in [
            r#""Canon""#,
            "12",
            "true",
            "[1]",
            r#"{"a": 1}"#,
            dt,
            dur,
            num,
            bogus,
        ] {
            assert_eq!(decided(MIXED, raw), Skip(C::MixedKey), "{raw}");
        }
        // A hinted value that does not read is the tool's to fix, before any fit.
        assert_eq!(
            decided(TEXT, r#"{"value": "never", "type": "datetime"}"#),
            Skip(C::HintDoesNotRead(H::Datetime))
        );
    }

    /// A JSON number for a text key is stored as text in the spelling the tool wrote.
    #[test]
    fn a_json_number_keeps_the_spelling_the_tool_wrote_for_a_text_key() {
        for raw in [
            "1.10",
            "1e5",
            "-0",
            "9007199254740993",
            "100000000000000000000",
        ] {
            assert_eq!(decided(TEXT, raw), text(raw));
        }
    }

    /// `null` is no value, whatever the key holds — two types included.
    #[test]
    fn null_is_no_value_for_every_key() {
        for state in [NEW, TEXT, NUM, TIME, MIXED] {
            assert_eq!(
                decided(state, "null"),
                Skip(SkipCause::NoValue),
                "{state:?}"
            );
        }
    }

    /// A key holding two types takes no value of any kind until it is converted.
    #[test]
    fn a_mixed_key_takes_no_value_of_any_kind() {
        for raw in [
            r#""x""#,
            r#""12""#,
            "12",
            "1.10",
            "false",
            "[]",
            "{}",
            r#"{"value": "2019", "type": "datetime"}"#,
            r#"{"value": "1:00", "type": "duration"}"#,
            r#"{"value": 5, "type": "number"}"#,
            r#"{"value": "twelve", "type": "number"}"#,
        ] {
            assert_eq!(decided(MIXED, raw), Skip(SkipCause::MixedKey), "{raw}");
        }
    }

    /// A hinted value that does not read as its hint is skipped for that, on every key that is
    /// not of mixed type — before any question of whether the hint fits the key.
    #[test]
    fn a_hinted_value_that_does_not_read_as_its_hint_is_the_tools_to_fix_on_every_key() {
        for (raw, hint) in [
            (
                r#"{"value": "0000:00:00 00:00:00", "type": "datetime"}"#,
                KnownHint::Datetime,
            ),
            (
                r#"{"value": 1.5e9, "type": "datetime"}"#,
                KnownHint::Datetime,
            ),
            (
                r#"{"value": true, "type": "datetime"}"#,
                KnownHint::Datetime,
            ),
            (
                r#"{"value": "soon", "type": "duration"}"#,
                KnownHint::Duration,
            ),
            (
                r#"{"value": null, "type": "duration"}"#,
                KnownHint::Duration,
            ),
            (
                r#"{"value": "twelve", "type": "number"}"#,
                KnownHint::Number,
            ),
        ] {
            for state in [NEW, TEXT, NUM, TIME] {
                assert_eq!(
                    decided(state, raw),
                    Skip(SkipCause::HintDoesNotRead(hint)),
                    "{state:?} {raw}"
                );
            }
        }
    }

    /// For a key that holds times, a number with the datetime hint reads as a year; an
    /// unhinted JSON number is skipped, whatever its value — never read as epoch seconds.
    #[test]
    fn a_hinted_year_reads_as_a_year_and_an_unhinted_number_is_skipped_for_a_time_key() {
        assert_eq!(
            decided(TIME, r#"{"value": 2019, "type": "datetime"}"#),
            time("2019")
        );
        assert_eq!(
            decided(TIME, r#"{"value": 2019.0, "type": "datetime"}"#),
            time("2019")
        );
        for raw in ["2019", "1690000000", "0", "2019.5"] {
            assert_eq!(decided(TIME, raw), Skip(SkipCause::NumberForTime), "{raw}");
        }
    }

    /// A JSON number written as an integer is skipped where Canon cannot hold it exactly as
    /// written; one it holds exactly lands, and a fraction lands as the nearest number held.
    #[test]
    fn a_json_integer_is_skipped_only_when_canon_cannot_hold_it_exactly() {
        for state in [NEW, NUM] {
            assert_eq!(
                decided(state, "9007199254740993"),
                Skip(SkipCause::NotExact),
                "{state:?}"
            );
            for raw in ["1e400", "-1e400"] {
                assert_eq!(decided(state, raw), Skip(SkipCause::NotExact), "{raw}");
            }
            assert_eq!(
                decided(state, "9007199254740994"),
                Out::Num(9007199254740994.0)
            );
            assert_eq!(decided(state, "100000000000000000000"), Out::Num(1e20));
            assert_eq!(decided(state, "0.30000000000000001"), Out::Num(0.3));
            assert_eq!(
                decided(state, "9007199254740993.0"),
                Out::Num(9007199254740992.0)
            );
            assert_eq!(
                decided(state, "9007199254740993e0"),
                Out::Num(9007199254740992.0),
                "an exponent is not an integer's spelling"
            );
        }
    }

    /// **Only a bare `null` is no value.** `decide` and `keys_named_twice` both ask
    /// `is_no_value`, so they agree by construction; what is pinned is the answer: a bare
    /// `null` is no value, and a hinted `null`, a list or object holding `null`, and every other
    /// value are values. `decide` counts exactly the first as a null.
    #[test]
    fn only_a_bare_null_is_no_value() {
        for (raw, no_value) in [
            ("null", true),
            (r#"{"value": null, "type": "datetime"}"#, false),
            (r#"{"value": null, "type": "number"}"#, false),
            (r#"{"value": null, "type": "bogus"}"#, false),
            (r#""x""#, false),
            ("12", false),
            ("true", false),
            ("[null]", false),
            (r#"{"a": null}"#, false),
        ] {
            let raw_value = RawValue::from_string(raw.to_string()).unwrap();
            let fact = JsonFact::from_raw(&raw_value).unwrap();
            assert_eq!(fact.is_no_value(), no_value, "{raw}");
            assert_eq!(
                matches!(decide(None, &fact), Disposition::Skip(SkipCause::NoValue)),
                no_value,
                "{raw}"
            );
        }
    }

    /// A key is named twice when two of a record's values read to it, however many other keys
    /// the record names; a key named once is not, and a `null` names no key.
    #[test]
    fn a_key_two_values_read_to_is_named_twice() {
        let x = JsonFact::Text("x".into());
        let null = JsonFact::Null;
        let hinted_null = JsonFact::Hinted {
            hint: Hint::Known(KnownHint::Datetime),
            value: Box::new(JsonFact::Null),
            written: "null".into(),
        };
        let twice = keys_named_twice([
            ("content.Model", &x),
            ("content.Lens", &x),
            ("content.Model", &x),
            ("content.Make", &x),
            ("content.Make", &x),
            ("content.Make", &x),
            ("content.Note", &null),
            ("content.Note", &x),
            ("content.Gone", &null),
            ("content.Gone", &null),
            ("content.Taken", &hinted_null),
            ("content.Taken", &x),
        ]);
        assert_eq!(
            twice.into_iter().collect::<Vec<_>>(),
            ["content.Make", "content.Model", "content.Taken"]
        );
        assert!(keys_named_twice([("content.A", &x), ("content.B", &x)]).is_empty());
    }

    /// A hint's value keeps the spelling the tool wrote, so no number in import passes
    /// through a float.
    #[test]
    fn a_hint_value_keeps_its_raw_spelling() {
        assert_eq!(
            decided(TEXT, r#"{"value": 1.10, "type": "number"}"#),
            text("1.10")
        );
        assert_eq!(
            decided(NUM, r#"{"value": 123456789012345678901, "type": "number"}"#),
            Skip(SkipCause::NotExact)
        );
        assert_eq!(
            decided(NEW, r#"{"value": 123456789012345678901, "type": "number"}"#),
            Skip(SkipCause::NotExact)
        );
    }

    /// A `number`-hinted value is decided as a JSON number with that spelling: the tool said it
    /// is a number, so its spelling is not judged for loss — only whether Canon holds it
    /// exactly. Whether it is written as a number is the grammar's answer; text that is not is
    /// the tool's to fix.
    #[test]
    fn a_number_hint_reads_its_value_as_a_json_number_would() {
        let hinted = |v: &str| format!(r#"{{"value": {v}, "type": "number"}}"#);
        assert_eq!(decided(NUM, &hinted(r#""12""#)), Out::Num(12.0));
        assert_eq!(decided(NEW, &hinted(r#""12""#)), Out::Num(12.0));
        assert_eq!(decided(TEXT, &hinted(r#""12""#)), text("12"));
        assert_eq!(
            decided(TIME, &hinted(r#""12""#)),
            Skip(SkipCause::NumberForTime)
        );
        assert_eq!(decided(NUM, &hinted(r#""01234""#)), Out::Num(1234.0));
        assert_eq!(decided(NEW, &hinted(r#""01234""#)), Out::Num(1234.0));
        for state in [NEW, TEXT, NUM, TIME] {
            for written in ["twelve", "inf", "+5", "NaN", ""] {
                assert_eq!(
                    decided(state, &hinted(&format!("\"{written}\""))),
                    Skip(SkipCause::HintDoesNotRead(KnownHint::Number)),
                    "{state:?} {written}"
                );
            }
        }
        for state in [NEW, NUM] {
            assert_eq!(
                decided(state, &hinted(r#""9007199254740993""#)),
                Skip(SkipCause::NotExact)
            );
            // Written as a number, too large to hold: the fit, as for a JSON number.
            assert_eq!(
                decided(state, &hinted(r#""1e400""#)),
                Skip(SkipCause::NotExact)
            );
        }
    }

    /// A name an import reads as no content fact: a source fact passed back, or another name
    /// starting with `source.`.
    #[test]
    fn a_reserved_name_is_a_source_fact_or_a_source_name() {
        for name in ["source.mtime", "source.size", "source.rel_path"] {
            assert_eq!(reserved_name_cause(name), SkipCause::SourceFact, "{name}");
        }
        for name in ["source.foo", "source.", "source.Mtime"] {
            assert_eq!(reserved_name_cause(name), SkipCause::SourceName, "{name}");
        }
    }

    // =========================================================================
    // Times, through the datetime hint
    // =========================================================================

    fn hinted_time(value: &str) -> TimeValue {
        let raw = format!(r#"{{"value": {value}, "type": "datetime"}}"#);
        match decide(None, &arrived(&raw)) {
            Disposition::Store(FactValue::Time(tv)) => tv,
            other => panic!("{value}: {other:?}"),
        }
    }

    fn hinted_time_skip(value: &str) -> SkipCause {
        let raw = format!(r#"{{"value": {value}, "type": "datetime"}}"#);
        match decide(None, &arrived(&raw)) {
            Disposition::Skip(cause) => cause,
            other => panic!("{value}: {other:?}"),
        }
    }

    #[test]
    fn an_rfc3339_time_with_an_offset_is_an_instant_that_keeps_it() {
        let tv = hinted_time(r#""2020-01-15T10:30:00+00:00""#);
        assert_eq!(tv.form(), TimeForm::InstantWithOffset);
        assert_eq!(tv.recorded_offset(), Some(jiff::tz::Offset::UTC));
        assert_eq!(tv.precision(), Precision::Second);
        assert_eq!(tv.legacy_epoch_number(), 1579084200);
    }

    #[test]
    fn an_iso_time_without_an_offset_is_a_clock_reading() {
        let tv = hinted_time(r#""2020-01-15T10:30:00""#);
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.precision(), Precision::Second);
        assert_eq!(tv.legacy_epoch_number(), 1579084200);
    }

    #[test]
    fn an_exif_time_without_an_offset_is_a_clock_reading() {
        let tv = hinted_time(r#""2020:07:23 11:06:32""#);
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.legacy_epoch_number(), 1595502392);
    }

    /// Text after a time is never cut off and dropped — a multi-byte character included, at
    /// any position — so an import never stops on it and never keeps part of it.
    #[test]
    fn an_exif_shaped_value_with_trailing_text_is_refused() {
        for value in [r#""2020:07:23 11:06:3é""#, r#""2020:07:23 11:06:32é""#] {
            assert_eq!(
                hinted_time_skip(value),
                SkipCause::HintDoesNotRead(KnownHint::Datetime),
                "{value}"
            );
        }
    }

    #[test]
    fn a_plain_year_string_is_a_clock_reading_at_year_precision() {
        let tv = hinted_time(r#""2005""#);
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.precision(), Precision::Year);
        assert_eq!(tv.legacy_epoch_number(), 1104537600);
    }

    #[test]
    fn text_that_is_not_a_time_is_refused() {
        for value in [r#""not a date""#, r#""""#, r#""99999""#] {
            assert_eq!(
                hinted_time_skip(value),
                SkipCause::HintDoesNotRead(KnownHint::Datetime),
                "{value}"
            );
        }
    }

    #[test]
    fn the_datetime_hint_on_a_string_reads_a_time() {
        let tv = hinted_time(r#""2020-01-15T10:30:00+00:00""#);
        assert_eq!(FactValue::Time(tv).kind(), FactType::Time);
    }

    // =========================================================================
    // Durations
    // =========================================================================

    #[test]
    fn test_try_parse_duration_hms() {
        let secs = try_parse_duration("1:30:45").unwrap();
        assert!((secs - 5445.0).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_ms() {
        let secs = try_parse_duration("3:30").unwrap();
        assert!((secs - 210.0).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_seconds() {
        let secs = try_parse_duration("45.5").unwrap();
        assert!((secs - 45.5).abs() < 0.01);
    }

    /// A JSON number with the duration hint is a number of seconds, read from its spelling.
    #[test]
    fn test_try_parse_duration_numeric_json() {
        assert_eq!(
            decided(NEW, r#"{"value": 120, "type": "duration"}"#),
            Out::Num(120.0)
        );
    }

    /// A duration's parts are numbers as Canon reads them everywhere. Where that grammar and
    /// Rust's own parser differ, Canon's holds: `inf`, `NaN`, a leading `+` and a point with no
    /// digit on one side (`.5`, `5.`) are refused, and so is a total too large to hold. The
    /// defect it names: the parts went through Rust's parser, so `inf` was stored, printed, and
    /// then refused by `--where` as not a number, and `NaN` reached the store as a constraint
    /// error.
    #[test]
    fn a_duration_reads_its_parts_as_canon_reads_a_number() {
        for written in [
            "inf",
            "-inf",
            "NaN",
            "+5",
            ".5",
            "5.",
            "1:30.",
            "1:inf",
            "inf:00:00",
            "1e308:00:00",
        ] {
            assert_eq!(try_parse_duration(written), None, "{written}");
        }
        assert_eq!(try_parse_duration("1e3"), Some(1000.0));
    }

    #[test]
    fn test_try_parse_duration_invalid() {
        assert!(try_parse_duration("not:a:duration:at:all").is_none());
        assert_eq!(
            decided(NEW, r#"{"value": null, "type": "duration"}"#),
            Skip(SkipCause::HintDoesNotRead(KnownHint::Duration))
        );
    }

    // =========================================================================
    // Plain values and the other hints
    // =========================================================================

    #[test]
    fn a_plain_string_is_text() {
        assert_eq!(decided(NEW, r#""hello""#), text("hello"));
    }

    #[test]
    fn a_plain_number_is_a_number() {
        assert_eq!(decided(NEW, "42"), Out::Num(42.0));
    }

    #[test]
    fn a_plain_boolean_is_a_number() {
        assert_eq!(decided(NEW, "true"), Out::Num(1.0));
    }

    #[test]
    fn the_duration_hint_reads_a_number_of_seconds() {
        assert_eq!(
            decided(NEW, r#"{"value": "3:30", "type": "duration"}"#),
            Out::Num(210.0)
        );
    }

    #[test]
    fn an_unknown_hint_is_refused() {
        assert_eq!(
            decided(NEW, r#"{"value": "hello", "type": "bogus"}"#),
            Skip(SkipCause::UnknownHint("bogus".into()))
        );
    }
}
