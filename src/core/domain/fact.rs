//! Fact domain model for canon.
//!
//! This module defines the `FactEntry` struct — a typed representation of a fact
//! associated with a source. Facts are stored on either sources or objects, but
//! for query purposes we associate them with sources (a source's "effective facts"
//! include both its direct facts AND its object's facts).
//!
//! ## Design Principles
//!
//! 1. **Single source of truth**: This struct is THE definition of a fact for read operations
//! 2. **Pure types**: No I/O or side effects in this module
//! 3. **The value vocabulary lives here**: `FactValue` and `FactType` are defined in this
//!    module; the expression engine imports them from it, never the reverse
//! 4. **Dependencies point inward**: Commands depend on this module, not vice versa
//!
//! ## Usage
//!
//! ```ignore
//! use crate::core::domain::fact::{FactEntry, FactValue, FactType};
//!
//! // Every fact each source carries, on the source and on its content. A source's one
//! // value for a key is picked from these elsewhere; here they are rows.
//! let facts = crate::core::repo::fact::batch_fetch_for_sources(conn, &source_ids)?;
//! for (source_id, entries) in &facts {
//!     for fact in entries {
//!         println!("{}: {} on source: {}", source_id, fact.key, fact.is_on_source());
//!     }
//! }
//! ```

use super::time::value::TimeValue;

/// A value's kind, without the value: text, number, time, or a derived path. The one
/// value-kind noun — import's type check, the `facts` report and the filter all speak it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactType {
    Text,
    Num,
    Time,
    Path, // Path source facts, which support segment indexing
}

impl FactType {
    /// The type's one printed word.
    pub fn as_str(&self) -> &'static str {
        match self {
            FactType::Text => "text",
            FactType::Num => "number",
            FactType::Time => "time",
            FactType::Path => "path",
        }
    }

    /// The type's noun, for one value or for many: `a number`/`numbers`, `a time`/`times`,
    /// `a path`/`paths`, and `text` for both — vocabulary beside [`FactType::as_str`], which a
    /// statement about a value and its type is built from.
    pub fn noun(self, n: Count) -> &'static str {
        match (self, n) {
            (FactType::Text, Count::One | Count::Many) => "text",
            (FactType::Num, Count::One) => "a number",
            (FactType::Num, Count::Many) => "numbers",
            (FactType::Time, Count::One) => "a time",
            (FactType::Time, Count::Many) => "times",
            (FactType::Path, Count::One) => "a path",
            (FactType::Path, Count::Many) => "paths",
        }
    }

    /// A stored type read from a word a user typed: the printed words, and quietly the
    /// older spellings `num` and `datetime`, which are accepted and never printed. Never
    /// `path` — a path is derived, so no stored fact holds one.
    pub fn parse_word(word: &str) -> Option<FactType> {
        match word {
            "text" => Some(FactType::Text),
            "number" | "num" => Some(FactType::Num),
            "time" | "datetime" => Some(FactType::Time),
            _ => None,
        }
    }
}

/// How many things a word speaks of: one, or any other number — which decides a noun's form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Count {
    One,
    Many,
}

impl Count {
    /// The count's form: one for exactly 1, many for any other number, 0 included.
    pub fn of(n: u64) -> Count {
        if n == 1 {
            Count::One
        } else {
            Count::Many
        }
    }
}

/// The types a key's facts hold, with how many facts hold each — read from every fact
/// of the key, wherever it sits (`core::repo::fact::key_types`). One entry is the key's
/// type; two or more is a key of mixed type, which only data older than typed import
/// can hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyTypes {
    /// Non-zero counts in a fixed order (text, number, time, path). A source fact or a
    /// built-in key holds no facts, so its one entry counts nothing.
    counts: Vec<(FactType, u64)>,
}

impl KeyTypes {
    /// A source fact's or a built-in key's type: one type, no facts counted.
    pub fn of(t: FactType) -> Self {
        KeyTypes {
            counts: vec![(t, 0)],
        }
    }

    /// The types from per-type fact counts; `None` where no count is above zero, because
    /// a key with no facts has no type.
    pub fn from_counts(counts: impl IntoIterator<Item = (FactType, u64)>) -> Option<Self> {
        let order = |t: FactType| match t {
            FactType::Text => 0,
            FactType::Num => 1,
            FactType::Time => 2,
            FactType::Path => 3,
        };
        let mut summed: Vec<(FactType, u64)> = Vec::new();
        for (t, n) in counts {
            match summed.iter_mut().find(|(seen, _)| *seen == t) {
                Some((_, total)) => *total += n,
                None => summed.push((t, n)),
            }
        }
        summed.retain(|&(_, n)| n > 0);
        summed.sort_by_key(|&(t, _)| order(t));
        (!summed.is_empty()).then_some(KeyTypes { counts: summed })
    }

    /// The key's one type, or `None` for a key of mixed type.
    pub fn single(&self) -> Option<FactType> {
        match self.counts.as_slice() {
            [(t, _)] => Some(*t),
            _ => None,
        }
    }

    /// Whether the key's facts hold more than one type.
    pub fn is_mixed(&self) -> bool {
        self.single().is_none()
    }

    /// The word the key's type prints as: its type's word, or `mixed`.
    pub fn word(&self) -> &'static str {
        self.single().map_or("mixed", |t| t.as_str())
    }

    /// Each type the key's facts hold and how many hold it.
    pub fn counts(&self) -> &[(FactType, u64)] {
        &self.counts
    }
}

impl std::fmt::Display for FactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A fact's value: text, a number, a time, or a derived path.
///
/// A time is the time noun ([`TimeValue`]): its form and precision as written, read on the
/// user's clock through the frame rule. A path is derived, never stored.
#[derive(Debug, Clone)]
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(TimeValue),
    Path(String), // Path that supports segment indexing (for the path source facts)
}

impl FactValue {
    /// The value's kind.
    pub fn kind(&self) -> FactType {
        match self {
            FactValue::Text(_) => FactType::Text,
            FactValue::Num(_) => FactType::Num,
            FactValue::Time(_) => FactType::Time,
            FactValue::Path(_) => FactType::Path,
        }
    }
}

/// **A stored value as Canon reads it**: readable, or a row whose value Canon cannot read (a
/// stored time outside the years every zone reads). An unreadable row is still a row: on the
/// content it decides a source's value, which is then none (the expression facility's
/// `source_value` takes it in its signature), so the source's own fact never stands in for it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Stored<T> {
    Readable(T),
    Unreadable,
}

impl<T> Stored<T> {
    /// The value, where it is readable.
    pub fn readable(self) -> Option<T> {
        match self {
            Stored::Readable(value) => Some(value),
            Stored::Unreadable => None,
        }
    }

    pub fn as_ref(&self) -> Stored<&T> {
        match self {
            Stored::Readable(value) => Stored::Readable(value),
            Stored::Unreadable => Stored::Unreadable,
        }
    }
}

/// **A key's census**: every fact of the key — the row set its type is read from — and every
/// source those facts sit on, whatever the source's presence, root or role.
#[derive(Debug, Clone, Default)]
pub struct KeyCensus {
    pub facts: Vec<CensusFact>,
    pub sources: Vec<CensusSource>,
}

/// One fact of a key, as the census reads it.
#[derive(Debug, Clone)]
pub struct CensusFact {
    pub id: i64,
    pub on: FactOn,
    pub value: Stored<FactValue>,
    /// The content's hash, for a fact stored on the content: what names content no source
    /// holds.
    pub object_hash: Option<String>,
}

/// Where a fact is stored: on a source (its id), or on the content (its object's id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FactOn {
    Source(i64),
    Content(i64),
}

/// A source a key's facts sit on, and whether its file was present at the last scan.
#[derive(Debug, Clone)]
pub struct CensusSource {
    pub source: super::source::Source,
    pub present: bool,
}

/// A source's facts as one batch fetch reads them: every readable row, on the source and on its
/// content, and the keys whose row on the content Canon cannot read — which decide that the
/// source has no value for them.
#[derive(Debug, Clone, Default)]
pub struct SourceFacts {
    pub entries: Vec<FactEntry>,
    pub unreadable_on_content: Vec<String>,
}

/// A single fact entry associated with a source.
///
/// Facts are stored in the database on either sources or objects, but for query
/// purposes we associate them with sources. When fetching facts for a source,
/// the result includes both:
/// - Facts stored on the source (`entity_type = "source"`)
/// - Facts stored on its object, via the source's `object_id` (`entity_type = "object"`)
///
/// The `entity_type` and `entity_id` fields preserve the original storage location,
/// which is useful for debugging and for operations that need to distinguish
/// between source-level and object-level facts.
#[derive(Debug, Clone)]
pub struct FactEntry {
    /// The fact key (e.g., "content.Make", "content.hash.sha256")
    pub key: String,
    /// The typed fact value
    pub value: FactValue,
    /// Where this fact is stored: "source" or "object"; read through `is_on_source`.
    pub entity_type: String,
    // Nothing reads the id today; it is kept beside the side it names, so an entry says
    // exactly which row it came from.
    #[allow(dead_code)]
    /// The entity ID (source_id or object_id depending on entity_type)
    pub entity_id: i64,
}

impl FactEntry {
    /// Whether the fact is stored on the source itself rather than on its content.
    pub fn is_on_source(&self) -> bool {
        Self::sits_on_source(&self.entity_type)
    }

    /// Whether a row of this entity type sits on the source rather than on its content — for a
    /// reader holding a row whose value it did not read.
    pub fn sits_on_source(entity_type: &str) -> bool {
        entity_type == "source"
    }

    /// Create a new FactEntry.
    ///
    /// This is a simple constructor for convenience in tests and repository code.
    pub fn new(key: String, value: FactValue, entity_type: String, entity_id: i64) -> Self {
        Self {
            key,
            value,
            entity_type,
            entity_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // FactEntry construction tests
    // =========================================================================

    #[test]
    fn fact_entry_new_creates_entry() {
        let entry = FactEntry::new(
            "content.Make".to_string(),
            FactValue::Text("Canon".to_string()),
            "object".to_string(),
            42,
        );

        assert_eq!(entry.key, "content.Make");
        assert_eq!(entry.entity_type, "object");
        assert_eq!(entry.entity_id, 42);
        match entry.value {
            FactValue::Text(s) => assert_eq!(s, "Canon"),
            _ => panic!("Expected Text variant"),
        }
    }

    #[test]
    fn fact_entry_with_num_value() {
        let entry = FactEntry::new(
            "source.size".to_string(),
            FactValue::Num(1024.0),
            "source".to_string(),
            1,
        );

        match entry.value {
            FactValue::Num(n) => assert_eq!(n, 1024.0),
            _ => panic!("Expected Num variant"),
        }
    }

    #[test]
    fn fact_entry_with_time_value() {
        let time = TimeValue::from_epoch_secs(1704067200).unwrap(); // 2024-01-01 00:00:00 UTC
        let entry = FactEntry::new(
            "content.DateTimeOriginal".to_string(),
            FactValue::Time(time),
            "object".to_string(),
            100,
        );

        match entry.value {
            FactValue::Time(tv) => assert_eq!(tv, time),
            _ => panic!("Expected Time variant"),
        }
    }

    #[test]
    fn fact_entry_with_path_value() {
        let entry = FactEntry::new(
            "source.rel_path".to_string(),
            FactValue::Path("/photos/2024/image.jpg".to_string()),
            "source".to_string(),
            5,
        );

        match entry.value {
            FactValue::Path(p) => assert_eq!(p, "/photos/2024/image.jpg"),
            _ => panic!("Expected Path variant"),
        }
    }

    // =========================================================================
    // The type words, a key's types, and the mixed-type statement
    // =========================================================================

    #[test]
    fn parse_word_reads_the_type_words_and_their_quiet_spellings() {
        for t in [FactType::Text, FactType::Num, FactType::Time] {
            assert_eq!(FactType::parse_word(t.as_str()), Some(t), "{t}");
        }
        assert_eq!(FactType::Num.as_str(), "number");
        assert_eq!(FactType::parse_word("num"), Some(FactType::Num));
        assert_eq!(FactType::parse_word("datetime"), Some(FactType::Time));
        for refused in ["path", "Number", "integer", ""] {
            assert_eq!(FactType::parse_word(refused), None, "{refused:?}");
        }
    }

    #[test]
    fn every_type_has_a_noun_for_one_and_for_many() {
        let expected = [
            (FactType::Text, "text", "text"),
            (FactType::Num, "a number", "numbers"),
            (FactType::Time, "a time", "times"),
            (FactType::Path, "a path", "paths"),
        ];
        for (t, one, many) in expected {
            assert_eq!(t.noun(Count::One), one, "{t}");
            assert_eq!(t.noun(Count::Many), many, "{t}");
        }
        assert_eq!(Count::of(1), Count::One);
        assert_eq!(Count::of(0), Count::Many);
        assert_eq!(Count::of(2), Count::Many);
    }

    #[test]
    fn key_types_name_one_type_or_mixed() {
        let one = KeyTypes::from_counts([(FactType::Num, 3), (FactType::Text, 0)]).unwrap();
        assert_eq!(one.single(), Some(FactType::Num));
        assert_eq!(one.word(), "number");
        assert_eq!(one.counts(), &[(FactType::Num, 3)]);

        let two = KeyTypes::from_counts([(FactType::Time, 1), (FactType::Text, 2)]).unwrap();
        assert_eq!(two.single(), None);
        assert!(two.is_mixed() && !one.is_mixed());
        assert_eq!(two.word(), "mixed");
        assert_eq!(two.counts(), &[(FactType::Text, 2), (FactType::Time, 1)]);

        assert_eq!(KeyTypes::from_counts([(FactType::Text, 0)]), None);
        assert_eq!(KeyTypes::of(FactType::Path).word(), "path");
    }

    // =========================================================================
    // FactEntry clone tests
    // =========================================================================

    #[test]
    fn fact_entry_clone_creates_independent_copy() {
        let original = FactEntry::new(
            "content.Make".to_string(),
            FactValue::Text("Canon".to_string()),
            "object".to_string(),
            42,
        );

        let cloned = original.clone();

        assert_eq!(cloned.key, original.key);
        assert_eq!(cloned.entity_type, original.entity_type);
        assert_eq!(cloned.entity_id, original.entity_id);
    }
}
