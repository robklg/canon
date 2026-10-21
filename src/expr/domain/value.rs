//! Fact value resolution — domain layer for getting fact values.
//!
//! This module provides the core operation: "get the value of fact K for source S."
//! It handles the keys a source's own row answers (`source_row_value`), stored facts
//! (from the facts table), and applies any transforms (accessors, modifiers).
//!
//! ## Design Principles
//!
//! 1. **Pure function**: `resolve_fact_value()` has no I/O or side effects
//! 2. **Single source of truth**: This is THE way to get a fact's value for a source
//! 3. **Handles both computed keys and stored facts**: Caller doesn't need to distinguish
//! 4. **Testable in isolation**: Can be unit tested without database
//!
//! ## Usage
//!
//! ```ignore
//! use crate::expr::{read_report, resolve_fact_value};
//!
//! let key = read_report(conn, Some("source.mtime|year"), &[], &filters)?.key.unwrap();
//! let value = resolve_fact_value(&source, &key, &stored_facts, &zone)?;
//! // value is Some("2024") or None if fact doesn't exist
//! ```

use std::collections::HashMap;

use anyhow::Result;

use super::key::{BuiltinKey, Key, ParsedFactKey, SourceFact};
use super::transform::{self, Modified};
use crate::core::domain::fact::FactEntry;
use crate::core::domain::fact::FactValue;
use crate::core::domain::source::Source;
use crate::core::domain::time::value::TimeValue;
use crate::core::domain::time::zone::UserZone;

/// Everything a source's own row answers a key from — the complete set, so a
/// key either resolves from these or is a stored fact.
///
/// Named as its own input because the resolver serves both halves of the
/// language and the two halves hold different things. The asking half has a
/// whole `Source` in hand; the shaping half has a manifest's lock entry and a
/// root/rel split it computes itself, and never a `Source` at all. That
/// difference is why the shaping half went unwired to this resolver for as
/// long as it did — a plumbing accident, not a decision — and naming the input
/// is what closes it.
///
/// The strings are owned rather than borrowed. Borrowing is free from a
/// `Source`, but the pattern half builds its relative path locally and hands
/// the context onward, so a borrow would dangle; the escapes are a private
/// owned mirror (two types for one concept) or an eight-argument call. Two
/// `String` clones per source, which is what the pattern half already spends
/// on its root path.
#[derive(Debug, Clone)]
pub struct SourceAttributes {
    pub id: i64,
    pub root_id: i64,
    pub root_path: String,
    pub rel_path: String,
    pub size: i64,
    pub mtime: i64,
    pub device: i64,
    pub inode: i64,
}

impl SourceAttributes {
    /// The absolute path, composed the one way Canon composes it: a source at
    /// its own root's top is the root.
    pub(super) fn path(&self) -> String {
        if self.rel_path.is_empty() {
            self.root_path.clone()
        } else {
            format!("{}/{}", self.root_path, self.rel_path)
        }
    }
}

impl From<&Source> for SourceAttributes {
    fn from(source: &Source) -> Self {
        SourceAttributes {
            id: source.id,
            root_id: source.root_id,
            root_path: source.root_path.clone(),
            rel_path: source.rel_path.clone(),
            size: source.size,
            mtime: source.mtime,
            device: source.device,
            inode: source.inode,
        }
    }
}

/// Resolve a fact value for a source.
///
/// Returns the resolved value as a display string, or None if the fact
/// doesn't exist for this source.
///
/// # Arguments
///
/// * `source` - The source to get the fact value for
/// * `key` - The parsed fact key (base key + accessor + modifiers)
/// * `stored_facts` - Map of fact key → FactEntry for this source's stored facts
/// * `zone` - The user's clock, which every time is read on
///
/// A modifier that gives no value (`|month` on a time known only to the year) resolves to
/// `None`, as a missing fact does.
///
/// # Errors
///
/// A [`crate::expr::ExpressionRefused`] answer where a stored value is not what the key was read
/// as — its type changed while the report ran ([`ParsedFactKey::check_type`]). Otherwise a
/// transform that fails is a defect `read_report` should have refused, and is raised as one.
pub fn resolve_fact_value(
    source: &Source,
    key: &ParsedFactKey,
    stored_facts: &HashMap<String, FactEntry>,
    zone: &UserZone,
) -> Result<Option<String>> {
    // 1. Get raw value: the source row for a key it answers, the stored facts
    // for everything else — the content hash included, which `hash` and
    // `hash_short` are read as. The partition is the key's
    // (`is_computed`), as in the filter's compare, never the resolver's `None`:
    // a computed key the row cannot answer (a save time outside the years Canon
    // reads) has no value, and never falls through to a stored row of its name.
    let raw_value = match Key::read(&key.base_key) {
        Some(read) if read.is_computed() => {
            source_row_value(&SourceAttributes::from(source), &read)
        }
        _ => match stored_facts.get(&key.base_key) {
            Some(entry) => {
                key.check_type(&entry.value)?;
                Some(entry.value.clone())
            }
            None => None,
        },
    };

    // 2. Apply transforms if value exists. The key was read through `read_report`, which
    // refuses a modifier its type cannot take, so a transform failing here is a defect and is
    // raised — as the filter raises one.
    match raw_value {
        Some(value) => apply_transforms(value, key, zone),
        None => Ok(None),
    }
}

/// A key's value for a source, read from the source's own row.
///
/// Every source fact has its arm here, and a built-in key with the same value as
/// a source fact (`size`, `mtime`, `ext`, `id`) is read through that source
/// fact's arm — one arm per value, never a copy. Returns `None` for the keys the
/// source row cannot answer: the hash keys and every content fact — and that
/// `None` is load-bearing beyond this function: it is what `Key::is_computed` is
/// derived against, so which keys the pattern context answers for itself is read
/// off these arms rather than listed anywhere.
pub fn source_row_value(source: &SourceAttributes, key: &Key) -> Option<FactValue> {
    if let Some(fact) = key.as_source_fact() {
        return source_fact_value(source, fact);
    }
    match key {
        Key::SourceFact(_) => unreachable!("every source fact is read above"),
        Key::BuiltinKey(builtin) => match builtin {
            BuiltinKey::Ext | BuiltinKey::Size | BuiltinKey::Mtime | BuiltinKey::Id => {
                unreachable!("read through its source fact above")
            }
            BuiltinKey::Filename => {
                let filename = std::path::Path::new(&source.rel_path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or(&source.rel_path)
                    .to_string();
                Some(FactValue::Text(filename))
            }
            BuiltinKey::Stem => {
                let stem = std::path::Path::new(&source.rel_path)
                    .file_stem()
                    .and_then(|f| f.to_str())
                    .unwrap_or("")
                    .to_string();
                Some(FactValue::Text(stem))
            }
            BuiltinKey::RootId => Some(FactValue::Num(source.root_id as f64)),
            // Read as the stored content hash, which needs the object, not the source row.
            BuiltinKey::Hash | BuiltinKey::HashShort => None,
        },
        Key::ContentFact(_) => None,
    }
}

/// A source fact's value, read from what the scanner recorded.
fn source_fact_value(source: &SourceAttributes, fact: SourceFact) -> Option<FactValue> {
    match fact {
        SourceFact::Ext => {
            let ext = std::path::Path::new(&source.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(str::to_string)
                .unwrap_or_default();
            Some(FactValue::Text(ext))
        }
        SourceFact::Size => Some(FactValue::Num(source.size as f64)),
        // A save time outside the years every zone reads in 0000–9999 has no value.
        SourceFact::Mtime => TimeValue::from_epoch_secs(source.mtime)
            .ok()
            .map(FactValue::Time),
        SourceFact::Path => Some(FactValue::Path(source.path())),
        SourceFact::Root => Some(FactValue::Path(source.root_path.clone())),
        SourceFact::RelPath => Some(FactValue::Path(source.rel_path.clone())),
        SourceFact::Id => Some(FactValue::Num(source.id as f64)),
        SourceFact::Device => Some(FactValue::Num(source.device as f64)),
        SourceFact::Inode => Some(FactValue::Num(source.inode as f64)),
    }
}

/// Apply accessor and modifiers to a value, returning its display string — or `None` where
/// a modifier gave no value, which a reader counts as a source without one.
fn apply_transforms(
    value: FactValue,
    key: &ParsedFactKey,
    zone: &UserZone,
) -> Result<Option<String>> {
    // for_display: true since this is for facts output
    Ok(
        match transform::apply_chain(
            value,
            key.accessor.as_ref(),
            &key.modifiers,
            &key.raw,
            true,
            zone,
        )? {
            Modified::Value(v) => Some(transform::rendered(
                key.modifiers.last().map(|call| call.modifier),
                &v,
                zone,
            )),
            Modified::NoValue(_) => None,
        },
    )
}

/// A source's value for a key: its content's fact, and its own only where the content has
/// none. Every reader of a stored key's value per source asks this, so a filter, a report, a
/// pattern and a worklist cannot see two different values for one source.
///
/// Generic over what it holds: the filter's cache holds references, the fetch owned entries.
pub fn source_value<T>(on_source: Option<T>, on_content: Option<T>) -> Option<T> {
    on_content.or(on_source)
}

/// A source's one value for each stored key it holds, from all of its facts on the source and
/// on its content, each picked by `source_value` — for a reader that already holds every fact
/// of a source and needs no second fetch.
pub fn source_values(facts: &[FactEntry]) -> HashMap<&str, &FactValue> {
    let mut rows: HashMap<&str, (Option<&FactValue>, Option<&FactValue>)> = HashMap::new();
    for fact in facts {
        let pair = rows.entry(fact.key.as_str()).or_default();
        if fact.is_on_source() {
            pair.0 = Some(&fact.value);
        } else {
            pair.1 = Some(&fact.value);
        }
    }
    rows.into_iter()
        .filter_map(|(key, (on_source, on_content))| {
            source_value(on_source, on_content).map(|value| (key, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::key::CONTENT_HASH;
    use super::*;
    use crate::core::domain::fact::FactValue;

    /// A source's facts in hand give one value per key, picked by the same rule: the
    /// content's where both rows hold the key, the source's where only it does.
    #[test]
    fn a_sources_facts_in_hand_give_one_value_per_key() {
        let fact = |on: &str, key: &str, value: &str| FactEntry {
            key: key.to_string(),
            value: FactValue::Text(value.to_string()),
            entity_type: on.to_string(),
            entity_id: 1,
        };
        let facts = [
            fact("source", "content.Make", "Nikon"),
            fact("object", "content.Make", "Canon"),
            fact("source", "content.Lens", "50mm"),
            fact("object", "content.Model", "R5"),
        ];
        let text = |values: HashMap<&str, &FactValue>| {
            let mut out: Vec<(String, String)> = values
                .into_iter()
                .map(|(k, v)| match v {
                    FactValue::Text(t) => (k.to_string(), t.clone()),
                    other => panic!("only text facts here: {other:?}"),
                })
                .collect();
            out.sort();
            out
        };
        let pairs = |list: &[(&str, &str)]| -> Vec<(String, String)> {
            list.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert_eq!(
            text(source_values(&facts)),
            pairs(&[
                ("content.Lens", "50mm"),
                ("content.Make", "Canon"),
                ("content.Model", "R5"),
            ])
        );
    }

    /// Every combination of a fact on the source and a fact on its content.
    #[test]
    fn a_source_reads_its_contents_fact_first() {
        let source = Some("source");
        let content = Some("content");
        for (on_source, on_content, expected) in [
            (source, content, content),
            (source, None, source),
            (None, content, content),
            (None, None, None),
        ] {
            assert_eq!(
                source_value(on_source, on_content),
                expected,
                "source {on_source:?}, content {on_content:?}"
            );
        }
    }

    /// A probe with every attribute populated, so a resolver arm that
    /// answers at all answers for this one. The equality below is only as
    /// strong as that: an arm returning `None` here is genuinely an arm the
    /// source cannot answer, not one this fixture happened to starve.
    fn probe() -> SourceAttributes {
        SourceAttributes {
            id: 42,
            root_id: 1,
            root_path: "/photos".to_string(),
            rel_path: "2024/vacation/IMG_001.jpg".to_string(),
            size: 1024000,
            mtime: 1718452800,
            device: 16777220,
            inode: 12345678,
        }
    }

    /// The built-in keys that are short names for a source fact are the vocabulary's — `ext`,
    /// `size`, `mtime`, `id` — as their one owner says (`BuiltinKey::source_fact`), and each
    /// has its source fact's value. `source_row_value` reads such a key through the owner
    /// before its own arms, so an arm written for one of them is never reached.
    #[test]
    fn a_built_in_key_has_its_source_fact_s_value() {
        use strum::IntoEnumIterator;

        let paired: Vec<(BuiltinKey, SourceFact)> = BuiltinKey::iter()
            .filter_map(|builtin| builtin.source_fact().map(|fact| (builtin, fact)))
            .collect();
        assert_eq!(
            paired,
            vec![
                (BuiltinKey::Ext, SourceFact::Ext),
                (BuiltinKey::Id, SourceFact::Id),
                (BuiltinKey::Size, SourceFact::Size),
                (BuiltinKey::Mtime, SourceFact::Mtime),
            ]
        );
        for (builtin, fact) in paired {
            let short = source_row_value(&probe(), &Key::BuiltinKey(builtin));
            let long = source_row_value(&probe(), &Key::SourceFact(fact));
            assert!(
                short.is_some(),
                "{builtin:?} has a value on a readable source"
            );
            assert_eq!(format!("{short:?}"), format!("{long:?}"), "{builtin:?}");
        }
    }

    /// The derived-surface equality: `is_computed` is not a second opinion
    /// about the resolver, it is a reading of it. A hand-written list gets
    /// this wrong — `hash`/`hash_short` are built-in keys the row cannot
    /// answer, and `content.hash.sha256` a content fact whose type is known
    /// without rows — so the classification is asserted against the resolver
    /// itself rather than against a list someone maintained. The probe is a
    /// source whose every attribute the resolver reads; a computed key the row
    /// cannot answer (a save time outside the years Canon reads) is still
    /// computed — it has no value, pinned at each reader that partitions on it
    /// (`a_computed_key_never_falls_through_to_a_stored_row` and its twin).
    ///
    /// **Its limit, which is half the point.** The equality holds over the
    /// source facts, the built-in keys and the content hash, and says nothing
    /// wider. `scope.rel_path` and `object.hash` are context-supplied and are
    /// *not* `Key`s; they are the hand-written half of `is_context_supplied`
    /// and are pinned there, separately. Widening this assertion to cover them
    /// would assert an equality that does not hold and is exactly the
    /// false-equality generator the limit rule exists to prevent.
    #[test]
    fn a_key_is_computed_exactly_when_the_resolver_answers_it() {
        let keys = Key::all().chain([Key::ContentFact(CONTENT_HASH.to_string())]);
        for key in keys {
            let name = key.name();
            assert_eq!(
                key.is_computed(),
                source_row_value(&probe(), &key).is_some(),
                "'{name}' is classified is_computed()={} but the resolver {} it",
                key.is_computed(),
                if source_row_value(&probe(), &key).is_some() {
                    "answers"
                } else {
                    "does not answer"
                }
            );
        }
    }

    /// Helper to create a Source with minimal required fields for testing.
    fn make_source() -> Source {
        Source {
            id: 42,
            root_id: 1,
            root_path: "/photos".to_string(),
            rel_path: "2024/vacation/IMG_001.jpg".to_string(),
            object_id: None,
            size: 1024000,
            mtime: 1718452800, // 2024-06-15 12:00:00 UTC
            excluded: false,
            object_excluded: None,
            device: 16777220,
            inode: 12345678,
            partial_hash: String::new(),
            basis_rev: 1,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    /// Helper to create a stored facts map
    fn make_stored_facts(entries: Vec<(&str, FactValue)>) -> HashMap<String, FactEntry> {
        entries
            .into_iter()
            .map(|(key, value)| {
                (
                    key.to_string(),
                    FactEntry::new(key.to_string(), value, "source".to_string(), 1),
                )
            })
            .collect()
    }

    // =========================================================================
    // Source-row value tests
    // =========================================================================

    #[test]
    fn resolve_source_ext() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.ext").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("jpg".to_string()));
    }

    #[test]
    fn resolve_source_ext_keeps_its_case() {
        let mut source = make_source();
        source.rel_path = "2024/IMG_001.JPG".to_string();
        let key = ParsedFactKey::parse("source.ext").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("JPG".to_string()));

        let key = ParsedFactKey::parse("source.ext|lowercase").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("jpg".to_string()));
    }

    #[test]
    fn resolve_source_ext_no_extension() {
        let mut source = make_source();
        source.rel_path = "Makefile".to_string();
        let key = ParsedFactKey::parse("source.ext").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("".to_string()));
    }

    #[test]
    fn resolve_source_size() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.size").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("1024000".to_string()));
    }

    #[test]
    fn resolve_source_mtime() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("2024-06-15 12:00:00".to_string()));
    }

    #[test]
    fn resolve_source_path() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.path").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(
            result,
            Some("/photos/2024/vacation/IMG_001.jpg".to_string())
        );
    }

    #[test]
    fn resolve_source_path_empty_rel() {
        let mut source = make_source();
        source.rel_path = "".to_string();
        let key = ParsedFactKey::parse("source.path").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("/photos".to_string()));
    }

    #[test]
    fn resolve_source_root() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.root").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("/photos".to_string()));
    }

    #[test]
    fn resolve_filename() {
        let source = make_source();
        let key = ParsedFactKey::parse("filename").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("IMG_001.jpg".to_string()));
    }

    #[test]
    fn resolve_stem() {
        let source = make_source();
        let key = ParsedFactKey::parse("stem").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("IMG_001".to_string()));
    }

    #[test]
    fn resolve_source_device_inode() {
        let source = make_source();

        let key = ParsedFactKey::parse("source.device").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("16777220".to_string()));

        let key = ParsedFactKey::parse("source.inode").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("12345678".to_string()));
    }

    // =========================================================================
    // Stored fact resolution tests
    // =========================================================================

    #[test]
    fn resolve_stored_text() {
        let source = make_source();
        let stored =
            make_stored_facts(vec![("content.Make", FactValue::Text("Canon".to_string()))]);
        let key = ParsedFactKey::parse("content.Make").unwrap();
        let result = resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap();
        assert_eq!(result, Some("Canon".to_string()));
    }

    #[test]
    fn resolve_stored_num() {
        let source = make_source();
        let stored = make_stored_facts(vec![("content.Width", FactValue::Num(4032.0))]);
        let key = ParsedFactKey::parse("content.Width").unwrap();
        let result = resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap();
        assert_eq!(result, Some("4032".to_string()));
    }

    #[test]
    fn resolve_stored_time() {
        let source = make_source();
        let stored = make_stored_facts(vec![(
            "content.DateTimeOriginal",
            // A migrated time fact: a clock reading at second precision.
            FactValue::Time(crate::core::testing::migrated_time(1704067200)),
        )]);
        let key = ParsedFactKey::parse("content.DateTimeOriginal").unwrap();
        let result = resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap();
        assert_eq!(result, Some("2024-01-01 00:00:00".to_string()));
    }

    #[test]
    fn resolve_stored_missing() {
        let source = make_source();
        let stored = HashMap::new();
        let key = ParsedFactKey::parse("content.NonExistent").unwrap();
        let result = resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap();
        assert_eq!(result, None);
    }

    // =========================================================================
    // Transform tests
    // =========================================================================

    #[test]
    fn resolve_with_accessor() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.rel_path[-1]").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("IMG_001.jpg".to_string()));
    }

    #[test]
    fn resolve_with_modifier() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.mtime|year").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("2024".to_string()));
    }

    #[test]
    fn resolve_with_accessor_and_modifier() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.rel_path[-1]|stem").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, Some("IMG_001".to_string()));
    }

    #[test]
    fn resolve_transform_type_mismatch() {
        let source = make_source();
        // A time modifier refuses a number: it never means seconds since 1970.
        let key = ParsedFactKey::parse("source.size|year").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires a time or text value"));
        // It reads text as a time, and text that does not read as one has no value.
        let key = ParsedFactKey::parse("source.ext|year").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc());
        assert_eq!(result.unwrap(), None);
    }

    // =========================================================================
    // Edge case tests
    // =========================================================================

    #[test]
    fn resolve_source_fact_takes_precedence() {
        // If a key is a source fact, a stored fact of its name is ignored
        let source = make_source();
        let stored = make_stored_facts(vec![(
            "source.size",
            FactValue::Num(999.0), // This should be ignored
        )]);
        let key = ParsedFactKey::parse("source.size").unwrap();
        let result = resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap();
        // Should use the source fact's value (1024000), not the stored one (999)
        assert_eq!(result, Some("1024000".to_string()));
    }

    /// A computed key the source row cannot answer has no value, and a stored row wearing its
    /// name is never read in its place: the partition is the key's, not the resolver's `None`.
    /// Its twin at the filter's compare is
    /// `a_computed_key_the_row_cannot_answer_matches_no_stored_row`.
    #[test]
    fn a_computed_key_never_falls_through_to_a_stored_row() {
        let mut source = make_source();
        source.mtime = 253402300799; // a save time some zone reads in year 10000
        let stored = make_stored_facts(vec![(
            "source.mtime",
            FactValue::Time(crate::core::domain::time::reading::read_time("2020").unwrap()),
        )]);
        for raw in ["source.mtime", "mtime", "source.mtime|year"] {
            let key = ParsedFactKey::parse(raw).unwrap();
            assert_eq!(
                resolve_fact_value(&source, &key, &stored, &UserZone::utc()).unwrap(),
                None,
                "{raw}"
            );
        }
    }

    #[test]
    fn resolve_unknown_key_no_stored() {
        let source = make_source();
        let key = ParsedFactKey::parse("unknown.key").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_hash_reads_the_stored_content_hash() {
        // The source row cannot answer a hash key; its value is the stored
        // fact, and with none stored there is no value.
        let source = make_source();
        let key = ParsedFactKey::parse("hash").unwrap();
        assert_eq!(
            resolve_fact_value(&source, &key, &HashMap::new(), &UserZone::utc()).unwrap(),
            None
        );

        let stored = make_stored_facts(vec![(
            "content.hash.sha256",
            FactValue::Text("abcdef12345".into()),
        )]);
        let short = ParsedFactKey::parse("hash_short").unwrap();
        assert_eq!(
            resolve_fact_value(&source, &short, &stored, &UserZone::utc()).unwrap(),
            Some("abcdef12".to_string())
        );
    }
}
