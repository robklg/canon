//! Fact value resolution — domain layer for getting fact values.
//!
//! This module provides the core operation: "get the value of fact K for source S."
//! It handles built-in facts (derived from Source fields), stored facts (from the
//! facts table), and applies any transforms (accessors, modifiers).
//!
//! ## Design Principles
//!
//! 1. **Pure function**: `resolve_fact_value()` has no I/O or side effects
//! 2. **Single source of truth**: This is THE way to get a fact's value for a source
//! 3. **Handles both built-in and stored facts**: Caller doesn't need to distinguish
//! 4. **Testable in isolation**: Can be unit tested without database
//!
//! ## Usage
//!
//! ```ignore
//! use crate::expr::resolve_fact_value;
//! use crate::expr::ParsedFactKey;
//!
//! let key = ParsedFactKey::parse("source.mtime|year")?;
//! let value = resolve_fact_value(&source, &key, &stored_facts)?;
//! // value is Some("2024") or None if fact doesn't exist
//! ```

use std::collections::HashMap;

use anyhow::Result;

use super::key::{BuiltinKey, ParsedFactKey};
use super::transform;
use crate::core::domain::fact::FactEntry;
use crate::core::domain::fact::FactValue;
use crate::core::domain::source::Source;

/// Everything the built-in vocabulary can be computed from — the complete set,
/// so a built-in either resolves from these or is a stored fact.
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
///
/// # Errors
///
/// Returns an error if a transform cannot be applied (e.g., `|year` on a text value).
pub fn resolve_fact_value(
    source: &Source,
    key: &ParsedFactKey,
    stored_facts: &HashMap<String, FactEntry>,
) -> Result<Option<String>> {
    // 1. Get raw value (built-in or stored)
    let raw_value = if let Some(builtin) = BuiltinKey::from_str(&key.base_key) {
        get_builtin_value(&SourceAttributes::from(source), builtin)
    } else {
        stored_facts.get(&key.base_key).map(|e| e.value.clone())
    };

    // 2. Apply transforms if value exists
    match raw_value {
        Some(value) => {
            // A transform that cannot be applied — `|year` on a text value —
            // is an error here and a plain non-match when the same key
            // appears in `--where`. The two surfaces mean different things by
            // failure: a report was asked for this key by name and would
            // otherwise print a blank where the user expected a value, while
            // a filter is asking a question whose honest answer is "no".
            // Deliberate, and not to be unified without first deciding which
            // of the two meanings wins.
            let transformed = apply_transforms(value, key)?;
            Ok(Some(transformed))
        }
        None => Ok(None),
    }
}

/// Extract a built-in fact value from a source's own attributes.
///
/// Built-in facts are derived from source columns without database lookup.
/// Returns None for hash-based facts which require object lookup — and that
/// `None` is load-bearing beyond this function: it is what
/// `BuiltinKey::is_computed` is derived against, so which keys the pattern
/// context answers for itself is read off these arms rather than listed
/// anywhere.
pub fn get_builtin_value(source: &SourceAttributes, builtin: BuiltinKey) -> Option<FactValue> {
    match builtin {
        BuiltinKey::SourceExt | BuiltinKey::Ext => {
            let ext = std::path::Path::new(&source.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();
            Some(FactValue::Text(ext))
        }
        BuiltinKey::SourceSize | BuiltinKey::Size => Some(FactValue::Num(source.size as f64)),
        BuiltinKey::SourceMtime | BuiltinKey::Mtime => Some(FactValue::Time(source.mtime)),
        BuiltinKey::SourcePath => Some(FactValue::Path(source.path())),
        BuiltinKey::SourceRoot => Some(FactValue::Path(source.root_path.clone())),
        BuiltinKey::SourceRelPath => Some(FactValue::Path(source.rel_path.clone())),
        BuiltinKey::SourceId | BuiltinKey::Id => Some(FactValue::Num(source.id as f64)),
        BuiltinKey::SourceDevice => Some(FactValue::Num(source.device as f64)),
        BuiltinKey::SourceInode => Some(FactValue::Num(source.inode as f64)),
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
        // Hash-based facts need object lookup, not available from Source alone
        BuiltinKey::Hash | BuiltinKey::HashShort | BuiltinKey::ContentHashSha256 => None,
    }
}

/// Apply accessor and modifiers to a value, returning display string.
fn apply_transforms(value: FactValue, key: &ParsedFactKey) -> Result<String> {
    let mut result = value;

    // Apply accessor if present
    if let Some(ref acc) = key.accessor {
        result = transform::apply_accessor(&result, acc, &key.raw)?;
    }

    // Apply modifiers (for_display: true since this is for facts output)
    for modifier_call in &key.modifiers {
        result = transform::apply_modifier(&result, modifier_call, &key.raw, true)?;
    }

    // Convert to display string
    Ok(fact_value_to_display(&result))
}

/// Convert a FactValue to a display string.
///
/// This handles formatting for human-readable output:
/// - Numbers: integers shown without decimal, floats shown with decimals
/// - Times: formatted as YYYY-MM-DD HH:MM:SS
/// - Text/Path: shown as-is
pub fn fact_value_to_display(value: &FactValue) -> String {
    match value {
        FactValue::Text(t) => t.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{n}")
            }
        }
        FactValue::Time(ts) => chrono::DateTime::from_timestamp(*ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| ts.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::fact::FactValue;

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

    /// The derived-surface equality: `is_computed` is not a second opinion
    /// about the resolver, it is a reading of it. A hand-written list gets
    /// this wrong — `content.hash.sha256` is a built-in *and* a genuinely
    /// stored fact, and `hash`/`hash_short` are the resolver's other `None`
    /// arms — so the classification is asserted against the resolver itself
    /// rather than against a list someone maintained.
    ///
    /// **Its limit, which is half the point.** The equality holds over
    /// `BuiltinKey` and says nothing wider. `scope.rel_path` and
    /// `object.hash` are context-supplied and are *not* `BuiltinKey`s; they
    /// are the hand-written half of `is_context_supplied` and are pinned
    /// there, separately. Widening this assertion to cover them would assert
    /// an equality that does not hold and is exactly the false-equality
    /// generator the limit rule exists to prevent.
    #[test]
    fn a_key_is_computed_exactly_when_the_resolver_answers_it() {
        use strum::IntoEnumIterator;

        for key in BuiltinKey::iter() {
            let name: &'static str = key.into();
            assert_eq!(
                key.is_computed(),
                get_builtin_value(&probe(), key).is_some(),
                "'{name}' is classified is_computed()={} but the resolver {} it",
                key.is_computed(),
                if get_builtin_value(&probe(), key).is_some() {
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
    // Built-in fact resolution tests
    // =========================================================================

    #[test]
    fn resolve_source_ext() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.ext").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("jpg".to_string()));
    }

    #[test]
    fn resolve_source_ext_no_extension() {
        let mut source = make_source();
        source.rel_path = "Makefile".to_string();
        let key = ParsedFactKey::parse("source.ext").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("".to_string()));
    }

    #[test]
    fn resolve_source_size() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.size").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("1024000".to_string()));
    }

    #[test]
    fn resolve_source_mtime() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("2024-06-15 12:00:00".to_string()));
    }

    #[test]
    fn resolve_source_path() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.path").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
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
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("/photos".to_string()));
    }

    #[test]
    fn resolve_source_root() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.root").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("/photos".to_string()));
    }

    #[test]
    fn resolve_filename() {
        let source = make_source();
        let key = ParsedFactKey::parse("filename").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("IMG_001.jpg".to_string()));
    }

    #[test]
    fn resolve_stem() {
        let source = make_source();
        let key = ParsedFactKey::parse("stem").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("IMG_001".to_string()));
    }

    #[test]
    fn resolve_source_device_inode() {
        let source = make_source();

        let key = ParsedFactKey::parse("source.device").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("16777220".to_string()));

        let key = ParsedFactKey::parse("source.inode").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
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
        let result = resolve_fact_value(&source, &key, &stored).unwrap();
        assert_eq!(result, Some("Canon".to_string()));
    }

    #[test]
    fn resolve_stored_num() {
        let source = make_source();
        let stored = make_stored_facts(vec![("content.Width", FactValue::Num(4032.0))]);
        let key = ParsedFactKey::parse("content.Width").unwrap();
        let result = resolve_fact_value(&source, &key, &stored).unwrap();
        assert_eq!(result, Some("4032".to_string()));
    }

    #[test]
    fn resolve_stored_time() {
        let source = make_source();
        let stored = make_stored_facts(vec![(
            "content.DateTimeOriginal",
            FactValue::Time(1704067200), // 2024-01-01 00:00:00 UTC
        )]);
        let key = ParsedFactKey::parse("content.DateTimeOriginal").unwrap();
        let result = resolve_fact_value(&source, &key, &stored).unwrap();
        assert_eq!(result, Some("2024-01-01 00:00:00".to_string()));
    }

    #[test]
    fn resolve_stored_missing() {
        let source = make_source();
        let stored = HashMap::new();
        let key = ParsedFactKey::parse("content.NonExistent").unwrap();
        let result = resolve_fact_value(&source, &key, &stored).unwrap();
        assert_eq!(result, None);
    }

    // =========================================================================
    // Transform tests
    // =========================================================================

    #[test]
    fn resolve_with_accessor() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.rel_path[-1]").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("IMG_001.jpg".to_string()));
    }

    #[test]
    fn resolve_with_modifier() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.mtime|year").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("2024".to_string()));
    }

    #[test]
    fn resolve_with_accessor_and_modifier() {
        let source = make_source();
        let key = ParsedFactKey::parse("source.rel_path[-1]|stem").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, Some("IMG_001".to_string()));
    }

    #[test]
    fn resolve_transform_type_mismatch() {
        let source = make_source();
        // Trying to apply |year to a text value should fail
        let key = ParsedFactKey::parse("source.ext|year").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("time-type"));
    }

    // =========================================================================
    // Edge case tests
    // =========================================================================

    #[test]
    fn resolve_builtin_takes_precedence() {
        // If a key matches a builtin, stored fact is ignored
        let source = make_source();
        let stored = make_stored_facts(vec![(
            "source.size",
            FactValue::Num(999.0), // This should be ignored
        )]);
        let key = ParsedFactKey::parse("source.size").unwrap();
        let result = resolve_fact_value(&source, &key, &stored).unwrap();
        // Should use the builtin value (1024000), not the stored one (999)
        assert_eq!(result, Some("1024000".to_string()));
    }

    #[test]
    fn resolve_unknown_key_no_stored() {
        let source = make_source();
        let key = ParsedFactKey::parse("unknown.key").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_hash_returns_none() {
        // Hash-based builtins return None (need object lookup)
        let source = make_source();
        let key = ParsedFactKey::parse("hash").unwrap();
        let result = resolve_fact_value(&source, &key, &HashMap::new()).unwrap();
        assert_eq!(result, None);
    }

    // =========================================================================
    // fact_value_to_display tests
    // =========================================================================

    #[test]
    fn display_integer_num() {
        assert_eq!(fact_value_to_display(&FactValue::Num(42.0)), "42");
    }

    #[test]
    fn display_float_num() {
        assert_eq!(fact_value_to_display(&FactValue::Num(12.375)), "12.375");
    }

    #[test]
    fn display_time() {
        // 2024-06-15 12:00:00 UTC
        assert_eq!(
            fact_value_to_display(&FactValue::Time(1718452800)),
            "2024-06-15 12:00:00"
        );
    }
}
