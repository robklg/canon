//! Import-vocabulary half of the fact domain model: the record of a content
//! fact stored on the source, which promotion reads, key classification, and
//! the keys an import stores and `facts delete` acts on.
//! A key's name is read by the expression facility's one reader
//! (`expr::read_key_name`). A value's kind is the fact noun's own
//! (`core::domain::fact::FactType`).

use crate::core::domain::fact::FactValue;
use crate::expr::{read_key_name, BuiltinKey, Key, ParsedFactKey, SourceFact};

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
/// `content.Make`), or a refusal for a name with none — any name starting with
/// `source.`, a source fact or not. Read once, before anything else the command
/// does, so no empty selection hides the refusal.
pub fn delete_key(name: &str) -> anyhow::Result<String> {
    read_key_name(name).content_key.ok_or_else(|| {
        anyhow::anyhow!("Cannot delete '{name}': names starting with 'source.' cannot be deleted.")
    })
}

/// The key an import stores a fact under: the name's content reading (`Make` →
/// `content.Make`), or `None` for a name with none — any name starting with
/// `source.`, which the import skips.
pub fn import_key(name: &str) -> Option<String> {
    read_key_name(name).content_key
}

/// Whether a report key names a source's root — `source.root` or `root_id`, quoted or not,
/// with no accessor or modifier — which a report shows and groups by root. Decided by the key
/// the name reads as (`ParsedFactKey::key`), never by its text.
pub fn names_the_root(key: &ParsedFactKey) -> bool {
    !key.has_transforms()
        && matches!(
            key.key,
            Some(Key::SourceFact(SourceFact::Root) | Key::BuiltinKey(BuiltinKey::RootId))
        )
}

/// Check if a key is a content fact (starts with "content.").
///
/// Content facts are stored on objects when available, and are eligible
/// for promotion from source to object when the source gets linked.
pub fn is_content_fact(key: &str) -> bool {
    key.starts_with("content.")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **The key-name law's verifier**: every command reads a key's name
    /// through one reader (`expr::read_key_name`), and each applies its own rule
    /// to the readings. For a name that is not a built-in key, import's reading,
    /// the filter's and `facts delete`'s reach the same content fact, so a filter
    /// finds, and a delete removes, a fact by the name it was imported under. The
    /// defect it names: the asking half kept `policy.`/`object.` keys as written
    /// while import stored them under `content.`, and `--where policy.reviewed?`
    /// selected nothing. Sampled prefixes.
    ///
    /// For a built-in key's name they differ, by design, and that half is
    /// pinned too: the filter reads the built-in key (`size` is the source's
    /// size), import and the delete the content reading (`content.size`).
    #[test]
    fn a_key_is_read_by_the_name_import_stored_it_under() {
        use crate::expr::ParsedFactKey;

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
            for name in ["Make", "reviewed", "x.y"] {
                let written = format!("{prefix}{name}");
                let imported = import_key(&written).unwrap();
                let filtered = ParsedFactKey::parse(&written).unwrap().base_key;
                assert_eq!(filtered, imported, "{written}");
                assert_eq!(delete_key(&written).unwrap(), imported, "{written}");
            }
        }

        // One built-in name, a deliberate difference: each command's own rule.
        assert_eq!(ParsedFactKey::parse("size").unwrap().base_key, "size");
        assert_eq!(import_key("size").as_deref(), Some("content.size"));
        assert_eq!(delete_key("size").unwrap(), "content.size");
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

    /// A report key names the root as `source.root` or `root_id`, bare, double- or
    /// single-quoted: a modifier, a prefix or a near spelling does not.
    #[test]
    fn only_the_two_root_names_name_the_root() {
        let names = |written: &str| names_the_root(&ParsedFactKey::parse(written).unwrap());
        for written in ["source.root", "root_id", "\"source.root\"", "'root_id'"] {
            assert!(names(written), "{written}");
        }
        for written in [
            "source.root|lowercase",
            "content.root_id",
            "root",
            "source.roots",
        ] {
            assert!(!names(written), "{written}");
        }
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
}
