//! Import-vocabulary half of the fact domain model: the source-fact record
//! promotion reads, and key normalization/classification. A value's kind is
//! the fact noun's own (`core::domain::fact::FactType`).

use crate::core::domain::fact::FactValue;

/// A source fact record for promotion processing.
///
/// When a source gets linked to an object, its content facts need to be
/// "promoted" from the source to the object. This struct holds the fact
/// data needed for that migration.
#[derive(Debug, Clone)]
pub struct SourceFact {
    /// The fact ID in the database
    pub id: i64,
    /// The fact key (e.g., "content.Make")
    pub key: String,
    /// The stored value
    pub value: FactValue,
    /// When this fact was observed
    pub observed_at: i64,
}

/// Normalize a fact key to use the content.* namespace.
///
/// - Keys starting with "source." are rejected (reserved namespace)
/// - Keys already starting with "content." are left as-is
/// - All other keys are prefixed with "content."
///
/// # Examples
///
/// ```ignore
/// assert_eq!(normalize_fact_key("Make"), Ok("content.Make".to_string()));
/// assert_eq!(normalize_fact_key("content.Make"), Ok("content.Make".to_string()));
/// assert!(normalize_fact_key("source.size").is_err());
/// ```
pub fn normalize_fact_key(key: &str) -> Result<String, &'static str> {
    if key.starts_with("source.") {
        return Err("source.* namespace is reserved for built-in facts");
    }
    if key.starts_with("content.") {
        return Ok(key.to_string());
    }
    Ok(format!("content.{key}"))
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

    /// Import and the asking half name an importable key the same way, so
    /// a filter finds a fact by the name it was imported under. The defect it
    /// names: the asking half kept `policy.`/`object.` keys as written while
    /// import stored them under `content.`, and `--where policy.reviewed?`
    /// selected nothing. Sampled prefixes; a key spelled as a built-in name
    /// (`size`, `hash`) is outside it — the asking half reads the built-in.
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
                let stored = normalize_fact_key(&written).unwrap();
                let read = ParsedFactKey::parse(&written).unwrap().base_key;
                assert_eq!(read, stored, "{written}");
            }
        }
    }

    // =========================================================================
    // normalize_fact_key tests
    // =========================================================================

    #[test]
    fn normalize_fact_key_adds_content_prefix() {
        assert_eq!(normalize_fact_key("Make"), Ok("content.Make".to_string()));
        assert_eq!(
            normalize_fact_key("hash.sha256"),
            Ok("content.hash.sha256".to_string())
        );
        assert_eq!(
            normalize_fact_key("DateTimeOriginal"),
            Ok("content.DateTimeOriginal".to_string())
        );
    }

    #[test]
    fn normalize_fact_key_preserves_content_prefix() {
        assert_eq!(
            normalize_fact_key("content.Make"),
            Ok("content.Make".to_string())
        );
        assert_eq!(
            normalize_fact_key("content.hash.sha256"),
            Ok("content.hash.sha256".to_string())
        );
    }

    #[test]
    fn normalize_fact_key_rejects_source_namespace() {
        assert!(normalize_fact_key("source.size").is_err());
        assert!(normalize_fact_key("source.mtime").is_err());
        assert!(normalize_fact_key("source.ext").is_err());

        let err = normalize_fact_key("source.size").unwrap_err();
        assert!(err.contains("reserved"));
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
    // SourceFact tests
    // =========================================================================

    #[test]
    fn source_fact_construction() {
        let fact = SourceFact {
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
    fn source_fact_with_num_value() {
        let fact = SourceFact {
            id: 2,
            key: "content.Duration".to_string(),
            value: FactValue::Num(120.5),
            observed_at: 1704067200,
        };

        assert!(matches!(fact.value, FactValue::Num(n) if n == 120.5));
    }

    #[test]
    fn source_fact_with_time_value() {
        let time = crate::core::domain::time::reading::read_time("2024-01-01T00:00:00").unwrap();
        let fact = SourceFact {
            id: 3,
            key: "content.DateTimeOriginal".to_string(),
            value: FactValue::Time(time),
            observed_at: 1704067200,
        };

        assert!(matches!(fact.value, FactValue::Time(t) if t == time));
    }
}
