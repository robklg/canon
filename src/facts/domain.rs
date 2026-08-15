//! Import-vocabulary half of the fact domain model: the storage type used
//! for type-consistency checking during import, the source-fact record
//! promotion reads, and key normalization/classification.

/// Fact storage type for type consistency checking during import.
///
/// When importing facts, we need to ensure type consistency: if a key was
/// previously stored as Time, new values for that key must also be Time.
/// This enum represents the storage type, which is separate from `FactValue`
/// (the runtime representation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactValueType {
    /// Stored in value_text column
    Text,
    /// Stored in value_num column
    Num,
    /// Stored in value_time column
    Time,
}

impl std::fmt::Display for FactValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FactValueType::Text => write!(f, "text"),
            FactValueType::Num => write!(f, "num"),
            FactValueType::Time => write!(f, "time"),
        }
    }
}

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
    /// Text value if stored as text
    pub value_text: Option<String>,
    /// Numeric value if stored as number
    pub value_num: Option<f64>,
    /// Timestamp value if stored as time
    pub value_time: Option<i64>,
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

    // =========================================================================
    // FactValueType tests
    // =========================================================================

    #[test]
    fn fact_value_type_display() {
        assert_eq!(format!("{}", FactValueType::Text), "text");
        assert_eq!(format!("{}", FactValueType::Num), "num");
        assert_eq!(format!("{}", FactValueType::Time), "time");
    }

    #[test]
    fn fact_value_type_equality() {
        assert_eq!(FactValueType::Text, FactValueType::Text);
        assert_ne!(FactValueType::Text, FactValueType::Num);
        assert_ne!(FactValueType::Num, FactValueType::Time);
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
            value_text: Some("Canon".to_string()),
            value_num: None,
            value_time: None,
            observed_at: 1704067200,
        };

        assert_eq!(fact.id, 1);
        assert_eq!(fact.key, "content.Make");
        assert_eq!(fact.value_text, Some("Canon".to_string()));
        assert!(fact.value_num.is_none());
        assert!(fact.value_time.is_none());
        assert_eq!(fact.observed_at, 1704067200);
    }

    #[test]
    fn source_fact_with_num_value() {
        let fact = SourceFact {
            id: 2,
            key: "content.Duration".to_string(),
            value_text: None,
            value_num: Some(120.5),
            value_time: None,
            observed_at: 1704067200,
        };

        assert_eq!(fact.value_num, Some(120.5));
    }

    #[test]
    fn source_fact_with_time_value() {
        let fact = SourceFact {
            id: 3,
            key: "content.DateTimeOriginal".to_string(),
            value_text: None,
            value_num: None,
            value_time: Some(1704067200),
            observed_at: 1704067200,
        };

        assert_eq!(fact.value_time, Some(1704067200));
    }
}
