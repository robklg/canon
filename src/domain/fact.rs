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
//! 3. **Reuse existing types**: `FactValue` and `FactType` come from `expr.rs`
//! 4. **Dependencies point inward**: Commands depend on this module, not vice versa
//!
//! ## Usage
//!
//! ```ignore
//! use canon::fact::{FactEntry, FactValue, FactType};
//! use canon::fact_repo;
//!
//! // Fetch a specific fact key for sources
//! let facts = fact_repo::batch_fetch_key_for_sources(conn, &source_ids, "content.Make")?;
//!
//! // Check each source's fact value
//! for (source_id, entry) in &facts {
//!     if let Some(fact) = entry {
//!         println!("{}: {:?}", source_id, fact.value);
//!     }
//! }
//! ```

// Re-export types from expr.rs for convenience.
// FactType will be used by fact_repo::count_fact_keys() in Phase 2.
#[allow(unused_imports)]
pub use crate::expr::eval::{FactType, FactValue};

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
    Ok(format!("content.{}", key))
}

/// Check if a key is a content fact (starts with "content.").
///
/// Content facts are stored on objects when available, and are eligible
/// for promotion from source to object when the source gets linked.
pub fn is_content_fact(key: &str) -> bool {
    key.starts_with("content.")
}

/// A single fact entry associated with a source.
///
/// Facts are stored in the database on either sources or objects, but for query
/// purposes we associate them with sources. When fetching facts for a source,
/// the result includes both:
/// - Direct source facts (`entity_type = "source"`)
/// - Object facts via the source's `object_id` (`entity_type = "object"`)
///
/// The `entity_type` and `entity_id` fields preserve the original storage location,
/// which is useful for debugging and for operations that need to distinguish
/// between source-level and object-level facts.
#[derive(Debug, Clone)]
pub struct FactEntry {
    /// The fact key (e.g., "content.Make", "source.policy.reviewed")
    pub key: String,
    /// The typed fact value
    pub value: FactValue,
    // Fields below are part of the domain model API but not currently used.
    // Kept for debugging/introspection and future use cases.
    #[allow(dead_code)]
    /// Where this fact is stored: "source" or "object"
    pub entity_type: String,
    #[allow(dead_code)]
    /// The entity ID (source_id or object_id depending on entity_type)
    pub entity_id: i64,
}

impl FactEntry {
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
        let entry = FactEntry::new(
            "content.DateTimeOriginal".to_string(),
            FactValue::Time(1704067200), // 2024-01-01 00:00:00 UTC
            "object".to_string(),
            100,
        );

        match entry.value {
            FactValue::Time(ts) => assert_eq!(ts, 1704067200),
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
