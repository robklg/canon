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
//! use crate::core::repo;
//!
//! // Fetch a specific fact key for sources
//! let facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, "content.Make")?;
//!
//! // Check each source's fact value
//! for (source_id, entry) in &facts {
//!     if let Some(fact) = entry {
//!         println!("{}: {:?}", source_id, fact.value);
//!     }
//! }
//! ```

/// Fact type classification (without the actual value).
/// Matches the typed columns in the facts table plus Path for derived facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactType {
    Text,
    Num,
    Time,
    Path, // Derived path facts that support segment indexing
}

impl FactType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FactType::Text => "text",
            FactType::Num => "num",
            FactType::Time => "time",
            FactType::Path => "path",
        }
    }
}

/// Fact value types.
/// Matches the typed columns in the facts table: value_text, value_num, value_time.
#[derive(Debug, Clone)]
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),    // Unix timestamp
    Path(String), // Path that supports segment indexing (for derived path facts)
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
    // Nothing reads the two fields below today. They are kept because the
    // fetch that produces these entries unions two SELECTs — facts stored on
    // the source and facts stored on its object — and these are the only
    // thing in the result that tells the two apart. Dropping them would make
    // the union's halves unrecoverable, which is the opposite of what the
    // union is for.
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
}
