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
//! // Fetch facts for sources
//! let facts_by_source = fact_repo::batch_fetch_for_sources(conn, &source_ids)?;
//!
//! // Iterate over a source's facts
//! for entry in facts_by_source.get(&source_id).unwrap_or(&vec![]) {
//!     println!("{}: {:?}", entry.key, entry.value);
//! }
//! ```

// Re-export types from expr.rs for convenience.
// FactType will be used by fact_repo::count_fact_keys() in Phase 2.
#[allow(unused_imports)]
pub use crate::expr::{FactType, FactValue};

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
    /// Where this fact is stored: "source" or "object"
    pub entity_type: String,
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

    /// Check if this fact is stored on a source (vs object).
    pub fn is_source_fact(&self) -> bool {
        self.entity_type == "source"
    }

    /// Check if this fact is stored on an object (vs source).
    pub fn is_object_fact(&self) -> bool {
        self.entity_type == "object"
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
    // FactEntry predicate tests
    // =========================================================================

    #[test]
    fn is_source_fact_returns_true_for_source() {
        let entry = FactEntry::new(
            "source.policy".to_string(),
            FactValue::Text("approved".to_string()),
            "source".to_string(),
            1,
        );

        assert!(entry.is_source_fact());
        assert!(!entry.is_object_fact());
    }

    #[test]
    fn is_object_fact_returns_true_for_object() {
        let entry = FactEntry::new(
            "content.Make".to_string(),
            FactValue::Text("Nikon".to_string()),
            "object".to_string(),
            42,
        );

        assert!(entry.is_object_fact());
        assert!(!entry.is_source_fact());
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
