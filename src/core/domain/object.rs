//! Object domain model for canon.
//!
//! This module defines the `Object` struct — a typed representation of content
//! identified by hash. Objects are created when sources are hashed during scanning.
//! Multiple sources can reference the same object (same content at different paths).
//!
//! ## Design Principles
//!
//! 1. **Single source of truth**: This struct is THE definition of an object
//! 2. **Pure types**: No I/O or side effects in this module
//! 3. **Dependencies point inward**: Commands depend on this module, not vice versa
//!
//! ## Usage
//!
//! ```ignore
//! use crate::core::domain::object::Object;
//! use crate::repo;
//!
//! // Fetch objects by ID
//! let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
//!
//! // Check exclusion status
//! for (id, obj) in &objects {
//!     if obj.is_excluded() {
//!         println!("Object {} is excluded", id);
//!     }
//! }
//! ```

/// Content identified by hash.
///
/// Objects represent unique content in canon. Multiple sources can reference
/// the same object (same content at different paths). Objects are created
/// when sources are hashed during scanning.
///
/// The `excluded` field supports content-based exclusion — when an object is
/// excluded, ALL sources linked to it are considered excluded. This is useful
/// for marking known bad files (e.g., corrupted, unwanted) by content rather
/// than by path.
#[derive(Debug, Clone)]
pub struct Object {
    /// Database ID
    pub id: i64,
    /// Hash algorithm (e.g., "sha256")
    pub hash_type: String,
    /// The content hash
    pub hash_value: String,
    /// Whether this object (and all its sources) is excluded
    pub excluded: bool,
}

impl Object {
    /// Check if this object is excluded.
    ///
    /// Object-level exclusion excludes ALL sources linked to this object.
    /// This is used for content-based exclusion (e.g., known bad files).
    ///
    /// Note: Source-level exclusion is checked via `Source.is_excluded()`,
    /// which considers BOTH source-level and object-level exclusion.
    /// This method is for direct object operations where you have an Object
    /// struct rather than going through Source.
    // Part of the domain model API but not currently used. Kept for API completeness.
    #[allow(dead_code)]
    pub fn is_excluded(&self) -> bool {
        self.excluded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test object with sensible defaults.
    fn make_object(id: i64, excluded: bool) -> Object {
        Object {
            id,
            hash_type: "sha256".to_string(),
            hash_value: format!("abc123def456_{id}"),
            excluded,
        }
    }

    // =========================================================================
    // Object struct tests
    // =========================================================================

    #[test]
    fn object_fields_populated() {
        let obj = Object {
            id: 42,
            hash_type: "sha256".to_string(),
            hash_value: "deadbeef".to_string(),
            excluded: false,
        };

        assert_eq!(obj.id, 42);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "deadbeef");
        assert!(!obj.excluded);
    }

    #[test]
    fn object_clone_creates_copy() {
        let original = make_object(1, true);
        let cloned = original.clone();

        assert_eq!(cloned.id, original.id);
        assert_eq!(cloned.hash_type, original.hash_type);
        assert_eq!(cloned.hash_value, original.hash_value);
        assert_eq!(cloned.excluded, original.excluded);
    }

    // =========================================================================
    // is_excluded predicate tests
    // =========================================================================

    #[test]
    fn object_is_excluded_true() {
        let obj = make_object(1, true);
        assert!(obj.is_excluded());
    }

    #[test]
    fn object_is_excluded_false() {
        let obj = make_object(1, false);
        assert!(!obj.is_excluded());
    }
}
