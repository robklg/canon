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
//! use crate::core::repo;
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
    pub fn is_excluded(&self) -> bool {
        self.excluded
    }
}

/// One archive-side copy of an object, with the standing of the root it
/// stands on.
///
/// The archived-ness projections return these rather than bare ids or bare
/// paths so that **the standing travels beside the evidence**. A copy on a
/// suspended root still counts — testifying is what a closed door permits —
/// but a surface citing it owes the reader the fact that the place is parked,
/// and a surface deriving a *claim* from it (coverage, the readiness review)
/// owes the reader how much of the claim rests there. Neither is answerable
/// from an id alone, and asking the roots table a second time is the
/// re-detection the closed door's own rule forbids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveCopy {
    pub root_id: i64,
    /// The root's door: `true` when the user has closed it.
    pub root_suspended: bool,
}

/// One archive-side copy located: where the content stands, and on what.
///
/// `path` is the copy's absolute path, composed the way every archive-path
/// answer has always composed it. `root` carries the same standing
/// [`ArchiveCopy`] does, so a caller that only wants paths maps over the
/// field and a caller that marks its citations reads the flag beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveLocation {
    pub path: String,
    pub root: ArchiveCopy,
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
