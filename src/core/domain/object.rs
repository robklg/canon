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

use super::source::Source;

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

/// The present sources holding some content, counted: by root role, and how many of them sit on
/// suspended roots. Empty files are counted apart and never as holders — every empty file shares
/// the one empty content, so counting them as its holders would claim they hold the same thing
/// (the contentless law).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Holders {
    pub in_source_roots: usize,
    pub in_archives: usize,
    /// Of the two above, how many are on suspended roots.
    pub on_suspended_roots: usize,
    /// Empty files among the sources given, never counted above.
    pub empty_files: usize,
}

impl Holders {
    /// Counts what it is given: callers pass present sources
    /// (`core::repo::source::fetch_sources_by_object_ids` reads only those).
    pub fn of<'a>(sources: impl IntoIterator<Item = &'a Source>) -> Holders {
        let mut holders = Holders::default();
        for source in sources {
            if source.is_contentless() {
                holders.empty_files += 1;
                continue;
            }
            if source.is_from_role("archive") {
                holders.in_archives += 1;
            } else {
                holders.in_source_roots += 1;
            }
            if !source.is_active() {
                holders.on_suspended_roots += 1;
            }
        }
        holders
    }

    pub fn add(&mut self, other: Holders) {
        self.in_source_roots += other.in_source_roots;
        self.in_archives += other.in_archives;
        self.on_suspended_roots += other.on_suspended_roots;
        self.empty_files += other.empty_files;
    }

    /// The holders of content, both roles: empty files are not among them.
    pub fn total(&self) -> usize {
        self.in_source_roots + self.in_archives
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A present source on a root of `role`, suspended or not, of `size` bytes.
    fn holder(role: &str, suspended: bool, size: i64) -> Source {
        Source {
            id: 1,
            root_id: 1,
            root_path: "/r".to_string(),
            rel_path: "f".to_string(),
            object_id: Some(1),
            size,
            mtime: 0,
            excluded: false,
            object_excluded: Some(false),
            device: 0,
            inode: 0,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: role.to_string(),
            root_suspended: suspended,
            decision_id: None,
        }
    }

    /// **Holders count by role and door**: each role counted once per source, the suspended
    /// ones a share of those two, never a third role.
    #[test]
    fn holders_count_by_role_and_door() {
        let sources = [
            holder("source", false, 10),
            holder("source", true, 10),
            holder("archive", false, 10),
            holder("archive", true, 10),
            holder("archive", false, 10),
        ];
        let holders = Holders::of(&sources);
        assert_eq!(
            holders,
            Holders {
                in_source_roots: 2,
                in_archives: 3,
                on_suspended_roots: 2,
                empty_files: 0,
            }
        );
        assert_eq!(holders.total(), 5);

        let mut both = holders;
        both.add(Holders::of(&sources[..1]));
        assert_eq!((both.in_source_roots, both.total()), (3, 6));
    }

    /// **Empty files are set apart, never holders** — on either role, open or suspended.
    #[test]
    fn holders_set_empty_files_apart() {
        let sources = [
            holder("source", false, 0),
            holder("archive", true, 0),
            holder("source", false, 10),
        ];
        let holders = Holders::of(&sources);
        assert_eq!(
            holders,
            Holders {
                in_source_roots: 1,
                in_archives: 0,
                on_suspended_roots: 0,
                empty_files: 2,
            }
        );
        assert_eq!(holders.total(), 1);
    }

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
