//! Source domain model for canon.
//!
//! This module defines the `Source` struct and its predicates — the authoritative
//! definition of what a source is and how to reason about it.
//!
//! ## Design Principles
//!
//! 1. **Single source of truth**: This struct is THE definition of a source for read operations
//! 2. **Pure predicates**: All methods are pure functions with no I/O or side effects
//! 3. **Testable in isolation**: Domain logic can be unit tested without database
//! 4. **Dependencies point inward**: Commands depend on this module, not vice versa
//!
//! ## Usage
//!
//! ```ignore
//! use crate::core::domain::source::Source;
//! use crate::core::domain::scope::ScopeMatch;
//!
//! // Filter sources using domain predicates
//! sources.iter()
//!     .filter(|s| s.is_active())
//!     .filter(|s| s.is_from_role("source"))
//!     .filter(|s| s.matches_scope(&scopes))
//!     .filter(|s| !s.is_excluded())
//! ```

use super::path::path_is_under;
use super::scope::ScopeMatch;

/// Input data for inserting a new source (destination) record.
///
/// This struct represents the data needed to register a file that has been
/// copied/moved to an archive. It contains only the fields that the caller
/// provides; database-generated fields (id, timestamps) are handled by the
/// repo layer.
///
/// ## Usage
///
/// ```ignore
/// use crate::core::domain::source::NewSource;
///
/// let new_source = NewSource {
///     root_id: archive_root_id,
///     rel_path: "2024/photo.jpg".to_string(),
///     size: metadata.len() as i64,
///     mtime: metadata.mtime(),
///     partial_hash: computed_hash,
///     object_id: Some(obj_id),
///     device: Some(metadata.dev()),
///     inode: Some(metadata.ino()),
/// };
///
/// let created = insert_destination(conn, &new_source)?;
/// ```
#[derive(Debug, Clone)]
pub struct NewSource {
    /// ID of the root this source belongs to
    pub root_id: i64,
    /// Path relative to root
    pub rel_path: String,
    /// File size in bytes
    pub size: i64,
    /// Modification time (Unix timestamp)
    pub mtime: i64,
    /// Partial hash for integrity validation
    pub partial_hash: String,
    /// ID of the content object (should always be Some for destinations)
    pub object_id: Option<i64>,
    /// Device ID (Unix only, for move detection)
    pub device: Option<i64>,
    /// Inode number (Unix only, for move detection)
    pub inode: Option<i64>,
    /// Decision that caused the most recent state transition (None when recording is off)
    pub decision_id: Option<i64>,
}

/// Core source data — sufficient for most read operations.
///
/// This struct contains all fields needed for source queries, including
/// denormalized root data (root_path, root_role, root_suspended) to enable
/// pure predicate functions without database access.
#[derive(Debug, Clone)]
pub struct Source {
    /// Database ID
    pub id: i64,
    /// ID of the root this source belongs to
    pub root_id: i64,
    /// Absolute path of the root (denormalized from roots table)
    pub root_path: String,
    /// Path relative to root
    pub rel_path: String,
    /// ID of the content object (None if not yet hashed)
    pub object_id: Option<i64>,
    /// File size in bytes
    pub size: i64,
    /// Modification time (Unix timestamp)
    pub mtime: i64,
    /// Source-level exclusion flag
    pub excluded: bool,
    /// Object-level exclusion flag (None if no object_id)
    pub object_excluded: Option<bool>,
    /// Device ID (for move detection).
    /// Passed through to worklist JSONL for external tool consumption.
    #[allow(dead_code)]
    pub device: i64,
    /// Inode number (for move detection).
    /// Passed through to worklist JSONL for external tool consumption.
    #[allow(dead_code)]
    pub inode: i64,
    /// Partial hash for integrity validation.
    /// Passed through to worklist JSONL for external tool consumption.
    #[allow(dead_code)]
    pub partial_hash: String,
    /// Basis revision for staleness detection (increments when file changes)
    pub basis_rev: i64,
    /// Role of the root: "source" or "archive" (denormalized)
    pub root_role: String,
    /// Whether the root is suspended (denormalized)
    pub root_suspended: bool,
    /// Decision that caused the most recent state transition
    pub decision_id: Option<i64>,
}

impl Source {
    /// Compute the full absolute path of this source.
    ///
    /// This is THE canonical way to get a source's path. Combines root_path
    /// and rel_path correctly, handling the edge case of empty rel_path.
    pub fn path(&self) -> String {
        if self.rel_path.is_empty() {
            self.root_path.clone()
        } else {
            format!("{}/{}", self.root_path, self.rel_path)
        }
    }

    /// Check if this source matches any of the given scopes.
    ///
    /// Returns true if:
    /// - No scopes are provided (empty slice means "match all")
    /// - The source's path exactly matches an ExactFile scope
    /// - The source's path is at or under an UnderDirectory scope
    ///
    /// ## The directory boundary is not decided here
    ///
    /// `/a/bc` is not under `/a/b`, and this asks
    /// [`path_is_under`](super::path::path_is_under) for that answer rather
    /// than working it out again. Containment has one owner; a second
    /// spelling of it here would be correct on the day it was written and
    /// would drift the day the owner was repaired. This site used to carry
    /// one — a byte prefix plus a probe at the prefix's length — and the two
    /// had already diverged on a directory named with a trailing slash.
    pub fn matches_scope(&self, scopes: &[ScopeMatch]) -> bool {
        if scopes.is_empty() {
            return true;
        }

        let full_path = self.path();
        scopes.iter().any(|scope| match scope {
            ScopeMatch::ExactFile(path) => full_path == *path,
            ScopeMatch::UnderDirectory(dir) => path_is_under(&full_path, dir),
        })
    }

    /// Check if this source is excluded.
    ///
    /// A source is excluded if:
    /// - The source itself is marked excluded, OR
    /// - The source's object is marked excluded
    ///
    /// This two-level check is critical for correctness. Object-level
    /// exclusion affects all sources with that content.
    pub fn is_excluded(&self) -> bool {
        self.excluded || self.object_excluded.unwrap_or(false)
    }

    /// An empty source is all shape, no content — **contentless**: content
    /// identity has nothing to identify, so it never participates in
    /// coverage, overlap, uniqueness, or archive-conflict claims; it is
    /// carried with its place and resolves with the place's fate. This is
    /// the contentless law's one vocabulary point — the only place
    /// `size == 0` is written; every other site consumes this predicate or a
    /// repo query documented as its SQL projection.
    pub fn is_contentless(&self) -> bool {
        self.size == 0
    }

    /// Check if this source is from a root with the given role.
    ///
    /// Common roles are "source" and "archive".
    pub fn is_from_role(&self, role: &str) -> bool {
        self.root_role == role
    }

    /// Check if this source's root is active (not suspended).
    ///
    /// Suspended roots are hidden from most operations.
    pub fn is_active(&self) -> bool {
        !self.root_suspended
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a Source with minimal required fields for testing.
    /// Uses sensible defaults for fields not under test.
    fn make_source(root_path: &str, rel_path: &str) -> Source {
        Source {
            id: 1,
            root_id: 1,
            root_path: root_path.to_string(),
            rel_path: rel_path.to_string(),
            object_id: None,
            size: 0,
            mtime: 0,
            excluded: false,
            object_excluded: None,
            device: 0,
            inode: 0,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    // =========================================================================
    // path() tests
    // =========================================================================

    #[test]
    fn path_combines_root_and_rel() {
        let s = make_source("/home/user/photos", "2024/january/photo.jpg");
        assert_eq!(s.path(), "/home/user/photos/2024/january/photo.jpg");
    }

    #[test]
    fn path_handles_empty_rel_path() {
        // Edge case: rel_path is empty (source is the root itself)
        let s = make_source("/home/user/photos", "");
        assert_eq!(s.path(), "/home/user/photos");
    }

    #[test]
    fn path_handles_single_segment_rel() {
        let s = make_source("/root", "file.txt");
        assert_eq!(s.path(), "/root/file.txt");
    }

    // =========================================================================
    // matches_scope() tests — defining what "under a directory" means
    // =========================================================================

    #[test]
    fn matches_scope_empty_scopes_matches_everything() {
        // Empty scopes = no filter = match all
        let s = make_source("/any/path", "any/file.txt");
        assert!(s.matches_scope(&[]));
    }

    #[test]
    fn matches_scope_exact_file_match() {
        let s = make_source("/home/user", "photos/photo.jpg");
        let scopes = vec![ScopeMatch::ExactFile(
            "/home/user/photos/photo.jpg".to_string(),
        )];
        assert!(s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_exact_file_no_match() {
        let s = make_source("/home/user", "photos/other.jpg");
        let scopes = vec![ScopeMatch::ExactFile(
            "/home/user/photos/photo.jpg".to_string(),
        )];
        assert!(!s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_under_directory() {
        let s = make_source("/home/user", "photos/2024/photo.jpg");
        let scopes = vec![ScopeMatch::UnderDirectory("/home/user/photos".to_string())];
        assert!(s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_directory_itself_matches() {
        // A file that IS the directory path should match
        // (handles case where a file has same name as would-be directory)
        let s = make_source("/home/user", "photos");
        let scopes = vec![ScopeMatch::UnderDirectory("/home/user/photos".to_string())];
        assert!(s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_not_under_similar_prefix() {
        // CRITICAL: /a/bc is NOT under /a/b
        // This is the edge case that broke scope matching before
        let s = make_source("/a", "bc");
        let scopes = vec![ScopeMatch::UnderDirectory("/a/b".to_string())];
        assert!(!s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_not_under_similar_prefix_deeper() {
        // Another variant: /home/user/photos-backup is NOT under /home/user/photos
        let s = make_source("/home/user", "photos-backup/file.jpg");
        let scopes = vec![ScopeMatch::UnderDirectory("/home/user/photos".to_string())];
        assert!(!s.matches_scope(&scopes));
    }

    /// The directory arm of `matches_scope` **is** the path law, not a second
    /// spelling that happens to agree with it.
    ///
    /// Each row is written as the law's own answer, taken from
    /// `path_is_under`, and then asserted of `matches_scope` — so a future
    /// repair to the owner moves both sides together, which is the whole
    /// point of there being one owner. The trailing-slash row is the one the
    /// old hand-spelled probe got wrong: it took a byte prefix and probed the
    /// character at the prefix's length, so a directory written `/a/b/`
    /// matched nothing at all — not the directory, and not one descendant,
    /// because the byte sitting at that offset is the descendant's first
    /// character rather than a separator. The same directory written `/a/b`
    /// matched descendants fine. Two spellings of one directory, two answers.
    #[test]
    fn matches_scope_under_a_directory_is_the_path_law() {
        // (root, rel_path, directory scope)
        const CASES: &[(&str, &str, &str)] = &[
            ("/a", "b/c", "/a/b"),
            ("/a", "bc", "/a/b"),
            ("/a", "b", "/a/b"),
            ("/a", "b", "/a/b/"),
            ("/a", "b/c", "/a/b/"),
            ("/home/user", "photos-backup/file.jpg", "/home/user/photos"),
        ];

        for (root, rel, dir) in CASES {
            let s = make_source(root, rel);
            let law = path_is_under(&s.path(), dir);
            let scopes = vec![ScopeMatch::UnderDirectory((*dir).to_string())];
            assert_eq!(
                s.matches_scope(&scopes),
                law,
                "matches_scope disagrees with the path law for path {} under {dir}: \
                 the law says {law}. Containment has one owner, so this arm must ask it \
                 rather than decide for itself.",
                s.path()
            );
        }
    }

    /// The one truth value this predicate's repair actually flips, asserted
    /// **absolutely** rather than by agreement with the law.
    ///
    /// The agreement pin above says "this arm is the law", which is the
    /// chartered claim — but it derives its expectation from the owner, so a
    /// later change to the owner moves both sides together and this
    /// behaviour could revert with the suite green. These are the rows a
    /// reader would have to look up otherwise, and the reachable one:
    /// `cluster refresh` carries a manifest scope naming a directory under no
    /// known root **verbatim**, trailing slash and all, so `/vol/` and `/`
    /// are both things a user can put in a file and Canon will act on.
    #[test]
    fn a_directory_spelled_with_a_trailing_slash_selects_its_subtree() {
        let s = make_source("/vol", "work/x");

        // The directory itself, and its subtree, under both spellings.
        for dir in ["/vol", "/vol/"] {
            let scopes = vec![ScopeMatch::UnderDirectory(dir.to_string())];
            assert!(
                s.matches_scope(&scopes),
                "/vol/work/x must be under {dir} — two spellings of one directory \
                 must not give two answers"
            );
        }

        // The root directory selects everything, which is what a manifest
        // scope of "/" asks for.
        let root_scope = vec![ScopeMatch::UnderDirectory("/".to_string())];
        assert!(s.matches_scope(&root_scope));

        // And the boundary still holds: a trailing slash widens nothing else.
        let sibling = make_source("/vol", "workshop/x");
        let scopes = vec![ScopeMatch::UnderDirectory("/vol/work/".to_string())];
        assert!(!sibling.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_multiple_scopes_any_match() {
        let s = make_source("/home/user", "documents/file.txt");
        let scopes = vec![
            ScopeMatch::UnderDirectory("/home/user/photos".to_string()),
            ScopeMatch::UnderDirectory("/home/user/documents".to_string()),
            ScopeMatch::ExactFile("/some/other/file.txt".to_string()),
        ];
        // Should match because of the documents directory scope
        assert!(s.matches_scope(&scopes));
    }

    #[test]
    fn matches_scope_multiple_scopes_none_match() {
        let s = make_source("/home/user", "videos/movie.mp4");
        let scopes = vec![
            ScopeMatch::UnderDirectory("/home/user/photos".to_string()),
            ScopeMatch::UnderDirectory("/home/user/documents".to_string()),
        ];
        assert!(!s.matches_scope(&scopes));
    }

    // =========================================================================
    // is_excluded() tests — defining Canon's two-level exclusion semantics
    // =========================================================================

    #[test]
    fn is_excluded_source_not_excluded() {
        let s = make_source("/root", "file.txt");
        assert!(!s.is_excluded());
    }

    #[test]
    fn is_excluded_source_level_exclusion() {
        let mut s = make_source("/root", "file.txt");
        s.excluded = true;
        assert!(s.is_excluded());
    }

    #[test]
    fn is_excluded_object_level_exclusion() {
        let mut s = make_source("/root", "file.txt");
        s.object_id = Some(42);
        s.object_excluded = Some(true);
        assert!(s.is_excluded());
    }

    #[test]
    fn is_excluded_both_levels() {
        let mut s = make_source("/root", "file.txt");
        s.excluded = true;
        s.object_id = Some(42);
        s.object_excluded = Some(true);
        assert!(s.is_excluded());
    }

    #[test]
    fn is_excluded_object_not_excluded() {
        let mut s = make_source("/root", "file.txt");
        s.object_id = Some(42);
        s.object_excluded = Some(false);
        assert!(!s.is_excluded());
    }

    #[test]
    fn is_excluded_no_object_not_excluded() {
        // Source with no object (not yet hashed) — object_excluded is None
        let s = make_source("/root", "file.txt");
        assert!(s.object_excluded.is_none());
        assert!(!s.is_excluded());
    }

    // =========================================================================
    // is_from_role() tests — defining role semantics
    // =========================================================================

    #[test]
    fn is_from_role_source() {
        let s = make_source("/root", "file.txt");
        assert!(s.is_from_role("source"));
        assert!(!s.is_from_role("archive"));
    }

    #[test]
    fn is_from_role_archive() {
        let mut s = make_source("/root", "file.txt");
        s.root_role = "archive".to_string();
        assert!(s.is_from_role("archive"));
        assert!(!s.is_from_role("source"));
    }

    // =========================================================================
    // is_active() tests — defining suspension semantics
    // =========================================================================

    #[test]
    fn is_active_when_not_suspended() {
        let s = make_source("/root", "file.txt");
        assert!(s.is_active());
    }

    #[test]
    fn is_active_when_suspended() {
        let mut s = make_source("/root", "file.txt");
        s.root_suspended = true;
        assert!(!s.is_active());
    }
}
