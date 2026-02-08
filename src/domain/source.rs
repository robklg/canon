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
//! use canon::source::Source;
//! use canon::scope::ScopeMatch;
//!
//! // Filter sources using domain predicates
//! sources.iter()
//!     .filter(|s| s.is_active())
//!     .filter(|s| s.is_from_role("source"))
//!     .filter(|s| s.matches_scope(&scopes))
//!     .filter(|s| !s.is_excluded())
//! ```

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
/// use canon::domain::source::NewSource;
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
/// let created = repo::source::insert_destination(conn, &new_source)?;
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
    /// - The source's path is under an UnderDirectory scope
    ///
    /// ## Edge Case: Similar Prefixes
    ///
    /// `/a/bc` is NOT under `/a/b`. We check for a path separator after
    /// the directory prefix to avoid false matches on similar prefixes.
    pub fn matches_scope(&self, scopes: &[ScopeMatch]) -> bool {
        if scopes.is_empty() {
            return true;
        }

        let full_path = self.path();
        scopes.iter().any(|scope| match scope {
            ScopeMatch::ExactFile(path) => full_path == *path,
            ScopeMatch::UnderDirectory(dir) => {
                // Match if:
                // 1. Path equals the directory exactly (the dir itself is a file)
                // 2. Path starts with dir AND has a '/' immediately after
                full_path == *dir
                    || (full_path.starts_with(dir)
                        && full_path.as_bytes().get(dir.len()) == Some(&b'/'))
            }
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
