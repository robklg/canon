//! Root domain concepts for canon.
//!
//! This module defines how roots are identified and resolved:
//! - Domain types: Root struct, RootSpec enum
//! - Pure domain functions: find_containing_root(), predicates, resolve_*
//!
//! All functions in this module are pure (no database I/O). Functions that
//! need filesystem access (path canonicalization) are clearly documented.
//! Callers must fetch roots via `repo::root::fetch_all()` before calling
//! resolution functions.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

use super::path::{canonicalize_maybe_missing, path_strip_prefix};

// ============================================================================
// Domain Concepts (pure, no I/O)
// ============================================================================

/// Parsed root specification - how a user identifies a root.
///
/// This enum represents the domain concept of "how do users specify a root?"
/// independent of how roots are stored or how paths are canonicalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootSpec {
    /// Root identified by database ID
    ById(i64),
    /// Root identified by path (not yet canonicalized)
    ByPath(String),
}

impl RootSpec {
    /// Parse a root spec string ("id:N" or "path:/foo").
    ///
    /// This is a pure parsing function - it does NOT canonicalize paths.
    /// The caller must canonicalize ByPath variants before database lookup.
    pub fn parse(spec: &str) -> Result<Self> {
        if let Some(id_str) = spec.strip_prefix("id:") {
            let id: i64 = id_str.parse().context("Invalid root ID")?;
            Ok(RootSpec::ById(id))
        } else if let Some(path) = spec.strip_prefix("path:") {
            Ok(RootSpec::ByPath(path.to_string()))
        } else {
            bail!("Invalid root spec '{spec}'. Use id:<N> or path:<path>")
        }
    }
}

/// Find which root contains the given canonical path.
///
/// Pure function - no I/O, no database access. Takes pre-fetched Root objects.
///
/// # Arguments
/// * `canonical_path` - The canonicalized path to look up
/// * `roots` - Slice of Root domain objects (typically from `repo::root::fetch_all()`)
///
/// # Returns
/// Some((root_id, root_path, role, relative_path)) if the path is under a root,
/// None otherwise.
pub fn find_containing_root(
    canonical_path: &str,
    roots: &[Root],
) -> Option<(i64, String, String, String)> {
    for root in roots {
        if canonical_path == root.path {
            return Some((root.id, root.path.clone(), root.role.clone(), String::new()));
        }
        if let Some(rel) = path_strip_prefix(canonical_path, &root.path) {
            return Some((
                root.id,
                root.path.clone(),
                root.role.clone(),
                rel.to_string(),
            ));
        }
    }
    None
}

/// A root directory registered in canon.
///
/// Roots are the top-level directories that canon manages. Each root has a role
/// (source or archive) that determines how its contents are treated.
///
/// This struct represents the domain model for roots — it contains all stored
/// root data and provides pure predicates for filtering and classification.
#[derive(Debug, Clone)]
pub struct Root {
    /// Database ID
    pub id: i64,
    /// Canonical absolute path
    pub path: String,
    /// Role: "source" or "archive"
    pub role: String,
    /// Optional user comment
    pub comment: Option<String>,
    /// Unix timestamp of last scan (None if never scanned)
    pub last_scanned_at: Option<i64>,
    /// Whether this root is suspended (hidden from most operations)
    pub suspended: bool,
}

impl Root {
    /// Check if this root is suspended.
    pub fn is_suspended(&self) -> bool {
        self.suspended
    }

    /// Check if this root is active (not suspended).
    pub fn is_active(&self) -> bool {
        !self.suspended
    }

    /// Check if this root has the "source" role.
    pub fn is_source(&self) -> bool {
        self.role == "source"
    }

    /// Check if this root has the "archive" role.
    // Part of the domain model API but not currently used. Kept for API completeness.
    #[allow(dead_code)]
    pub fn is_archive(&self) -> bool {
        self.role == "archive"
    }

    /// Check if this root matches a scope path.
    ///
    /// Matching is bidirectional:
    /// - Root is at or under the scope (root path starts with scope)
    /// - Scope is at or under the root (scope starts with root path)
    ///
    /// This is used by `canon roots <scope>` to find related roots.
    pub fn matches_scope(&self, scope: &str) -> bool {
        self.path.starts_with(scope) || scope.starts_with(&self.path)
    }
}

// ============================================================================
// Root Resolution Functions (pure domain + filesystem only, no database)
// ============================================================================

/// Parse root spec (id:N or path:/path) with optional role validation.
/// Excludes suspended roots. Use parse_root_spec_any() to include them.
///
/// Callers must fetch roots via `repo::root::fetch_all()` first.
pub fn parse_root_spec(roots: &[Root], spec: &str, required_role: Option<&str>) -> Result<i64> {
    parse_root_spec_impl(roots, spec, required_role, false)
}

/// Parse root spec including suspended roots. Used for suspend/unsuspend commands.
///
/// Callers must fetch roots via `repo::root::fetch_all()` first.
pub fn parse_root_spec_any(roots: &[Root], spec: &str) -> Result<i64> {
    parse_root_spec_impl(roots, spec, None, true)
}

fn parse_root_spec_impl(
    roots: &[Root],
    spec: &str,
    required_role: Option<&str>,
    include_suspended: bool,
) -> Result<i64> {
    // Parse the spec (pure domain logic)
    let parsed = RootSpec::parse(spec)?;

    // Filter roots by suspension status
    let candidates: Vec<&Root> = roots
        .iter()
        .filter(|r| include_suspended || r.is_active())
        .collect();

    // Find matching root
    let (id, role) = match parsed {
        RootSpec::ById(id) => {
            let root = candidates
                .iter()
                .find(|r| r.id == id)
                .ok_or_else(|| anyhow::anyhow!("No root with id {id}"))?;
            (root.id, root.role.clone())
        }
        RootSpec::ByPath(path) => {
            // Canonicalize (filesystem I/O - only infrastructure in this function)
            let realpath = fs::canonicalize(&path)
                .with_context(|| format!("Failed to resolve path: {path}"))?;
            let realpath_str = realpath
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

            let root = candidates
                .iter()
                .find(|r| r.path == realpath_str)
                .ok_or_else(|| anyhow::anyhow!("No root for path: {path}"))?;
            (root.id, root.role.clone())
        }
    };

    // Validate role (domain logic)
    if let Some(req_role) = required_role {
        if role != req_role {
            bail!("Root {id} has role '{role}', expected '{req_role}'");
        }
    }
    Ok(id)
}

/// Resolve a path to its containing root (any role) and relative subdir.
/// Excludes suspended roots. Use resolve_root_path_any() to include them.
///
/// Callers must fetch roots via `repo::root::fetch_all()` first.
///
/// Returns Some((root_id, root_path, role, relative_subdir)) if inside a root, None otherwise.
pub fn resolve_root_path(
    roots: &[Root],
    path: &Path,
) -> Result<Option<(i64, String, String, String)>> {
    resolve_root_path_impl(roots, path, false)
}

/// Resolve a path to its containing root, including suspended roots.
/// Used for internal operations like unsuspend and overlap checking.
///
/// Callers must fetch roots via `repo::root::fetch_all()` first.
pub fn resolve_root_path_any(
    roots: &[Root],
    path: &Path,
) -> Result<Option<(i64, String, String, String)>> {
    resolve_root_path_impl(roots, path, true)
}

fn resolve_root_path_impl(
    roots: &[Root],
    path: &Path,
    include_suspended: bool,
) -> Result<Option<(i64, String, String, String)>> {
    // Canonicalize (filesystem I/O - only infrastructure in this function)
    let canon_path = fs::canonicalize(path)
        .with_context(|| format!("Failed to resolve path: {}", path.display()))?;
    let path_str = canon_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

    // Filter roots by suspension status
    let candidates: Vec<Root> = roots
        .iter()
        .filter(|r| include_suspended || r.is_active())
        .cloned()
        .collect();

    // Find containing root (pure domain logic)
    Ok(find_containing_root(path_str, &candidates))
}

/// Resolve a path to its containing archive root and relative subdir.
/// Unlike parse_root_spec which requires exact root match, this accepts any path
/// inside an archive root and extracts the relative portion.
/// The path does not need to exist - only an ancestor within an archive root must exist.
///
/// Callers must fetch roots via `repo::root::fetch_all()` first.
///
/// Returns (root_id, root_path, relative_subdir) or error if not in an archive.
pub fn resolve_archive_path(roots: &[Root], path: &Path) -> Result<(i64, String, String)> {
    // Canonicalize path (allowing non-existent subdirs) - filesystem I/O
    let path_str = canonicalize_maybe_missing(path)?;

    // Filter to active roots only
    let candidates: Vec<&Root> = roots.iter().filter(|r| r.is_active()).collect();

    for root in candidates {
        if path_str == root.path {
            if !root.is_archive() {
                bail!(
                    "Path '{}' is inside a {} root, not an archive",
                    path.display(),
                    root.role
                );
            }
            return Ok((root.id, root.path.clone(), String::new()));
        }
        if let Some(rel) = path_strip_prefix(&path_str, &root.path) {
            if !root.is_archive() {
                bail!(
                    "Path '{}' is inside a {} root, not an archive",
                    path.display(),
                    root.role
                );
            }
            return Ok((root.id, root.path.clone(), rel.to_string()));
        }
    }

    bail!(
        "Path '{}' is not inside any registered archive root",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // RootSpec::parse() tests
    // ========================================================================

    #[test]
    fn parse_root_spec_by_id() {
        assert_eq!(RootSpec::parse("id:123").unwrap(), RootSpec::ById(123));
    }

    #[test]
    fn parse_root_spec_by_id_zero() {
        assert_eq!(RootSpec::parse("id:0").unwrap(), RootSpec::ById(0));
    }

    #[test]
    fn parse_root_spec_by_path() {
        assert_eq!(
            RootSpec::parse("path:/foo/bar").unwrap(),
            RootSpec::ByPath("/foo/bar".to_string())
        );
    }

    #[test]
    fn parse_root_spec_by_path_relative() {
        // Relative paths are accepted (canonicalization is caller's job)
        assert_eq!(
            RootSpec::parse("path:./relative").unwrap(),
            RootSpec::ByPath("./relative".to_string())
        );
    }

    #[test]
    fn parse_root_spec_invalid_id() {
        assert!(RootSpec::parse("id:abc").is_err());
    }

    #[test]
    fn parse_root_spec_invalid_format() {
        assert!(RootSpec::parse("garbage").is_err());
        assert!(RootSpec::parse("").is_err());
    }

    // ========================================================================
    // find_containing_root() tests
    // ========================================================================

    /// Helper to create a Root with a specific id, path, and role.
    fn make_root_with(id: i64, path: &str, role: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: role.to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn find_containing_root_exact_match() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/b", &roots);
        assert_eq!(
            result,
            Some((1, "/a/b".to_string(), "source".to_string(), String::new()))
        );
    }

    #[test]
    fn find_containing_root_under_root() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/b/c/d", &roots);
        assert_eq!(
            result,
            Some((
                1,
                "/a/b".to_string(),
                "source".to_string(),
                "c/d".to_string()
            ))
        );
    }

    #[test]
    fn find_containing_root_not_found() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/x/y/z", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_not_under_similar_prefix() {
        // /a/bc is NOT under /a/b (different directory)
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/bc/d", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_multiple_roots_first_match() {
        let roots = vec![
            make_root_with(1, "/a", "source"),
            make_root_with(2, "/a/b", "archive"),
        ];
        // First matching root wins
        let result = find_containing_root("/a/b/c", &roots);
        assert_eq!(
            result,
            Some((1, "/a".to_string(), "source".to_string(), "b/c".to_string()))
        );
    }

    #[test]
    fn find_containing_root_empty_roots() {
        let roots: Vec<Root> = vec![];
        let result = find_containing_root("/a/b", &roots);
        assert_eq!(result, None);
    }

    // ========================================================================
    // Root struct and predicates tests
    // ========================================================================

    /// Helper to create a Root with sensible defaults for testing.
    fn make_root() -> Root {
        Root {
            id: 1,
            path: "/test/path".to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn is_suspended_true() {
        let root = Root {
            suspended: true,
            ..make_root()
        };
        assert!(root.is_suspended());
    }

    #[test]
    fn is_suspended_false() {
        let root = Root {
            suspended: false,
            ..make_root()
        };
        assert!(!root.is_suspended());
    }

    #[test]
    fn is_active_when_not_suspended() {
        let root = Root {
            suspended: false,
            ..make_root()
        };
        assert!(root.is_active());
    }

    #[test]
    fn is_active_when_suspended() {
        let root = Root {
            suspended: true,
            ..make_root()
        };
        assert!(!root.is_active());
    }

    #[test]
    fn is_source_true() {
        let root = Root {
            role: "source".to_string(),
            ..make_root()
        };
        assert!(root.is_source());
        assert!(!root.is_archive());
    }

    #[test]
    fn is_source_false() {
        let root = Root {
            role: "archive".to_string(),
            ..make_root()
        };
        assert!(!root.is_source());
    }

    #[test]
    fn is_archive_true() {
        let root = Root {
            role: "archive".to_string(),
            ..make_root()
        };
        assert!(root.is_archive());
        assert!(!root.is_source());
    }

    #[test]
    fn is_archive_false() {
        let root = Root {
            role: "source".to_string(),
            ..make_root()
        };
        assert!(!root.is_archive());
    }

    #[test]
    fn matches_scope_root_under_scope() {
        // Root /a/b/c is under scope /a/b
        let root = Root {
            path: "/a/b/c".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b"));
    }

    #[test]
    fn matches_scope_scope_under_root() {
        // Scope /a/b/c/d is under root /a/b
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b/c/d"));
    }

    #[test]
    fn matches_scope_exact_match() {
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b"));
    }

    #[test]
    fn matches_scope_no_match() {
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(!root.matches_scope("/x/y"));
    }

    #[test]
    fn matches_scope_similar_prefix_no_match() {
        // /a/bc is not under /a/b (different directory, not a child)
        // But with starts_with, "/a/bc".starts_with("/a/b") is true!
        // This is the current behavior in roots.rs - it uses starts_with.
        // Note: This differs from find_containing_root which uses path_strip_prefix.
        let root = Root {
            path: "/a/bc".to_string(),
            ..make_root()
        };
        // Current behavior: starts_with matches similar prefixes
        // This matches how roots.rs:list() currently works
        assert!(root.matches_scope("/a/b"));
    }

    // ========================================================================
    // parse_root_spec() tests (with &[Root] input)
    // ========================================================================

    #[test]
    fn parse_root_spec_impl_by_id_found() {
        let roots = vec![
            make_root_with(1, "/a", "source"),
            make_root_with(2, "/b", "archive"),
        ];
        let result = parse_root_spec(&roots, "id:2", None);
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn parse_root_spec_impl_by_id_not_found() {
        let roots = vec![make_root_with(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:999", None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No root with id 999"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_accepts_source() {
        let roots = vec![make_root_with(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_rejects_archive() {
        let roots = vec![make_root_with(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'archive', expected 'source'"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_accepts_archive() {
        let roots = vec![make_root_with(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("archive"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_rejects_source() {
        let roots = vec![make_root_with(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:1", Some("archive"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'source', expected 'archive'"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_none_accepts_any() {
        let roots = vec![
            make_root_with(1, "/a", "source"),
            make_root_with(2, "/b", "archive"),
        ];
        // None means accept any role
        assert_eq!(parse_root_spec(&roots, "id:1", None).unwrap(), 1);
        assert_eq!(parse_root_spec(&roots, "id:2", None).unwrap(), 2);
    }

    #[test]
    fn parse_root_spec_impl_excludes_suspended() {
        let mut suspended_root = make_root_with(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        // parse_root_spec (not _any) should exclude suspended roots
        let result = parse_root_spec(&roots, "id:1", None);
        assert!(result.is_err());
    }

    #[test]
    fn parse_root_spec_any_includes_suspended() {
        let mut suspended_root = make_root_with(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        // parse_root_spec_any should include suspended roots
        let result = parse_root_spec_any(&roots, "id:1");
        assert_eq!(result.unwrap(), 1);
    }
}
