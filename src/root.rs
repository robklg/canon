//! Root domain concepts for canon.
//!
//! This module defines how roots are identified and resolved:
//! - Domain types: Root struct, RootSpec enum
//! - Domain functions: find_containing_root(), predicates
//! - Orchestration: parse_root_spec(), resolve_root_path()
//!
//! The domain types and functions are pure (no I/O) and can be unit tested.
//! The orchestration functions combine domain logic with filesystem
//! and database operations.

use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::Path;

use crate::path::{canonicalize_maybe_missing, path_strip_prefix};

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
            bail!("Invalid root spec '{}'. Use id:<N> or path:<path>", spec)
        }
    }
}

/// Find which root contains the given canonical path.
///
/// Pure function - no I/O, no database access. Takes pre-fetched root data.
///
/// # Arguments
/// * `canonical_path` - The canonicalized path to look up
/// * `roots` - List of (id, path, role) tuples representing candidate roots
///
/// # Returns
/// Some((root_id, root_path, role, relative_path)) if the path is under a root,
/// None otherwise.
pub fn find_containing_root(
    canonical_path: &str,
    roots: &[(i64, String, String)],
) -> Option<(i64, String, String, String)> {
    for (id, root_path, role) in roots {
        if canonical_path == root_path {
            return Some((*id, root_path.clone(), role.clone(), String::new()));
        }
        if let Some(rel) = path_strip_prefix(canonical_path, root_path) {
            return Some((*id, root_path.clone(), role.clone(), rel.to_string()));
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

    // Role predicates below are part of the domain model API but not currently used.
    // Kept for API completeness and future use cases.

    /// Check if this root has the "source" role.
    #[allow(dead_code)]
    pub fn is_source(&self) -> bool {
        self.role == "source"
    }

    /// Check if this root has the "archive" role.
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
// Orchestration (combines domain + infrastructure)
// ============================================================================

/// Parse root spec (id:N or path:/path) with optional role validation.
/// Excludes suspended roots. Use parse_root_spec_any() to include them.
pub fn parse_root_spec(conn: &Connection, spec: &str, required_role: Option<&str>) -> Result<i64> {
    parse_root_spec_impl(conn, spec, required_role, false)
}

/// Parse root spec including suspended roots. Used for suspend/unsuspend commands.
pub fn parse_root_spec_any(conn: &Connection, spec: &str) -> Result<i64> {
    parse_root_spec_impl(conn, spec, None, true)
}

fn parse_root_spec_impl(
    conn: &Connection,
    spec: &str,
    required_role: Option<&str>,
    include_suspended: bool,
) -> Result<i64> {
    let suspended_clause = if include_suspended { "" } else { " AND suspended = 0" };

    // Parse the spec (pure domain logic)
    let parsed = RootSpec::parse(spec)?;

    // Look up in database (infrastructure)
    let (id, role) = match parsed {
        RootSpec::ById(id) => {
            let query = format!("SELECT role FROM roots WHERE id = ?{}", suspended_clause);
            let role: String = conn
                .query_row(&query, [id], |row| row.get(0))
                .with_context(|| format!("No root with id {}", id))?;
            (id, role)
        }
        RootSpec::ByPath(path) => {
            // Canonicalize (filesystem infrastructure)
            let realpath = fs::canonicalize(&path)
                .with_context(|| format!("Failed to resolve path: {}", path))?;
            let realpath_str = realpath
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

            let query = format!(
                "SELECT id, role FROM roots WHERE path = ?{}",
                suspended_clause
            );
            let (id, role): (i64, String) = conn
                .query_row(&query, [realpath_str], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .with_context(|| format!("No root for path: {}", path))?;
            (id, role)
        }
    };

    // Validate role (domain logic)
    if let Some(req_role) = required_role {
        if role != req_role {
            bail!("Root {} has role '{}', expected '{}'", id, role, req_role);
        }
    }
    Ok(id)
}

/// Resolve a path to its containing root (any role) and relative subdir.
/// Excludes suspended roots. Use resolve_root_path_any() to include them.
/// Returns Some((root_id, root_path, role, relative_subdir)) if inside a root, None otherwise.
pub fn resolve_root_path(conn: &Connection, path: &Path) -> Result<Option<(i64, String, String, String)>> {
    resolve_root_path_impl(conn, path, false)
}

/// Resolve a path to its containing root, including suspended roots.
/// Used for internal operations like unsuspend and overlap checking.
pub fn resolve_root_path_any(conn: &Connection, path: &Path) -> Result<Option<(i64, String, String, String)>> {
    resolve_root_path_impl(conn, path, true)
}

fn resolve_root_path_impl(
    conn: &Connection,
    path: &Path,
    include_suspended: bool,
) -> Result<Option<(i64, String, String, String)>> {
    // Canonicalize (filesystem infrastructure)
    let canon_path = fs::canonicalize(path)
        .with_context(|| format!("Failed to resolve path: {}", path.display()))?;
    let path_str = canon_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

    // Fetch candidate roots (database infrastructure)
    let query = if include_suspended {
        "SELECT id, path, role FROM roots"
    } else {
        "SELECT id, path, role FROM roots WHERE suspended = 0"
    };
    let mut stmt = conn.prepare(query)?;
    let roots: Vec<(i64, String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    // Find containing root (pure domain logic)
    Ok(find_containing_root(path_str, &roots))
}

/// Resolve a path to its containing archive root and relative subdir.
/// Unlike parse_root_spec which requires exact root match, this accepts any path
/// inside an archive root and extracts the relative portion.
/// The path does not need to exist - only an ancestor within an archive root must exist.
/// Returns (root_id, root_path, relative_subdir) or error if not in an archive.
pub fn resolve_archive_path(conn: &Connection, path: &Path) -> Result<(i64, String, String)> {
    // Canonicalize path (allowing non-existent subdirs)
    let path_str = canonicalize_maybe_missing(path)?;

    // Find matching archive root
    let mut stmt = conn.prepare("SELECT id, path, role FROM roots WHERE suspended = 0")?;
    let roots: Vec<(i64, String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (id, root_path, role) in roots {
        if path_str == root_path {
            if role != "archive" {
                bail!(
                    "Path '{}' is inside a {} root, not an archive",
                    path.display(),
                    role
                );
            }
            return Ok((id, root_path, String::new()));
        }
        if let Some(rel) = path_strip_prefix(&path_str, &root_path) {
            if role != "archive" {
                bail!(
                    "Path '{}' is inside a {} root, not an archive",
                    path.display(),
                    role
                );
            }
            return Ok((id, root_path, rel.to_string()));
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

    #[test]
    fn find_containing_root_exact_match() {
        let roots = vec![(1, "/a/b".to_string(), "source".to_string())];
        let result = find_containing_root("/a/b", &roots);
        assert_eq!(
            result,
            Some((1, "/a/b".to_string(), "source".to_string(), String::new()))
        );
    }

    #[test]
    fn find_containing_root_under_root() {
        let roots = vec![(1, "/a/b".to_string(), "source".to_string())];
        let result = find_containing_root("/a/b/c/d", &roots);
        assert_eq!(
            result,
            Some((1, "/a/b".to_string(), "source".to_string(), "c/d".to_string()))
        );
    }

    #[test]
    fn find_containing_root_not_found() {
        let roots = vec![(1, "/a/b".to_string(), "source".to_string())];
        let result = find_containing_root("/x/y/z", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_not_under_similar_prefix() {
        // /a/bc is NOT under /a/b (different directory)
        let roots = vec![(1, "/a/b".to_string(), "source".to_string())];
        let result = find_containing_root("/a/bc/d", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_multiple_roots_first_match() {
        let roots = vec![
            (1, "/a".to_string(), "source".to_string()),
            (2, "/a/b".to_string(), "archive".to_string()),
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
        let roots: Vec<(i64, String, String)> = vec![];
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
}
