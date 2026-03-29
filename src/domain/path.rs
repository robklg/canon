//! Path utilities for canon.
//!
//! This module contains:
//! - Pure path manipulation functions (no I/O)
//! - Path canonicalization helpers (filesystem I/O for resolving paths)
//!
//! No database dependencies.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::root::{find_containing_root, Root};

/// Check if a path is equal to or under a directory prefix.
/// Uses Path::starts_with which correctly handles directory boundaries.
/// Example: path_is_under("/a/bc/d", "/a/b") → false
///          path_is_under("/a/b/d", "/a/b") → true
pub fn path_is_under(path: &str, prefix: &str) -> bool {
    Path::new(path).starts_with(prefix)
}

/// Strip a directory prefix from a path, returning the relative portion.
/// Uses Path::strip_prefix which correctly handles directory boundaries.
/// Returns None if path is not under prefix.
/// Example: path_strip_prefix("/a/b/c", "/a/b") → Some("c")
///          path_strip_prefix("/a/bc", "/a/b") → None
pub fn path_strip_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    Path::new(path)
        .strip_prefix(prefix)
        .ok()
        .and_then(|p| p.to_str())
}

// ============================================================================
// Soft Path Resolution (offline-capable)
// ============================================================================

/// Clean a path lexically: make absolute (relative to cwd), resolve `.` and `..`
/// without filesystem access. Does NOT resolve symlinks or normalize case.
pub fn clean_path(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut components = Vec::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Don't pop past the root
                if components
                    .last()
                    .is_some_and(|c| matches!(c, Component::Normal(_)))
                {
                    components.pop();
                }
            }
            other => components.push(other),
        }
    }
    components.iter().collect()
}

/// Resolve a single path against known roots, falling back to fs::canonicalize().
/// Use for source-querying commands. File-accessing commands (scan) use
/// fs::canonicalize directly.
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Result<String> {
    let cleaned = clean_path(path, cwd);
    let cleaned_str = cleaned.to_string_lossy();

    // Try matching against known roots first (works offline)
    if find_containing_root(&cleaned_str, roots).is_some() {
        return Ok(cleaned_str.into_owned());
    }

    // Fall back to fs::canonicalize (requires path to exist on disk)
    match fs::canonicalize(path) {
        Ok(canonical) => Ok(canonical.to_string_lossy().into_owned()),
        Err(_) => bail!(
            "Failed to resolve path: {}\n\
             Path does not match any known root and is not accessible \
             on disk (is the storage attached?)",
            path.display()
        ),
    }
}

/// Resolve multiple paths against known roots.
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root]) -> Result<Vec<String>> {
    let cwd = std::env::current_dir().context("Failed to determine current directory")?;
    paths.iter().map(|p| resolve_path(p, roots, &cwd)).collect()
}

// ============================================================================
// Path Canonicalization (Filesystem I/O)
// ============================================================================

/// Canonicalize a path that may not exist yet by finding the nearest existing
/// ancestor and appending the remaining components.
pub fn canonicalize_maybe_missing(path: &Path) -> Result<String> {
    // Try canonicalizing the full path first
    if let Ok(canon) = fs::canonicalize(path) {
        return Ok(canon.to_string_lossy().to_string());
    }

    // Walk up to find existing ancestor
    let mut existing = path.to_path_buf();
    let mut missing_parts = Vec::new();

    while !existing.exists() {
        if let Some(name) = existing.file_name() {
            missing_parts.push(name.to_os_string());
        }
        if !existing.pop() {
            bail!("Cannot resolve path: {}", path.display());
        }
    }

    // Canonicalize the existing part
    let canon_existing = fs::canonicalize(&existing)
        .with_context(|| format!("Failed to resolve path: {}", existing.display()))?;

    // Append missing parts
    let mut result = canon_existing;
    for part in missing_parts.into_iter().rev() {
        result.push(part);
    }

    Ok(result.to_string_lossy().to_string())
}

/// Format a path for display: relative when under cwd, absolute otherwise.
/// Returns "." when path equals cwd exactly.
pub fn format_path(full_path: &str, cwd: Option<&str>) -> String {
    if let Some(cwd) = cwd {
        if full_path == cwd {
            ".".to_string()
        } else if let Some(rel) = path_strip_prefix(full_path, cwd) {
            rel.to_string()
        } else {
            full_path.to_string()
        }
    } else {
        full_path.to_string()
    }
}

/// Verify that all resolved paths are under a known root.
/// Returns error on the first path not under any root.
/// Uses find_containing_root() which checks all roots (including suspended),
/// preserving offline root support — paths matching known roots pass
/// regardless of whether the root's disk is mounted.
pub fn validate_paths_in_roots(paths: &[String], roots: &[Root]) -> Result<()> {
    for path in paths {
        if find_containing_root(path, roots).is_none() {
            bail!("{path} is not under any known root");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // path_is_under tests

    #[test]
    fn path_is_under_exact_match() {
        assert!(path_is_under("/a/b", "/a/b"));
    }

    #[test]
    fn path_is_under_child() {
        assert!(path_is_under("/a/b/c", "/a/b"));
    }

    #[test]
    fn path_is_under_deep_child() {
        assert!(path_is_under("/a/b/c/d/e", "/a/b"));
    }

    #[test]
    fn path_is_under_false_positive_prevention() {
        // Critical: /a/bc is NOT under /a/b (different directory)
        assert!(!path_is_under("/a/bc", "/a/b"));
        assert!(!path_is_under("/a/bc/d", "/a/b"));
    }

    #[test]
    fn path_is_under_unrelated() {
        assert!(!path_is_under("/x/y/z", "/a/b"));
    }

    #[test]
    fn path_is_under_root() {
        assert!(path_is_under("/a/b/c", "/"));
    }

    // path_strip_prefix tests

    #[test]
    fn path_strip_prefix_basic() {
        assert_eq!(path_strip_prefix("/a/b/c", "/a/b"), Some("c"));
    }

    #[test]
    fn path_strip_prefix_deep() {
        assert_eq!(path_strip_prefix("/a/b/c/d", "/a/b"), Some("c/d"));
    }

    #[test]
    fn path_strip_prefix_exact_match() {
        // When path equals prefix, result is empty string
        assert_eq!(path_strip_prefix("/a/b", "/a/b"), Some(""));
    }

    #[test]
    fn path_strip_prefix_not_under() {
        // /a/bc is not under /a/b
        assert_eq!(path_strip_prefix("/a/bc", "/a/b"), None);
    }

    #[test]
    fn path_strip_prefix_unrelated() {
        assert_eq!(path_strip_prefix("/x/y", "/a/b"), None);
    }

    // ========================================================================
    // clean_path tests (pure, no filesystem)
    // ========================================================================

    #[test]
    fn clean_absolute_no_dots() {
        let result = clean_path(Path::new("/a/b/c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_relative_joins_cwd() {
        let result = clean_path(Path::new("b/c"), Path::new("/a"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_dotdot() {
        let result = clean_path(Path::new("/a/b/../c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/c"));
    }

    #[test]
    fn clean_dot() {
        let result = clean_path(Path::new("/a/./b/c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_multiple_dotdot() {
        let result = clean_path(Path::new("/a/b/c/../../d"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/d"));
    }

    #[test]
    fn clean_dotdot_past_root() {
        let result = clean_path(Path::new("/a/../../b"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/b"));
    }

    #[test]
    fn clean_relative_with_dotdot() {
        let result = clean_path(Path::new("../b"), Path::new("/a/c"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    #[test]
    fn clean_trailing_slash() {
        let result = clean_path(Path::new("/a/b/"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    #[test]
    fn clean_just_root() {
        let result = clean_path(Path::new("/"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/"));
    }

    #[test]
    fn clean_empty_relative() {
        let result = clean_path(Path::new(""), Path::new("/a/b"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    // ========================================================================
    // resolve_path tests (using fake Root structs, no DB)
    // ========================================================================

    fn make_test_root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn resolve_matches_known_root_exact() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/b"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b");
    }

    #[test]
    fn resolve_matches_under_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/b/c/d"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c/d");
    }

    #[test]
    fn resolve_relative_matches_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("b/c"), &roots, Path::new("/a"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    #[test]
    fn resolve_dotdot_matches_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/x/../b/c"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    #[test]
    fn resolve_no_match_returns_error() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/nonexistent/path"), &roots, Path::new("/any"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not accessible on disk"));
        assert!(err.contains("is the storage attached?"));
    }

    #[test]
    fn resolve_suspended_root_still_matches() {
        let roots = vec![Root {
            suspended: true,
            ..make_test_root(1, "/a/b")
        }];
        let result = resolve_path(Path::new("/a/b/c"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    // ========================================================================
    // format_path tests
    // ========================================================================

    #[test]
    fn test_format_path_strips_cwd() {
        assert_eq!(format_path("/a/b/c/file.jpg", Some("/a/b")), "c/file.jpg");
    }

    #[test]
    fn test_format_path_absolute_fallback() {
        assert_eq!(format_path("/x/y/z.jpg", Some("/a/b")), "/x/y/z.jpg");
    }

    #[test]
    fn test_format_path_cwd_itself() {
        assert_eq!(format_path("/a/b", Some("/a/b")), ".");
    }

    #[test]
    fn test_format_path_no_cwd() {
        assert_eq!(format_path("/a/b/c.jpg", None), "/a/b/c.jpg");
    }

    // ========================================================================
    // validate_paths_in_roots tests
    // ========================================================================

    #[test]
    fn validate_path_under_active_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        assert!(validate_paths_in_roots(&["/a/b/c".to_string()], &roots).is_ok());
    }

    #[test]
    fn validate_path_under_suspended_root() {
        let roots = vec![Root {
            suspended: true,
            ..make_test_root(1, "/a/b")
        }];
        // Suspended root is still "known"
        assert!(validate_paths_in_roots(&["/a/b/c".to_string()], &roots).is_ok());
    }

    #[test]
    fn validate_path_not_under_any_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = validate_paths_in_roots(&["/x/y/z".to_string()], &roots);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not under any known root"));
    }

    #[test]
    fn validate_multiple_valid_paths() {
        let roots = vec![make_test_root(1, "/a/b"), make_test_root(2, "/c/d")];
        assert!(
            validate_paths_in_roots(&["/a/b/c".to_string(), "/c/d/e".to_string()], &roots).is_ok()
        );
    }

    #[test]
    fn validate_multiple_paths_second_invalid() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result =
            validate_paths_in_roots(&["/a/b/c".to_string(), "/x/y".to_string()], &roots);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("/x/y"));
    }

    #[test]
    fn validate_empty_paths() {
        let roots = vec![make_test_root(1, "/a/b")];
        assert!(validate_paths_in_roots(&[], &roots).is_ok());
    }
}
