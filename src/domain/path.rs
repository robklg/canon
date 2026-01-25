//! Path utilities for canon.
//!
//! This module contains:
//! - Pure path manipulation functions (no I/O)
//! - Path canonicalization helpers (filesystem I/O for resolving paths)
//!
//! No database dependencies.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

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
// Path Canonicalization (Filesystem I/O)
// ============================================================================

/// Canonicalize an optional scope path for use in SQL path matching.
/// Returns the canonical (absolute, resolved) path as a string.
pub fn canonicalize_scope(scope_path: Option<&Path>) -> Result<Option<String>> {
    match scope_path {
        Some(p) => Ok(Some(
            fs::canonicalize(p)
                .with_context(|| format!("Failed to resolve path: {}", p.display()))?
                .to_string_lossy()
                .to_string(),
        )),
        None => Ok(None),
    }
}

/// Canonicalize multiple scope paths for use in SQL path matching.
/// Returns canonical (absolute, resolved) paths as strings.
pub fn canonicalize_scopes(paths: &[PathBuf]) -> Result<Vec<String>> {
    paths
        .iter()
        .map(|p| {
            fs::canonicalize(p)
                .with_context(|| format!("Failed to resolve path: {}", p.display()))
                .map(|cp| cp.to_string_lossy().to_string())
        })
        .collect()
}

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
}
