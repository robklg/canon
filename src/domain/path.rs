//! Path utilities for canon.
//!
//! Pure path manipulation only — no I/O. Soft-match-then-fallback resolution
//! against known roots (whose fallback branch touches the filesystem) lives
//! in `ops::scope`.
//!
//! No database dependencies.

use anyhow::{bail, Result};
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

/// Whether a supposedly-relative directory would land outside the directory
/// it is joined onto: absolute (`PathBuf::join` replaces the base wholesale)
/// or traversing through a parent component. Purely lexical — no
/// normalization, no filesystem access — so a value that merely *contains*
/// `..` as a component is rejected rather than resolved.
pub fn rel_dir_escapes(dir: &str) -> bool {
    let p = Path::new(dir);
    p.is_absolute()
        || p.components()
            .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
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

/// Resolve a single path against known roots (pure, offline — no filesystem
/// access). Returns `None` when the (lexically cleaned) path doesn't match
/// any known root; a filesystem fallback for unmatched paths lives in
/// `ops::scope::resolve_path`.
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Option<String> {
    let cleaned = clean_path(path, cwd);
    let cleaned_str = cleaned.to_string_lossy();
    if find_containing_root(&cleaned_str, roots).is_some() {
        Some(cleaned_str.into_owned())
    } else {
        None
    }
}

/// Resolve multiple paths against known roots (pure, offline).
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root], cwd: &Path) -> Vec<Option<String>> {
    paths.iter().map(|p| resolve_path(p, roots, cwd)).collect()
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

/// Component-wise common prefix of a set of paths taken whole — no trailing
/// component is dropped, because the inputs are already directory locations
/// rather than item paths. The display collapse for a group of placement
/// locations: one location returns itself, divergent tops return `""`.
/// Pure, no I/O.
pub fn common_path_prefix<'a>(paths: impl Iterator<Item = &'a str>) -> String {
    let mut common: Option<Vec<&str>> = None;
    for path in paths {
        let components: Vec<&str> = path.split('/').collect();
        common = Some(match common {
            None => components,
            Some(prev) => prev
                .into_iter()
                .zip(components)
                .take_while(|(a, b)| a == b)
                .map(|(a, _)| a)
                .collect(),
        });
    }
    common.unwrap_or_default().join("/")
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

    // rel_dir_escapes tests

    #[test]
    fn rel_dir_escapes_rejects_parent_components() {
        assert!(rel_dir_escapes(".."));
        assert!(rel_dir_escapes("../../tmp"));
        assert!(rel_dir_escapes("photos/../../../tmp"));
    }

    #[test]
    fn rel_dir_escapes_rejects_absolute() {
        assert!(rel_dir_escapes("/tmp/evil"));
    }

    #[test]
    fn rel_dir_escapes_accepts_nested_relative() {
        assert!(!rel_dir_escapes(""));
        assert!(!rel_dir_escapes("photos"));
        assert!(!rel_dir_escapes("photos/2020/summer"));
        // A filename merely containing dots is not a traversal.
        assert!(!rel_dir_escapes("photos/..hidden"));
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
    fn resolve_no_match_returns_none() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/nonexistent/path"), &roots, Path::new("/any"));
        assert!(result.is_none());
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not under any known root"));
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
        let result = validate_paths_in_roots(&["/a/b/c".to_string(), "/x/y".to_string()], &roots);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("/x/y"));
    }

    #[test]
    fn validate_empty_paths() {
        let roots = vec![make_test_root(1, "/a/b")];
        assert!(validate_paths_in_roots(&[], &roots).is_ok());
    }

    // common_path_prefix tests

    #[test]
    fn common_path_prefix_single_path_returns_itself() {
        // No trailing component is dropped — the input is already a
        // directory location, not an item path.
        assert_eq!(common_path_prefix(["m/03"].into_iter()), "m/03");
        assert_eq!(common_path_prefix(["/arch/m/03"].into_iter()), "/arch/m/03");
    }

    #[test]
    fn common_path_prefix_collapses_to_shared_ancestor() {
        assert_eq!(common_path_prefix(["m/01", "m/02"].into_iter()), "m");
        assert_eq!(
            common_path_prefix(["/arch/m/01", "/arch/m/02"].into_iter()),
            "/arch/m"
        );
        assert_eq!(common_path_prefix(["m", "m/01"].into_iter()), "m");
        assert_eq!(
            common_path_prefix(["фото/2016", "фото/2017"].into_iter()),
            "фото"
        );
    }

    #[test]
    fn common_path_prefix_root_level_and_divergent() {
        assert_eq!(common_path_prefix(["", "m/01"].into_iter()), "");
        assert_eq!(common_path_prefix(["a/x", "b/y"].into_iter()), "");
        assert_eq!(common_path_prefix(std::iter::empty()), "");
    }
}
