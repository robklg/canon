//! Scope resolution for Canon commands.
//!
//! All scope-taking commands follow a unified scope model: CWD is an intentional
//! context switch. When the user is inside a scanned root, Canon scopes to that
//! directory. When outside any root, Canon operates globally. The `--global` flag
//! overrides CWD defaulting.
//!
//! This applies to both discovery commands (ls, survey, facts, coverage, worklist)
//! and effectful commands (cluster generate, exclude set/clear/duplicates).

use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::domain;
use crate::domain::path::{resolve_paths, validate_paths_in_roots};
use crate::domain::root::Root;
use crate::repo::{self, Connection};

/// Result of resolving scope for a discovery command.
#[derive(Debug)]
pub struct ResolvedScope {
    /// Resolved scope path strings (empty = global).
    pub prefixes: Vec<String>,
    /// Whether the scope came from CWD defaulting (controls relative path display).
    pub from_cwd: bool,
    /// Whether to auto-include archived sources (scope is inside an archive root).
    pub auto_include_archived: bool,
}

impl ResolvedScope {
    /// True when operating globally (no scope restriction).
    pub fn is_global(&self) -> bool {
        self.prefixes.is_empty()
    }
}

/// Resolve scope for a discovery command.
///
/// Resolution order:
/// 1. Explicit paths given → resolve, validate they're under known roots, return
/// 2. `--global` flag → return empty (global)
/// 3. No paths, no `--global` → try CWD:
///    - CWD inside a known active root → scope to CWD
///    - CWD not inside any root → global fallback (silent)
///    - `current_dir()` fails → global fallback (silent)
///
/// When CWD or explicit path is inside an archive root, sets `auto_include_archived`.
pub fn resolve_scope(
    conn: &Connection,
    explicit_paths: &[PathBuf],
    global: bool,
    roots: &[Root],
) -> Result<ResolvedScope> {
    // Case 1: Explicit paths given
    if !explicit_paths.is_empty() {
        let prefixes = resolve_paths(explicit_paths, roots)?;
        validate_paths_in_roots(&prefixes, roots)?;
        validate_sources_exist(conn, &prefixes, roots)?;
        // Check if any path is inside an archive root
        let auto_include_archived = prefixes.iter().any(|p| {
            domain::root::find_containing_root(p, roots)
                .map(|(_, _, role, _)| role == "archive")
                .unwrap_or(false)
        });
        return Ok(ResolvedScope {
            prefixes,
            from_cwd: false,
            auto_include_archived,
        });
    }

    // Case 2: --global flag
    if global {
        return Ok(ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        });
    }

    // Case 3: No paths, no --global — try CWD
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(_) => {
            return Ok(ResolvedScope {
                prefixes: Vec::new(),
                from_cwd: false,
                auto_include_archived: false,
            });
        }
    };

    // Check if CWD is inside a known active root (excludes suspended)
    match domain::resolve_root_path(roots, &cwd)? {
        Some((_, _, role, _)) => Ok(ResolvedScope {
            prefixes: resolve_paths(&[cwd], roots)?,
            from_cwd: true,
            auto_include_archived: role == "archive",
        }),
        None => Ok(ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        }),
    }
}

/// Validate that sources exist at each scope path.
/// Errors on the first path with no known sources.
/// Skips root-level paths (empty rel_path) — roots are always valid.
/// Assumes paths are already validated as under known roots.
pub fn validate_sources_exist(
    conn: &Connection,
    paths: &[String],
    roots: &[Root],
) -> Result<()> {
    for path in paths {
        if let Some((root_id, _root_path, _role, rel_path)) =
            domain::root::find_containing_root(path, roots)
        {
            if !rel_path.is_empty()
                && !repo::source::sources_exist_at_scope(conn, root_id, &rel_path)?
            {
                bail!("no sources known at {path}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_root, insert_source, setup_test_db};

    fn make_test_root(id: i64, path: &str, role: &str) -> Root {
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
    fn resolve_explicit_paths() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/a/b", "source", false);
        insert_source(&conn, root_id, "c/file.txt", None);
        let roots = vec![make_test_root(root_id, "/a/b", "source")];
        let result = resolve_scope(&conn, &[PathBuf::from("/a/b/c")], false, &roots).unwrap();
        assert!(!result.is_global());
        assert!(!result.from_cwd);
        assert!(!result.auto_include_archived);
        assert_eq!(result.prefixes, vec!["/a/b/c".to_string()]);
    }

    #[test]
    fn resolve_explicit_path_in_archive_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/archive", "archive", false);
        insert_source(&conn, root_id, "photos/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/archive", "archive")];
        let result = resolve_scope(&conn, &[PathBuf::from("/archive/photos")], false, &roots).unwrap();
        assert!(result.auto_include_archived);
        assert!(!result.from_cwd);
    }

    #[test]
    fn resolve_explicit_path_not_in_root_errors() {
        let conn = setup_test_db();
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = resolve_scope(&conn, &[PathBuf::from("/x/y/z")], false, &roots);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_global_flag() {
        let conn = setup_test_db();
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = resolve_scope(&conn, &[], true, &roots).unwrap();
        assert!(result.is_global());
        assert!(!result.from_cwd);
        assert!(!result.auto_include_archived);
    }

    #[test]
    fn resolve_global_flag_with_explicit_paths() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/a/b", "source", false);
        insert_source(&conn, root_id, "c/file.txt", None);
        let roots = vec![make_test_root(root_id, "/a/b", "source")];
        let result = resolve_scope(&conn, &[PathBuf::from("/a/b/c")], true, &roots).unwrap();
        assert!(!result.is_global());
        assert_eq!(result.prefixes, vec!["/a/b/c".to_string()]);
    }

    #[test]
    fn resolve_scope_errors_on_unknown_subpath() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Path under root but no sources there
        let result = resolve_scope(&conn, &[PathBuf::from("/photos/typo")], false, &roots);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
    }

    #[test]
    fn resolve_scope_accepts_path_with_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = resolve_scope(&conn, &[PathBuf::from("/photos/2011")], false, &roots).unwrap();
        assert_eq!(result.prefixes, vec!["/photos/2011".to_string()]);
    }

    #[test]
    fn validate_sources_exist_errors_on_unknown() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = validate_sources_exist(&conn, &["/photos/typo".to_string()], &roots);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
    }

    #[test]
    fn validate_sources_exist_accepts_root_level() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Root-level path (empty rel_path) should always pass, even with no sources
        let result = validate_sources_exist(&conn, &["/photos".to_string()], &roots);
        assert!(result.is_ok());
    }
}
