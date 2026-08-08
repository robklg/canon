//! Scope resolution for Canon commands.
//!
//! All scope-taking commands follow a unified scope model: CWD is an intentional
//! context switch. When the user is inside a scanned root, Canon scopes to that
//! directory. When outside any root, Canon operates globally. The `--global` flag
//! overrides CWD defaulting.
//!
//! This applies to both discovery commands (ls, survey, facts, coverage, worklist)
//! and effectful commands (cluster generate, exclude set/clear/duplicates).
//!
//! This module also hosts path and root resolution against known roots, with
//! a filesystem fallback for the offline-pure predicates in `domain::path`/
//! `domain::root` — the soft-match-then-fallback pattern for source-querying
//! commands (file-accessing commands like `scan` use `fs::canonicalize` directly).

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

use crate::domain;
use crate::domain::path::{clean_path, path_strip_prefix, validate_paths_in_roots};
use crate::domain::root::{find_containing_root, Root, RootSpec};
use crate::ops::fs::canonicalize_maybe_missing;
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

// ============================================================================
// Path & Root Resolution (soft-match against known roots, fs fallback)
// ============================================================================

/// Resolve a single path against known roots, falling back to fs::canonicalize().
/// Use for source-querying commands. File-accessing commands (scan) use
/// fs::canonicalize directly.
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Result<String> {
    if let Some(matched) = domain::path::resolve_path(path, roots, cwd) {
        return Ok(matched);
    }

    // Fall back to fs::canonicalize (requires path to exist on disk)
    match std::fs::canonicalize(path) {
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
    domain::path::resolve_paths(paths, roots, &cwd)
        .into_iter()
        .zip(paths.iter())
        .map(|(matched, path)| match matched {
            Some(s) => Ok(s),
            None => resolve_path(path, roots, &cwd),
        })
        .collect()
}

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
        RootSpec::ByPath(ref path) => {
            let cwd = std::env::current_dir().context("Failed to determine current directory")?;
            // Resolve against ALL roots (including suspended) for path recognition
            let canonical = resolve_path(Path::new(path), roots, &cwd)?;
            // Find among filtered candidates (respects suspension filter)
            let root = candidates
                .iter()
                .find(|r| r.path == canonical)
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
    let cwd = std::env::current_dir().context("Failed to determine current directory")?;
    // Resolve against ALL roots for path recognition (soft resolution)
    let path_str = resolve_path(path, roots, &cwd)?;

    // Filter roots by suspension status for the containing-root lookup
    let candidates: Vec<Root> = roots
        .iter()
        .filter(|r| include_suspended || r.is_active())
        .cloned()
        .collect();

    // Find containing root (pure domain logic)
    Ok(find_containing_root(&path_str, &candidates))
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
    // Soft resolution: try matching against known roots first (works offline),
    // then fall back to canonicalize_maybe_missing (tolerates non-existent subdirs)
    let cwd = std::env::current_dir().context("Failed to determine current directory")?;
    let cleaned = clean_path(path, &cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let path_str = if find_containing_root(&cleaned_str, roots).is_some() {
        cleaned_str.into_owned()
    } else {
        // Fallback: canonicalize (tolerating non-existent subdirectories)
        canonicalize_maybe_missing(path)?
    };

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
    match resolve_root_path(roots, &cwd)? {
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

/// History-tense resolution: an explicit path under a live root resolves
/// even when no sources stand there anymore. The source-existence gate is
/// right for present-tense commands ("no sources known" is `ls`'s honest
/// answer) — but the trail's subject is what *happened*, and a place fully
/// moved into the archive still holds its history (extraction rows, notes).
/// Returns `None` when the paths don't resolve under live roots at all —
/// the caller propagates its original error untouched.
pub fn resolve_history_scope(explicit_paths: &[PathBuf], roots: &[Root]) -> Option<ResolvedScope> {
    if explicit_paths.is_empty() {
        return None;
    }
    let prefixes = resolve_paths(explicit_paths, roots).ok()?;
    validate_paths_in_roots(&prefixes, roots).ok()?;
    let auto_include_archived = prefixes.iter().any(|p| {
        domain::root::find_containing_root(p, roots)
            .map(|(_, _, role, _)| role == "archive")
            .unwrap_or(false)
    });
    Some(ResolvedScope {
        prefixes,
        from_cwd: false,
        auto_include_archived,
    })
}

/// Validate that sources exist at each scope path.
/// Errors on the first path with no known sources.
/// Skips root-level paths (empty rel_path) — roots are always valid.
/// Assumes paths are already validated as under known roots.
pub fn validate_sources_exist(conn: &Connection, paths: &[String], roots: &[Root]) -> Result<()> {
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
        let result =
            resolve_scope(&conn, &[PathBuf::from("/archive/photos")], false, &roots).unwrap();
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
    fn history_scope_resolves_an_emptied_place_on_a_live_root() {
        // No sources stand at /photos/moved-away — a move-mode apply took
        // them all — but the path is under a live root, so the history
        // question still resolves.
        let roots = vec![make_test_root(1, "/photos", "source")];
        let resolved =
            resolve_history_scope(&[PathBuf::from("/photos/moved-away")], &roots).unwrap();
        assert_eq!(resolved.prefixes, vec!["/photos/moved-away".to_string()]);
        assert!(!resolved.from_cwd);
        assert!(!resolved.auto_include_archived);
    }

    #[test]
    fn history_scope_declines_paths_under_no_root() {
        let roots = vec![make_test_root(1, "/photos", "source")];
        assert!(resolve_history_scope(&[PathBuf::from("/elsewhere/x")], &roots).is_none());
        assert!(resolve_history_scope(&[], &roots).is_none());
    }

    #[test]
    fn history_scope_carries_the_archive_include_rule() {
        let roots = vec![make_test_root(2, "/archive", "archive")];
        let resolved =
            resolve_history_scope(&[PathBuf::from("/archive/media/old")], &roots).unwrap();
        assert!(resolved.auto_include_archived);
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

    // ========================================================================
    // parse_root_spec() tests (with &[Root] input)
    // ========================================================================

    #[test]
    fn parse_root_spec_impl_by_id_found() {
        let roots = vec![
            make_test_root(1, "/a", "source"),
            make_test_root(2, "/b", "archive"),
        ];
        let result = parse_root_spec(&roots, "id:2", None);
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn parse_root_spec_impl_by_id_not_found() {
        let roots = vec![make_test_root(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:999", None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No root with id 999"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_accepts_source() {
        let roots = vec![make_test_root(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_rejects_archive() {
        let roots = vec![make_test_root(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'archive', expected 'source'"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_accepts_archive() {
        let roots = vec![make_test_root(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("archive"));
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_rejects_source() {
        let roots = vec![make_test_root(1, "/a", "source")];
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
            make_test_root(1, "/a", "source"),
            make_test_root(2, "/b", "archive"),
        ];
        // None means accept any role
        assert_eq!(parse_root_spec(&roots, "id:1", None).unwrap(), 1);
        assert_eq!(parse_root_spec(&roots, "id:2", None).unwrap(), 2);
    }

    #[test]
    fn parse_root_spec_impl_excludes_suspended() {
        let mut suspended_root = make_test_root(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        // parse_root_spec (not _any) should exclude suspended roots
        let result = parse_root_spec(&roots, "id:1", None);
        assert!(result.is_err());
    }

    #[test]
    fn parse_root_spec_any_includes_suspended() {
        let mut suspended_root = make_test_root(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        // parse_root_spec_any should include suspended roots
        let result = parse_root_spec_any(&roots, "id:1");
        assert_eq!(result.unwrap(), 1);
    }

    // ========================================================================
    // parse_root_spec ByPath tests (soft resolution)
    // ========================================================================

    #[test]
    fn parse_root_spec_by_path_matches_root() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = parse_root_spec(&roots, "path:/a/b", None);
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn parse_root_spec_by_path_no_match() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = parse_root_spec(&roots, "path:/nonexistent", None);
        assert!(result.is_err());
    }

    #[test]
    fn parse_root_spec_by_path_role_filter() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        // Root exists but has wrong role
        let result = parse_root_spec(&roots, "path:/a/b", Some("archive"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'source', expected 'archive'"));
    }

    #[test]
    fn parse_root_spec_by_path_suspended_excluded() {
        let mut root = make_test_root(1, "/a/b", "source");
        root.suspended = true;
        let roots = vec![root];
        // Path resolves (against all roots) but root is filtered out of candidates
        let result = parse_root_spec(&roots, "path:/a/b", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No root for path"));
    }

    #[test]
    fn parse_root_spec_any_by_path_suspended_included() {
        let mut root = make_test_root(1, "/a/b", "source");
        root.suspended = true;
        let roots = vec![root];
        // parse_root_spec_any includes suspended roots
        let result = parse_root_spec_any(&roots, "path:/a/b");
        assert_eq!(result.unwrap(), 1);
    }

    // ========================================================================
    // resolve_archive_path tests
    // ========================================================================

    #[test]
    fn resolve_archive_path_exact_root_match_offline() {
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/archive")).unwrap();
        assert_eq!(result, (1, "/archive".to_string(), String::new()));
    }

    #[test]
    fn resolve_archive_path_under_root_offline() {
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/archive/photos/2020")).unwrap();
        assert_eq!(
            result,
            (1, "/archive".to_string(), "photos/2020".to_string())
        );
    }

    #[test]
    fn resolve_archive_path_rejects_source_role() {
        let roots = vec![make_test_root(1, "/archive", "source")];
        let result = resolve_archive_path(&roots, Path::new("/archive/photos"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an archive"));
    }

    #[test]
    fn resolve_archive_path_not_under_any_root() {
        // No known root matches offline; canonicalize_maybe_missing resolves
        // the made-up absolute path against the always-existing "/" ancestor,
        // then the containing-root search comes up empty.
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/totally/unregistered/path"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not inside any registered archive root"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_archive_path_falls_back_to_real_canonicalize_via_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_archive = dir.path().join("real-archive");
        std::fs::create_dir(&real_archive).unwrap();
        let canonical_archive = std::fs::canonicalize(&real_archive)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // A symlink alias whose own path does not lexically start with the
        // root's canonical path — only fs::canonicalize (the fallback) can
        // recognize it, never the offline string match.
        let alias = dir.path().join("alias-to-archive");
        symlink(&real_archive, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_archive, "archive")];
        let result = resolve_archive_path(&roots, &alias.join("photos")).unwrap();
        assert_eq!(result, (1, canonical_archive, "photos".to_string()));
    }

    // ========================================================================
    // resolve_path fallback tests (real disk, no known-root match offline)
    // ========================================================================

    #[cfg(unix)]
    #[test]
    fn resolve_path_matches_known_root_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // The alias's own path does not lexically start with the root's
        // canonical path — only fs::canonicalize (the fallback) resolves it.
        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_root, "source")];
        let result = resolve_path(&alias, &roots, dir.path()).unwrap();
        assert_eq!(result, canonical_root);
    }

    #[test]
    fn resolve_path_arbitrary_real_disk_path_resolves_via_plain_canonicalize() {
        // A real path that matches no known root still resolves — the
        // compare/exclude case of an arbitrary disk path outside any root.
        let dir = tempfile::tempdir().unwrap();
        let roots = vec![make_test_root(1, "/nowhere/near/here", "source")];
        let result = resolve_path(dir.path(), &roots, dir.path()).unwrap();
        let expected = std::fs::canonicalize(dir.path())
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn resolve_path_neither_root_nor_disk_matches_errors() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = resolve_path(Path::new("/nonexistent/path"), &roots, Path::new("/any"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not accessible on disk"));
        assert!(err.contains("is the storage attached?"));
    }

    #[cfg(unix)]
    #[test]
    fn parse_root_spec_by_path_matches_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root).unwrap();

        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(
            1,
            &canonical_root.to_string_lossy(),
            "source",
        )];
        let result = parse_root_spec(&roots, &format!("path:{}", alias.display()), None);
        assert_eq!(result.unwrap(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_path_matches_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root)
            .unwrap()
            .to_string_lossy()
            .to_string();

        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_root, "source")];
        let result = resolve_root_path(&roots, &alias).unwrap();
        assert_eq!(
            result,
            Some((1, canonical_root, "source".to_string(), String::new()))
        );
    }
}
