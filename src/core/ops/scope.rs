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
//! a filesystem fallback for the offline-pure predicates in
//! `core::domain::path`/`core::domain::root` — the soft-match-then-fallback
//! pattern for source-querying
//! commands (file-accessing commands like `scan` use `fs::canonicalize` directly).

use anyhow::{bail, Result};
use std::path::{Path, PathBuf};

use super::fs::canonicalize_maybe_missing;
use crate::core::domain;
use crate::core::domain::path::{clean_path, path_strip_prefix, validate_paths_in_roots};
use crate::core::domain::root::{find_containing_root, Root, RootSpec};
use crate::core::domain::scope::ScopeMatch;
use crate::core::repo::{self, Connection};

/// Classify a canonicalized path as file or directory scope.
///
/// Asks the filesystem whether the path names a file; anything else — a
/// directory, or a path that no longer exists — is treated as a prefix. The
/// look at the disk is why this composes here rather than on `ScopeMatch`
/// itself, which stays free of I/O.
pub fn classify(path: &str) -> ScopeMatch {
    if Path::new(path).is_file() {
        ScopeMatch::ExactFile(path.to_string())
    } else {
        ScopeMatch::UnderDirectory(path.to_string())
    }
}

/// Classify multiple canonicalized paths.
pub fn classify_all(paths: &[String]) -> Vec<ScopeMatch> {
    paths.iter().map(|p| classify(p)).collect()
}

/// Classify canonicalized paths for a command that acts on what it matches,
/// asking the index whatever the disk cannot answer.
///
/// [`classify`] reads the disk, and a path whose storage is detached answers
/// neither "file" nor "directory" — it falls to the prefix arm. On a reporting
/// command that only widens a view; on an effectful one it widens the act: an
/// exclusion aimed at one file the drive no longer offers would take every row
/// recorded beneath that path with it. Dismissing content needs no mount (the
/// judgment must outlive the drive it clears), so the command must mean the
/// same thing attached or detached. When the disk is silent, the index
/// answers: a source standing at exactly this path makes it a file, and
/// anything else stays a prefix.
pub fn classify_all_indexed(conn: &Connection, paths: &[String]) -> Result<Vec<ScopeMatch>> {
    let roots = repo::root::fetch_all(conn)?;
    paths
        .iter()
        .map(|path| {
            let p = Path::new(path);
            if p.is_file() {
                return Ok(ScopeMatch::ExactFile(path.clone()));
            }
            if !p.is_dir() && indexed_as_file(conn, path, &roots)? {
                return Ok(ScopeMatch::ExactFile(path.clone()));
            }
            Ok(ScopeMatch::UnderDirectory(path.clone()))
        })
        .collect()
}

fn indexed_as_file(conn: &Connection, path: &str, roots: &[Root]) -> Result<bool> {
    match find_containing_root(path, roots) {
        Some((root_id, _, _, rel)) if !rel.is_empty() => {
            repo::source::source_exists_at_path(conn, root_id, &rel)
        }
        _ => Ok(false),
    }
}

/// Result of resolving scope for a discovery command.
#[derive(Debug)]
pub struct ResolvedScope {
    /// Resolved scope path strings (empty = global), in the byte-form the
    /// index stores.
    pub prefixes: Vec<String>,
    /// Paths that were asked for, sit under a known root, and have no known
    /// sources — set aside rather than run. Never empty without
    /// [`prefixes`](Self::prefixes) also being non-empty: a scope that kept
    /// nothing is an error, not a silent narrowing. Every command states
    /// these on the channel it states its scope on; nothing acts on them,
    /// and they never become a recorded decision scope.
    pub set_aside: Vec<String>,
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

/// The first relative path among the inputs, if any — the only case that
/// needs a current directory at all. Pure.
pub fn needs_cwd<'a>(paths: &[&'a Path]) -> Option<&'a Path> {
    paths.iter().copied().find(|p| p.is_relative())
}

/// A current directory to resolve these paths against.
///
/// A working directory can be deleted out from under a running process, and
/// asking for one that no longer exists fails. That failure is only fatal
/// when it is actually needed: an absolute path is resolved without ever
/// reading the CWD, so `canon scan /some/absolute/path` must not depend on
/// where it was launched from. When every input is absolute the placeholder
/// is never read — [`clean_path`] ignores its `cwd` argument for absolute
/// inputs. When a relative path is present, the error names it.
pub fn cwd_for(paths: &[&Path]) -> Result<PathBuf> {
    match std::env::current_dir() {
        Ok(cwd) => Ok(cwd),
        Err(_) => match needs_cwd(paths) {
            None => Ok(PathBuf::from("/")),
            Some(relative) => bail!(
                "cannot resolve relative path '{}': the current directory is unavailable",
                relative.display()
            ),
        },
    }
}

/// Resolve multiple paths against known roots.
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root]) -> Result<Vec<String>> {
    let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
    let cwd = cwd_for(&refs)?;
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
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
pub fn parse_root_spec(roots: &[Root], spec: &str, required_role: Option<&str>) -> Result<i64> {
    parse_root_spec_impl(roots, spec, required_role, false)
}

/// Parse root spec including suspended roots. Used for suspend/unsuspend commands.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
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
            let cwd = cwd_for(&[Path::new(path)])?;
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
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
///
/// Returns Some((root_id, root_path, role, relative_subdir)) if inside a root, None otherwise.
fn resolve_root_path(roots: &[Root], path: &Path) -> Result<Option<(i64, String, String, String)>> {
    resolve_root_path_impl(roots, path, false)
}

/// Resolve a path to its containing root, including suspended roots.
/// Used for internal operations like unsuspend and overlap checking.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
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
    let cwd = cwd_for(&[path])?;
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
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
///
/// Returns (root_id, root_path, relative_subdir) or error if not in an archive.
pub fn resolve_archive_path(roots: &[Root], path: &Path) -> Result<(i64, String, String)> {
    // Soft resolution: try matching against known roots first (works offline),
    // then fall back to canonicalize_maybe_missing (tolerates non-existent subdirs)
    let cwd = cwd_for(&[path])?;
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
        let ScopePartition { kept, set_aside } =
            apply_source_existence_policy(conn, prefixes, roots)?;
        // One derivation of "is any of this inside an archive root": the
        // same role partition survey reads for its frame statement.
        let auto_include_archived = !domain::root::partition_prefixes_by_role(&kept, roots)
            .archive_side
            .is_empty();
        return Ok(ResolvedScope {
            prefixes: kept,
            set_aside,
            from_cwd: false,
            auto_include_archived,
        });
    }

    // Case 2: --global flag
    if global {
        return Ok(ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
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
                set_aside: Vec::new(),
                from_cwd: false,
                auto_include_archived: false,
            });
        }
    };

    // Check if CWD is inside a known active root (excludes suspended)
    match resolve_root_path(roots, &cwd)? {
        Some((_, _, role, _)) => Ok(ResolvedScope {
            prefixes: resolve_paths(&[cwd], roots)?,
            set_aside: Vec::new(),
            from_cwd: true,
            auto_include_archived: role == "archive",
        }),
        None => Ok(ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
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
    let auto_include_archived = !domain::root::partition_prefixes_by_role(&prefixes, roots)
        .archive_side
        .is_empty();
    Some(ResolvedScope {
        prefixes,
        set_aside: Vec::new(),
        from_cwd: false,
        auto_include_archived,
    })
}

/// The index's answer to "which byte-form of this path does Canon know?" —
/// the form-tolerance rule at the source-existence gate.
///
/// The path's root has already been matched (root containment is
/// form-tolerant in its own right); only the relative remainder is retried
/// here, so a root and the content beneath it may each have been stored in
/// whichever form their disk handed over. Returns the path rebuilt from the
/// candidate the index knows sources under — the stored bytes, which is what
/// every downstream comparison (Rust prefix matching, the SQL boundary
/// spellings) must see. `None` means no form of this path has sources: it is
/// genuinely sourceless, and only then does policy see it.
///
/// Root-level paths (empty remainder) are always known — a root is valid
/// whether or not anything has been scanned into it yet.
fn stored_form_with_sources(
    conn: &Connection,
    path: &str,
    roots: &[Root],
) -> Result<Option<String>> {
    let Some((root_id, root_path, _role, rel_path)) =
        domain::root::find_containing_root(path, roots)
    else {
        // Not under any known root: root membership is validated separately
        // and this gate has nothing to say about it.
        return Ok(Some(path.to_string()));
    };
    if rel_path.is_empty() {
        return Ok(Some(path.to_string()));
    }
    for candidate in domain::path::normalization_candidates(&rel_path) {
        if repo::source::sources_exist_at_scope(conn, root_id, &candidate)? {
            return Ok(Some(
                Path::new(&root_path)
                    .join(&candidate)
                    .to_string_lossy()
                    .into_owned(),
            ));
        }
    }
    Ok(None)
}

/// How a scope's asked-for paths came out of the source-existence gate.
#[derive(Debug)]
pub struct ScopePartition {
    /// Paths with known sources, in the byte-form the index stores.
    pub kept: Vec<String>,
    /// Paths under a known root with no known sources, in any form.
    pub set_aside: Vec<String>,
}

/// The source-existence policy at the scope boundary, spoken once.
///
/// What was asked and what runs is never a silent difference. A path under a
/// known root that Canon has no sources for cannot be worked on — but among
/// several paths, one empty folder is a reason to say so, not a reason to
/// refuse the whole invocation. The rules:
///
/// - root-level paths (empty remainder) are always kept — a root is valid
///   whether or not anything has been scanned into it yet;
/// - a single path with no sources is an error, unchanged: with nothing else
///   asked for there is no work left to do, and "scan first" is the honest
///   answer;
/// - several paths with at least one keeper proceed, and the rest come back
///   as set-asides for the caller to state;
/// - several paths with no keeper at all is an error naming every one of
///   them — a scope that kept nothing must never look like a narrowing.
///
/// Paths not under any known root are a separate, harder failure and are
/// rejected before this is reached.
fn apply_source_existence_policy(
    conn: &Connection,
    paths: Vec<String>,
    roots: &[Root],
) -> Result<ScopePartition> {
    let single = paths.len() == 1;
    let mut kept = Vec::new();
    let mut set_aside = Vec::new();
    for path in paths {
        match stored_form_with_sources(conn, &path, roots)? {
            Some(stored) => kept.push(stored),
            None if single => bail!("no sources known at {path}"),
            None => set_aside.push(path),
        }
    }
    if kept.is_empty() {
        bail!("no sources known at {}", set_aside.join(", "));
    }
    Ok(ScopePartition { kept, set_aside })
}

/// Validate that sources exist at each scope path, returning the paths in
/// the byte-form the index stores them under.
///
/// Errors on the first path with no known sources — the abort spelling of
/// the source-existence gate. Its callers are exactly the carve-outs from
/// the boundary's proceed-and-state policy: locations that are load-bearing
/// to the question being asked, where proceeding without one would change
/// the question rather than narrow it (`compare`'s two sides,
/// `exclude duplicates`' scope and prefer paths, `survey --other`'s
/// reference location). Every other scope-taking command goes through
/// [`resolve_scope`], which sets a sourceless path aside and says so.
///
/// Skips root-level paths (empty rel_path) — roots are always valid.
/// Assumes paths are already validated as under known roots.
pub fn validate_sources_exist(
    conn: &Connection,
    paths: &[String],
    roots: &[Root],
) -> Result<Vec<String>> {
    paths
        .iter()
        .map(|path| match stored_form_with_sources(conn, path, roots)? {
            Some(stored) => Ok(stored),
            None => bail!("no sources known at {path}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_root, insert_source, setup_test_db};

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

    /// The detached-drive guard: an act means the same thing whether or not
    /// the storage is mounted. Neither path here exists on disk, so the disk
    /// answers nothing and the index has to.
    #[test]
    fn a_path_the_disk_cannot_answer_for_is_classified_by_the_index() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", false);
        insert_source(&conn, root_id, "photos/IMG_1.jpg", None);
        insert_source(&conn, root_id, "photos/album/IMG_2.jpg", None);

        let scopes = classify_all_indexed(
            &conn,
            &[
                "/mnt/drive/photos/IMG_1.jpg".to_string(),
                "/mnt/drive/photos".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(
            scopes,
            vec![
                ScopeMatch::ExactFile("/mnt/drive/photos/IMG_1.jpg".to_string()),
                ScopeMatch::UnderDirectory("/mnt/drive/photos".to_string()),
            ]
        );
        // What the disk-only classification would have said about the file —
        // a prefix, sweeping in whatever the index records beneath it.
        assert_eq!(
            classify("/mnt/drive/photos/IMG_1.jpg"),
            ScopeMatch::UnderDirectory("/mnt/drive/photos/IMG_1.jpg".to_string())
        );
    }

    /// A path the index has never heard of stays a prefix: the classification
    /// narrows on evidence, it does not invent a file.
    #[test]
    fn an_unknown_path_the_disk_cannot_answer_for_stays_a_prefix() {
        let conn = setup_test_db();
        insert_root(&conn, "/mnt/drive", "source", false);

        let scopes =
            classify_all_indexed(&conn, &["/mnt/drive/photos/gone.jpg".to_string()]).unwrap();
        assert_eq!(
            scopes,
            vec![ScopeMatch::UnderDirectory(
                "/mnt/drive/photos/gone.jpg".to_string()
            )]
        );
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

    // ========================================================================
    // Lazy CWD: a working directory is read only when one is needed
    // ========================================================================

    #[test]
    fn needs_cwd_names_the_first_relative_path() {
        let a = Path::new("/abs/one");
        let b = Path::new("rel/two");
        let c = Path::new("rel/three");
        assert_eq!(needs_cwd(&[a, b, c]), Some(b));
    }

    #[test]
    fn needs_cwd_is_none_when_every_path_is_absolute() {
        assert_eq!(needs_cwd(&[Path::new("/a"), Path::new("/b")]), None);
        assert_eq!(needs_cwd(&[]), None);
    }

    // ========================================================================
    // The scope-boundary honesty policy
    // ========================================================================

    /// A scope with two folders where one is empty is a scope with one
    /// folder's worth of work in it — and a line saying so.
    #[test]
    fn a_multi_path_scope_proceeds_past_a_sourceless_member() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2011"), PathBuf::from("/photos/2012")],
            false,
            &roots,
        )
        .unwrap();
        assert_eq!(resolved.prefixes, vec!["/photos/2011".to_string()]);
        assert_eq!(resolved.set_aside, vec!["/photos/2012".to_string()]);
    }

    /// The "scan first" contract: with nothing else asked for, there is no
    /// work left to narrow to.
    #[test]
    fn a_single_sourceless_path_still_errors() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = resolve_scope(&conn, &[PathBuf::from("/photos/2012")], false, &roots);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known at /photos/2012"), "{err}");
    }

    #[test]
    fn an_all_sourceless_multi_path_scope_errors_naming_every_path() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2012"), PathBuf::from("/photos/2013")],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("/photos/2012"), "{err}");
        assert!(err.contains("/photos/2013"), "{err}");
    }

    /// Root membership is the harder failure and keeps its precedence: a path
    /// under no known root is refused whatever else was asked for — never set
    /// aside beside the keepers. The temp directory stands in for "a real
    /// place on disk that Canon has never been told about", so resolution
    /// succeeds and root membership is what does the refusing.
    #[test]
    fn a_non_root_path_errors_even_beside_valid_members() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let outsider = std::env::temp_dir();
        let result = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2011"), outsider],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not under any known root"), "{err}");
    }

    #[test]
    fn root_level_paths_are_always_kept() {
        let conn = setup_test_db();
        // Nothing has ever been scanned into this root.
        let root_id = insert_root(&conn, "/photos", "source", false);
        let other_id = insert_root(&conn, "/scans", "source", false);
        insert_source(&conn, other_id, "a.jpg", None);
        let roots = vec![
            make_test_root(root_id, "/photos", "source"),
            make_test_root(other_id, "/scans", "source"),
        ];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from("/photos"), PathBuf::from("/scans")],
            false,
            &roots,
        )
        .unwrap();
        assert_eq!(
            resolved.prefixes,
            vec!["/photos".to_string(), "/scans".to_string()]
        );
        assert!(resolved.set_aside.is_empty());
    }

    /// The record must never claim a place the invocation did not touch.
    /// Decomposition reads the kept prefixes; a set-aside is not among them.
    #[test]
    fn a_set_aside_never_becomes_a_decision_scope() {
        use crate::core::domain::scope::DecisionScope;

        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2011"), PathBuf::from("/photos/2012")],
            false,
            &roots,
        )
        .unwrap();
        assert_eq!(resolved.set_aside, vec!["/photos/2012".to_string()]);

        let scopes = DecisionScope::decompose(&resolved.prefixes, &roots);
        assert_eq!(
            scopes,
            vec![DecisionScope::new(
                root_id,
                "/photos".to_string(),
                "2011".to_string()
            )]
        );
        assert!(scopes.iter().all(|sc| sc.rel_prefix != "2012"));
    }

    // ========================================================================
    // Form tolerance at the source-existence gate
    // ========================================================================

    /// A composed `é` (U+00E9) and its decomposed twin (`e` + U+0301) — one
    /// visible folder name, two byte-forms.
    const NFC_DIR: &str = "caf\u{e9}";
    const NFD_DIR: &str = "cafe\u{301}";

    #[test]
    fn an_nfc_argument_finds_sources_stored_in_nfd() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/file.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
            false,
            &roots,
        )
        .unwrap();
        assert_eq!(resolved.prefixes, vec![format!("/photos/{NFD_DIR}")]);
    }

    #[test]
    fn an_nfd_argument_finds_sources_stored_in_nfc() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFC_DIR}/file.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from(format!("/photos/{NFD_DIR}"))],
            false,
            &roots,
        )
        .unwrap();
        assert_eq!(resolved.prefixes, vec![format!("/photos/{NFC_DIR}")]);
    }

    /// The form-tolerance rule's pin: the argument bends to the stored form,
    /// and the kept prefix is the stored bytes — so it actually selects the
    /// rows it names, in Rust and in SQL alike.
    #[test]
    fn the_kept_prefix_is_byte_identical_to_the_stored_form() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let stored_rel = format!("{NFD_DIR}/file.jpg");
        insert_source(&conn, root_id, &stored_rel, None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = resolve_scope(
            &conn,
            &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
            false,
            &roots,
        )
        .unwrap();

        let kept = &resolved.prefixes[0];
        let (_, _, _, rel) = domain::root::find_containing_root(kept, &roots).unwrap();
        assert_eq!(rel.as_bytes(), NFD_DIR.as_bytes());
        assert!(repo::source::sources_exist_at_scope(&conn, root_id, &rel).unwrap());
    }

    #[test]
    fn a_path_sourceless_under_every_form_reaches_the_policy_as_sourceless() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Neither form of this folder is known — the gate says so rather
        // than silently keeping one of the candidates.
        let result = resolve_scope(
            &conn,
            &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
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
