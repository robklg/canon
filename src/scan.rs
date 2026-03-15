use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::domain::resolve_root_path_any;
use crate::ops;
use crate::ops::scan::{FileToHash, ScanOptions, ScanStats};
use crate::progress::Progress;
use crate::repo::{self, Connection, Db};

/// Mark all present sources under a path as missing (present=0).
/// Used with `--missing` for deleted folders that no longer exist on disk.
fn mark_missing_path(
    conn: &Connection,
    path: &Path,
    roots: &[crate::domain::root::Root],
    now: i64,
    stats: &mut ScanStats,
) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let cleaned = crate::domain::path::clean_path(path, &cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let (root_id, rel_prefix) = match crate::domain::root::find_containing_root(&cleaned_str, roots)
    {
        Some((id, _root_path, _role, rel)) => (id, rel),
        None => {
            bail!(
                "Cannot mark missing: {} is not under any known root",
                path.display()
            );
        }
    };

    // Fetch present source IDs under this prefix
    let prefix_arg = if rel_prefix.is_empty() {
        None
    } else {
        Some(rel_prefix.as_str())
    };
    let source_ids = repo::source::fetch_source_ids_for_root(conn, root_id, prefix_arg)?;

    if source_ids.is_empty() {
        eprintln!("No present sources found under {}", path.display());
        return Ok(());
    }

    let marked = repo::source::mark_missing(conn, &source_ids, now)?;
    stats.missing += marked;

    Ok(())
}

/// ScanProgress implementation that writes warnings to stderr.
struct StderrProgress;
impl ops::scan::ScanProgress for StderrProgress {
    fn on_file(&self, _path: &str, _action: &ops::scan::FileAction) {}
    fn on_walk_error(&self, error: &str) {
        eprintln!("Warning: {error}");
    }
    fn on_process_error(&self, path: &str, error: &str) {
        eprintln!("Warning: Failed to process {path}: {error}");
    }
}

/// HashProgress implementation that displays a progress bar on stderr.
#[derive(Default)]
struct StderrHashProgress {
    progress: std::cell::RefCell<Option<Progress>>,
}
impl ops::scan::HashProgress for StderrHashProgress {
    fn on_start(&self, total: usize) {
        eprintln!("Computing hashes for {total} files...");
        *self.progress.borrow_mut() = Some(Progress::new(total));
    }
    fn on_hash(&self, index: usize, _path: &Path) {
        if let Some(ref p) = *self.progress.borrow() {
            p.update(index);
        }
    }
    fn on_hash_error(&self, path: &Path, error: &str) {
        eprintln!("\nWarning: Failed to hash {}: {error}", path.display());
    }
    fn on_unexpected_change(&self, path: &Path) {
        eprintln!(
            "\nWarning: hash changed for {} (file may be corrupted or was modified without mtime change)",
            path.display()
        );
    }
    fn on_finish(&self) {
        if let Some(ref p) = *self.progress.borrow() {
            p.finish();
        }
    }
}

pub fn run(
    db: &Db,
    paths: &[PathBuf],
    role: Option<&str>,
    add_root: bool,
    comment: Option<&str>,
    all_roots: bool,
    no_hash: bool,
    verify: bool,
    ignore_device_id: bool,
    missing: bool,
) -> Result<()> {
    // Validate role if provided
    if let Some(r) = role {
        if r != "source" && r != "archive" {
            bail!("Invalid role '{r}'. Must be 'source' or 'archive'");
        }
    }

    let conn = db.conn();
    let now = ops::scan::current_timestamp();

    // Fetch all roots for path resolution and --all filtering
    let roots = repo::root::fetch_all(conn)?;

    // If --all, get all root paths (excluding suspended, optionally filtered by role)
    let paths_to_scan: Vec<PathBuf> = if all_roots {
        let filtered: Vec<&crate::domain::root::Root> = roots
            .iter()
            .filter(|r| r.is_active())
            .filter(|r| match role {
                Some(role_filter) => r.role == role_filter,
                None => true,
            })
            .collect();

        if filtered.is_empty() {
            println!("No roots to scan.");
            return Ok(());
        }

        println!("Scanning {} roots...", filtered.len());
        filtered
            .into_iter()
            .map(|r| PathBuf::from(&r.path))
            .collect()
    } else {
        paths.to_vec()
    };

    let mut total_stats = ScanStats::default();
    let mut all_files_to_hash: Vec<FileToHash> = Vec::new();

    for path in &paths_to_scan {
        let canonical = match fs::canonicalize(path) {
            Ok(p) => p,
            Err(e) => {
                if missing {
                    // User explicitly wants to mark this path's sources as missing
                    mark_missing_path(conn, path, &roots, now, &mut total_stats)?;
                    continue;
                }
                eprintln!("Warning: skipping {}: {}", path.display(), e);
                continue;
            }
        };

        // Check if path is inside an existing root (including suspended)
        let (root_id, root_path, scan_prefix, _root_role) = match resolve_root_path_any(
            &roots, &canonical,
        )? {
            Some((id, root_path, existing_role, rel_path)) => {
                // Path is inside an existing root - check if suspended using cached roots
                let root = roots.iter().find(|r| r.id == id);
                if let Some(r) = root {
                    if r.is_suspended() {
                        bail!(
                            "Root '{root_path}' is suspended. Use 'canon roots unsuspend' to reactivate."
                        );
                    }
                }

                // Path is inside an existing active root
                if add_root {
                    bail!(
                        "Path '{}' is already inside {} root '{}'. Remove --add to scan as subtree.",
                        canonical.display(),
                        existing_role,
                        root_path
                    );
                }
                // Check role matches if --role was specified
                if let Some(r) = role {
                    if existing_role != r {
                        bail!(
                            "Root '{root_path}' has role '{existing_role}', cannot scan with --role {r}"
                        );
                    }
                }
                let scan_prefix = if rel_path.is_empty() {
                    None // Scanning entire root
                } else {
                    Some(rel_path) // Scanning subtree
                };
                (id, PathBuf::from(root_path), scan_prefix, existing_role)
            }
            None => {
                // Path is not inside any root
                if !add_root {
                    bail!(
                        "Path '{}' is not inside any existing root. Use --add to create a new root.",
                        canonical.display()
                    );
                }
                // role is guaranteed to be Some when add_root is true (validated in main.rs)
                let new_role = role.expect("--role is required with --add");
                check_overlapping_roots(&roots, &canonical)?;
                let new_root = create_root(conn, &canonical, new_role, comment)?;
                (new_root.id, canonical.clone(), None, new_role.to_string())
            }
        };

        // Determine if we should hash this root
        // Default: hash new/changed files; --no-hash to skip; --verify to rehash all
        let scan_options = ScanOptions {
            hash: !no_hash,
            hash_all: verify,
            ignore_device_id,
        };

        // Create directory walker — the interface owns walk configuration
        let walk_path = match &scan_prefix {
            Some(prefix) => root_path.join(prefix),
            None => root_path.clone(),
        };
        let walker = WalkDir::new(&walk_path).follow_links(false);

        let result = ops::scan::scan_root(
            conn,
            root_id,
            root_path.to_str().context("Root path is not valid UTF-8")?,
            scan_prefix.as_deref(),
            walker.into_iter(),
            &scan_options,
            &StderrProgress,
            now,
        )?;

        // Display warnings from ops layer
        for warning in &result.warnings {
            eprintln!("Warning: {warning}");
        }

        // Update last_scanned_at only for full root scans (not subdirectory scans)
        if scan_prefix.is_none() {
            repo::root::update_last_scanned_at(conn, root_id, now)?;
        }

        total_stats.scanned += result.stats.scanned;
        total_stats.new += result.stats.new;
        total_stats.updated += result.stats.updated;
        total_stats.moved += result.stats.moved;
        total_stats.unchanged += result.stats.unchanged;
        total_stats.missing += result.stats.missing;
        total_stats.disconnected += result.stats.disconnected;
        total_stats.skipped += result.stats.skipped;

        // Collect files for hashing
        all_files_to_hash.extend(result.files_to_hash);
    }

    // Build summary message
    let mut summary = format!(
        "Scanned {} files: {} new, {} updated, {} moved, {} unchanged, {} missing",
        total_stats.scanned,
        total_stats.new,
        total_stats.updated,
        total_stats.moved,
        total_stats.unchanged,
        total_stats.missing
    );
    if total_stats.skipped > 0 {
        summary.push_str(&format!(", {} skipped (read errors)", total_stats.skipped));
    }
    if total_stats.disconnected > 0 {
        summary.push_str(&format!(
            ", {} skipped (disconnected)",
            total_stats.disconnected
        ));
    }
    println!("{summary}");

    // Hash collected files via ops layer
    if !all_files_to_hash.is_empty() {
        let hash_progress = StderrHashProgress::default();
        let hash_stats = ops::scan::hash_files(conn, &all_files_to_hash, &hash_progress)?;
        total_stats.hashed = hash_stats.hashed;
        total_stats.unexpected_hash_changes = hash_stats.unexpected_hash_changes;
        println!("Hashed {} files", hash_stats.hashed);
    }

    // Exit with error if there were unexpected hash changes (possible corruption)
    if total_stats.unexpected_hash_changes > 0 {
        bail!(
            "{} files have unexpected hash changes (file may be corrupted or was modified without mtime change)",
            total_stats.unexpected_hash_changes
        );
    }

    // Update query planner statistics after significant bulk changes.
    // ANALYZE is expensive on large databases — only run when row counts
    // changed enough to meaningfully affect query planning.
    let rows_changed = total_stats.new + total_stats.missing;
    if rows_changed >= 100 {
        db.run_analyze()?;
    }

    Ok(())
}

fn create_root(
    conn: &Connection,
    path: &Path,
    role: &str,
    comment: Option<&str>,
) -> Result<crate::domain::root::Root> {
    let path_str = path.to_str().context("Path is not valid UTF-8")?;
    repo::root::create(conn, path_str, role, comment)
}

fn check_overlapping_roots(all_roots: &[crate::domain::root::Root], new_path: &Path) -> Result<()> {
    let new_path_str = new_path.to_str().context("Path is not valid UTF-8")?;

    for root in all_roots {
        let existing = &root.path;
        if existing == new_path_str {
            continue; // Same path, not overlapping
        }

        let existing_path = Path::new(&existing);

        // Check if new path is inside existing root
        if new_path.starts_with(existing_path) {
            bail!(
                "Path {} overlaps with existing root {}",
                new_path.display(),
                existing
            );
        }

        // Check if existing root is inside new path
        if existing_path.starts_with(new_path) {
            bail!(
                "Path {} overlaps with existing root {}",
                new_path.display(),
                existing
            );
        }
    }

    Ok(())
}

/// Find directories with files that aren't under any root
pub fn find_candidates(db: &Db, scope_path: &Path) -> Result<()> {
    let conn = db.conn();
    let scope = fs::canonicalize(scope_path)
        .with_context(|| format!("Failed to canonicalize path: {}", scope_path.display()))?;

    // Fetch all roots for path resolution
    let all_roots = repo::root::fetch_all(conn)?;

    // Check if scope is already a root or under a root (including suspended)
    if let Some((id, root_path, role, _)) = resolve_root_path_any(&all_roots, &scope)? {
        // Look up suspension status from cached roots
        let root = all_roots.iter().find(|r| r.id == id);
        let suspended = root.map(|r| r.is_suspended()).unwrap_or(false);
        let suspended_str = if suspended { " (suspended)" } else { "" };

        if scope.to_string_lossy() == root_path {
            println!(
                "{} is already a {} root{}",
                scope.display(),
                role,
                suspended_str
            );
        } else {
            println!(
                "{} is already under {} root {}{}",
                scope.display(),
                role,
                root_path,
                suspended_str
            );
        }
        return Ok(());
    }

    // Get all existing roots (excluding suspended for candidate discovery)
    let roots: Vec<PathBuf> = all_roots
        .iter()
        .filter(|r| r.is_active())
        .map(|r| PathBuf::from(&r.path))
        .collect();

    // Find directories with files, skipping tracked subtrees
    let mut dirs_with_files: HashSet<PathBuf> = HashSet::new();
    scan_for_untracked(&scope, &roots, &mut dirs_with_files)?;

    if dirs_with_files.is_empty() {
        println!(
            "No untracked directories with files found under {}",
            scope.display()
        );
        return Ok(());
    }

    // Find shortest common ancestors (bounded by scope)
    let candidates = find_common_ancestors(&dirs_with_files, &roots, &scope);

    println!("Candidate roots to add:");
    for (path, count) in candidates {
        if count == 1 {
            println!("  {}  (1 directory with files)", path.display());
        } else {
            println!("  {}  ({} directories with files)", path.display(), count);
        }
    }

    Ok(())
}

/// Recursively scan for untracked directories with files
fn scan_for_untracked(dir: &Path, roots: &[PathBuf], result: &mut HashSet<PathBuf>) -> Result<()> {
    // Skip if this directory is under an existing root
    if roots
        .iter()
        .any(|root| dir == root || dir.starts_with(root))
    {
        return Ok(());
    }

    let entries: Vec<_> = match fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(e) => {
            eprintln!("Warning: cannot read {}: {}", dir.display(), e);
            return Ok(());
        }
    };

    // Check if this directory has any files (stop at first one found)
    let has_file = entries
        .iter()
        .any(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false));

    // Check if this directory contains any root (can't be added as a root - invariant)
    let contains_root = roots
        .iter()
        .any(|root| root.starts_with(dir) && root != dir);

    if has_file && !contains_root {
        // Found a file and directory doesn't contain any roots: record it
        result.insert(dir.to_path_buf());
    } else {
        // Either no files here, or directory contains roots: recurse into subdirs
        for entry in entries {
            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                scan_for_untracked(&entry.path(), roots, result)?;
            }
        }
    }

    Ok(())
}

/// Find the shortest common ancestors for a set of directories
fn find_common_ancestors(
    dirs_with_files: &HashSet<PathBuf>,
    roots: &[PathBuf],
    scope: &Path,
) -> Vec<(PathBuf, usize)> {
    use std::collections::HashMap;

    let mut ancestors: HashMap<PathBuf, usize> = HashMap::new();

    for dir in dirs_with_files {
        // Walk up the path, find the highest ancestor not under a root
        let mut current = dir.clone();
        let mut highest_untracked = dir.clone();

        while let Some(parent) = current.parent() {
            // Stop if we've reached the scope boundary (don't walk up to scope itself)
            if parent == scope || !parent.starts_with(scope) {
                break;
            }

            // Stop if we hit a root
            if roots
                .iter()
                .any(|root| parent == root || parent.starts_with(root))
            {
                break;
            }

            // Stop if parent contains a root (don't suggest parent of existing root)
            if roots.iter().any(|root| root.starts_with(parent)) {
                break;
            }

            highest_untracked = parent.to_path_buf();
            current = parent.to_path_buf();
        }

        *ancestors.entry(highest_untracked).or_insert(0) += 1;
    }

    // Sort by path for consistent output
    let mut result: Vec<_> = ancestors.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Pipeline tests (process_file, mark_missing_sources) moved to ops::scan::tests
    // =========================================================================
    // mark_missing_path tests
    // =========================================================================

    fn make_test_root(id: i64, path: &str) -> crate::domain::root::Root {
        crate::domain::root::Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn mark_missing_path_marks_sources() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        // Insert 5 sources under vacation/
        for i in 0..5 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("vacation/img{i}.jpg"),
                1,
                100 + i,
                1000,
                1000,
            );
        }

        let mut stats = ScanStats::default();
        mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &roots,
            9999,
            &mut stats,
        )
        .unwrap();

        assert_eq!(stats.missing, 5);

        // Verify all sources are not present
        let present: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(present, 0);
    }

    #[test]
    fn mark_missing_path_not_under_any_root() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        let mut stats = ScanStats::default();
        let result = mark_missing_path(
            &conn,
            Path::new("/nonexistent/path"),
            &roots,
            9999,
            &mut stats,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not under any known root"));
    }

    #[test]
    fn mark_missing_path_prefix_matches_subset() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        // Insert sources under vacation/ and work/
        for i in 0..3 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("vacation/img{i}.jpg"),
                1,
                200 + i,
                1000,
                1000,
            );
        }
        for i in 0..2 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("work/doc{i}.pdf"),
                1,
                300 + i,
                1000,
                1000,
            );
        }

        let mut stats = ScanStats::default();
        mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &roots,
            9999,
            &mut stats,
        )
        .unwrap();

        assert_eq!(stats.missing, 3);

        // Work sources should still be present
        let work_present: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path LIKE 'work/%' AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(work_present, 2);
    }

    #[test]
    fn mark_missing_path_already_not_present() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        let sid = repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 400, 1000, 1000);
        // Mark not-present manually
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [sid])
            .unwrap();

        let mut stats = ScanStats::default();
        mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &roots,
            9999,
            &mut stats,
        )
        .unwrap();

        // mark_missing only updates present=1 rows, so count is 0
        assert_eq!(stats.missing, 0);
    }

    #[test]
    fn mark_missing_path_empty_prefix_marks_all() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        for i in 0..4 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("img{i}.jpg"),
                1,
                500 + i,
                1000,
                1000,
            );
        }

        let mut stats = ScanStats::default();
        mark_missing_path(&conn, Path::new("/photos"), &roots, 9999, &mut stats).unwrap();

        assert_eq!(stats.missing, 4);
    }

    #[test]
    fn mark_missing_path_no_sources_found() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos")];

        // No sources inserted — path resolves but nothing to mark
        let mut stats = ScanStats::default();
        mark_missing_path(&conn, Path::new("/photos/empty"), &roots, 9999, &mut stats).unwrap();

        assert_eq!(stats.missing, 0);
    }
}
