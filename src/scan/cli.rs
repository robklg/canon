use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::scope::{cwd_for, resolve_root_path_any};
use crate::core::repo::{self, Connection, Db};
use crate::progress::Progress;
use crate::scan::ops::hash::HashProgress;
use crate::scan::ops::pipeline::{mark_missing_path, scan_root};
use crate::scan::ops::receipt::write_deletion_receipts;
use crate::scan::ops::types::{
    current_timestamp, DeletionReceiptItem, FileAction, FileToHash, ScanOptions, ScanProgress,
    ScanStats,
};
use crate::scan::ops::{candidates::find_root_candidates, hash::run_hash_pass};

/// ScanProgress implementation that writes warnings to stderr.
struct StderrProgress;
impl ScanProgress for StderrProgress {
    fn on_file(&self, _path: &str, _action: &FileAction) {}
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
impl HashProgress for StderrHashProgress {
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

#[allow(clippy::too_many_arguments)]
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
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    // Validate role if provided
    if let Some(r) = role {
        if r != "source" && r != "archive" {
            bail!("Invalid role '{r}'. Must be 'source' or 'archive'");
        }
    }

    let conn = db.conn();
    let now = current_timestamp();

    // Fetch all roots for path resolution and --all filtering
    let roots = repo::root::fetch_all(conn)?;

    // If --all, get all root paths (excluding suspended, optionally filtered by role)
    let paths_to_scan: Vec<PathBuf> = if all_roots {
        let filtered: Vec<&crate::core::domain::root::Root> = roots
            .iter()
            .filter(|r| r.is_active())
            .filter(|r| match role {
                Some(role_filter) => r.role == role_filter,
                None => true,
            })
            .collect();

        if filtered.is_empty() {
            println!("No roots to scan.");
            // Returning before the recorder starts is deliberate: a scan with
            // nothing to walk records no decision.
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

    // Canonicalize the scan paths so they can be matched to their roots. A
    // path the disk can no longer answer for — the `--missing` case, whose
    // whole premise is that the folder is gone — still resolves lexically
    // against the roots Canon already knows, so the decision is scoped from
    // the start rather than only at completion. A path that isn't under a
    // known root at all (a `--add` root doesn't exist at start()) simply
    // produces no DecisionScope — the type makes a stray "." unrecordable,
    // and record_scopes() captures the new root at completion.
    let scan_scope: Vec<String> = paths_to_scan
        .iter()
        .filter_map(|p| match std::fs::canonicalize(p) {
            Ok(c) => Some(c.to_string_lossy().into_owned()),
            Err(_) => {
                let cwd = cwd_for(std::slice::from_ref(&p.as_path())).ok()?;
                crate::core::domain::path::resolve_path(p, &roots, &cwd)
            }
        })
        .collect();
    let decision = DecisionParams {
        command: DecisionCommand::Scan,
        scope: DecisionScope::decompose(&scan_scope, &roots),
        command_line: command_line.to_string(),
        reason: reason
            .map(|r| r.to_string())
            .filter(|r| !r.trim().is_empty()),
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
    };
    let mut recorder = DecisionRecorder::start(conn, &decision, None);

    // Everything that can fail after the decision row exists runs inside this
    // closure, so there is exactly one exit for a failure. Two-phase recording
    // exists to bracket work that cannot be rolled back; a `started` row left
    // behind by an early return would read as a scan killed mid-walk, which is
    // a different and more alarming claim than the error that actually
    // occurred.
    let body = (|| -> Result<(ScanStats, String)> {
        let mut total_stats = ScanStats::default();
        let mut all_files_to_hash: Vec<FileToHash> = Vec::new();
        // Deletions grouped by the root that lost them — one source-local receipt each.
        let mut deleted_by_root: Vec<(i64, String, Vec<DeletionReceiptItem>)> = Vec::new();
        // Resolved scope of each walked path, recorded at completion so a --add scan's
        // freshly created root lands in the durable scope index (it didn't exist at start()).
        let mut scope_pairs: Vec<DecisionScope> = Vec::new();

        for path in &paths_to_scan {
            let canonical = match fs::canonicalize(path) {
                Ok(p) => p,
                Err(e) => {
                    // Reaching this arm is the condition for --missing: the path
                    // could not be resolved, so it cannot be walked. Handling
                    // --missing anywhere else would mark live files deleted.
                    if missing {
                        // The folder is gone, so it can't be walked — mark its sources
                        // deleted directly, with the same stamp + source-local receipt
                        // the sweep produces.
                        let cwd = cwd_for(std::slice::from_ref(&path.as_path()))?;
                        let result = mark_missing_path(
                            conn,
                            path,
                            &roots,
                            &cwd,
                            now,
                            recorder.decision_id(),
                            decision.receipt_enabled,
                        )?;
                        // The folder this deletion was aimed at is the decision's
                        // scope. Without it the record would say `global` — a
                        // whole-universe claim for an act on one folder.
                        scope_pairs.push(DecisionScope::new(
                            result.root_id,
                            result.root_path.clone(),
                            result.rel_prefix.clone(),
                        ));
                        if result.missing_count == 0 {
                            eprintln!("No present sources found under {}", path.display());
                        } else {
                            total_stats.missing += result.missing_count;
                            if !result.deleted_items.is_empty() {
                                deleted_by_root.push((
                                    result.root_id,
                                    result.root_path,
                                    result.deleted_items,
                                ));
                            }
                        }
                        continue;
                    }
                    // A relative path against a working directory that no
                    // longer exists fails as "no such file", which names the
                    // wrong thing: the path may well be there, and only the
                    // place it was to be resolved from is gone. Ask for the
                    // directory this path actually needs and let its refusal
                    // speak instead — a scan that could resolve nothing must
                    // not report a walk of nothing.
                    cwd_for(std::slice::from_ref(&path.as_path()))?;
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
                    crate::scan::domain::check_no_overlap(&roots, &canonical)?;
                    let new_root = create_root(conn, &canonical, new_role, comment)?;
                    (new_root.id, canonical.clone(), None, new_role.to_string())
                }
            };

            // Record this path's resolved scope (root + subtree). Captures roots just
            // created above, which weren't present for the start()-time decomposition.
            scope_pairs.push(DecisionScope::new(
                root_id,
                root_path.to_string_lossy().to_string(),
                scan_prefix.clone().unwrap_or_default(),
            ));

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
            let walker = scan_walker(&walk_path);

            let result = scan_root(
                conn,
                root_id,
                root_path.to_str().context("Root path is not valid UTF-8")?,
                scan_prefix.as_deref(),
                walker,
                &scan_options,
                &StderrProgress,
                now,
                recorder.decision_id(),
                decision.receipt_enabled,
            )?;

            // Display warnings from ops layer
            for warning in &result.warnings {
                eprintln!("Warning: {warning}");
            }

            // Update last_scanned_at only for full root scans (not subdirectory scans)
            if scan_prefix.is_none() {
                crate::scan::repo::root::update_last_scanned_at(conn, root_id, now)?;
            }

            total_stats.absorb(&result.stats);

            // Collect files for hashing
            all_files_to_hash.extend(result.files_to_hash);

            // Group deletions by their root for source-local receipts.
            if !result.deleted_items.is_empty() {
                deleted_by_root.push((
                    root_id,
                    root_path.to_string_lossy().to_string(),
                    result.deleted_items,
                ));
            }
        }

        // Hash collected files via ops layer, then read what is still unhashed in
        // the scanned scope — one call, so the debt can only ever be counted after
        // the pay-down it survived.
        let hash_progress = StderrHashProgress::default();
        let pass = run_hash_pass(conn, &all_files_to_hash, &scope_pairs, &hash_progress)?;
        total_stats.carry_hash_pass(&pass);

        // Print summary (composed by ops)
        let summary = total_stats.compose_summary();
        println!("{}", summary);

        // Record the scope index for every walked root (including any created this run)
        // before linking receipts to those roots. Idempotent with the start()-time write.
        scope_pairs.sort();
        scope_pairs.dedup();
        recorder.record_scopes(conn, &scope_pairs);

        // Write source-local deletion receipts (one per root that lost sources) before
        // finalizing the decision. Skipped when receipts are disabled or nothing was deleted.
        write_deletion_receipts(conn, &mut recorder, &decision, deleted_by_root, &summary);

        Ok((total_stats, summary))
    })();

    let (total_stats, summary) = match body {
        Ok(finished) => finished,
        Err(e) => {
            recorder.complete(
                conn,
                DecisionStatus::Interrupted,
                DecisionCounts {
                    attempted: None,
                    completed: None,
                    failed: None,
                    skipped: None,
                },
                &format!("interrupted: {e}"),
            );
            for w in recorder.take_warnings() {
                eprintln!("{w}");
            }
            return Err(e);
        }
    };

    // Complete decision recording
    let total_processed =
        total_stats.new + total_stats.updated + total_stats.moved + total_stats.unchanged;
    recorder.complete(
        conn,
        DecisionStatus::Completed,
        DecisionCounts {
            attempted: Some(total_stats.scanned as i64),
            completed: Some(total_processed as i64),
            failed: None,
            skipped: Some(total_stats.skipped as i64),
        },
        &summary,
    );
    for w in recorder.take_warnings() {
        eprintln!("{w}");
    }

    // Exit with error if there were unexpected hash changes (possible corruption).
    // Raised only after the decision and its receipts are finalized above —
    // the scan's observations must be recorded even when it exits non-zero.
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
        eprintln!("Updating query statistics...");
        db.run_analyze()?;
    }

    Ok(())
}

fn create_root(
    conn: &Connection,
    path: &Path,
    role: &str,
    comment: Option<&str>,
) -> Result<crate::core::domain::root::Root> {
    let path_str = path.to_str().context("Path is not valid UTF-8")?;
    crate::scan::repo::root::create(conn, path_str, role, comment)
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

    // Get active root paths for candidate discovery
    let root_paths: Vec<PathBuf> = all_roots
        .iter()
        .filter(|r| r.is_active())
        .map(|r| PathBuf::from(&r.path))
        .collect();

    let result = find_root_candidates(&scope, &root_paths)?;

    for warning in &result.warnings {
        eprintln!("Warning: {warning}");
    }

    if result.candidates.is_empty() {
        println!(
            "No untracked directories with files found under {}",
            scope.display()
        );
        return Ok(());
    }

    println!("Candidate roots to add:");
    for candidate in &result.candidates {
        if candidate.dir_count == 1 {
            println!("  {}  (1 directory with files)", candidate.path.display());
        } else {
            println!(
                "  {}  ({} directories with files)",
                candidate.path.display(),
                candidate.dir_count
            );
        }
    }

    Ok(())
}

/// The scan's directory walker. The `.canon-ledger` filter here is the only
/// thing keeping receipt files out of the index — without it, canon would
/// index its own ledger as sources.
fn scan_walker(
    path: &std::path::Path,
) -> impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>> {
    WalkDir::new(path)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !(e.file_type().is_dir() && e.file_name() == ".canon-ledger"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Pipeline tests (process_file, mark_missing_sources) and mark_missing_path
    // (relocated in the deletion-fate work) live in ops::pipeline::tests.

    // =========================================================================
    // Scan honesty: what a --missing scan records, and what an error leaves behind
    // =========================================================================

    #[allow(clippy::too_many_arguments)]
    fn run_scan(db: &Db, paths: &[PathBuf], missing: bool) -> Result<()> {
        run(
            db,
            paths,
            None,
            false,
            None,
            false,
            true,
            false,
            true,
            missing,
            "canon scan",
            &LedgerConfig {
                recording: RecordingMode::Records,
                ..LedgerConfig::default()
            },
            true,
            None,
        )
    }

    /// A deletion aimed at one folder must be recorded against that folder.
    /// Without its own scope the decision reads `global` in the trail — a
    /// whole-universe claim for an act on one place.
    #[test]
    fn a_missing_scan_records_the_folder_it_was_aimed_at() {
        let conn = repo::open_in_memory_for_test();
        // Neither the root nor the folder exists on disk — which is the whole
        // premise of `--missing`.
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash,
                                  scanned_at, last_seen_at, device, inode, present)
             VALUES (?, 'vacation/img.jpg', 10, 1000, '', 1000, 1000, 1, 200, 1)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let db = Db::from_connection(conn);

        run_scan(&db, &[PathBuf::from("/photos/vacation")], true).unwrap();

        let (root, rel): (i64, String) = db
            .conn()
            .query_row("SELECT root_id, rel_prefix FROM decision_scopes", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(root, root_id);
        assert_eq!(rel, "vacation");

        // And the display column the trail renders is not NULL.
        let scope: Option<String> = db
            .conn()
            .query_row("SELECT scope FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert!(
            scope
                .as_deref()
                .unwrap_or_default()
                .contains("/photos/vacation"),
            "the display column names the folder, not the whole universe: {scope:?}"
        );
    }

    /// Start-time decomposition falls back to lexical resolution, so the
    /// scope is on the record from the moment the decision row exists — not
    /// only backfilled at completion.
    ///
    /// The distinction is only visible on a run that never *reaches*
    /// completion, which is what this drives: a suspended root makes the
    /// `--missing` arm bail after `start()`, so `record_scopes` never runs
    /// and the only scope row that can exist is the one start-time wrote.
    /// Deleting the lexical fallback leaves an interrupted decision claiming
    /// nowhere.
    #[test]
    fn a_scan_scope_survives_a_path_that_no_longer_canonicalizes() {
        assert!(
            std::fs::canonicalize("/photos/vacation").is_err(),
            "the fixture path must not exist on disk"
        );
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", true);
        let db = Db::from_connection(conn);

        let err = run_scan(&db, &[PathBuf::from("/photos/vacation")], true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("suspended"), "{err}");

        let status: String = db
            .conn()
            .query_row("SELECT status FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "interrupted", "the run must not reach completion");

        let (root, rel): (i64, String) = db
            .conn()
            .query_row("SELECT root_id, rel_prefix FROM decision_scopes", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("start-time decomposition must have written the scope row");
        assert_eq!(root, root_id);
        assert_eq!(rel, "vacation");
    }

    /// The two-phase recorder exists to bracket work that cannot be rolled
    /// back. An error after `start()` must close its own row: a `started` row
    /// left behind reads as a scan killed mid-walk, which is a different and
    /// more alarming claim than the error that actually happened.
    #[test]
    fn a_scan_that_errors_after_start_completes_its_decision_as_interrupted() {
        let conn = repo::open_in_memory_for_test();
        // A suspended root: the walk arm refuses it, after the decision row
        // has already been opened.
        repo::insert_test_root(&conn, "/photos", "source", true);
        let db = Db::from_connection(conn);

        let err = run_scan(&db, &[PathBuf::from("/photos")], true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("suspended"), "{err}");

        let (status, summary): (String, Option<String>) = db
            .conn()
            .query_row("SELECT status, summary FROM decisions", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(status, "interrupted");
        assert!(
            summary.as_deref().unwrap_or_default().contains("suspended"),
            "the summary carries the error: {summary:?}"
        );
    }

    // =========================================================================
    // .canon-ledger/ scan exclusion tests
    // =========================================================================

    use std::fs;

    struct NoopProgress;
    impl ScanProgress for NoopProgress {
        fn on_file(&self, _: &str, _: &FileAction) {}
        fn on_walk_error(&self, _: &str) {}
        fn on_process_error(&self, _: &str, _: &str) {}
    }

    fn run_filtered_scan(root_path: &std::path::Path) -> crate::scan::ops::types::ScanRootResult {
        let conn = repo::open_in_memory_for_test();
        let root_path_str = root_path.to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path_str, "source", false);
        let options = ScanOptions {
            hash: false,
            hash_all: false,
            ignore_device_id: true,
        };
        let now = current_timestamp();

        // The production walker itself, not a copy of its filter — deleting
        // the filter from scan_walker must fail these tests.
        let walker = scan_walker(root_path);

        scan_root(
            &conn,
            root_id,
            root_path_str,
            None,
            walker,
            &options,
            &NoopProgress,
            now,
            None,
            false,
        )
        .unwrap()
    }

    #[test]
    fn test_scan_skips_canon_ledger_dir() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        // A regular file that should be indexed
        fs::write(root.join("photo.jpg"), "data").unwrap();

        // Files inside .canon-ledger/ — should be skipped entirely
        fs::create_dir(root.join(".canon-ledger")).unwrap();
        fs::write(root.join(".canon-ledger").join("receipt.toml"), "data").unwrap();
        fs::write(root.join(".canon-ledger").join("other.toml"), "data").unwrap();

        let result = run_filtered_scan(root);

        // Only photo.jpg should be scanned, nothing from .canon-ledger/
        assert_eq!(result.stats.new, 1);
        assert_eq!(result.stats.scanned, 1);
    }

    #[test]
    fn test_scan_does_not_skip_similar_names() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        // These directories should NOT be excluded
        fs::create_dir(root.join(".canon-ledger-old")).unwrap();
        fs::write(root.join(".canon-ledger-old").join("file.toml"), "data").unwrap();

        fs::create_dir(root.join("canon-ledger")).unwrap();
        fs::write(root.join("canon-ledger").join("file.toml"), "data").unwrap();

        let result = run_filtered_scan(root);

        // Both files should be scanned
        assert_eq!(result.stats.new, 2);
        assert_eq!(result.stats.scanned, 2);
    }
}
