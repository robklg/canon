use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format::shell_quote;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::scope::{cwd_for, resolve_root_path_any};
use crate::core::repo::{self, Connection, Db};
use crate::progress::Progress;
use crate::scan::ops::hash::HashProgress;
use crate::scan::ops::pipeline::{mark_missing_path, observe_file, resolve_scan_target, scan_root};
use crate::scan::ops::receipt::write_deletion_receipts;
use crate::scan::ops::types::{
    current_timestamp, DeletionReceiptItem, FileAction, FileToHash, ScanOptions, ScanProgress,
    ScanStats, ScanTarget,
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
        let (open_roots_for_all, _) = crate::core::domain::root::open_roots(&roots);
        let filtered: Vec<&crate::core::domain::root::Root> = open_roots_for_all
            .into_iter()
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
    // The door, asked once and before anything is written. A scan is an act,
    // and an act behind a closed door is refused by name with the way back —
    // never performed, and never leaving a `started` decision row behind to
    // read as a scan killed mid-walk. This precedes the recorder for exactly
    // that reason.
    //
    // Both of scan's arms arrive here: a walk resolves its path by
    // canonicalizing, and `--missing` — whose whole premise is that the
    // folder is gone — resolves lexically against the roots Canon already
    // knows, which is the same list `scan_scope` was just built from.
    for path in &scan_scope {
        if let Some(parked) = crate::core::ops::scope::parked_root_of(path, &roots) {
            return Err(crate::core::domain::root::DoorRefused::new(
                &parked,
                crate::core::domain::root::DoorVerb::Refused,
                path,
            )
            .into());
        }
    }

    // Every path is settled before the row opens, so one refused path stops
    // the whole scan with nothing walked and nothing recorded.
    let targets = paths_to_scan
        .iter()
        .map(|p| resolve_scan_target(conn, p, &roots, add_root, role, missing))
        .collect::<Result<Vec<_>>>()?;

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
        zone: ReadTimesOn::of_user(crate::user_zone()),
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

        for target in targets {
            let (canonical, names_a_directory, root_id, root_path, scan_prefix) = match target {
                ScanTarget::Known {
                    canonical,
                    names_a_directory,
                    root_id,
                    root_path,
                    scan_prefix,
                } => (
                    canonical,
                    names_a_directory,
                    root_id,
                    root_path,
                    scan_prefix,
                ),
                ScanTarget::NewRoot { canonical, role } => {
                    let new_root = create_root(conn, &canonical, &role, comment)?;
                    (canonical.clone(), true, new_root.id, canonical, None)
                }
                ScanTarget::Gone { path, cwd } => {
                    // The folder is gone, so it can't be walked — mark its sources
                    // deleted directly, with the same stamp + source-local receipt
                    // the sweep produces.
                    let result = mark_missing_path(
                        conn,
                        &path,
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
                ScanTarget::Unreadable { path, error, cwd } => {
                    eprintln!("Warning: skipping {}: {}", path.display(), error);
                    if let Some(hint) = missing_hint(conn, &path, &roots, &cwd) {
                        eprintln!("{hint}");
                    }
                    continue;
                }
            };

            // A root whose own path is not a folder is a fact about that root:
            // said and skipped, like every other per-path trouble in this loop.
            // Refused *before* the scope is recorded — a path that produced no
            // observation must not leave a decision claiming the whole root.
            if !names_a_directory && scan_prefix.is_none() {
                eprintln!(
                    "Warning: skipping {}: a root is a folder, and this root's own path is a file.",
                    root_path.display()
                );
                continue;
            }

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

            let result = if names_a_directory {
                // Create directory walker — the interface owns walk configuration
                let walk_path = match &scan_prefix {
                    Some(prefix) => root_path.join(prefix),
                    None => root_path.clone(),
                };
                let walker = scan_walker(&walk_path);

                scan_root(
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
                )?
            } else {
                // The named path is a single file: observed singly, never
                // walked. The remainder is non-empty by construction — a file
                // at a root's own top was refused above, before anything was
                // recorded about it.
                let rel = scan_prefix.as_deref().unwrap_or_default();
                observe_file(
                    conn,
                    root_id,
                    root_path.to_str().context("Root path is not valid UTF-8")?,
                    rel,
                    &canonical,
                    &scan_options,
                    &StderrProgress,
                    now,
                    recorder.decision_id(),
                )?
            };

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

        let summary = total_stats.compose_summary();

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

    // Printed last, because a closed pipe ends the process at this line: the
    // user may lose the summary, never the record of what the scan deleted,
    // nor a warning that the record could not be written.
    println!("{summary}");

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

    // Candidate discovery reads directories and reports directories. A file
    // argument is refused by name rather than answered with an empty list,
    // which would read as "nothing untracked here".
    if !scope.is_dir() {
        bail!(
            "Cannot look for candidate roots under '{}': it is a file, not a folder. Name its directory '{}' instead.",
            scope.display(),
            scope.parent().map(|p| p.display().to_string()).unwrap_or_default()
        );
    }

    // Fetch all roots for path resolution
    let all_roots = repo::root::fetch_all(conn)?;

    // Check if scope is already a root or under a root (including suspended)
    if let Some((id, root_path, role, _)) = resolve_root_path_any(&all_roots, &scope)? {
        // Look up suspension status from cached roots
        let root = all_roots.iter().find(|r| r.id == id);
        let parked = root.and_then(|r| r.parked());
        let suspended_str = crate::core::domain::root::suspended_marker(parked.is_some());

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

    // Get open root paths for candidate discovery
    let (open_for_candidates, _) = crate::core::domain::root::open_roots(&all_roots);
    let root_paths: Vec<PathBuf> = open_for_candidates
        .into_iter()
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

/// Where to make the assertion a scan cannot make for itself.
///
/// The sentence only; **whether** to say it is
/// `ops::pipeline::missing_is_worth_suggesting`, which is where the reasons
/// are written down. They are two — would the act be accepted, and is the
/// storage there to have lost anything — and the second reads the database,
/// so neither can be asked from a screen. A hint that names an act Canon
/// would refuse, or one it would perform and should not, is worse than no
/// hint at all.
fn missing_hint(
    conn: &Connection,
    path: &Path,
    roots: &[crate::core::domain::root::Root],
    cwd: &Path,
) -> Option<String> {
    crate::scan::ops::pipeline::missing_is_worth_suggesting(conn, path, roots, cwd).then(|| {
        format!(
            "  If it is gone for good, record it with: canon scan --missing {}",
            shell_quote(&path.display().to_string())
        )
    })
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

    // Pipeline tests (process_file, mark_missing_sources, mark_missing_path)
    // live in ops::pipeline::tests.

    // =========================================================================
    // Scan honesty: what a --missing scan records, and what an error leaves behind
    // =========================================================================

    #[allow(clippy::too_many_arguments)]
    fn run_scan(db: &Db, paths: &[PathBuf], missing: bool) -> Result<()> {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
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
            },
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
    /// completion, so this drives one: a planted failure stops the marking
    /// after `start()`, `record_scopes` never runs, and the only scope row
    /// that can exist is the one start-time wrote. Deleting the lexical
    /// fallback leaves an interrupted decision claiming nowhere.
    #[test]
    fn a_scan_scope_survives_a_path_that_no_longer_canonicalizes() {
        assert!(
            std::fs::canonicalize("/photos/vacation").is_err(),
            "the fixture path must not exist on disk"
        );
        let (db, root_id) = gone_folder_under_a_root();
        crate::core::testing::plant_failure(db.conn(), "UPDATE", "sources");

        let err = run_scan(&db, &[PathBuf::from("/photos/vacation")], true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("planted"), "{err}");

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
    ///
    /// **Scan's projection of the status conjugation**, examined and declared
    /// conforming: the row's `started` is a claim registered before the run
    /// finished, and this arm settles it at the run's last act. The conjugation
    /// is owned by `core/ops/decision.rs`; scan supplies the word, because
    /// which outcome an error deserves is the caller's knowledge. This test is
    /// that projection's pin — it fails if the arm stops settling.
    ///
    /// It plants a failure while `--missing` marks a real source, because a
    /// refusal never reaches `start()`: `a_refused_scan_writes_no_row` is this
    /// test's refusal twin.
    #[test]
    fn a_scan_that_errors_after_start_completes_its_decision_as_interrupted() {
        let (db, _root_id) = gone_folder_under_a_root();
        crate::core::testing::plant_failure(db.conn(), "UPDATE", "sources");

        let err = run_scan(&db, &[PathBuf::from("/photos/vacation")], true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("planted"), "{err}");

        let (status, summary): (String, Option<String>) = db
            .conn()
            .query_row("SELECT status, summary FROM decisions", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(status, "interrupted");
        assert!(
            summary.as_deref().unwrap_or_default().contains("planted"),
            "the summary carries the error: {summary:?}"
        );
    }

    /// A root that is not on disk, with one present source under a folder that
    /// is not on disk either: the whole premise of `--missing`.
    fn gone_folder_under_a_root() -> (Db, i64) {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        crate::core::testing::insert_source(&conn, root_id, "vacation/img.jpg", None);
        (Db::from_connection(conn), root_id)
    }

    /// **The door precedes the recorder**: a scan aimed behind a closed door
    /// is refused by name with the way back, and leaves nothing behind — not
    /// even the `started` row that would read as a scan killed mid-walk.
    #[test]
    fn a_scan_at_a_parked_root_is_refused_before_any_row_is_written() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", true);
        let db = Db::from_connection(conn);

        let err = run_scan(&db, &[PathBuf::from("/photos/vacation")], true).unwrap_err();
        let line = err.to_string();
        assert!(
            line.starts_with("/photos suspended — rejected:"),
            "the one grammar: {line}"
        );
        assert!(
            line.contains("canon roots unsuspend path:/photos"),
            "{line}"
        );
        assert!(
            err.downcast_ref::<crate::core::domain::root::DoorRefused>()
                .is_some(),
            "carried as the refusal the front door prints without `Error:`"
        );

        let rows: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a refused act writes no decision row");
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

    // =========================================================================
    // File grain: a path argument names a place, and a place may be one file
    // =========================================================================

    /// A scan of real paths on disk, with hashing and receipts under the
    /// caller's control — the flags the file-grain arm actually turns on.
    fn run_scan_on(
        db: &Db,
        paths: &[PathBuf],
        role: Option<&str>,
        add: bool,
        no_hash: bool,
        recording: RecordingMode,
        missing: bool,
    ) -> Result<()> {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                run(
                    db,
                    paths,
                    role,
                    add,
                    None,
                    false,
                    no_hash,
                    false,
                    true,
                    missing,
                    "canon scan",
                    &LedgerConfig {
                        recording,
                        ..LedgerConfig::default()
                    },
                    false,
                    None,
                )
            },
        )
    }

    /// A source root on disk with one file in it, registered — and **nothing
    /// indexed**: the database holds the root and no sources until a caller
    /// scans. A test whose assertion depends on a row existing must run a scan
    /// first, or it asserts over an empty table and passes whatever the code
    /// does.
    fn registered_root(temp: &TempDir, suspended: bool) -> (Db, i64, PathBuf) {
        let dir = temp.path().join("library");
        std::fs::create_dir_all(dir.join("inbox")).unwrap();
        std::fs::write(dir.join("inbox/photo.jpg"), "photo bytes").unwrap();
        let conn = repo::open_in_memory_for_test();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        let root_id =
            repo::insert_test_root(&conn, canonical.to_str().unwrap(), "source", suspended);
        (Db::from_connection(conn), root_id, canonical)
    }

    /// A decision aimed at one file must say so. The `--missing` precedent:
    /// without its own scope the row reads `global` — a whole-universe claim
    /// for an act on one path.
    #[test]
    fn a_file_scan_records_one_decision_scoped_to_the_named_paths() {
        let temp = TempDir::new().unwrap();
        let (db, root_id, dir) = registered_root(&temp, false);

        run_scan_on(
            &db,
            &[dir.join("inbox/photo.jpg")],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap();

        let scopes: Vec<(i64, String)> = db
            .conn()
            .prepare("SELECT root_id, rel_prefix FROM decision_scopes")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(scopes, vec![(root_id, "inbox/photo.jpg".to_string())]);

        let scope: Option<String> = db
            .conn()
            .query_row("SELECT scope FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert!(
            scope.as_deref().unwrap_or_default().contains("photo.jpg"),
            "the display column names the file: {scope:?}"
        );
    }

    /// The hash pass runs over what was observed, on the case the remedy
    /// exists for: a file whose content changed. The `Modified` arm clears
    /// `object_id`, so without the pass the refreshed lock would drop the very
    /// file the remedy was run to save.
    #[test]
    fn a_named_file_is_hashed_like_any_other_observation() {
        let temp = TempDir::new().unwrap();
        let (db, root_id, dir) = registered_root(&temp, false);
        let file = dir.join("inbox/photo.jpg");
        let object_of = |db: &Db| -> Option<i64> {
            db.conn()
                .query_row(
                    "SELECT object_id FROM sources WHERE root_id = ?",
                    [root_id],
                    |r| r.get(0),
                )
                .unwrap()
        };

        // Indexed and hashed by an ordinary walk first.
        run_scan_on(
            &db,
            std::slice::from_ref(&dir),
            None,
            false,
            false,
            RecordingMode::Records,
            false,
        )
        .unwrap();
        let before = object_of(&db).expect("the walk gave it an identity");

        // Now the friction's own shape: the content changes, and only this
        // file is named.
        std::fs::write(&file, "photo bytes, edited and longer").unwrap();
        run_scan_on(
            &db,
            std::slice::from_ref(&file),
            None,
            false,
            false,
            RecordingMode::Records,
            false,
        )
        .unwrap();

        let after = object_of(&db).expect("the observed file still carries an identity");
        assert_ne!(
            after, before,
            "and it is the new content's, not the one the Modified arm cleared"
        );
    }

    /// The closed door: a named file on a suspended root is refused by name,
    /// with the way back stated.
    #[test]
    fn a_scanned_file_on_a_suspended_root_is_refused_by_name() {
        let temp = TempDir::new().unwrap();
        let (db, _root_id, dir) = registered_root(&temp, true);

        let err = run_scan_on(
            &db,
            &[dir.join("inbox/photo.jpg")],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("suspended"), "{err}");
        assert!(err.contains("roots unsuspend"), "the way back: {err}");
    }

    /// A file outside every root is refused by name — and pointed at its
    /// directory, because that is what `--add` can make a root from.
    #[test]
    fn a_scanned_file_outside_every_root_is_refused_by_name() {
        let temp = TempDir::new().unwrap();
        let loose = temp.path().join("loose.jpg");
        std::fs::write(&loose, "bytes").unwrap();
        let db = Db::from_connection(repo::open_in_memory_for_test());

        let err = run_scan_on(
            &db,
            std::slice::from_ref(&loose),
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("loose.jpg"), "the file is named: {err}");
        assert!(
            err.contains(&format!(
                "'{}'",
                std::fs::canonicalize(temp.path()).unwrap().display()
            )),
            "and so is the directory to scan instead: {err}"
        );
    }

    /// A root is a folder, so `--add` naming a file has nothing to make one
    /// from. Refused by name rather than creating a root over a file.
    #[test]
    fn scan_refuses_add_with_a_file_argument() {
        let temp = TempDir::new().unwrap();
        let loose = temp.path().join("loose.jpg");
        std::fs::write(&loose, "bytes").unwrap();
        let db = Db::from_connection(repo::open_in_memory_for_test());

        let err = run_scan_on(
            &db,
            std::slice::from_ref(&loose),
            Some("source"),
            true,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("a root is a folder"), "{err}");
        assert!(
            err.contains(&format!(
                "'{}'",
                std::fs::canonicalize(temp.path()).unwrap().display()
            )),
            "and the directory that could be one is named: {err}"
        );
        let roots: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM roots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(roots, 0, "no root was created from a file");
    }

    /// Candidate discovery reads and reports directories. A file argument is
    /// refused rather than answered with an empty list, which would read as
    /// "nothing untracked here".
    #[test]
    fn scan_candidates_refuses_a_file_argument() {
        let temp = TempDir::new().unwrap();
        let loose = temp.path().join("loose.jpg");
        std::fs::write(&loose, "bytes").unwrap();
        let db = Db::from_connection(repo::open_in_memory_for_test());

        let err = find_candidates(&db, &loose).unwrap_err().to_string();
        assert!(err.contains("it is a file, not a folder"), "{err}");
        assert!(
            err.contains(&format!(
                "'{}'",
                std::fs::canonicalize(temp.path()).unwrap().display()
            )),
            "and its directory is named: {err}"
        );
    }

    /// The two-sided control on the story's forbidden class. A named path that
    /// is gone is skipped and hinted at — never marked deleted; the same path
    /// under `--missing` is marked, stamped and receipted, which is where that
    /// assertion has always lived.
    #[test]
    fn a_scanned_file_never_infers_deletion() {
        let temp = TempDir::new().unwrap();
        let (db, root_id, dir) = registered_root(&temp, false);
        let file = dir.join("inbox/photo.jpg");
        run_scan_on(
            &db,
            std::slice::from_ref(&dir),
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap();
        std::fs::remove_file(&file).unwrap();

        // The observing side: gone is not evidence of deleted.
        run_scan_on(
            &db,
            std::slice::from_ref(&file),
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap();
        let present: i64 = db
            .conn()
            .query_row(
                "SELECT present FROM sources WHERE root_id = ?",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(present, 1, "nothing inferred the deletion");
        let roots = repo::root::fetch_all(db.conn()).unwrap();
        let hint = missing_hint(db.conn(), &file, &roots, temp.path())
            .expect("a named file inside a live root is hinted");
        assert!(hint.contains("--missing"), "the way to assert it: {hint}");
        assert!(hint.contains("photo.jpg"));

        // The asserting side, unchanged: --missing marks it, with its receipt.
        run_scan_on(&db, &[file], None, false, true, RecordingMode::Full, true).unwrap();
        let (present, decision_id): (i64, Option<i64>) = db
            .conn()
            .query_row(
                "SELECT present, decision_id FROM sources WHERE root_id = ?",
                [root_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0, "the explicit assertion marks it");
        assert!(decision_id.is_some(), "and stamps it");
        let ledger = dir.join(".canon-ledger");
        assert!(
            ledger.is_dir() && std::fs::read_dir(&ledger).unwrap().next().is_some(),
            "with a source-local deletion receipt"
        );
    }

    fn decision_count(db: &Db) -> i64 {
        db.conn()
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap()
    }

    fn source_count(db: &Db) -> i64 {
        db.conn()
            .query_row("SELECT COUNT(*) FROM sources", [], |r| r.get(0))
            .unwrap()
    }

    /// **Every per-path refusal precedes the recorder.** Each one says its own
    /// sentence and leaves no row: a row reading `interrupted` would claim a
    /// run that never started. The allowed case is the same library with a
    /// path inside the known root.
    #[test]
    fn a_refused_scan_writes_no_row() {
        let temp = TempDir::new().unwrap();
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let loose = outside.join("loose.jpg");
        std::fs::write(&loose, "bytes").unwrap();
        let outside = std::fs::canonicalize(outside).unwrap();
        let top = std::fs::canonicalize(temp.path()).unwrap();

        struct Case<'a> {
            name: &'a str,
            path: PathBuf,
            role: Option<&'a str>,
            add: bool,
            missing: bool,
            says: &'a str,
        }
        let library = temp.path().join("library");
        let cases = [
            Case {
                name: "directory in no root",
                path: outside.clone(),
                role: None,
                add: false,
                missing: false,
                says: "Use --add to create a new root",
            },
            Case {
                name: "file in no root",
                path: loose.clone(),
                role: None,
                add: false,
                missing: false,
                says: "with --add to create a new root",
            },
            Case {
                name: "--add of a file",
                path: loose.clone(),
                role: Some("source"),
                add: true,
                missing: false,
                says: "a root is a folder",
            },
            Case {
                name: "--add inside a root",
                path: library.join("inbox"),
                role: Some("source"),
                add: true,
                missing: false,
                says: "Remove --add to scan as subtree",
            },
            Case {
                name: "--role mismatch",
                path: library.join("inbox"),
                role: Some("archive"),
                add: false,
                missing: false,
                says: "cannot scan with --role archive",
            },
            Case {
                name: "--add overlapping a root",
                path: top,
                role: Some("source"),
                add: true,
                missing: false,
                says: "overlaps with existing root",
            },
            Case {
                name: "--add at the top of the filesystem",
                path: PathBuf::from("/"),
                role: Some("source"),
                add: true,
                missing: false,
                says: "every path lies under it",
            },
            Case {
                name: "--missing under no root",
                path: PathBuf::from("/elsewhere/gone"),
                role: None,
                add: false,
                missing: true,
                says: "not under any known root",
            },
        ];

        let (db, _root_id, dir) = registered_root(&temp, false);
        for case in &cases {
            let err = run_scan_on(
                &db,
                std::slice::from_ref(&case.path),
                case.role,
                case.add,
                true,
                RecordingMode::Records,
                case.missing,
            )
            .expect_err(case.name)
            .to_string();
            assert!(err.contains(case.says), "{}: {err}", case.name);
            assert_eq!(decision_count(&db), 0, "{}: no row", case.name);
        }

        // The allowed case, in the same state.
        run_scan_on(
            &db,
            &[dir.join("inbox")],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap();
        let status: String = db
            .conn()
            .query_row("SELECT status FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "completed");
    }

    /// One refused path stops the whole scan before anything is walked: a good
    /// path named first is not half of a run that then fails. A path that is
    /// merely gone from disk is a different thing, warned about and skipped.
    #[test]
    fn a_refused_path_among_several_walks_nothing() {
        let temp = TempDir::new().unwrap();
        let (db, _root_id, dir) = registered_root(&temp, false);
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();

        let err = run_scan_on(
            &db,
            &[dir.join("inbox"), outside],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("Use --add to create a new root"), "{err}");
        assert_eq!(decision_count(&db), 0, "no row");
        assert_eq!(source_count(&db), 0, "the good path was not walked");

        // Allowed: the path beside the good one does not exist on disk.
        run_scan_on(
            &db,
            &[dir.join("inbox"), dir.join("gone")],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .expect("a gone path is warned about and skipped");
        assert_eq!(source_count(&db), 1, "the good path was walked");
        let status: String = db
            .conn()
            .query_row("SELECT status FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "completed");
    }

    /// A root whose own path is no longer a folder is a fact about that root.
    /// It is said and skipped, like every other per-path trouble in the loop —
    /// a run over several roots must not end because one of them broke.
    #[test]
    fn a_file_shaped_root_is_skipped_not_fatal() {
        let temp = TempDir::new().unwrap();
        let (db, _root_id, dir) = registered_root(&temp, false);
        // A second root that is a file on disk. Registered directly: the CLI
        // refuses to create one, which is the other half of this pair.
        let broken = temp.path().join("broken-root");
        std::fs::write(&broken, "not a folder").unwrap();
        let canonical = std::fs::canonicalize(&broken).unwrap();
        repo::insert_test_root(db.conn(), canonical.to_str().unwrap(), "source", false);

        run_scan_on(
            &db,
            &[canonical, dir.join("inbox/photo.jpg")],
            None,
            false,
            true,
            RecordingMode::Records,
            false,
        )
        .expect("the broken root does not end the run");

        let status: String = db
            .conn()
            .query_row("SELECT status FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "completed", "and the decision completes");
        // Both named paths are scoped, and that is right: the scan was aimed at
        // the broken root, and start-time decomposition records what a run was
        // aimed at rather than what it managed to observe. Only the completion
        // path is selective, and it records nothing for a path that produced no
        // observation — so the whole-root claim here has exactly one author.
        let mut scopes: Vec<String> = db
            .conn()
            .prepare("SELECT rel_prefix FROM decision_scopes")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        scopes.sort();
        assert_eq!(scopes, vec!["".to_string(), "inbox/photo.jpg".to_string()]);
        // The file that could be observed was, and the run reached the hash
        // debt report past the skipped root.
        let observed: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE rel_path = 'inbox/photo.jpg'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(observed, 1, "the good path was still observed");
    }
}
