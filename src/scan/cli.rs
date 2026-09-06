use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format::shell_quote;
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::scope::{cwd_for, resolve_root_path_any};
use crate::core::repo::{self, Connection, Db};
use crate::progress::Progress;
use crate::scan::ops::hash::HashProgress;
use crate::scan::ops::pipeline::{mark_missing_path, observe_file, scan_root};
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
                    let cwd = cwd_for(std::slice::from_ref(&path.as_path()))?;
                    eprintln!("Warning: skipping {}: {}", path.display(), e);
                    if let Some(hint) = missing_hint(path, &roots, &cwd) {
                        eprintln!("{hint}");
                    }
                    continue;
                }
            };

            // A path argument names a place, and a place may be a single
            // file. Everything that is not a directory takes the file-grain
            // path below — regular files, and the residue the observer counts
            // and declines.
            let names_a_directory = canonical.is_dir();

            // Check if path is inside an existing root (including suspended)
            let (root_id, root_path, scan_prefix, _root_role) = match resolve_root_path_any(
                &roots, &canonical,
            )? {
                Some((id, root_path, existing_role, rel_path)) => {
                    // The door was asked about above, once, over the same
                    // roots and the same resolved paths — a second check here
                    // would be a second spelling of one rule, free to drift
                    // from the sentence the first one speaks.

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
                    // A root is a folder. A file outside every root is refused
                    // by name, and both arms point at its directory: `--add`
                    // has nothing to make a root from, and sending the user to
                    // `--add` with the file path would only land them in the
                    // other refusal.
                    if !names_a_directory {
                        let dir = canonical
                            .parent()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default();
                        if add_root {
                            bail!(
                            "Cannot create a root from '{}': a root is a folder. Scan its directory '{dir}' with --add instead.",
                            canonical.display()
                        );
                        }
                        bail!(
                        "Path '{}' is not inside any existing root. Scan its directory '{dir}' with --add to create a new root.",
                        canonical.display()
                    );
                    }
                    // Path is not inside any root
                    if !add_root {
                        bail!(
                        "Path '{}' is not inside any existing root. Use --add to create a new root.",
                        canonical.display()
                    );
                    }
                    // role is guaranteed to be Some when add_root is true (validated in main.rs)
                    let new_role = role.expect("--role is required with --add");
                    crate::scan::domain::check_not_filesystem_root(&canonical)?;
                    crate::scan::domain::check_no_overlap(&roots, &canonical)?;
                    let new_root = create_root(conn, &canonical, new_role, comment)?;
                    (new_root.id, canonical.clone(), None, new_role.to_string())
                }
            };

            // A root whose own path is not a folder is a fact about that root:
            // said and skipped, like every other per-path trouble in this loop.
            // Refused *before* the scope is recorded — a path that produced no
            // observation must not leave a decision claiming the whole root,
            // which is the `--missing` scope repair's own lesson.
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

/// Where to make the assertion a scan cannot make for itself — offered only
/// where making it would be sound.
///
/// Nothing here infers absence. A path the disk cannot answer for may be
/// deleted, unmounted, or mistyped, and one look cannot tell those apart — so
/// recording a deletion stays the user's own explicit assertion, and this only
/// says where to make it.
///
/// **Which is exactly why it is not offered everywhere the disk goes quiet.**
/// `--missing` marks every source at or under the path it is given, lexically,
/// without asking whether the storage is there — taking the user's word is the
/// whole point of it. So a hint on a path Canon cannot reach is Canon
/// recommending a deletion over the one storage least able to contradict it,
/// and the cost is not recoverable: the rows revive on the next real scan, but
/// the decision row and the source-local receipt asserting a deletion that
/// never happened do not, and writing that receipt recreates the root's own
/// directory on disk — into the mountpoint shell, where it can stand between
/// the volume and its own mount.
///
/// Two questions, and the hint needs both answered:
///
/// 1. **Would the act be accepted?** Asked of `resolve_missing_target`, which
///    is the act's own precondition check, so a suggestion can never name
///    something Canon would refuse. Never re-derived here — two spellings of
///    one rule drift silently, because nothing fails when a hint and a refusal
///    disagree.
/// 2. **Is the storage there to have lost anything?** The act does not ask
///    this and should not; the suggestion must. The evidence is whether the
///    **root's own path** resolves — mount-presence evidence, device's one
///    sanctioned job under the physical-identity law, and the same question the
///    move check asks before believing a file is gone. An unreachable root
///    reads as though everything under it vanished, and a named subtree under
///    absent storage fails to resolve exactly as a deleted one does. A root
///    that answers, with something missing below it, is the case the hint is
///    for.
///
/// The root's own top is withheld for both reasons at once: if the root path
/// does not resolve the storage is the likelier story, and if it does resolve
/// then nothing about it is missing.
fn missing_hint(
    path: &Path,
    roots: &[crate::core::domain::root::Root],
    cwd: &Path,
) -> Option<String> {
    let target = crate::scan::ops::pipeline::resolve_missing_target(path, roots, cwd).ok()?;
    if target.rel_prefix.is_empty() {
        return None;
    }
    if !Path::new(&target.root_path).exists() {
        return None;
    }
    Some(format!(
        "  If it is gone for good, record it with: canon scan --missing {}",
        shell_quote(&path.display().to_string())
    ))
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
    /// completion, so this drives one: a second `--missing` path under no
    /// known root bails after `start()`, `record_scopes` never runs, and the
    /// only scope row that can exist is the one start-time wrote for the
    /// first path. Deleting the lexical fallback leaves an interrupted
    /// decision claiming nowhere.
    ///
    /// **The vehicle used to be a suspended root**, which no longer reaches
    /// this far: the door is asked about once, before the recorder opens a
    /// row, so a refused scan writes no decision at all. The subject here is
    /// start-time decomposition, not the door, and it needs only *some*
    /// failure after `start()`.
    #[test]
    fn a_scan_scope_survives_a_path_that_no_longer_canonicalizes() {
        assert!(
            std::fs::canonicalize("/photos/vacation").is_err(),
            "the fixture path must not exist on disk"
        );
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let db = Db::from_connection(conn);

        let err = run_scan(
            &db,
            &[
                PathBuf::from("/photos/vacation"),
                PathBuf::from("/elsewhere/gone"),
            ],
            true,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not under any known root"), "{err}");

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
    /// The vehicle is a `--missing` path under no known root; a suspended
    /// root used to serve, and no longer reaches past the door.
    #[test]
    fn a_scan_that_errors_after_start_completes_its_decision_as_interrupted() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", false);
        let db = Db::from_connection(conn);

        let err = run_scan(&db, &[PathBuf::from("/elsewhere/gone")], true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not under any known root"), "{err}");

        let (status, summary): (String, Option<String>) = db
            .conn()
            .query_row("SELECT status, summary FROM decisions", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(status, "interrupted");
        assert!(
            summary
                .as_deref()
                .unwrap_or_default()
                .contains("not under any known root"),
            "the summary carries the error: {summary:?}"
        );
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
            line.starts_with("/photos suspended — refused:"),
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
    }

    /// A source root on disk, registered, with one file already indexed.
    fn indexed_root(temp: &TempDir, suspended: bool) -> (Db, i64, PathBuf) {
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
        let (db, root_id, dir) = indexed_root(&temp, false);

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
        let (db, root_id, dir) = indexed_root(&temp, false);
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
        let (db, _root_id, dir) = indexed_root(&temp, true);

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
        let (db, root_id, dir) = indexed_root(&temp, false);
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
        let hint = missing_hint(&file, &roots, temp.path())
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

    /// The hint names the one command that marks sources deleted without
    /// asking the storage anything, so where it is offered is load-bearing.
    /// Withheld wherever taking it would be wrong or useless; offered for the
    /// case it exists for.
    #[test]
    fn the_missing_hint_is_withheld_where_asserting_would_be_wrong() {
        let temp = TempDir::new().unwrap();
        let (db, root_id, dir) = indexed_root(&temp, false);
        let roots = repo::root::fetch_all(db.conn()).unwrap();

        // The permitting control: a named place inside a live root.
        assert!(
            missing_hint(&dir.join("inbox/photo.jpg"), &roots, temp.path()).is_some(),
            "the case the hint is for"
        );

        // A root's own top. An unplugged drive and a deleted folder look
        // identical here, and the blast radius is every source in the root.
        assert_eq!(
            missing_hint(&dir, &roots, temp.path()),
            None,
            "never suggest asserting a whole root gone"
        );

        // A named place under storage that is not there. This is the shape the
        // hint's first repair missed: canonicalization fails identically for
        // every path below an absent root, so keying only on the root's top
        // left the natural gesture — naming the library, not the volume —
        // still recommending a whole-library deletion.
        let parked = temp.path().join("parked");
        std::fs::create_dir_all(parked.join("inbox")).unwrap();
        let parked_root = std::fs::canonicalize(&parked).unwrap();
        repo::insert_test_root(db.conn(), parked_root.to_str().unwrap(), "source", false);
        let roots_with_parked = repo::root::fetch_all(db.conn()).unwrap();
        assert!(
            missing_hint(&parked_root.join("inbox"), &roots_with_parked, temp.path()).is_some(),
            "the control: a subtree of a root whose storage answers"
        );
        std::fs::remove_dir_all(&parked_root).unwrap();
        assert_eq!(
            missing_hint(&parked_root.join("inbox"), &roots_with_parked, temp.path()),
            None,
            "the root itself cannot be reached, so nothing below it is evidence of anything"
        );

        // A path under no known root: --missing refuses it, so the hint would
        // send the user into a dead end.
        assert_eq!(
            missing_hint(&temp.path().join("elsewhere/x.jpg"), &roots, temp.path()),
            None
        );

        // A suspended root: --missing refuses that too.
        db.conn()
            .execute("UPDATE roots SET suspended = 1 WHERE id = ?", [root_id])
            .unwrap();
        let roots = repo::root::fetch_all(db.conn()).unwrap();
        assert_eq!(
            missing_hint(&dir.join("inbox/photo.jpg"), &roots, temp.path()),
            None,
            "the closed door refuses the act, so it is not suggested"
        );
    }

    /// A root whose own path is no longer a folder is a fact about that root.
    /// It is said and skipped, like every other per-path trouble in the loop —
    /// a run over several roots must not end because one of them broke.
    #[test]
    fn a_file_shaped_root_is_skipped_not_fatal() {
        let temp = TempDir::new().unwrap();
        let (db, _root_id, dir) = indexed_root(&temp, false);
        // A second root that is a file on disk. Registered directly: the CLI
        // now refuses to create one, which is the other half of this pair.
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
