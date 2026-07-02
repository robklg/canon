//! Scan pipeline: observe→reconcile→persist.
//!
//! The interface creates the directory walker and passes entries here.
//! This module processes each entry through the pipeline, detects missing
//! sources, and returns typed results. A `ScanProgress` trait provides
//! per-file observability without writing to stderr.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{Transaction, TransactionBehavior};

use crate::domain::decision::DecisionStatus;
use crate::domain::scan::{find_missing, reconcile, FileObservation, Reconciliation};
use crate::ops::decision::{DecisionParams, DecisionRecorder};
use crate::ops::fs::compute_partial_hash;
use crate::ops::receipt::{DeletionReceipt, DeletionReceiptItem, ReceiptPlacement};
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

/// Classification of a source's fate during scan.
pub enum SourceOutcome {
    Seen,
    Missing,
    Disconnected,
}

/// Action taken for a processed file.
pub enum FileAction {
    New,
    Modified,
    Moved,
    Unchanged,
}

/// Accumulated scan statistics.
#[derive(Default)]
pub struct ScanStats {
    pub scanned: u64,
    pub new: u64,
    pub updated: u64,
    pub moved: u64,
    pub unchanged: u64,
    pub missing: u64,
    pub disconnected: u64,
    pub skipped: u64,
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
}

impl ScanStats {
    /// Compose the scan summary message.
    pub fn compose_summary(&self) -> String {
        let mut summary = format!(
            "Scanned {} files: {} new, {} updated, {} moved, {} unchanged, {} missing",
            self.scanned, self.new, self.updated, self.moved, self.unchanged, self.missing
        );
        if self.skipped > 0 {
            summary.push_str(&format!(", {} skipped (read errors)", self.skipped));
        }
        if self.disconnected > 0 {
            summary.push_str(&format!(", {} skipped (disconnected)", self.disconnected));
        }
        if self.hashed > 0 {
            summary.push_str(&format!("\nHashed {} files", self.hashed));
        }
        summary
    }
}

/// A file that needs full hashing after the walk completes.
pub struct FileToHash {
    pub source_id: i64,
    pub full_path: PathBuf,
    pub old_object_id: Option<i64>,
    pub basis_changed: bool,
}

/// Result of scanning a single root.
pub struct ScanRootResult {
    pub stats: ScanStats,
    pub files_to_hash: Vec<FileToHash>,
    /// Sources that went missing during this scan, captured before the
    /// `present → absent` flip for the deletion receipt. Empty when receipt
    /// capture is off (`capture_deletions = false`) or nothing was deleted.
    pub deleted_items: Vec<DeletionReceiptItem>,
    /// Warnings collected during scan (disconnected storage, errors).
    pub warnings: Vec<String>,
}

/// Parameters controlling scan behavior.
pub struct ScanOptions {
    /// Whether to compute partial hashes during the walk.
    pub hash: bool,
    /// Whether to re-hash files that already have a hash.
    pub hash_all: bool,
    /// Whether to treat device ID mismatches as missing (--ignore-device-id).
    pub ignore_device_id: bool,
}

/// Observability for the scan pipeline. The interface implements this
/// to update progress bars, emit warnings, etc.
pub trait ScanProgress {
    /// Called after each file is processed.
    fn on_file(&self, path: &str, action: &FileAction);
    /// Called when a walk error is encountered (e.g., permission denied).
    fn on_walk_error(&self, error: &str);
    /// Called when process_file fails for a specific file.
    fn on_process_error(&self, path: &str, error: &str);
}

// ============================================================================
// Pipeline functions
// ============================================================================

/// Scan a root directory, processing each entry through the
/// observe→reconcile→persist pipeline.
///
/// The interface creates the directory walker and passes entries here.
/// This function:
/// 1. Fetches expected source IDs (for missing detection)
/// 2. Processes each entry via process_file()
/// 3. Detects missing sources via domain::scan::find_missing()
/// 4. Marks missing/disconnected via mark_missing_sources()
///
/// Returns accumulated stats, files needing hashing, and warnings.
pub fn scan_root(
    conn: &Connection,
    root_id: i64,
    root_path: &str,
    scan_prefix: Option<&str>,
    entries: impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>>,
    options: &ScanOptions,
    progress: &dyn ScanProgress,
    now: i64,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<ScanRootResult> {
    let root_path = Path::new(root_path);
    let mut stats = ScanStats::default();
    let mut seen_source_ids: HashSet<i64> = HashSet::new();
    let mut files_to_hash: Vec<FileToHash> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    // Track outcomes for sources (for mount protection)
    let mut outcomes: Vec<(i64, SourceOutcome)> = Vec::new();
    let mut handled_ids: HashSet<i64> = HashSet::new();

    // Batch buffer for unchanged file updates (source_id, device, inode)
    let mut unchanged_batch: Vec<(i64, i64, i64)> = Vec::new();

    // Snapshot the walk path's device ID before the walk starts.
    // If the mount disappears mid-scan (NAS disconnect, volume ejected),
    // the OS silently tears down the mount — files just vanish rather than
    // returning errors. We detect this by comparing device IDs before and
    // after the walk; a mismatch means the mount changed and missing
    // detection would be unreliable.
    let walk_root = match scan_prefix {
        Some(prefix) => root_path.join(prefix),
        None => root_path.to_path_buf(),
    };
    let pre_walk_device = get_dir_device(&walk_root);

    // Fetch expected source IDs at start (for missing detection via pure function)
    let expected_ids: HashSet<i64> =
        repo::source::fetch_source_ids_for_root(conn, root_id, scan_prefix)?
            .into_iter()
            .collect();

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                progress.on_walk_error(&e.to_string());
                continue;
            }
        };

        // Handle empty directories — may contain sources on disconnected mounts
        if entry.file_type().is_dir() {
            if is_empty_dir(entry.path()) {
                if let Some(current_dev) = get_dir_device(entry.path()) {
                    let rel = entry
                        .path()
                        .strip_prefix(root_path)
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    let (dir_outcomes, dir_warnings) =
                        classify_sources_in_empty_dir(conn, root_id, &rel, current_dev)?;
                    warnings.extend(dir_warnings);
                    for (id, outcome) in dir_outcomes {
                        handled_ids.insert(id);
                        outcomes.push((id, outcome));
                    }
                }
            }
            continue;
        }

        if !entry.file_type().is_file() {
            continue;
        }

        let full_path = entry.path();
        let rel_path = full_path
            .strip_prefix(root_path)
            .context("Failed to strip root prefix")?;

        let rel_path_str = rel_path.to_str().context("Path is not valid UTF-8")?;

        let metadata = match fs::metadata(full_path) {
            Ok(m) => m,
            Err(e) => {
                progress.on_process_error(
                    &full_path.display().to_string(),
                    &format!("Failed to stat: {e}"),
                );
                continue;
            }
        };

        let device = metadata.dev() as i64;
        let inode = metadata.ino() as i64;
        let size = metadata.size() as i64;
        let mtime = metadata.mtime();

        stats.scanned += 1;

        // Phase 1: Reconcile (read DB state, determine outcome, compute partial hash)
        let reconciled = match reconcile_file(
            conn,
            root_id,
            rel_path_str,
            full_path,
            device,
            inode,
            size,
            mtime,
        ) {
            Ok(r) => r,
            Err(e) => {
                progress.on_process_error(&full_path.display().to_string(), &e.to_string());
                stats.skipped += 1;
                continue;
            }
        };

        // Phase 2: Persist — unchanged files are batched, others get individual transactions
        let (action, source_id, old_object_id) = match &reconciled.reconciliation {
            Reconciliation::Unchanged { source_id } => {
                unchanged_batch.push((*source_id, device, inode));
                if unchanged_batch.len() >= UNCHANGED_BATCH_SIZE {
                    flush_unchanged(conn, &unchanged_batch, now)?;
                    unchanged_batch.clear();
                }
                (
                    FileAction::Unchanged,
                    *source_id,
                    reconciled.source_at_path.and_then(|s| s.object_id),
                )
            }
            _ => {
                // Only New reconciliations receive decision_id (conservative scan semantics).
                // Modified, Moved preserve the existing value via omission in SQL.
                let file_decision_id = match &reconciled.reconciliation {
                    Reconciliation::New => decision_id,
                    _ => None,
                };
                let source = match persist_file(
                    conn,
                    &reconciled.observation,
                    &reconciled.reconciliation,
                    now,
                    file_decision_id,
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        progress.on_process_error(&full_path.display().to_string(), &e.to_string());
                        stats.skipped += 1;
                        continue;
                    }
                };
                let (action, old_oid) = match &reconciled.reconciliation {
                    Reconciliation::New => (FileAction::New, None),
                    Reconciliation::Modified { old_object_id, .. } => {
                        (FileAction::Modified, *old_object_id)
                    }
                    Reconciliation::Moved { old_object_id, .. } => {
                        (FileAction::Moved, *old_object_id)
                    }
                    Reconciliation::Unchanged { .. } => unreachable!(),
                };
                (action, source.id, old_oid)
            }
        };

        // Notify progress
        progress.on_file(rel_path_str, &action);

        // Track seen sources
        seen_source_ids.insert(source_id);
        outcomes.push((source_id, SourceOutcome::Seen));

        match action {
            FileAction::New => stats.new += 1,
            FileAction::Modified => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }

        // Collect files for hashing based on mode
        if options.hash {
            let needs_hash = match action {
                FileAction::New | FileAction::Modified => true,
                FileAction::Moved | FileAction::Unchanged => options.hash_all,
            };
            if needs_hash {
                files_to_hash.push(FileToHash {
                    source_id,
                    full_path: full_path.to_path_buf(),
                    old_object_id,
                    basis_changed: matches!(action, FileAction::New | FileAction::Modified),
                });
            }
        }
    }

    // Flush remaining unchanged files
    flush_unchanged(conn, &unchanged_batch, now)?;

    // Check if the mount is still the same device after the walk.
    // If the device changed (or disappeared), the mount was disrupted during
    // the scan — skip missing detection to avoid falsely marking files as gone.
    let post_walk_device = get_dir_device(&walk_root);
    let mount_stable = pre_walk_device.is_some()
        && post_walk_device.is_some()
        && pre_walk_device == post_walk_device;

    if mount_stable {
        // Identify sources that are truly missing using pure domain function
        // Sources not seen during walk AND not handled by empty-dir logic are missing
        let all_accounted: HashSet<i64> = seen_source_ids.union(&handled_ids).copied().collect();
        let missing_ids = find_missing(&expected_ids, &all_accounted);
        for id in missing_ids {
            outcomes.push((id, SourceOutcome::Missing));
        }
    } else if pre_walk_device != post_walk_device {
        warnings.push(
            "Mount changed during scan — skipping missing detection to avoid data loss".to_string(),
        );
    }

    // Mark missing/disconnected files based on outcomes. Deletion receipt items
    // are captured before the flip (when capturing), so they carry each source's
    // pre-flip provenance link.
    let (missing_count, disconnected_count, deleted_items, missing_warnings) =
        mark_missing_sources(
            conn,
            &outcomes,
            now,
            options.ignore_device_id,
            decision_id,
            capture_deletions,
        )?;
    warnings.extend(missing_warnings);
    stats.missing = missing_count;
    stats.disconnected = disconnected_count;

    Ok(ScanRootResult {
        stats,
        files_to_hash,
        deleted_items,
        warnings,
    })
}

/// Intermediate result from reconciling a file against DB state (before persistence).
struct ReconcileResult {
    observation: FileObservation,
    reconciliation: Reconciliation,
    /// The source at this path before reconciliation (for old_object_id on Unchanged).
    source_at_path: Option<crate::domain::source::Source>,
}

/// Reconcile a single file: read DB state, determine outcome, compute partial hash if needed.
/// Does NOT persist — caller decides how to write (batch or individual transaction).
fn reconcile_file(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    full_path: &Path,
    device: i64,
    inode: i64,
    size: i64,
    mtime: i64,
) -> Result<ReconcileResult> {
    let mut observation = FileObservation {
        root_id,
        rel_path: rel_path.to_string(),
        device: device as u64,
        inode: inode as u64,
        size,
        mtime,
        partial_hash: None,
    };

    let source_at_path = repo::source::fetch_by_path(conn, root_id, rel_path)?;
    let source_by_inode = repo::source::fetch_by_inode(conn, device as u64, inode as u64)?;

    let reconciliation = reconcile(
        &observation,
        source_at_path.as_ref(),
        source_by_inode.as_ref(),
    );

    // Compute partial_hash outside the transaction — filesystem I/O can be slow
    // on NAS/network storage and must not hold the write lock
    if reconciliation.needs_partial_hash() {
        observation.partial_hash = Some(compute_partial_hash(full_path, size as u64)?);
    }

    Ok(ReconcileResult {
        observation,
        reconciliation,
        source_at_path,
    })
}

/// Persist a non-unchanged reconciliation in its own transaction.
fn persist_file(
    conn: &Connection,
    observation: &FileObservation,
    reconciliation: &Reconciliation,
    now: i64,
    decision_id: Option<i64>,
) -> Result<crate::domain::source::Source> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let source =
        repo::source::apply_reconciliation(&tx, observation, reconciliation, now, decision_id)?;
    tx.commit()?;
    Ok(source)
}

/// Flush accumulated unchanged file updates in a single transaction.
fn flush_unchanged(conn: &Connection, batch: &[(i64, i64, i64)], now: i64) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    repo::source::batch_update_unchanged(&tx, batch, now)?;
    tx.commit()?;
    Ok(())
}

const UNCHANGED_BATCH_SIZE: usize = 500;

/// Translate source outcomes to DB mutations.
///
/// When `capture_deletions` is set, the sources about to be marked missing are
/// snapshotted **before** the flip (so their captured `previous_decision_id` is the
/// pre-flip value) and returned as receipt items; otherwise the returned Vec is empty.
///
/// Returns (missing_count, disconnected_count, deleted_items, warnings).
fn mark_missing_sources(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<(u64, u64, Vec<DeletionReceiptItem>, Vec<String>)> {
    let mut missing_ids: Vec<i64> = Vec::new();
    let mut disconnected_count = 0u64;

    for (id, outcome) in outcomes {
        match outcome {
            SourceOutcome::Seen => {}
            SourceOutcome::Missing => {
                missing_ids.push(*id);
            }
            SourceOutcome::Disconnected => {
                if ignore_device_id {
                    missing_ids.push(*id);
                } else {
                    disconnected_count += 1;
                }
            }
        }
    }

    // Capture receipt items before the flip; mark_missing then stamps decision_id
    // on exactly these sources (stamp-set = receipt-set).
    let deleted_items = if capture_deletions {
        capture_deletion_items(conn, &missing_ids)?
    } else {
        Vec::new()
    };

    let missing_count = repo::source::mark_missing(conn, &missing_ids, now, decision_id)?;

    let mut warnings = Vec::new();
    if disconnected_count > 0 {
        warnings.push(format!(
            "Skipped {disconnected_count} files (device ID mismatch - possibly disconnected storage)"
        ));
        warnings.push(
            "  If device IDs changed (e.g., NAS remount), re-run with --ignore-device-id"
                .to_string(),
        );
    }

    Ok((missing_count, disconnected_count, deleted_items, warnings))
}

/// Snapshot sources about to be marked missing into deletion-receipt items.
/// Must be called before `mark_missing` flips them so each item's
/// `previous_decision_id` is the pre-flip provenance link. Items are sorted by
/// rel_path for a stable, readable receipt.
fn capture_deletion_items(
    conn: &Connection,
    missing_ids: &[i64],
) -> Result<Vec<DeletionReceiptItem>> {
    if missing_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut items: Vec<DeletionReceiptItem> = repo::source::fetch_for_receipt(conn, missing_ids)?
        .into_iter()
        .map(|s| DeletionReceiptItem {
            rel_path: s.rel_path,
            hash: s.hash,
            size: s.size,
            mtime: s.mtime,
            previous_decision_id: s.previous_decision_id,
        })
        .collect();
    items.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(items)
}

/// Write source-local deletion receipts — one per root that lost sources — under
/// the single scan decision.
///
/// Placement and existence are known only after the walk, so this runs at
/// completion. Gated on `params.receipt_enabled` and a live decision id; a root
/// with no deleted items is skipped, so a scan that deletes nothing writes no
/// receipt. Each receipt lands at its own root's `.canon-ledger/` — the loss
/// travels with that drive. Every written receipt is linked to its root in the
/// scope index (`decision_scopes.receipt_rel_path`) so a by-root query recovers
/// the decision and its receipt — the many-receipts-per-decision case the single
/// `decisions.receipt_*` columns can't hold. Write and index failures are
/// collected as recorder warnings, never halting the scan.
pub fn write_deletion_receipts(
    conn: &Connection,
    recorder: &mut DecisionRecorder,
    params: &DecisionParams,
    per_root: Vec<(i64, String, Vec<DeletionReceiptItem>)>,
    summary: &str,
) {
    if !params.receipt_enabled {
        return;
    }
    let Some(decision_id) = recorder.decision_id() else {
        return;
    };
    let command = params.command.as_str();

    for (root_id, root_path, items) in per_root {
        if items.is_empty() {
            continue;
        }
        let receipt = DeletionReceipt {
            meta: params.receipt_meta(decision_id, DecisionStatus::Completed, summary, None),
            items,
        };
        let placement = ReceiptPlacement::LedgerRoot { root_id, root_path };
        if let Some(receipt_ref) =
            recorder.write_placed_receipt(&placement, command, &receipt, summary)
        {
            if let Err(e) = repo::decision::set_scope_receipt(
                conn,
                decision_id,
                receipt_ref.root_id,
                &receipt_ref.rel_path,
            ) {
                recorder.push_warning(format!(
                    "Warning: failed to index deletion receipt for root {}: {e}",
                    receipt_ref.root_id
                ));
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Classify sources under an empty directory by comparing stored device to current device.
/// Returns outcomes and any warnings about disconnected storage.
fn classify_sources_in_empty_dir(
    conn: &Connection,
    root_id: i64,
    rel_prefix: &str,
    current_device: i64,
) -> Result<(Vec<(i64, SourceOutcome)>, Vec<String>)> {
    let sources = repo::source::fetch_device_info_by_prefix(conn, root_id, rel_prefix)?;

    let mut disconnected_count = 0usize;
    let results: Vec<_> = sources
        .into_iter()
        .map(|(id, stored_device)| {
            let outcome = match stored_device {
                Some(dev) if dev != current_device => {
                    disconnected_count += 1;
                    SourceOutcome::Disconnected
                }
                _ => SourceOutcome::Missing,
            };
            (id, outcome)
        })
        .collect();

    let mut warnings = Vec::new();
    if disconnected_count > 0 {
        let path_desc = if rel_prefix.is_empty() {
            "(root)"
        } else {
            rel_prefix
        };
        warnings.push(format!(
            "{path_desc} contains {disconnected_count} files on different device (possibly disconnected storage)"
        ));
    }

    Ok((results, warnings))
}

/// Get device ID of a directory (Unix only).
fn get_dir_device(path: &Path) -> Option<i64> {
    fs::metadata(path).ok().map(|m| m.dev() as i64)
}

/// Check if a directory is empty (no entries).
fn is_empty_dir(path: &Path) -> bool {
    fs::read_dir(path)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
}

// ============================================================================
// Root candidate discovery
// ============================================================================

/// A candidate root directory discovered by scanning for untracked files.
pub struct RootCandidate {
    /// Absolute path to the candidate directory.
    pub path: PathBuf,
    /// Number of directories with files under this candidate.
    pub dir_count: usize,
}

/// Result of scanning for untracked root candidates.
pub struct CandidateResult {
    /// Candidate root directories, sorted by path.
    pub candidates: Vec<RootCandidate>,
    /// Warnings encountered during filesystem walk (e.g., permission errors).
    pub warnings: Vec<String>,
}

/// Scan a scope directory for untracked files not under any known root,
/// then collapse the results into candidate root directories.
///
/// `root_paths` should contain only active (non-suspended) root paths.
pub fn find_root_candidates(scope: &Path, root_paths: &[PathBuf]) -> Result<CandidateResult> {
    let mut dirs_with_files: HashSet<PathBuf> = HashSet::new();
    let mut warnings: Vec<String> = Vec::new();

    scan_for_untracked(scope, root_paths, &mut dirs_with_files, &mut warnings)?;

    let candidates = find_common_ancestors(&dirs_with_files, root_paths, scope)
        .into_iter()
        .map(|(path, dir_count)| RootCandidate { path, dir_count })
        .collect();

    Ok(CandidateResult {
        candidates,
        warnings,
    })
}

/// Recursively scan for directories with files not under any root.
fn scan_for_untracked(
    dir: &Path,
    roots: &[PathBuf],
    result: &mut HashSet<PathBuf>,
    warnings: &mut Vec<String>,
) -> Result<()> {
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
            warnings.push(format!("cannot read {}: {e}", dir.display()));
            return Ok(());
        }
    };

    // Check if this directory has any files (stop at first one found)
    let has_file = entries
        .iter()
        .any(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false));

    // Check if this directory contains any root (can't be added as a root — invariant)
    let contains_root = roots
        .iter()
        .any(|root| root.starts_with(dir) && root != dir);

    if has_file && !contains_root {
        result.insert(dir.to_path_buf());
    } else {
        for entry in entries {
            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                scan_for_untracked(&entry.path(), roots, result, warnings)?;
            }
        }
    }

    Ok(())
}

/// Find shortest common ancestors for a set of directories,
/// bounded by scope and not crossing root boundaries.
fn find_common_ancestors(
    dirs_with_files: &HashSet<PathBuf>,
    roots: &[PathBuf],
    scope: &Path,
) -> Vec<(PathBuf, usize)> {
    let mut ancestors: HashMap<PathBuf, usize> = HashMap::new();

    for dir in dirs_with_files {
        let mut current = dir.clone();
        let mut highest_untracked = dir.clone();

        while let Some(parent) = current.parent() {
            if parent == scope || !parent.starts_with(scope) {
                break;
            }
            if roots
                .iter()
                .any(|root| parent == root || parent.starts_with(root))
            {
                break;
            }
            if roots.iter().any(|root| root.starts_with(parent)) {
                break;
            }

            highest_untracked = parent.to_path_buf();
            current = parent.to_path_buf();
        }

        *ancestors.entry(highest_untracked).or_insert(0) += 1;
    }

    let mut result: Vec<_> = ancestors.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

pub fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

// ============================================================================
// Hash pipeline
// ============================================================================

/// Observability for the hash pipeline. The interface implements this
/// to display progress bars, emit warnings, etc.
pub trait HashProgress {
    fn on_start(&self, total: usize);
    fn on_hash(&self, index: usize, path: &Path);
    fn on_hash_error(&self, path: &Path, error: &str);
    fn on_unexpected_change(&self, path: &Path);
    fn on_finish(&self);
}

/// Result of the hash pipeline.
#[derive(Default)]
pub struct HashStats {
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
    pub errors: u64,
}

/// Hash files collected during scan, linking each to its content object.
///
/// For each file: computes full SHA256, creates/looks up the object,
/// links the source, stores the hash fact. Each file is wrapped in its
/// own Immediate transaction for atomicity without blocking concurrent
/// processes for long periods.
///
/// Individual hash I/O errors are reported via `progress` and skipped
/// (not fatal). DB/transaction errors propagate as `Err`.
pub fn hash_files(
    conn: &Connection,
    files: &[FileToHash],
    progress: &dyn HashProgress,
) -> Result<HashStats> {
    if files.is_empty() {
        return Ok(HashStats::default());
    }

    progress.on_start(files.len());

    let mut stats = HashStats::default();

    for (i, file) in files.iter().enumerate() {
        progress.on_hash(i, &file.full_path);

        // Compute full SHA256 hash
        let hash_value = match crate::ops::fs::compute_full_hash(&file.full_path) {
            Ok(h) => h,
            Err(e) => {
                progress.on_hash_error(&file.full_path, &format!("{:#}", e));
                stats.errors += 1;
                continue;
            }
        };

        // Wrap object creation + source linking + fact storage in a single
        // transaction for atomicity. Uses Immediate for reliable busy-handler
        // support under concurrency.
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

        let new_object = get_or_create_object(&tx, "sha256", &hash_value)?;

        // Check for unexpected hash change (only if basis didn't change and file had existing hash)
        if !file.basis_changed {
            if let Some(old_oid) = file.old_object_id {
                if old_oid != new_object.id {
                    progress.on_unexpected_change(&file.full_path);
                    stats.unexpected_hash_changes += 1;
                }
            }
        }

        repo::source::set_object_id(&tx, file.source_id, new_object.id)?;

        repo::fact::store_object_fact(
            &tx,
            new_object.id,
            "content.hash.sha256",
            &hash_value,
            current_timestamp(),
        )?;

        tx.commit()?;

        stats.hashed += 1;
    }

    progress.on_finish();

    Ok(stats)
}

/// Get or create an object by hash, returning the Object.
pub fn get_or_create_object(
    conn: &Connection,
    hash_type: &str,
    hash_value: &str,
) -> Result<crate::domain::object::Object> {
    repo::object::get_or_create(conn, hash_type, hash_value)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    /// No-op progress implementation for tests.
    struct NoopProgress;
    impl ScanProgress for NoopProgress {
        fn on_file(&self, _path: &str, _action: &FileAction) {}
        fn on_walk_error(&self, _error: &str) {}
        fn on_process_error(&self, _path: &str, _error: &str) {}
    }

    /// Test result from process_file helper.
    struct ProcessResult {
        source_id: i64,
        action: FileAction,
        old_object_id: Option<i64>,
    }

    /// Test helper: reconcile + persist a single file (replicates old process_file behavior).
    fn process_file(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        full_path: &Path,
        device: i64,
        inode: i64,
        size: i64,
        mtime: i64,
        now: i64,
    ) -> Result<ProcessResult> {
        let reconciled = reconcile_file(
            conn, root_id, rel_path, full_path, device, inode, size, mtime,
        )?;

        match &reconciled.reconciliation {
            Reconciliation::Unchanged { source_id } => {
                // Persist unchanged inline (no batching in tests)
                flush_unchanged(conn, &[(*source_id, device, inode)], now)?;
                Ok(ProcessResult {
                    source_id: *source_id,
                    action: FileAction::Unchanged,
                    old_object_id: reconciled.source_at_path.and_then(|s| s.object_id),
                })
            }
            _ => {
                let source = persist_file(
                    conn,
                    &reconciled.observation,
                    &reconciled.reconciliation,
                    now,
                    None,
                )?;
                let (action, old_object_id) = match &reconciled.reconciliation {
                    Reconciliation::New => (FileAction::New, None),
                    Reconciliation::Modified { old_object_id, .. } => {
                        (FileAction::Modified, *old_object_id)
                    }
                    Reconciliation::Moved { old_object_id, .. } => {
                        (FileAction::Moved, *old_object_id)
                    }
                    Reconciliation::Unchanged { .. } => unreachable!(),
                };
                Ok(ProcessResult {
                    source_id: source.id,
                    action,
                    old_object_id,
                })
            }
        }
    }

    /// Create a temp file with content and return (path, device, inode, size, mtime).
    fn create_temp_file(dir: &TempDir, name: &str, content: &str) -> (PathBuf, u64, u64, i64, i64) {
        let path = dir.path().join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        drop(file);

        let meta = fs::metadata(&path).unwrap();
        (
            path,
            meta.dev(),
            meta.ino(),
            meta.len() as i64,
            meta.modified()
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        )
    }

    #[test]
    fn process_file_new() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) = create_temp_file(&temp_dir, "new.txt", "content");
        let now = current_timestamp();

        let result = process_file(
            &conn,
            root_id,
            "new.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::New));

        let source = repo::source::fetch_by_path(&conn, root_id, "new.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_unchanged() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "unchanged.txt", "content");

        repo::insert_test_source(
            &conn,
            root_id,
            "unchanged.txt",
            device as i64,
            inode as i64,
            size,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "unchanged.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Unchanged));
    }

    #[test]
    fn process_file_modified_size() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "modified.txt", "new content");

        repo::insert_test_source(
            &conn,
            root_id,
            "modified.txt",
            device as i64,
            inode as i64,
            5,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "modified.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Modified));
    }

    #[test]
    fn process_file_moved() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "new_name.txt", "content");

        repo::insert_test_source(
            &conn,
            root_id,
            "old_name.txt",
            device as i64,
            inode as i64,
            size,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "new_name.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Moved));
    }

    #[test]
    fn process_file_device_changed() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) = create_temp_file(&temp_dir, "file.txt", "content");

        // Pre-insert with DIFFERENT device but same inode
        repo::insert_test_source(&conn, root_id, "file.txt", 99999, inode as i64, size, mtime);

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "file.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Unchanged));

        let source = repo::source::fetch_by_path(&conn, root_id, "file.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.device, device as i64);
    }

    #[test]
    fn process_file_replaced() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "replaced.txt", "new content");

        let old_inode = inode + 99999;
        repo::insert_test_source(
            &conn,
            root_id,
            "replaced.txt",
            device as i64,
            old_inode as i64,
            50,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "replaced.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::New));

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "replaced.txt"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let source = repo::source::fetch_by_path(&conn, root_id, "replaced.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.inode, inode as i64);
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_revives_stale_record() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "revived.txt", "new content");

        let old_source_id = repo::insert_test_source(&conn, root_id, "revived.txt", 1, 1, 50, 1000);
        conn.execute(
            "UPDATE sources SET present = 0 WHERE id = ?",
            [old_source_id],
        )
        .unwrap();

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "revived.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::New));

        let source = repo::source::fetch_by_path(&conn, root_id, "revived.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.id, old_source_id);
        assert_eq!(source.inode, inode as i64);
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_independent_operations() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path1, dev1, ino1, size1, mtime1) =
            create_temp_file(&temp_dir, "new.txt", "new content");
        let (path2, dev2, ino2, size2, mtime2) =
            create_temp_file(&temp_dir, "existing.txt", "existing");
        let (path3, dev3, ino3, size3, mtime3) =
            create_temp_file(&temp_dir, "modified.txt", "modified content");

        repo::insert_test_source(
            &conn,
            root_id,
            "existing.txt",
            dev2 as i64,
            ino2 as i64,
            size2,
            mtime2,
        );
        repo::insert_test_source(
            &conn,
            root_id,
            "modified.txt",
            dev3 as i64,
            ino3 as i64,
            5,
            mtime3,
        );

        let now = current_timestamp();

        let r1 = process_file(
            &conn,
            root_id,
            "new.txt",
            &path1,
            dev1 as i64,
            ino1 as i64,
            size1,
            mtime1,
            now,
        )
        .unwrap();
        let r2 = process_file(
            &conn,
            root_id,
            "existing.txt",
            &path2,
            dev2 as i64,
            ino2 as i64,
            size2,
            mtime2,
            now,
        )
        .unwrap();
        let r3 = process_file(
            &conn,
            root_id,
            "modified.txt",
            &path3,
            dev3 as i64,
            ino3 as i64,
            size3,
            mtime3,
            now,
        )
        .unwrap();

        assert!(matches!(r1.action, FileAction::New));
        assert!(matches!(r2.action, FileAction::Unchanged));
        assert!(matches!(r3.action, FileAction::Modified));

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn mark_missing_sources_counts_correctly() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "file1.txt", 1, 1, 100, 1000);
        let id2 = repo::insert_test_source(&conn, root_id, "file2.txt", 1, 2, 100, 1000);
        let id3 = repo::insert_test_source(&conn, root_id, "file3.txt", 1, 3, 100, 1000);

        let outcomes = vec![
            (id1, SourceOutcome::Seen),
            (id2, SourceOutcome::Missing),
            (id3, SourceOutcome::Disconnected),
        ];

        let now = current_timestamp();
        let (missing_count, disconnected_count, _items, warnings) =
            mark_missing_sources(&conn, &outcomes, now, false, None, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 1);
        assert!(!warnings.is_empty()); // Should have disconnected warning

        let s1 = repo::source::fetch_by_path(&conn, root_id, "file1.txt").unwrap();
        assert!(s1.is_some());

        let s2: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id2], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s2, 0);

        let s3: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id3], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s3, 1);
    }

    #[test]
    fn mark_missing_sources_disconnected_with_ignore_flag() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "file1.txt", 1, 1, 100, 1000);

        let outcomes = vec![(id1, SourceOutcome::Disconnected)];

        let now = current_timestamp();
        let (missing_count, disconnected_count, _items, warnings) =
            mark_missing_sources(&conn, &outcomes, now, true, None, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 0);
        assert!(warnings.is_empty()); // No warnings when ignore_device_id=true
    }

    #[test]
    fn mark_missing_sources_stamps_decision_id() {
        // The sweep threads the scan decision_id into the deletion transition.
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        let outcomes = vec![(id1, SourceOutcome::Missing)];

        let now = current_timestamp();
        let (missing_count, _, _items, _) =
            mark_missing_sources(&conn, &outcomes, now, false, Some(123), false).unwrap();
        assert_eq!(missing_count, 1);

        let decision_id: Option<i64> = conn
            .query_row("SELECT decision_id FROM sources WHERE id = ?", [id1], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(decision_id, Some(123));
    }

    // =========================================================================
    // Deletion receipt capture + writing
    // =========================================================================

    use crate::domain::config::{LedgerConfig, RecordingMode};
    use crate::domain::decision::DecisionCommand;
    use crate::ops::decision::DecisionCounts;
    use walkdir::WalkDir;

    /// Build a `.canon-ledger`-filtered walker over `root`, like the interface does.
    fn walk(root: &Path) -> impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>> {
        WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !(e.file_type().is_dir() && e.file_name() == ".canon-ledger"))
    }

    fn no_hash_options() -> ScanOptions {
        ScanOptions {
            hash: false,
            hash_all: false,
            ignore_device_id: false,
        }
    }

    #[test]
    fn scan_root_captures_deletion_before_flip() {
        // A source whose file is gone is captured for the receipt with its pre-scan
        // decision_id, then stamped with the scan's — stamp-set = receipt-set.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        // "gone.txt" is expected (present in DB) but absent on disk → missing.
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        conn.execute("UPDATE sources SET decision_id = 42 WHERE id = ?", [gone])
            .unwrap();
        // A real file keeps the walk non-empty (New, seen).
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(temp.path()),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(99),
            true,
        )
        .unwrap();

        // Receipt-set: exactly the deleted source, with its pre-flip provenance link.
        assert_eq!(result.deleted_items.len(), 1);
        let item = &result.deleted_items[0];
        assert_eq!(item.rel_path, "gone.txt");
        assert_eq!(item.previous_decision_id, Some(42));
        assert!(item.hash.is_none());

        // Stamp-set: the same source is now absent and stamped with the scan decision.
        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [gone],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(99));
    }

    #[test]
    fn scan_root_no_capture_when_disabled() {
        // Records mode (receipts off): the source is still marked missing and
        // stamped, but no items are captured for a receipt.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        // A real file keeps the root non-empty so "gone.txt" is inferred missing
        // (not routed through empty-dir device classification).
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(temp.path()),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(7),
            false,
        )
        .unwrap();

        assert!(result.deleted_items.is_empty());
        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [gone],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(7));
    }

    #[test]
    fn scan_root_unstable_mount_records_no_deletion() {
        // When the walk root's device is unavailable (an unstable mount), missing
        // detection is skipped, so a pre-inserted source is neither marked missing
        // nor captured — the guard prevents false deletion records.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let missing_root = temp.path().join("not-there");
        let root_path = missing_root.to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(&missing_root),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert!(result.deleted_items.is_empty());
        assert_eq!(result.stats.missing, 0);
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [gone], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            present, 1,
            "source must not be marked missing on unstable mount"
        );
    }

    fn scan_params(recording: RecordingMode, no_receipt: bool) -> DecisionParams {
        DecisionParams {
            command: DecisionCommand::Scan,
            scope: None,
            command_line: "canon scan".to_string(),
            reason: None,
            record_enabled: recording != RecordingMode::Off,
            receipt_enabled: recording == RecordingMode::Full && !no_receipt,
            ledger_config: LedgerConfig {
                recording,
                ..LedgerConfig::default()
            },
        }
    }

    fn sample_items() -> Vec<DeletionReceiptItem> {
        vec![DeletionReceiptItem {
            rel_path: "gone.txt".to_string(),
            hash: None,
            size: 100,
            mtime: 1000,
            previous_decision_id: Some(3),
        }]
    }

    #[test]
    fn write_deletion_receipts_writes_source_local_file() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![(1, root_path.clone(), sample_items())],
            "Scanned 0 files: 0 new, 0 updated, 0 moved, 0 unchanged, 1 missing",
        );

        let receipt = temp
            .path()
            .join(".canon-ledger")
            .join(format!("{id:06}-scan.toml"));
        assert!(receipt.exists(), "receipt should land on the drive");
        let body = std::fs::read_to_string(&receipt).unwrap();
        assert!(body.contains("command = \"scan\""));
        assert!(body.contains("rel_path = \"gone.txt\""));
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn write_deletion_receipts_skipped_when_receipts_disabled() {
        // Records mode: DB row yes, receipt file no.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Records, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![(1, root_path, sample_items())],
            "summary",
        );

        assert!(!temp.path().join(".canon-ledger").exists());
    }

    #[test]
    fn write_deletion_receipts_zero_deletions_no_file_but_decision_row_exists() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        // No deletions this scan.
        write_deletion_receipts(&conn, &mut recorder, &params, Vec::new(), "summary");
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(0),
                completed: Some(0),
                failed: None,
                skipped: Some(0),
            },
            "summary",
        );

        assert!(
            !temp.path().join(".canon-ledger").exists(),
            "no receipt for a scan that deleted nothing"
        );
        let d = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(d.status, "completed");
    }

    #[test]
    fn write_deletion_receipts_multi_root_writes_and_indexes_each() {
        // One scan decision, deletions in two roots → one source-local receipt per
        // root, each indexed in decision_scopes for a by-root lookup.
        let conn = repo::open_in_memory_for_test();
        let temp_a = TempDir::new().unwrap();
        let temp_b = TempDir::new().unwrap();
        let root_a = temp_a.path().to_str().unwrap().to_string();
        let root_b = temp_b.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![
                (11, root_a.clone(), sample_items()),
                (22, root_b.clone(), sample_items()),
            ],
            "summary",
        );

        // Each receipt lands on its own drive.
        let name = format!("{id:06}-scan.toml");
        assert!(temp_a.path().join(".canon-ledger").join(&name).exists());
        assert!(temp_b.path().join(".canon-ledger").join(&name).exists());

        // The indexed rel_path is relative to the root (includes .canon-ledger/),
        // matching decisions.receipt_rel_path semantics.
        let rel_path = format!(".canon-ledger/{name}");

        // Both roots are indexed; the retirement query (WHERE root_id = ?) recovers
        // the decision and its receipt for each.
        for root_id in [11_i64, 22] {
            let (did, receipt): (i64, String) = conn
                .query_row(
                    "SELECT decision_id, receipt_rel_path FROM decision_scopes
                     WHERE root_id = ? AND receipt_rel_path IS NOT NULL",
                    [root_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(did, id);
            assert_eq!(receipt, rel_path);
        }
        assert!(recorder.take_warnings().is_empty());
    }
}
