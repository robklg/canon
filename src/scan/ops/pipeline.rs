//! The walk pipeline: observe→reconcile→persist.
//!
//! The interface creates the directory walker and passes entries here.
//! This module processes each entry through the pipeline, detects missing
//! sources, and returns typed results. A `ScanProgress` trait provides
//! per-file observability without writing to stderr.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rusqlite::{Transaction, TransactionBehavior};

use crate::core::ops::fs::compute_partial_hash;
use crate::core::repo::{self, Connection};
use crate::scan::domain::{
    find_missing, reconcile, reconcile_at_path, FileObservation, Reconciliation,
};
use crate::scan::repo as scan_repo;

use super::types::{
    DeletionReceiptItem, FileAction, FileToHash, MarkMissingPathResult, ScanOptions, ScanProgress,
    ScanRootResult, ScanStats, SourceOutcome,
};

/// Scan a root directory, processing each entry through the
/// observe→reconcile→persist pipeline.
///
/// The interface creates the directory walker and passes entries here.
/// This function:
/// 1. Fetches expected source IDs (for missing detection)
/// 2. Processes each entry via process_file()
/// 3. Detects missing sources via scan::domain::find_missing()
/// 4. Marks missing/disconnected via mark_missing_sources()
///
/// Returns accumulated stats, files needing hashing, and warnings.
#[allow(clippy::too_many_arguments)]
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
        scan_repo::source::fetch_source_ids_for_root(conn, root_id, scan_prefix)?
            .into_iter()
            .collect();

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                // Counted, not just warned: an unreadable entry means part of
                // the tree went unseen, which gates missing detection below —
                // unseen must never read as deleted.
                stats.walk_errors += 1;
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

        // Reconcile: read DB state, determine outcome, compute partial hash
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

        // Persist: unchanged files are batched, others get individual transactions
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

    if mount_stable && stats.walk_errors == 0 {
        // Identify sources that are truly missing using pure domain function
        // Sources not seen during walk AND not handled by empty-dir logic are missing
        let all_accounted: HashSet<i64> = seen_source_ids.union(&handled_ids).copied().collect();
        let missing_ids = find_missing(&expected_ids, &all_accounted);
        for id in missing_ids {
            outcomes.push((id, SourceOutcome::Missing));
        }
    } else {
        // Inferred absence cannot be trusted on an unstable or incomplete
        // walk: unseen is not evidence of gone. Every inferred Missing is
        // discarded — the post-walk difference is never computed, and Missing
        // classifications from empty directories are dropped too (their
        // Disconnected siblings stand; those never mark deletion by
        // themselves). The skip is counted in the stats — not just warned
        // about — so the decision summary durably records that this scan
        // could not verify absence (a scan that couldn't observe deletions
        // must be distinguishable in the trail from one that observed none).
        outcomes.retain(|(_, outcome)| !matches!(outcome, SourceOutcome::Missing));
        if !mount_stable {
            stats.missing_detection_skipped = 1;
            let detail = if pre_walk_device != post_walk_device {
                "Mount changed during scan"
            } else {
                "Mount device could not be verified"
            };
            warnings.push(format!(
                "{detail} — skipping missing detection to avoid data loss"
            ));
        } else {
            warnings.push(format!(
                "{} walk errors — skipping missing detection to avoid data loss",
                stats.walk_errors
            ));
        }
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
    source_at_path: Option<crate::core::domain::source::Source>,
}

/// Reconcile a single file: read DB state, determine outcome, compute partial hash if needed.
/// Does NOT persist — caller decides how to write (batch or individual transaction).
#[allow(clippy::too_many_arguments)]
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

    // Every partial hash below is computed outside the transaction — filesystem
    // I/O can be slow on NAS/network storage and must not hold the write lock.
    let reconciliation = match source_at_path.as_ref() {
        Some(existing) => {
            // The path holds a row, so the row is this file's; the only question
            // is whether the content under it moved. The head read is demanded
            // exactly when the inode changed beneath a standing path — that is
            // the one case where a bit-identical recreation and a real edit look
            // alike from metadata alone. A stored inode of 0 was never tracked
            // (a platform that records none), so nothing says it moved and the
            // fingerprint decides, as it always has.
            let inode_changed = existing.inode != 0 && existing.inode as u64 != observation.inode;
            if inode_changed {
                observation.partial_hash = Some(compute_partial_hash(full_path, size as u64)?);
            }
            reconcile_at_path(&observation, existing, observation.partial_hash.as_deref())
                .into_reconciliation()
        }
        None => {
            let source_by_inode =
                scan_repo::source::fetch_by_inode(conn, device as u64, inode as u64)?;
            reconcile(source_by_inode.as_ref())
        }
    };

    // A head read taken for the decision is the one the write needs — a file is
    // never opened twice for the same observation.
    if reconciliation.needs_partial_hash() && observation.partial_hash.is_none() {
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
) -> Result<crate::core::domain::source::Source> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let source = scan_repo::source::apply_reconciliation(
        &tx,
        observation,
        reconciliation,
        now,
        decision_id,
    )?;
    tx.commit()?;
    Ok(source)
}

/// Flush accumulated unchanged file updates in a single transaction.
fn flush_unchanged(conn: &Connection, batch: &[(i64, i64, i64)], now: i64) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    scan_repo::source::batch_update_unchanged(&tx, batch, now)?;
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
    let mut items: Vec<DeletionReceiptItem> =
        scan_repo::source::fetch_for_receipt(conn, missing_ids)?
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

/// Mark every present source under `path` as deleted, for `--missing` on a
/// folder that no longer exists on disk (so it can't be walked).
///
/// This is the second deletion-detection path; it receives the same treatment as
/// the sweep: the flip is stamped with `decision_id`, and — when `capture_deletions`
/// is set — the sources are snapshotted **before** the flip into receipt items
/// carrying their pre-flip provenance link. The caller writes the source-local
/// receipt via [`write_deletion_receipts`].
///
/// `path` must resolve to a known root (relative paths are cleaned against `cwd`);
/// otherwise this errors. A path that matches a root but has no present sources
/// returns `missing_count = 0` and no items — the caller decides how to report it.
///
/// [`write_deletion_receipts`]: super::receipt::write_deletion_receipts
pub fn mark_missing_path(
    conn: &Connection,
    path: &Path,
    roots: &[crate::core::domain::root::Root],
    cwd: &Path,
    now: i64,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<MarkMissingPathResult> {
    let cleaned = crate::core::domain::path::clean_path(path, cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let (root_id, root_path, rel_prefix) =
        match crate::core::domain::root::find_containing_root(&cleaned_str, roots) {
            Some((id, root_path, _role, rel)) => (id, root_path, rel),
            None => bail!(
                "Cannot mark missing: {} is not under any known root",
                path.display()
            ),
        };

    // The same suspended-root refusal the walk path makes. It matters most
    // here: a suspended root is precisely one whose path fails to
    // canonicalize, so --missing is the arm that would otherwise reach it
    // and mark a disconnected drive's sources deleted.
    if let Some(root) = roots.iter().find(|r| r.id == root_id) {
        if root.is_suspended() {
            bail!("Root '{root_path}' is suspended. Use 'canon roots unsuspend' to reactivate.");
        }
    }

    let prefix_arg = if rel_prefix.is_empty() {
        None
    } else {
        Some(rel_prefix.as_str())
    };
    let source_ids = scan_repo::source::fetch_source_ids_for_root(conn, root_id, prefix_arg)?;

    // Capture receipt items before the flip so each item's previous_decision_id
    // is the pre-flip value (stamp-set = receipt-set).
    let deleted_items = if capture_deletions {
        capture_deletion_items(conn, &source_ids)?
    } else {
        Vec::new()
    };

    let missing_count = repo::source::mark_missing(conn, &source_ids, now, decision_id)?;

    Ok(MarkMissingPathResult {
        root_id,
        root_path,
        missing_count,
        deleted_items,
    })
}

/// A source's id paired with its classified outcome.
type SourceOutcomes = Vec<(i64, SourceOutcome)>;

/// Classify sources under an empty directory by comparing stored device to current device.
/// Returns outcomes and any warnings about disconnected storage.
fn classify_sources_in_empty_dir(
    conn: &Connection,
    root_id: i64,
    rel_prefix: &str,
    current_device: i64,
) -> Result<(SourceOutcomes, Vec<String>)> {
    let sources = scan_repo::source::fetch_device_info_by_prefix(conn, root_id, rel_prefix)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::TempDir;

    use super::super::types::current_timestamp;

    /// No-op progress implementation for tests.
    struct NoopProgress;
    impl ScanProgress for NoopProgress {
        fn on_file(&self, _path: &str, _action: &FileAction) {}
        fn on_walk_error(&self, _error: &str) {}
        fn on_process_error(&self, _path: &str, _error: &str) {}
    }

    /// Test result from process_file helper.
    struct ProcessResult {
        // Mirrors persist_file's real return data; only `action` is currently
        // asserted on, kept available for tests that need to check identity.
        #[allow(dead_code)]
        source_id: i64,
        action: FileAction,
        #[allow(dead_code)]
        old_object_id: Option<i64>,
    }

    /// Test helper: reconcile + persist a single file (replicates old process_file behavior).
    #[allow(clippy::too_many_arguments)]
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
                .duration_since(std::time::UNIX_EPOCH)
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

        // The path stands and its content changed: the file the user has is an
        // edited one, whatever inode the editor left behind.
        assert!(matches!(result.action, FileAction::Modified));

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
    // Deletion capture before the flip (scan_root)
    // =========================================================================

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

    // =========================================================================
    // Identity at a standing path (scan_root, end to end)
    // =========================================================================

    /// One full pass of the pipeline over `dir`, as the interface drives it.
    fn scan_pass(
        conn: &Connection,
        root_id: i64,
        dir: &Path,
        decision_id: Option<i64>,
    ) -> ScanRootResult {
        scan_root(
            conn,
            root_id,
            dir.to_str().unwrap(),
            None,
            walk(dir),
            &no_hash_options(),
            &NoopProgress,
            current_timestamp(),
            decision_id,
            false,
        )
        .unwrap()
    }

    /// The headline invariant, asserted wherever a test has just changed state:
    /// scanning an unchanged disk reports nothing. Applied ambiently rather
    /// than in one dedicated test, because it must hold after every kind of
    /// mutation, not merely once.
    fn assert_second_scan_quiet(conn: &Connection, root_id: i64, dir: &Path) {
        let stats = scan_pass(conn, root_id, dir, Some(9_999)).stats;
        assert_eq!(
            (
                stats.new,
                stats.updated,
                stats.moved,
                stats.missing,
                stats.skipped
            ),
            (0, 0, 0, 0, 0),
            "a second scan of an unchanged disk must report nothing"
        );
    }

    /// Replace `path` the way an atomic-save editor does: write a sibling temp
    /// file and rename it over the path. The result is a new inode at a
    /// standing path — and, unlike delete-then-create, the old inode cannot be
    /// handed straight back, so the test's premise is guaranteed.
    fn atomic_save(path: &Path, content: &str) {
        let tmp = path.with_extension("tmp-save");
        std::fs::write(&tmp, content).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    fn inode_of(path: &Path) -> u64 {
        fs::metadata(path).unwrap().ino()
    }

    fn row_decision_id(conn: &Connection, source_id: i64) -> Option<i64> {
        conn.query_row(
            "SELECT decision_id FROM sources WHERE id = ?",
            [source_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn an_atomic_save_replacement_reads_as_updated() {
        // The report speaks in the user's terms: they edited one file, so the
        // scan says one file was updated — never "new", which would claim the
        // path had never been seen.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let notes = temp.path().join("notes.md");
        std::fs::write(&notes, "first draft").unwrap();

        let first = scan_pass(&conn, root_id, temp.path(), Some(1));
        assert_eq!(first.stats.new, 1);
        let source_id = repo::source::fetch_by_path(&conn, root_id, "notes.md")
            .unwrap()
            .unwrap()
            .id;
        let first_inode = inode_of(&notes);

        atomic_save(&notes, "second draft, rewritten");
        assert_ne!(inode_of(&notes), first_inode, "the editor left a new inode");

        let second = scan_pass(&conn, root_id, temp.path(), Some(2));
        assert_eq!(second.stats.updated, 1);
        assert_eq!(second.stats.new, 0);
        assert_eq!(second.stats.missing, 0);

        // The same row, carried through — not a new one beside a lost one.
        let after = repo::source::fetch_by_path(&conn, root_id, "notes.md")
            .unwrap()
            .unwrap();
        assert_eq!(after.id, source_id);
        assert_eq!(after.inode, inode_of(&notes) as i64);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_replacement_preserves_the_standing_decision_id() {
        // A scan observes; it does not act. The row keeps pointing at the
        // decision that last performed something on it, so story and trail keep
        // narrating the judgment rather than the observation that noticed it.
        // Asserted end to end, because what changed is which arm the pipeline
        // reaches: a path still routed to New would pass every arm-level guard
        // and stamp anyway.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let doc = temp.path().join("report.txt");
        std::fs::write(&doc, "original").unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(11));
        let source_id = repo::source::fetch_by_path(&conn, root_id, "report.txt")
            .unwrap()
            .unwrap()
            .id;
        // First indexing is a state transition of its own, and takes the stamp.
        assert_eq!(row_decision_id(&conn, source_id), Some(11));

        atomic_save(&doc, "rewritten entirely");
        let second = scan_pass(&conn, root_id, temp.path(), Some(22));
        assert_eq!(second.stats.updated, 1);
        assert_eq!(
            row_decision_id(&conn, source_id),
            Some(11),
            "the replacement observation must not overwrite the standing link"
        );

        assert_second_scan_quiet(&conn, root_id, temp.path());
        assert_eq!(
            row_decision_id(&conn, source_id),
            Some(11),
            "nor may a quiet scan"
        );
    }

    #[test]
    fn an_identical_content_recreation_reports_nothing() {
        // A restore or a dedup pass can hand back a byte-identical file under a
        // fresh inode. Nothing happened to the user's data, so nothing is
        // reported — the location metadata is refreshed and the row stands.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let restored = temp.path().join("restored.bin");
        std::fs::write(&restored, "identical bytes").unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let before = repo::source::fetch_by_path(&conn, root_id, "restored.bin")
            .unwrap()
            .unwrap();
        let original_mtime =
            filetime::FileTime::from_last_modification_time(&fs::metadata(&restored).unwrap());

        atomic_save(&restored, "identical bytes");
        filetime::set_file_mtime(&restored, original_mtime).unwrap();
        assert_ne!(inode_of(&restored), before.inode as u64);

        let second = scan_pass(&conn, root_id, temp.path(), Some(2));
        assert_eq!(
            (
                second.stats.new,
                second.stats.updated,
                second.stats.moved,
                second.stats.missing
            ),
            (0, 0, 0, 0),
            "recreating a file byte for byte is not an event"
        );
        assert_eq!(second.stats.unchanged, 1);

        // Refreshed silently: the row now carries where the file actually is.
        let after = repo::source::fetch_by_path(&conn, root_id, "restored.bin")
            .unwrap()
            .unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.inode, inode_of(&restored) as i64);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_head_read_is_taken_only_when_the_inode_moved() {
        // Reading every file's head on every scan would cost a whole-library
        // read; reading none would let a replacement pass as unchanged. The
        // line is the inode: it decides *whether to look*, never what the
        // answer is.
        //
        // Each row below carries a stored partial hash that disagrees with its
        // file, so the head read is observable by its verdict alone: where one
        // is taken the file reads as updated, and where none is taken the
        // fingerprint carries the row.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);

        let mut rows = Vec::new();
        for (name, stored_inode) in [
            ("steady.txt", None),         // the file's own inode: nothing moved
            ("untracked.txt", Some(0)),   // never tracked: nothing says it moved
            ("swapped.txt", Some(4_242)), // a different inode: look
        ] {
            let path = temp.path().join(name);
            std::fs::write(&path, format!("contents of {name}")).unwrap();
            let meta = fs::metadata(&path).unwrap();
            let inode = stored_inode.unwrap_or_else(|| meta.ino() as i64);
            rows.push(repo::insert_test_source(
                &conn,
                root_id,
                name,
                meta.dev() as i64,
                inode,
                meta.size() as i64,
                meta.mtime(),
            ));
        }

        let stats = scan_pass(&conn, root_id, temp.path(), Some(1)).stats;
        assert_eq!(stats.unchanged, 2, "no head read where the inode stood");
        assert_eq!(stats.updated, 1, "a head read where the inode moved");
        assert_eq!(stats.new, 0);

        assert_second_scan_quiet(&conn, root_id, temp.path());
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
    fn scan_root_scoped_prefix_does_not_sweep_string_prefix_siblings() {
        // A scan scoped to "alpha" must not treat sources under "alpha-beta" —
        // a sibling directory sharing "alpha" as a string prefix — as expected
        // and then missing. The sibling's file stands on disk untouched the
        // whole time; only the path boundary separates it from a false deletion.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        std::fs::create_dir(temp.path().join("alpha")).unwrap();
        std::fs::create_dir(temp.path().join("alpha-beta")).unwrap();
        std::fs::write(temp.path().join("alpha").join("a.txt"), "a").unwrap();
        std::fs::write(temp.path().join("alpha-beta").join("b.txt"), "b").unwrap();
        let sibling = repo::insert_test_source(&conn, root_id, "alpha-beta/b.txt", 1, 1, 1, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("alpha"),
            walk(&temp.path().join("alpha")),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(9),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn scan_root_scoped_prefix_treats_wildcard_bytes_literally() {
        // '_' in a scope path is a path byte, not a pattern: a scan scoped to
        // "alpha_beta" must not expect sources under "alphaXbeta" — a sibling
        // one wildcard-match away — and then sweep them into false deletion.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        std::fs::create_dir(temp.path().join("alpha_beta")).unwrap();
        std::fs::create_dir(temp.path().join("alphaXbeta")).unwrap();
        std::fs::write(temp.path().join("alpha_beta").join("a.txt"), "a").unwrap();
        std::fs::write(temp.path().join("alphaXbeta").join("b.txt"), "b").unwrap();
        let sibling = repo::insert_test_source(&conn, root_id, "alphaXbeta/b.txt", 1, 1, 1, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("alpha_beta"),
            walk(&temp.path().join("alpha_beta")),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(9),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
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

    // =========================================================================
    // mark_missing_path (the --missing deleted-folder path)
    // =========================================================================

    /// Fetch roots as domain objects, the way the interface passes them in.
    /// Test paths are absolute, so the cwd handed to `mark_missing_path` is a
    /// placeholder (`/`).
    fn all_roots(conn: &Connection) -> Vec<crate::core::domain::root::Root> {
        repo::root::fetch_all(conn).unwrap()
    }

    #[test]
    fn scan_root_walk_error_skips_missing_detection() {
        // An unreadable directory means part of the tree went unseen; a source
        // that merely wasn't seen must not be marked deleted on such a walk.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let unseen = repo::insert_test_source(&conn, root_id, "sub/gone.txt", 1, 1, 100, 1000);
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        // A real walk error, produced by walking a path that does not exist,
        // chained after the real entries.
        let err = WalkDir::new(temp.path().join("no-such-dir"))
            .into_iter()
            .next()
            .unwrap()
            .unwrap_err();
        let entries = walk(temp.path()).chain(std::iter::once(Err(err)));

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            entries,
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.walk_errors, 1);
        assert_eq!(result.stats.missing, 0);
        assert_eq!(result.stats.missing_detection_skipped, 0);
        assert!(result.deleted_items.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.contains("walk errors — skipping missing detection")));
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [unseen], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn scan_root_unstable_mount_discards_empty_dir_missing() {
        // Missing classifications from empty directories obey the mount guard
        // too: a walk whose device story cannot be verified must not delete —
        // otherwise one scan could both say "missing detection skipped" and
        // still write a deletion record.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        // The scan is scoped to "vanishing", which holds an empty dir whose
        // DB source sits on the same device — the empty-dir classifier will
        // mark it Missing mid-walk.
        let scoped = temp.path().join("vanishing");
        std::fs::create_dir(&scoped).unwrap();
        std::fs::create_dir(scoped.join("emptied")).unwrap();
        let device = get_dir_device(temp.path()).unwrap();
        let gone = repo::insert_test_source(
            &conn,
            root_id,
            "vanishing/emptied/gone.txt",
            device,
            7,
            100,
            1000,
        );

        // An extra entry outside the scoped dir, captured up front; yielding
        // it removes the scoped dir so the post-walk device check fails.
        std::fs::create_dir(temp.path().join("other")).unwrap();
        std::fs::write(temp.path().join("other").join("x.txt"), "x").unwrap();
        let extra = WalkDir::new(temp.path().join("other"))
            .into_iter()
            .nth(1)
            .unwrap();
        let scoped_clone = scoped.clone();
        let entries = walk(&scoped).chain(std::iter::once_with(move || {
            std::fs::remove_dir_all(&scoped_clone).unwrap();
            extra
        }));

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("vanishing"),
            entries,
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing_detection_skipped, 1);
        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [gone], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn mark_missing_path_marks_sources() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
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

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 5);
        assert_eq!(result.root_id, root_id);
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
    fn mark_missing_path_refuses_a_suspended_root() {
        // A suspended root's path is exactly one that fails to canonicalize,
        // so --missing lands here — without the refusal it would mark a
        // disconnected drive's sources deleted.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", true);
        let src = repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 100, 1000, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("suspended"));
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [src], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            present, 1,
            "no source on a suspended root is marked deleted"
        );
    }

    #[test]
    fn mark_missing_path_not_under_any_root() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", false);

        let result = mark_missing_path(
            &conn,
            Path::new("/nonexistent/path"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
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

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 3);
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
    fn mark_missing_path_stops_at_the_path_boundary() {
        // Marking "vacation" missing must not touch "vacation-2023" — a sibling
        // directory sharing the name as a string prefix is outside the scope.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 200, 1000, 1000);
        let sibling =
            repo::insert_test_source(&conn, root_id, "vacation-2023/img.jpg", 1, 201, 1000, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn mark_missing_path_already_not_present() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 400, 1000, 1000);
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [sid])
            .unwrap();

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        // mark_missing only flips present=1 rows, so nothing is marked here.
        assert_eq!(result.missing_count, 0);
    }

    #[test]
    fn mark_missing_path_empty_prefix_marks_all() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
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

        let result = mark_missing_path(
            &conn,
            Path::new("/photos"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 4);
    }

    #[test]
    fn mark_missing_path_no_sources_found() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", false);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/empty"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 0);
        assert!(result.deleted_items.is_empty());
    }

    #[test]
    fn mark_missing_path_stamps_decision_id() {
        // --missing threads the scan decision_id into the deletion transition,
        // exactly as the sweep does.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "gone.jpg", 1, 1, 100, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            Some(555),
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        let did: Option<i64> = conn
            .query_row("SELECT decision_id FROM sources WHERE id = ?", [sid], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(did, Some(555));
    }

    #[test]
    fn mark_missing_path_captures_items_before_flip() {
        // With capture on, --missing snapshots sources before the flip — the same
        // treatment the sweep gives — so each item carries its pre-flip
        // decision_id, and the source ends up absent and stamped with the scan.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "vacation/gone.jpg", 1, 1, 100, 1000);
        conn.execute("UPDATE sources SET decision_id = 42 WHERE id = ?", [sid])
            .unwrap();

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            Some(99),
            true,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        assert_eq!(result.deleted_items.len(), 1);
        let captured = &result.deleted_items[0];
        assert_eq!(captured.rel_path, "vacation/gone.jpg");
        assert_eq!(captured.previous_decision_id, Some(42));

        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [sid],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(99));
    }
}
