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

use crate::domain::scan::{find_missing, reconcile, FileObservation, Reconciliation};
use crate::ops::fs::compute_partial_hash;
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

/// Per-file processing result.
pub struct ProcessResult {
    pub source_id: i64,
    pub action: FileAction,
    pub old_object_id: Option<i64>,
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
) -> Result<ScanRootResult> {
    let root_path = Path::new(root_path);
    let mut stats = ScanStats::default();
    let mut seen_source_ids: HashSet<i64> = HashSet::new();
    let mut files_to_hash: Vec<FileToHash> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    // Track outcomes for sources (for mount protection)
    let mut outcomes: Vec<(i64, SourceOutcome)> = Vec::new();
    let mut handled_ids: HashSet<i64> = HashSet::new();

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

        let result = match process_file(
            conn,
            root_id,
            rel_path_str,
            full_path,
            device,
            inode,
            size,
            mtime,
            now,
        ) {
            Ok(r) => r,
            Err(e) => {
                progress.on_process_error(
                    &full_path.display().to_string(),
                    &e.to_string(),
                );
                stats.skipped += 1;
                continue;
            }
        };

        // Notify progress
        progress.on_file(rel_path_str, &result.action);

        // Track seen sources
        seen_source_ids.insert(result.source_id);
        outcomes.push((result.source_id, SourceOutcome::Seen));

        match result.action {
            FileAction::New => stats.new += 1,
            FileAction::Modified => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }

        // Collect files for hashing based on mode
        if options.hash {
            let needs_hash = match result.action {
                FileAction::New | FileAction::Modified => true,
                FileAction::Moved | FileAction::Unchanged => options.hash_all,
            };
            if needs_hash {
                files_to_hash.push(FileToHash {
                    source_id: result.source_id,
                    full_path: full_path.to_path_buf(),
                    old_object_id: result.old_object_id,
                    basis_changed: matches!(result.action, FileAction::New | FileAction::Modified),
                });
            }
        }
    }

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

    // Mark missing/disconnected files based on outcomes
    let (missing_count, disconnected_count, missing_warnings) =
        mark_missing_sources(conn, &outcomes, now, options.ignore_device_id)?;
    warnings.extend(missing_warnings);
    stats.missing = missing_count;
    stats.disconnected = disconnected_count;

    Ok(ScanRootResult {
        stats,
        files_to_hash,
        warnings,
    })
}

/// Process a single file through observe→reconcile→persist.
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
    // Create observation from file metadata
    let mut observation = FileObservation {
        root_id,
        rel_path: rel_path.to_string(),
        device: device as u64,
        inode: inode as u64,
        size,
        mtime,
        partial_hash: None, // Computed after reconciliation if needed
    };

    // Read DB state to determine what happened (no write lock held)
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

    // Short write transaction (DB-only, no filesystem I/O).
    // Uses Immediate for reliable busy-handler support under concurrency.
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let source = repo::source::apply_reconciliation(&tx, &observation, &reconciliation, now)?;
    tx.commit()?;

    // Map reconciliation to FileAction and extract old_object_id
    let (action, old_object_id) = match &reconciliation {
        Reconciliation::New => (FileAction::New, None),
        Reconciliation::Unchanged { .. } => (
            FileAction::Unchanged,
            source_at_path.and_then(|s| s.object_id),
        ),
        Reconciliation::Modified { old_object_id, .. } => (FileAction::Modified, *old_object_id),
        Reconciliation::Moved { old_object_id, .. } => (FileAction::Moved, *old_object_id),
    };

    Ok(ProcessResult {
        source_id: source.id,
        action,
        old_object_id,
    })
}

/// Translate source outcomes to DB mutations.
/// Returns (missing_count, disconnected_count, warnings).
fn mark_missing_sources(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
) -> Result<(u64, u64, Vec<String>)> {
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

    let missing_count = repo::source::mark_missing(conn, &missing_ids, now)?;

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

    Ok((missing_count, disconnected_count, warnings))
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
pub fn find_root_candidates(
    scope: &Path,
    root_paths: &[PathBuf],
) -> Result<CandidateResult> {
    let mut dirs_with_files: HashSet<PathBuf> = HashSet::new();
    let mut warnings: Vec<String> = Vec::new();

    scan_for_untracked(scope, root_paths, &mut dirs_with_files, &mut warnings)?;

    let candidates = find_common_ancestors(&dirs_with_files, root_paths, scope)
        .into_iter()
        .map(|(path, dir_count)| RootCandidate { path, dir_count })
        .collect();

    Ok(CandidateResult { candidates, warnings })
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

        repo::insert_test_source(&conn, root_id, "modified.txt", device as i64, inode as i64, 5, mtime);

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
            &conn, root_id, "old_name.txt", device as i64, inode as i64, size, mtime,
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
            &conn, root_id, "file.txt", &path, device as i64, inode as i64, size, mtime, now,
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
            &conn, root_id, "replaced.txt", device as i64, old_inode as i64, 50, mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn, root_id, "replaced.txt", &path, device as i64, inode as i64, size, mtime, now,
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
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [old_source_id])
            .unwrap();

        let now = current_timestamp();
        let result = process_file(
            &conn, root_id, "revived.txt", &path, device as i64, inode as i64, size, mtime, now,
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
            &conn, root_id, "existing.txt", dev2 as i64, ino2 as i64, size2, mtime2,
        );
        repo::insert_test_source(
            &conn, root_id, "modified.txt", dev3 as i64, ino3 as i64, 5, mtime3,
        );

        let now = current_timestamp();

        let r1 = process_file(
            &conn, root_id, "new.txt", &path1, dev1 as i64, ino1 as i64, size1, mtime1, now,
        ).unwrap();
        let r2 = process_file(
            &conn, root_id, "existing.txt", &path2, dev2 as i64, ino2 as i64, size2, mtime2, now,
        ).unwrap();
        let r3 = process_file(
            &conn, root_id, "modified.txt", &path3, dev3 as i64, ino3 as i64, size3, mtime3, now,
        ).unwrap();

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
        let (missing_count, disconnected_count, warnings) =
            mark_missing_sources(&conn, &outcomes, now, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 1);
        assert!(!warnings.is_empty()); // Should have disconnected warning

        let s1 = repo::source::fetch_by_path(&conn, root_id, "file1.txt").unwrap();
        assert!(s1.is_some());

        let s2: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id2], |r| r.get(0))
            .unwrap();
        assert_eq!(s2, 0);

        let s3: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id3], |r| r.get(0))
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
        let (missing_count, disconnected_count, warnings) =
            mark_missing_sources(&conn, &outcomes, now, true).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 0);
        assert!(warnings.is_empty()); // No warnings when ignore_device_id=true
    }
}
