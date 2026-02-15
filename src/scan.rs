use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use rusqlite::{Transaction, TransactionBehavior};

use crate::domain::resolve_root_path_any;
use crate::domain::scan::{find_missing, reconcile, FileObservation, Reconciliation};
use crate::progress::Progress;
use crate::repo::{self, Connection, Db};

/// Outcome for a source during scan - determines what action to take
enum SourceOutcome {
    Seen,         // Found during walk - confirmed present
    Missing,      // Not found, parent device matches - truly gone
    Disconnected, // Not found, parent device differs - mount offline
}

/// Get device ID of a directory (Unix only)
fn get_dir_device(path: &Path) -> Option<i64> {
    fs::metadata(path).ok().map(|m| m.dev() as i64)
}

/// Check if a directory is empty (no entries)
fn is_empty_dir(path: &Path) -> bool {
    fs::read_dir(path)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
}

/// Classify sources under an empty directory by comparing stored device to current device.
/// Emits a warning if sources are on a different device (possibly disconnected storage).
fn classify_sources_in_empty_dir(
    conn: &Connection,
    root_id: i64,
    rel_prefix: &str,
    current_device: i64,
) -> Result<Vec<(i64, SourceOutcome)>> {
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
                _ => SourceOutcome::Missing, // Same device or no device info
            };
            (id, outcome)
        })
        .collect();

    // Emit warning immediately so user knows which directory had disconnected sources
    if disconnected_count > 0 {
        let path_desc = if rel_prefix.is_empty() {
            "(root)"
        } else {
            rel_prefix
        };
        eprintln!(
            "Warning: {path_desc} contains {disconnected_count} files on different device (possibly disconnected storage)"
        );
    }

    Ok(results)
}

#[derive(Default)]
struct ScanStats {
    scanned: u64,
    new: u64,
    updated: u64,
    moved: u64,
    unchanged: u64,
    missing: u64,
    disconnected: u64,
    skipped: u64,
    hashed: u64,
    unexpected_hash_changes: u64,
}

/// File info collected during scan for hashing
struct FileToHash {
    source_id: i64,
    full_path: PathBuf,
    old_object_id: Option<i64>,
    basis_changed: bool, // True if file was new/updated (mtime/size changed)
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
        let should_hash = !no_hash;
        let hash_all = verify;

        let result = scan_root(
            conn,
            root_id,
            &root_path,
            scan_prefix.as_deref(),
            now,
            should_hash,
            hash_all,
            ignore_device_id,
        )?;

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

    // Hash collected files with progress indicator
    if !all_files_to_hash.is_empty() {
        let total = all_files_to_hash.len();
        let progress = Progress::new(total);
        eprintln!("Computing hashes for {total} files...");

        for (i, file) in all_files_to_hash.iter().enumerate() {
            progress.update(i);

            // Compute full SHA256 hash
            let hash_value = match compute_full_hash(&file.full_path) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!(
                        "\nWarning: Failed to hash {}: {}",
                        file.full_path.display(),
                        e
                    );
                    continue;
                }
            };

            // Wrap object creation + source linking + fact storage in a single
            // transaction for atomicity. Without this, a crash could leave an
            // object created but not linked, or linked but without the hash fact.
            // Uses Immediate for reliable busy-handler support under concurrency.
            let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

            let new_object = get_or_create_object(&tx, "sha256", &hash_value)?;

            // Check for unexpected hash change (only if basis didn't change and file had existing hash)
            if !file.basis_changed {
                if let Some(old_oid) = file.old_object_id {
                    if old_oid != new_object.id {
                        eprintln!(
                            "\nWarning: hash changed for {} (file may be corrupted or was modified without mtime change)",
                            file.full_path.display()
                        );
                        total_stats.unexpected_hash_changes += 1;
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

            total_stats.hashed += 1;
        }

        progress.finish();

        println!("Hashed {} files", total_stats.hashed);
    }

    // Exit with error if there were unexpected hash changes (possible corruption)
    if total_stats.unexpected_hash_changes > 0 {
        bail!(
            "{} files have unexpected hash changes (file may be corrupted or was modified without mtime change)",
            total_stats.unexpected_hash_changes
        );
    }

    // Update query planner statistics after bulk changes
    db.run_analyze()?;

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

struct ScanRootResult {
    stats: ScanStats,
    files_to_hash: Vec<FileToHash>,
}

fn scan_root(
    conn: &Connection,
    root_id: i64,
    root_path: &Path,
    scan_prefix: Option<&str>,
    now: i64,
    should_hash: bool,
    hash_all: bool,
    ignore_device_id: bool,
) -> Result<ScanRootResult> {
    let mut stats = ScanStats::default();
    let mut seen_source_ids: HashSet<i64> = HashSet::new();
    let mut files_to_hash: Vec<FileToHash> = Vec::new();

    // Track outcomes for sources (for mount protection)
    let mut outcomes: Vec<(i64, SourceOutcome)> = Vec::new();
    let mut handled_ids: HashSet<i64> = HashSet::new();

    // Fetch expected source IDs at start (for missing detection via pure function)
    let expected_ids: HashSet<i64> =
        repo::source::fetch_source_ids_for_root(conn, root_id, scan_prefix)?
            .into_iter()
            .collect();

    // Determine the actual path to walk
    let walk_path = match scan_prefix {
        Some(prefix) => root_path.join(prefix),
        None => root_path.to_path_buf(),
    };

    for entry in WalkDir::new(&walk_path).follow_links(false) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: {e}");
                continue;
            }
        };

        // Handle empty directories - may contain sources on disconnected mounts
        if entry.file_type().is_dir() {
            if is_empty_dir(entry.path()) {
                if let Some(current_dev) = get_dir_device(entry.path()) {
                    let rel = entry
                        .path()
                        .strip_prefix(root_path)
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    let dir_outcomes =
                        classify_sources_in_empty_dir(conn, root_id, &rel, current_dev)?;
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
                eprintln!("Warning: Failed to stat {}: {}", full_path.display(), e);
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
                eprintln!("Warning: Failed to process {}: {}", full_path.display(), e);
                stats.skipped += 1;
                continue;
            }
        };

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
        if should_hash {
            let needs_hash = match result.action {
                FileAction::New | FileAction::Modified => true, // New/changed files always need hash
                FileAction::Moved | FileAction::Unchanged => hash_all, // Only if --compute-hashes=all
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

    // Identify sources that are truly missing using pure domain function (D3 in spec)
    // Sources not seen during walk AND not handled by empty-dir logic are missing
    let all_accounted: HashSet<i64> = seen_source_ids.union(&handled_ids).copied().collect();
    let missing_ids = find_missing(&expected_ids, &all_accounted);
    for id in missing_ids {
        outcomes.push((id, SourceOutcome::Missing));
    }

    // Mark missing/disconnected files based on outcomes
    let (missing_count, disconnected_count) =
        mark_missing_sources(conn, &outcomes, now, ignore_device_id)?;
    stats.missing = missing_count;
    stats.disconnected = disconnected_count;

    Ok(ScanRootResult {
        stats,
        files_to_hash,
    })
}

enum FileAction {
    New,
    Modified,
    Moved,
    Unchanged,
}

struct ProcessResult {
    source_id: i64,
    action: FileAction,
    old_object_id: Option<i64>, // For detecting unexpected hash changes
}

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

fn mark_missing_sources(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
) -> Result<(u64, u64)> {
    // Collect IDs to mark as missing
    let mut missing_ids: Vec<i64> = Vec::new();
    let mut disconnected_count = 0u64;

    for (id, outcome) in outcomes {
        match outcome {
            SourceOutcome::Seen => {
                // Nothing to do - already updated during walk
            }
            SourceOutcome::Missing => {
                missing_ids.push(*id);
            }
            SourceOutcome::Disconnected => {
                if ignore_device_id {
                    // User explicitly opted out of protection
                    missing_ids.push(*id);
                } else {
                    disconnected_count += 1;
                }
            }
        }
    }

    // Batch mark all missing sources using repo function
    let missing_count = repo::source::mark_missing(conn, &missing_ids, now)?;

    if disconnected_count > 0 {
        eprintln!(
            "Skipped {disconnected_count} files (device ID mismatch - possibly disconnected storage)"
        );
        eprintln!("  If device IDs changed (e.g., NAS remount), re-run with --ignore-device-id");
    }

    Ok((missing_count, disconnected_count))
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

const PARTIAL_HASH_CHUNK_SIZE: usize = 8192; // 8KB

/// Compute SHA256 hash of first 8KB + last 8KB of a file.
/// For files <= 16KB, hash the entire file.
pub fn compute_partial_hash(path: &Path, size: u64) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for partial hash: {}", path.display()))?;
    let mut hasher = Sha256::new();

    if size <= (PARTIAL_HASH_CHUNK_SIZE * 2) as u64 {
        // Small file - hash entire content
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        hasher.update(&buf);
    } else {
        // Large file - hash first 8KB + last 8KB
        let mut buf = [0u8; PARTIAL_HASH_CHUNK_SIZE];

        // Read first 8KB
        file.read_exact(&mut buf)?;
        hasher.update(buf);

        // Seek to last 8KB and read
        file.seek(SeekFrom::End(-(PARTIAL_HASH_CHUNK_SIZE as i64)))?;
        file.read_exact(&mut buf)?;
        hasher.update(buf);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Compute full SHA256 hash of a file
fn compute_full_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536]; // 64KB buffer

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Get or create an object by hash, returning the Object
fn get_or_create_object(
    conn: &Connection,
    hash_type: &str,
    hash_value: &str,
) -> Result<crate::domain::object::Object> {
    repo::object::get_or_create(conn, hash_type, hash_value)
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
    use std::io::Write;
    use tempfile::TempDir;

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

        // Verify source was inserted
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

        // Pre-insert source with matching metadata
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

        // Pre-insert source with different size
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

        // Pre-insert source at different path with same inode
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
        // Test: file exists at path with different device ID (e.g., NAS remount)
        // Should be processed normally, not skipped as disconnected
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) = create_temp_file(&temp_dir, "file.txt", "content");

        // Pre-insert source with DIFFERENT device but same inode
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

        // Should be Unchanged - device changes are legitimate metadata updates
        assert!(matches!(result.action, FileAction::Unchanged));

        // Verify the device was updated in the database
        let source = repo::source::fetch_by_path(&conn, root_id, "file.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.device, device as i64);
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
        let (missing_count, disconnected_count) =
            mark_missing_sources(&conn, &outcomes, now, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 1);

        // Verify id1 is still present (Seen) - fetch_by_path only returns present sources
        let s1 = repo::source::fetch_by_path(&conn, root_id, "file1.txt").unwrap();
        assert!(s1.is_some());

        // Verify id2 is not present (Missing)
        let s2: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id2], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s2, 0);

        // Verify id3 is still present (Disconnected, not marked missing)
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
        let (missing_count, disconnected_count) =
            mark_missing_sources(&conn, &outcomes, now, true).unwrap();

        // With ignore_device_id=true, disconnected should be treated as missing
        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 0);
    }

    #[test]
    fn process_file_replaced() {
        // Test: file at path has different inode (old file deleted, new file created at same path)
        // The old source record (present=1) gets updated with new file's attributes
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "replaced.txt", "new content");

        // Pre-insert source at same path but with DIFFERENT inode (simulates file that will be replaced)
        let old_inode = inode + 99999; // Different inode
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

        // Should be New because inode differs (replacement)
        assert!(matches!(result.action, FileAction::New));

        // Only one source record should exist (the old one was updated)
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "replaced.txt"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // The source should have the NEW file's attributes
        let source = repo::source::fetch_by_path(&conn, root_id, "replaced.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.inode, inode as i64);
        assert_eq!(source.size, size);
        assert_eq!(source.basis_rev, 0); // Reset for new file
    }

    #[test]
    fn process_file_revives_stale_record() {
        // Test: file appears at path where a stale (present=0) record exists
        // The stale record should be revived with new file's attributes
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "revived.txt", "new content");

        // Pre-insert a STALE source (present=0) at same path - simulates file that was previously missing
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

        // Should be New (no present source at path, no source with this inode)
        assert!(matches!(result.action, FileAction::New));

        // The stale record should be revived (same ID, now present=1)
        let source = repo::source::fetch_by_path(&conn, root_id, "revived.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.id, old_source_id); // Same record was revived
        assert_eq!(source.inode, inode as i64); // Updated to new inode
        assert_eq!(source.size, size); // Updated to new size
        assert_eq!(source.basis_rev, 0); // Reset for new file

        // Verify only one record exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "revived.txt"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn process_file_independent_operations() {
        // Test: multiple file operations are independent (no cross-contamination)
        // This verifies that each process_file call operates correctly regardless of others
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        // Create three files with different scenarios
        let (path1, dev1, ino1, size1, mtime1) =
            create_temp_file(&temp_dir, "new.txt", "new content");
        let (path2, dev2, ino2, size2, mtime2) =
            create_temp_file(&temp_dir, "existing.txt", "existing");
        let (path3, dev3, ino3, size3, mtime3) =
            create_temp_file(&temp_dir, "modified.txt", "modified content");

        // Pre-insert existing file (unchanged)
        repo::insert_test_source(
            &conn,
            root_id,
            "existing.txt",
            dev2 as i64,
            ino2 as i64,
            size2,
            mtime2,
        );
        // Pre-insert modified file (with different size)
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

        // Process all three files
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

        // Each should have the correct action
        assert!(matches!(r1.action, FileAction::New));
        assert!(matches!(r2.action, FileAction::Unchanged));
        assert!(matches!(r3.action, FileAction::Modified));

        // All three sources should exist and be present
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }
}
