use anyhow::{bail, Context, Result};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::repo::{Connection, Db};
use crate::domain::resolve_root_path_any;
use crate::progress::Progress;

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
    let prefix_pattern = if rel_prefix.is_empty() {
        "%".to_string()
    } else {
        format!("{}/%", rel_prefix)
    };

    let sources: Vec<(i64, Option<i64>)> = conn
        .prepare("SELECT id, device FROM sources WHERE root_id = ? AND rel_path LIKE ? AND present = 1")?
        .query_map(params![root_id, prefix_pattern], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

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
            "Warning: {} contains {} files on different device (possibly disconnected storage)",
            path_desc, disconnected_count
        );
    }

    Ok(results)
}

/// Identify sources that are missing (not seen during walk, not already handled).
/// No device check needed - if we walked through the parent dir, the file is gone.
fn identify_missing_sources(
    conn: &Connection,
    root_id: i64,
    scan_prefix: Option<&str>,
    seen_ids: &HashSet<i64>,
    handled_ids: &HashSet<i64>,
) -> Result<Vec<(i64, SourceOutcome)>> {
    let candidate_ids: Vec<i64> = match scan_prefix {
        Some(prefix) => {
            let pattern = format!("{}%", prefix);
            conn.prepare(
                "SELECT id FROM sources WHERE root_id = ? AND present = 1 AND rel_path LIKE ?",
            )?
            .query_map(params![root_id, pattern], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        }
        None => conn
            .prepare("SELECT id FROM sources WHERE root_id = ? AND present = 1")?
            .query_map([root_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(candidate_ids
        .into_iter()
        .filter(|id| !seen_ids.contains(id) && !handled_ids.contains(id))
        .map(|id| (id, SourceOutcome::Missing))
        .collect())
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
    basis_changed: bool,  // True if file was new/updated (mtime/size changed)
}

pub fn run(db: &Db, paths: &[PathBuf], role: Option<&str>, add_root: bool, comment: Option<&str>, all_roots: bool, no_hash: bool, verify: bool, ignore_device_id: bool) -> Result<()> {
    // Validate role if provided
    if let Some(r) = role {
        if r != "source" && r != "archive" {
            bail!("Invalid role '{}'. Must be 'source' or 'archive'", r);
        }
    }

    let conn = db.conn();
    let now = current_timestamp();

    // If --all, get all root paths from the database (excluding suspended)
    let paths_to_scan: Vec<PathBuf> = if all_roots {
        let role_filter = match role {
            Some(r) => format!("AND role = '{}'", r),
            None => String::new(),
        };
        let query = format!(
            "SELECT path FROM roots WHERE suspended = 0 {} ORDER BY id",
            role_filter
        );
        let roots: Vec<String> = conn
            .prepare(&query)?
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if roots.is_empty() {
            println!("No roots to scan.");
            return Ok(());
        }

        println!("Scanning {} roots...", roots.len());
        roots.into_iter().map(PathBuf::from).collect()
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
        let (root_id, root_path, scan_prefix, _root_role) = match resolve_root_path_any(conn, &canonical)? {
            Some((id, root_path, existing_role, rel_path)) => {
                // Path is inside an existing root - check if suspended
                let suspended: bool = conn.query_row(
                    "SELECT suspended FROM roots WHERE id = ?",
                    [id],
                    |row| row.get(0),
                )?;
                if suspended {
                    bail!(
                        "Root '{}' is suspended. Use 'canon roots unsuspend' to reactivate.",
                        root_path
                    );
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
                            "Root '{}' has role '{}', cannot scan with --role {}",
                            root_path,
                            existing_role,
                            r
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
                check_overlapping_roots(&conn, &canonical)?;
                let root_id = create_root(&conn, &canonical, new_role, comment)?;
                (root_id, canonical.clone(), None, new_role.to_string())
            }
        };

        // Determine if we should hash this root
        // Default: hash new/changed files; --no-hash to skip; --verify to rehash all
        let should_hash = !no_hash;
        let hash_all = verify;

        let result = scan_root(&conn, root_id, &root_path, scan_prefix.as_deref(), now, should_hash, hash_all, ignore_device_id)?;

        // Update last_scanned_at only for full root scans (not subdirectory scans)
        if scan_prefix.is_none() {
            conn.execute(
                "UPDATE roots SET last_scanned_at = ? WHERE id = ?",
                params![now, root_id],
            )?;
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
        summary.push_str(&format!(", {} skipped (disconnected)", total_stats.disconnected));
    }
    println!("{}", summary);

    // Hash collected files with progress indicator
    if !all_files_to_hash.is_empty() {
        let total = all_files_to_hash.len();
        let progress = Progress::new(total);
        eprintln!("Computing hashes for {} files...", total);

        for (i, file) in all_files_to_hash.iter().enumerate() {
            progress.update(i);

            // Compute full SHA256 hash
            let hash_value = match compute_full_hash(&file.full_path) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("\nWarning: Failed to hash {}: {}", file.full_path.display(), e);
                    continue;
                }
            };

            // Get or create object
            let new_object_id = get_or_create_object(conn, "sha256", &hash_value)?;

            // Check for unexpected hash change (only if basis didn't change and file had existing hash)
            if !file.basis_changed {
                if let Some(old_oid) = file.old_object_id {
                    if old_oid != new_object_id {
                        eprintln!(
                            "\nWarning: hash changed for {} (file may be corrupted or was modified without mtime change)",
                            file.full_path.display()
                        );
                        total_stats.unexpected_hash_changes += 1;
                    }
                }
            }

            // Link source to object
            conn.execute(
                "UPDATE sources SET object_id = ? WHERE id = ?",
                params![new_object_id, file.source_id],
            )?;

            // Store hash as fact on object
            store_hash_fact(conn, new_object_id, &hash_value)?;

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

fn create_root(conn: &Connection, path: &Path, role: &str, comment: Option<&str>) -> Result<i64> {
    let path_str = path.to_str().context("Path is not valid UTF-8")?;

    conn.execute(
        "INSERT INTO roots (path, role, comment) VALUES (?, ?, ?)",
        params![path_str, role, comment],
    )?;
    Ok(conn.last_insert_rowid())
}

fn check_overlapping_roots(conn: &Connection, new_path: &Path) -> Result<()> {
    let new_path_str = new_path.to_str().context("Path is not valid UTF-8")?;

    let mut stmt = conn.prepare("SELECT path FROM roots")?;
    let roots: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for existing in roots {
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

    // Determine the actual path to walk
    let walk_path = match scan_prefix {
        Some(prefix) => root_path.join(prefix),
        None => root_path.to_path_buf(),
    };

    for entry in WalkDir::new(&walk_path).follow_links(false) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: {}", e);
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

        seen_source_ids.insert(result.source_id);
        outcomes.push((result.source_id, SourceOutcome::Seen));

        match result.action {
            FileAction::New => stats.new += 1,
            FileAction::Updated => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }

        // Collect files for hashing based on mode
        if should_hash {
            let needs_hash = match result.action {
                FileAction::New | FileAction::Updated => true, // New/changed files always need hash
                FileAction::Moved | FileAction::Unchanged => hash_all, // Only if --compute-hashes=all
            };
            if needs_hash {
                files_to_hash.push(FileToHash {
                    source_id: result.source_id,
                    full_path: full_path.to_path_buf(),
                    old_object_id: result.old_object_id,
                    basis_changed: matches!(result.action, FileAction::New | FileAction::Updated),
                });
            }
        }
    }

    // Identify sources that are truly missing (not seen, not already handled via empty-dir logic)
    let missing_outcomes =
        identify_missing_sources(conn, root_id, scan_prefix, &seen_source_ids, &handled_ids)?;
    outcomes.extend(missing_outcomes);

    // Mark missing/disconnected files based on outcomes
    let (missing_count, disconnected_count) = mark_missing(conn, &outcomes, now, ignore_device_id)?;
    stats.missing = missing_count;
    stats.disconnected = disconnected_count;

    Ok(ScanRootResult { stats, files_to_hash })
}

enum FileAction {
    New,
    Updated,
    Moved,
    Unchanged,
}

struct ProcessResult {
    source_id: i64,
    action: FileAction,
    old_object_id: Option<i64>,  // For detecting unexpected hash changes
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
    // First, check if we have an existing source at this path
    let existing_by_path: Option<(i64, Option<i64>, Option<i64>, i64, i64, i64, Option<i64>)> = conn
        .query_row(
            "SELECT id, device, inode, size, mtime, basis_rev, object_id FROM sources
             WHERE root_id = ? AND rel_path = ?",
            params![root_id, rel_path],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .optional()?;

    if let Some((id, _old_device, _old_inode, old_size, old_mtime, old_basis_rev, old_object_id)) = existing_by_path {
        // Source exists at this path
        // Detect content changes via size/mtime only. Device/inode changes don't
        // indicate content changes (e.g., NAS remounts) so they don't affect basis_rev.
        // We skip partial_hash for performance (would read 16KB per file per scan).
        // Content changes without mtime update are caught during transfer validation
        // via partial_hash, or flagged as "unexpected hash change" during hashing.
        let basis_changed = size != old_size || mtime != old_mtime;

        if basis_changed {
            let new_basis_rev = old_basis_rev + 1;
            let partial_hash = compute_partial_hash(full_path, size as u64)?;
            conn.execute(
                "UPDATE sources SET device = ?, inode = ?, size = ?, mtime = ?,
                 partial_hash = ?, basis_rev = ?, last_seen_at = ?, present = 1 WHERE id = ?",
                params![device, inode, size, mtime, partial_hash, new_basis_rev, now, id],
            )?;
            return Ok(ProcessResult {
                source_id: id,
                action: FileAction::Updated,
                old_object_id,
            });
        } else {
            // Just update last_seen_at
            conn.execute(
                "UPDATE sources SET last_seen_at = ?, present = 1 WHERE id = ?",
                params![now, id],
            )?;
            return Ok(ProcessResult {
                source_id: id,
                action: FileAction::Unchanged,
                old_object_id,
            });
        }
    }

    // Check if we have an existing source with this device+inode (moved file)
    let existing_by_inode: Option<(i64, i64, String, i64, Option<i64>)> = conn
        .query_row(
            "SELECT id, root_id, rel_path, basis_rev, object_id FROM sources
             WHERE device = ? AND inode = ?",
            params![device, inode],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;

    if let Some((id, _old_root_id, _old_rel_path, _old_basis_rev, old_object_id)) = existing_by_inode {
        // File was moved (same device+inode found at different path)
        // Moving a file (even across roots) doesn't invalidate facts. Source facts
        // describe content, not location - they're often "object facts not yet promoted"
        // waiting for the content hash. Since content is unchanged, facts remain valid.
        // We update location and device+inode but preserve basis_rev and partial_hash.
        conn.execute(
            "UPDATE sources SET root_id = ?, rel_path = ?, device = ?, inode = ?,
             size = ?, mtime = ?, last_seen_at = ?, present = 1 WHERE id = ?",
            params![root_id, rel_path, device, inode, size, mtime, now, id],
        )?;
        return Ok(ProcessResult {
            source_id: id,
            action: FileAction::Moved,
            old_object_id,
        });
    }

    // New file
    let partial_hash = compute_partial_hash(full_path, size as u64)?;
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
         partial_hash, basis_rev, scanned_at, last_seen_at, present)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 1)",
        params![root_id, rel_path, device, inode, size, mtime, partial_hash, now, now],
    )?;

    Ok(ProcessResult {
        source_id: conn.last_insert_rowid(),
        action: FileAction::New,
        old_object_id: None,
    })
}

fn mark_missing(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
) -> Result<(u64, u64)> {
    let mut missing_count = 0u64;
    let mut disconnected_count = 0u64;

    for (id, outcome) in outcomes {
        match outcome {
            SourceOutcome::Seen => {
                // Nothing to do - already updated during walk
            }
            SourceOutcome::Missing => {
                conn.execute(
                    "UPDATE sources SET present = 0, last_seen_at = ? WHERE id = ?",
                    params![now, id],
                )?;
                missing_count += 1;
            }
            SourceOutcome::Disconnected => {
                if ignore_device_id {
                    // User explicitly opted out of protection
                    conn.execute(
                        "UPDATE sources SET present = 0, last_seen_at = ? WHERE id = ?",
                        params![now, id],
                    )?;
                    missing_count += 1;
                } else {
                    disconnected_count += 1;
                }
            }
        }
    }

    if disconnected_count > 0 {
        eprintln!(
            "Skipped {} files (device ID mismatch - possibly disconnected storage)",
            disconnected_count
        );
        eprintln!(
            "  If device IDs changed (e.g., NAS remount), re-run with --ignore-device-id"
        );
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
        hasher.update(&buf);

        // Seek to last 8KB and read
        file.seek(SeekFrom::End(-(PARTIAL_HASH_CHUNK_SIZE as i64)))?;
        file.read_exact(&mut buf)?;
        hasher.update(&buf);
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

/// Get or create an object by hash, returning its ID
fn get_or_create_object(conn: &Connection, hash_type: &str, hash_value: &str) -> Result<i64> {
    use rusqlite::OptionalExtension;

    // Try to find existing object
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM objects WHERE hash_type = ? AND hash_value = ?",
            params![hash_type, hash_value],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(id) = existing {
        return Ok(id);
    }

    // Create new object
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES (?, ?)",
        params![hash_type, hash_value],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Store the content hash as a fact on the object
fn store_hash_fact(conn: &Connection, object_id: i64, hash_value: &str) -> Result<()> {
    let now = current_timestamp();
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at)
         VALUES ('object', ?, 'content.hash.sha256', ?, ?)
         ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
           value_text = excluded.value_text,
           observed_at = excluded.observed_at",
        params![object_id, hash_value, now],
    )?;
    Ok(())
}

/// Find directories with files that aren't under any root
pub fn find_candidates(db: &Db, scope_path: &Path) -> Result<()> {
    let conn = db.conn();
    let scope = fs::canonicalize(scope_path)
        .with_context(|| format!("Failed to canonicalize path: {}", scope_path.display()))?;

    // Check if scope is already a root or under a root (including suspended)
    if let Some((id, root_path, role, _)) = resolve_root_path_any(conn, &scope)? {
        let suspended: bool = conn.query_row(
            "SELECT suspended FROM roots WHERE id = ?",
            [id],
            |row| row.get(0),
        )?;
        let suspended_str = if suspended { " (suspended)" } else { "" };

        if scope.to_string_lossy() == root_path {
            println!("{} is already a {} root{}", scope.display(), role, suspended_str);
        } else {
            println!("{} is already under {} root {}{}", scope.display(), role, root_path, suspended_str);
        }
        return Ok(());
    }

    // Get all existing roots (excluding suspended for candidate discovery)
    let roots: Vec<PathBuf> = conn
        .prepare("SELECT path FROM roots WHERE suspended = 0")?
        .query_map([], |row| Ok(PathBuf::from(row.get::<_, String>(0)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    // Find directories with files, skipping tracked subtrees
    let mut dirs_with_files: HashSet<PathBuf> = HashSet::new();
    scan_for_untracked(&scope, &roots, &mut dirs_with_files)?;

    if dirs_with_files.is_empty() {
        println!("No untracked directories with files found under {}", scope.display());
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
fn scan_for_untracked(
    dir: &Path,
    roots: &[PathBuf],
    result: &mut HashSet<PathBuf>,
) -> Result<()> {
    // Skip if this directory is under an existing root
    if roots.iter().any(|root| dir == root || dir.starts_with(root)) {
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
    let has_file = entries.iter().any(|e| {
        e.file_type().map(|ft| ft.is_file()).unwrap_or(false)
    });

    // Check if this directory contains any root (can't be added as a root - invariant)
    let contains_root = roots.iter().any(|root| root.starts_with(dir) && root != dir);

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
            if roots.iter().any(|root| parent == root || parent.starts_with(root)) {
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
