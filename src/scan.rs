use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::db;

#[derive(Default)]
struct ScanStats {
    scanned: u64,
    new: u64,
    updated: u64,
    moved: u64,
    unchanged: u64,
    missing: u64,
}

pub fn run(db_path: &Path, paths: &[PathBuf]) -> Result<()> {
    let conn = db::open(db_path)?;
    let now = current_timestamp();

    let mut total_stats = ScanStats::default();

    for path in paths {
        let canonical = fs::canonicalize(path)
            .with_context(|| format!("Failed to canonicalize path: {}", path.display()))?;

        let root_id = get_or_create_root(&conn, &canonical)?;
        check_overlapping_roots(&conn, &canonical)?;

        let stats = scan_root(&conn, root_id, &canonical, now)?;

        total_stats.scanned += stats.scanned;
        total_stats.new += stats.new;
        total_stats.updated += stats.updated;
        total_stats.moved += stats.moved;
        total_stats.unchanged += stats.unchanged;
        total_stats.missing += stats.missing;
    }

    println!(
        "Scanned {} files: {} new, {} updated, {} moved, {} unchanged, {} missing",
        total_stats.scanned,
        total_stats.new,
        total_stats.updated,
        total_stats.moved,
        total_stats.unchanged,
        total_stats.missing
    );

    Ok(())
}

fn get_or_create_root(conn: &Connection, path: &Path) -> Result<i64> {
    let path_str = path.to_str().context("Path is not valid UTF-8")?;

    // Try to find existing root
    let existing: Option<i64> = conn
        .query_row("SELECT id FROM roots WHERE path = ?", [path_str], |row| {
            row.get(0)
        })
        .optional()?;

    if let Some(id) = existing {
        return Ok(id);
    }

    // Create new root
    conn.execute("INSERT INTO roots (path) VALUES (?)", [path_str])?;
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

fn scan_root(conn: &Connection, root_id: i64, root_path: &Path, now: i64) -> Result<ScanStats> {
    let mut stats = ScanStats::default();
    let mut seen_source_ids: HashSet<i64> = HashSet::new();

    for entry in WalkDir::new(root_path).follow_links(false) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: {}", e);
                continue;
            }
        };

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

        let result = process_file(
            conn,
            root_id,
            rel_path_str,
            device,
            inode,
            size,
            mtime,
            now,
        )?;

        seen_source_ids.insert(result.source_id);

        match result.action {
            FileAction::New => stats.new += 1,
            FileAction::Updated => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }
    }

    // Mark missing files
    stats.missing = mark_missing(conn, root_id, &seen_source_ids, now)?;

    Ok(stats)
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
}

fn process_file(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    device: i64,
    inode: i64,
    size: i64,
    mtime: i64,
    now: i64,
) -> Result<ProcessResult> {
    // First, check if we have an existing source at this path
    let existing_by_path: Option<(i64, Option<i64>, Option<i64>, i64, i64, i64)> = conn
        .query_row(
            "SELECT id, device, inode, size, mtime, basis_rev FROM sources
             WHERE root_id = ? AND rel_path = ?",
            params![root_id, rel_path],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;

    if let Some((id, old_device, old_inode, old_size, old_mtime, old_basis_rev)) = existing_by_path {
        // Source exists at this path
        let basis_changed = size != old_size
            || mtime != old_mtime
            || Some(device) != old_device
            || Some(inode) != old_inode;

        if basis_changed {
            let new_basis_rev = old_basis_rev + 1;
            conn.execute(
                "UPDATE sources SET device = ?, inode = ?, size = ?, mtime = ?,
                 basis_rev = ?, last_seen_at = ?, present = 1 WHERE id = ?",
                params![device, inode, size, mtime, new_basis_rev, now, id],
            )?;
            return Ok(ProcessResult {
                source_id: id,
                action: FileAction::Updated,
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
            });
        }
    }

    // Check if we have an existing source with this device+inode (moved file)
    let existing_by_inode: Option<(i64, i64, String, i64)> = conn
        .query_row(
            "SELECT id, root_id, rel_path, basis_rev FROM sources
             WHERE device = ? AND inode = ?",
            params![device, inode],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;

    if let Some((id, old_root_id, _old_rel_path, old_basis_rev)) = existing_by_inode {
        // File was moved
        // Note: We might need to handle cross-root moves differently, but for now
        // we'll just update to the new location
        let basis_changed = old_root_id != root_id; // Cross-root move is a basis change
        let new_basis_rev = if basis_changed {
            old_basis_rev + 1
        } else {
            old_basis_rev
        };

        conn.execute(
            "UPDATE sources SET root_id = ?, rel_path = ?, size = ?, mtime = ?,
             basis_rev = ?, last_seen_at = ?, present = 1 WHERE id = ?",
            params![root_id, rel_path, size, mtime, new_basis_rev, now, id],
        )?;
        return Ok(ProcessResult {
            source_id: id,
            action: FileAction::Moved,
        });
    }

    // New file
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
         basis_rev, scanned_at, last_seen_at, present)
         VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 1)",
        params![root_id, rel_path, device, inode, size, mtime, now, now],
    )?;

    Ok(ProcessResult {
        source_id: conn.last_insert_rowid(),
        action: FileAction::New,
    })
}

fn mark_missing(
    conn: &Connection,
    root_id: i64,
    seen_ids: &HashSet<i64>,
    now: i64,
) -> Result<u64> {
    // Get all source IDs for this root that are currently present
    let mut stmt = conn.prepare(
        "SELECT id FROM sources WHERE root_id = ? AND present = 1"
    )?;

    let all_ids: Vec<i64> = stmt
        .query_map([root_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut missing_count = 0u64;
    for id in all_ids {
        if !seen_ids.contains(&id) {
            conn.execute(
                "UPDATE sources SET present = 0, last_seen_at = ? WHERE id = ?",
                params![now, id],
            )?;
            missing_count += 1;
        }
    }

    Ok(missing_count)
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}
