use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};

use crate::db::Db;
use crate::root::{parse_root_spec, parse_root_spec_any, Root};
use crate::root_repo;

pub fn list(db: &Db, scope: Option<&Path>, suspended_only: bool) -> Result<()> {
    let conn = db.conn();

    // Canonicalize scope path if provided
    let scope_str = match scope {
        Some(p) => Some(
            fs::canonicalize(p)
                .map_err(|e| anyhow::anyhow!("Failed to resolve path '{}': {}", p.display(), e))?
                .to_string_lossy()
                .to_string(),
        ),
        None => None,
    };

    // Fetch all roots using repository layer
    let all_roots = root_repo::fetch_all(conn)?;

    // Apply domain predicates for filtering
    let filtered_roots: Vec<&Root> = all_roots
        .iter()
        .filter(|r| {
            // Filter by suspended status
            if suspended_only {
                r.is_suspended()
            } else {
                r.is_active()
            }
        })
        .filter(|r| {
            // Filter by scope if provided
            match &scope_str {
                Some(scope) => r.matches_scope(scope),
                None => true,
            }
        })
        .collect();

    if filtered_roots.is_empty() {
        if scope.is_some() {
            println!("No roots at or beneath this path.");
        } else {
            println!("No roots registered. Use `canon scan --add --role <source|archive> <path>` to add one.");
        }
        return Ok(());
    }

    // Fetch file counts for the filtered roots
    let root_ids: Vec<i64> = filtered_roots.iter().map(|r| r.id).collect();
    let file_counts = fetch_file_counts(conn, &root_ids)?;

    // Print header
    println!(
        "{:<4} {:<8} {:>8}  {:<16}  {}",
        "ID", "ROLE", "FILES", "LAST SCAN", "PATH"
    );

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    for root in filtered_roots {
        let file_count = file_counts.get(&root.id).copied().unwrap_or(0);
        let scan_ago = format_time_ago(root.last_scanned_at, now);
        let suspended_marker = if root.is_suspended() {
            " [suspended]"
        } else {
            ""
        };
        let path_with_info = match &root.comment {
            Some(c) => format!("{}{} ({})", root.path, suspended_marker, c),
            None => format!("{}{}", root.path, suspended_marker),
        };
        println!(
            "{:<4} {:<8} {:>8}  {:<16}  {}",
            root.id, root.role, file_count, scan_ago, path_with_info
        );
    }

    Ok(())
}

/// Fetch file counts for a set of root IDs.
///
/// Returns a HashMap from root_id to count of present sources.
fn fetch_file_counts(
    conn: &rusqlite::Connection,
    root_ids: &[i64],
) -> Result<HashMap<i64, i64>> {
    if root_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders: Vec<&str> = root_ids.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT root_id, COUNT(*) FROM sources WHERE present = 1 AND root_id IN ({}) GROUP BY root_id",
        placeholders.join(",")
    );

    let params: Vec<rusqlite::types::Value> = root_ids
        .iter()
        .map(|&id| rusqlite::types::Value::from(id))
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut counts = HashMap::new();
    for row in rows {
        let (root_id, count) = row?;
        counts.insert(root_id, count);
    }

    Ok(counts)
}

fn format_time_ago(timestamp: Option<i64>, now: i64) -> String {
    match timestamp {
        None => "never".to_string(),
        Some(ts) => {
            let secs = now - ts;
            if secs < 0 {
                "just now".to_string()
            } else if secs < 60 {
                format!("{}s ago", secs)
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else if secs < 86400 {
                format!("{}h ago", secs / 3600)
            } else {
                format!("{}d ago", secs / 86400)
            }
        }
    }
}

pub fn remove(db: &Db, spec: &str, yes: bool) -> Result<()> {
    let conn = db.conn();

    // Parse the spec to get root id and validate it exists
    let root_id = parse_root_spec(conn, spec, None)?;

    // Get root info for display
    let (path, role): (String, String) = conn.query_row(
        "SELECT path, role FROM roots WHERE id = ?",
        [root_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    // Count sources
    let source_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sources WHERE root_id = ?",
        [root_id],
        |row| row.get(0),
    )?;

    // Count sources whose content is in an archive (same object_id exists in an archive root)
    let in_archive_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sources s
         WHERE s.root_id = ?
           AND s.object_id IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM sources s2
               JOIN roots r2 ON s2.root_id = r2.id
               WHERE s2.object_id = s.object_id
                 AND r2.role = 'archive'
                 AND s2.present = 1
           )",
        [root_id],
        |row| row.get(0),
    )?;
    let not_in_archive = source_count - in_archive_count;

    if !yes {
        eprintln!("About to remove {} root: {}", role, path);
        eprintln!(
            "This will forget {} sources ({} in archive, {} not in archive).",
            source_count, in_archive_count, not_in_archive
        );
        eprintln!("Files on disk will NOT be deleted.");
        eprintln!();
        eprintln!("To see which sources will be forgotten:");
        eprintln!("  canon ls {}", path);
        eprintln!();
        eprint!("Proceed? [y/N] ");
        io::stderr().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            bail!("Aborted");
        }
    }

    // Delete facts for sources in this root
    conn.execute(
        "DELETE FROM facts WHERE entity_type = 'source' AND entity_id IN (
            SELECT id FROM sources WHERE root_id = ?
        )",
        [root_id],
    )?;

    // Delete sources
    let deleted_sources = conn.execute("DELETE FROM sources WHERE root_id = ?", [root_id])?;

    // Delete the root
    conn.execute("DELETE FROM roots WHERE id = ?", [root_id])?;

    println!("Removed root {} and {} sources", root_id, deleted_sources);

    Ok(())
}

pub fn set_comment(db: &Db, spec: &str, comment: Option<&str>) -> Result<()> {
    let conn = db.conn();

    // Parse the spec to get root id and validate it exists
    let root_id = parse_root_spec(conn, spec, None)?;

    conn.execute(
        "UPDATE roots SET comment = ? WHERE id = ?",
        rusqlite::params![comment, root_id],
    )?;

    match comment {
        Some(c) => println!("Set comment on root {}: {}", root_id, c),
        None => println!("Cleared comment on root {}", root_id),
    }

    Ok(())
}

pub fn suspend(db: &Db, spec: &str) -> Result<()> {
    let conn = db.conn();

    // Use parse_root_spec_any to allow suspending already-suspended roots (no-op)
    let root_id = parse_root_spec_any(conn, spec)?;

    // Get root info for display
    let (path, suspended): (String, bool) =
        conn.query_row("SELECT path, suspended FROM roots WHERE id = ?", [root_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;

    if suspended {
        println!("Root {} is already suspended: {}", root_id, path);
        return Ok(());
    }

    conn.execute("UPDATE roots SET suspended = 1 WHERE id = ?", [root_id])?;
    println!("Suspended root {}: {}", root_id, path);
    Ok(())
}

pub fn unsuspend(db: &Db, spec: &str) -> Result<()> {
    let conn = db.conn();

    // Use parse_root_spec_any to find suspended roots
    let root_id = parse_root_spec_any(conn, spec)?;

    // Get root info for display
    let (path, suspended): (String, bool) =
        conn.query_row("SELECT path, suspended FROM roots WHERE id = ?", [root_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;

    if !suspended {
        println!("Root {} is not suspended: {}", root_id, path);
        return Ok(());
    }

    conn.execute("UPDATE roots SET suspended = 0 WHERE id = ?", [root_id])?;
    println!("Unsuspended root {}: {}", root_id, path);
    Ok(())
}
