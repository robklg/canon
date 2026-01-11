use anyhow::{bail, Result};
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{parse_root_spec, parse_root_spec_any, Db};

pub fn list(db: &Db, scope: Option<&Path>, show_suspended: bool) -> Result<()> {
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

    let suspended_clause = if show_suspended { "1=1" } else { "r.suspended = 0" };
    let query = format!(
        "SELECT r.id, r.role, r.path, r.comment, r.last_scanned_at, r.suspended, COUNT(s.id) as file_count
         FROM roots r
         LEFT JOIN sources s ON s.root_id = r.id AND s.present = 1
         WHERE {}
         GROUP BY r.id
         ORDER BY r.id",
        suspended_clause
    );
    let mut stmt = conn.prepare(&query)?;

    let roots: Vec<(i64, String, String, Option<String>, Option<i64>, bool, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Filter roots by scope if provided
    let filtered_roots: Vec<_> = match &scope_str {
        Some(scope) => roots
            .into_iter()
            .filter(|(_, _, path, _, _, _, _)| {
                // Root is at or beneath scope: root path starts with scope
                // OR scope is beneath root: scope starts with root path
                path.starts_with(scope) || scope.starts_with(path)
            })
            .collect(),
        None => roots,
    };

    if filtered_roots.is_empty() {
        if scope.is_some() {
            println!("No roots at or beneath this path.");
        } else {
            println!("No roots registered. Use `canon scan --add --role <source|archive> <path>` to add one.");
        }
        return Ok(());
    }

    // Print header
    println!("{:<4} {:<8} {:>8}  {:<16}  {}", "ID", "ROLE", "FILES", "LAST SCAN", "PATH");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    for (id, role, path, comment, last_scanned_at, suspended, file_count) in filtered_roots {
        let scan_ago = format_time_ago(last_scanned_at, now);
        let suspended_marker = if suspended { " [suspended]" } else { "" };
        let path_with_info = match comment {
            Some(c) => format!("{}{} ({})", path, suspended_marker, c),
            None => format!("{}{}", path, suspended_marker),
        };
        println!("{:<4} {:<8} {:>8}  {:<16}  {}", id, role, file_count, scan_ago, path_with_info);
    }

    Ok(())
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
