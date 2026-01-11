use anyhow::{bail, Result};
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{parse_root_spec, Db};

pub fn list(db: &Db, scope: Option<&Path>) -> Result<()> {
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

    let mut stmt = conn.prepare(
        "SELECT r.id, r.role, r.path, r.comment, r.last_scanned_at, COUNT(s.id) as file_count
         FROM roots r
         LEFT JOIN sources s ON s.root_id = r.id AND s.present = 1
         GROUP BY r.id
         ORDER BY r.id",
    )?;

    let roots: Vec<(i64, String, String, Option<String>, Option<i64>, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Filter roots by scope if provided
    let filtered_roots: Vec<_> = match &scope_str {
        Some(scope) => roots
            .into_iter()
            .filter(|(_, _, path, _, _, _)| {
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

    for (id, role, path, comment, last_scanned_at, file_count) in filtered_roots {
        let scan_ago = format_time_ago(last_scanned_at, now);
        let path_with_comment = match comment {
            Some(c) => format!("{} ({})", path, c),
            None => path,
        };
        println!("{:<4} {:<8} {:>8}  {:<16}  {}", id, role, file_count, scan_ago, path_with_comment);
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

    if !yes {
        eprintln!("About to remove {} root: {}", role, path);
        eprintln!("This will forget {} sources from the database.", source_count);
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
