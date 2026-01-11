use anyhow::{bail, Result};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

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
        "SELECT r.id, r.role, r.path, r.comment, COUNT(s.id) as file_count
         FROM roots r
         LEFT JOIN sources s ON s.root_id = r.id AND s.present = 1
         GROUP BY r.id
         ORDER BY r.id",
    )?;

    let roots: Vec<(i64, String, String, Option<String>, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Filter roots by scope if provided
    let filtered_roots: Vec<_> = match &scope_str {
        Some(scope) => roots
            .into_iter()
            .filter(|(_, _, path, _, _)| {
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
    println!("{:<4} {:<8} {:>8}  {}", "ID", "ROLE", "FILES", "PATH");

    for (id, role, path, comment, file_count) in filtered_roots {
        if let Some(c) = comment {
            println!("{:<4} {:<8} {:>8}  {} ({})", id, role, file_count, path, c);
        } else {
            println!("{:<4} {:<8} {:>8}  {}", id, role, file_count, path);
        }
    }

    Ok(())
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
