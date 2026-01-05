use anyhow::{bail, Result};
use std::io::{self, Write};

use crate::db::{parse_root_spec, Db};

pub fn list(db: &Db) -> Result<()> {
    let conn = db.conn();

    let mut stmt = conn.prepare(
        "SELECT r.id, r.role, r.path, COUNT(s.id) as file_count
         FROM roots r
         LEFT JOIN sources s ON s.root_id = r.id AND s.present = 1
         GROUP BY r.id
         ORDER BY r.id",
    )?;

    let roots: Vec<(i64, String, String, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    if roots.is_empty() {
        println!("No roots registered. Use `canon scan --add --role <source|archive> <path>` to add one.");
        return Ok(());
    }

    // Print header
    println!("{:<4} {:<8} {:>8}  {}", "ID", "ROLE", "FILES", "PATH");

    for (id, role, path, file_count) in roots {
        println!("{:<4} {:<8} {:>8}  {}", id, role, file_count, path);
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
        eprintln!("  canon ls path:{}", path);
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
