use anyhow::Result;
use serde::Serialize;
use std::io::{self, Write};
use std::path::Path;

use crate::db;

#[derive(Serialize)]
struct WorklistEntry {
    source_id: i64,
    path: String,
    root_id: i64,
    size: i64,
    mtime: i64,
    basis_rev: i64,
}

pub fn run(db_path: &Path) -> Result<()> {
    let conn = db::open(db_path)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    let mut stmt = conn.prepare(
        "SELECT s.id, r.path, s.rel_path, s.root_id, s.size, s.mtime, s.basis_rev
         FROM sources s
         JOIN roots r ON s.root_id = r.id
         WHERE s.present = 1
         ORDER BY s.id"
    )?;

    let entries = stmt.query_map([], |row| {
        let root_path: String = row.get(1)?;
        let rel_path: String = row.get(2)?;
        let full_path = if rel_path.is_empty() {
            root_path
        } else {
            format!("{}/{}", root_path, rel_path)
        };

        Ok(WorklistEntry {
            source_id: row.get(0)?,
            path: full_path,
            root_id: row.get(3)?,
            size: row.get(4)?,
            mtime: row.get(5)?,
            basis_rev: row.get(6)?,
        })
    })?;

    for entry in entries {
        let entry = entry?;
        let json = serde_json::to_string(&entry)?;
        writeln!(handle, "{}", json)?;
    }

    Ok(())
}
