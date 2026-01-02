use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::io::{self, Write};
use std::path::Path;

use crate::db;
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

#[derive(Serialize)]
struct WorklistEntry {
    source_id: i64,
    path: String,
    root_id: i64,
    size: i64,
    mtime: i64,
    basis_rev: i64,
}

struct FetchResult {
    entries: Vec<WorklistEntry>,
    max_id_seen: Option<i64>,
}

pub fn run(db_path: &Path, scope_path: Option<&Path>, filter_strs: &[String], include_archived: bool, include_excluded: bool) -> Result<()> {
    // Parse filters upfront
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope path to realpath if provided
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Check excluded count if we're skipping them
    let conn = db::open(db_path)?;
    let excluded_count = if !include_excluded {
        exclude::count_excluded(&conn, scope_prefix.as_deref(), include_archived)?
    } else {
        0
    };
    drop(conn);

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut last_id: i64 = 0;
    let mut output_count: i64 = 0;

    loop {
        // Open connection for each batch to avoid holding locks
        let conn = db::open(db_path)?;
        let result = fetch_batch(&conn, last_id, scope_prefix.as_deref(), &filters, include_archived, include_excluded)?;

        // If we didn't see any source IDs, we're done
        let max_id = match result.max_id_seen {
            Some(id) => id,
            None => break,
        };

        for entry in &result.entries {
            let json = serde_json::to_string(entry)?;
            writeln!(handle, "{}", json)?;
            output_count += 1;
        }

        last_id = max_id;
        // Connection dropped here, releasing any locks
    }

    // Report stats to stderr
    if include_excluded && excluded_count > 0 {
        eprintln!("Included {} excluded sources", excluded_count);
    } else if !include_excluded && excluded_count > 0 {
        eprintln!("Skipped {} excluded sources", excluded_count);
    }

    Ok(())
}

fn fetch_batch(
    conn: &Connection,
    after_id: i64,
    scope_prefix: Option<&str>,
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<FetchResult> {
    // Build the query based on options
    let role_clause = if include_archived {
        "1=1" // Include all roles
    } else {
        "r.role = 'source'"
    };

    let exclude_clause = exclude::exclude_clause(include_excluded);

    let source_ids: Vec<i64> = if let Some(prefix) = scope_prefix {
        // Filter by path prefix
        conn.prepare(&format!(
            "SELECT s.id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {} AND s.id > ?
               AND (r.path || '/' || s.rel_path) LIKE ? || '%'
             ORDER BY s.id
             LIMIT ?",
            role_clause, exclude_clause
        ))?
        .query_map(rusqlite::params![after_id, prefix, BATCH_SIZE], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?
    } else {
        conn.prepare(&format!(
            "SELECT s.id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {} AND s.id > ?
             ORDER BY s.id
             LIMIT ?",
            role_clause, exclude_clause
        ))?
        .query_map(rusqlite::params![after_id, BATCH_SIZE], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?
    };

    if source_ids.is_empty() {
        return Ok(FetchResult {
            entries: Vec::new(),
            max_id_seen: None,
        });
    }

    // Track the max ID we fetched (for pagination), before filtering
    let max_id_seen = source_ids.last().copied();

    // Apply filters
    let filtered_ids = if filters.is_empty() {
        source_ids
    } else {
        filter::apply_filters(conn, &source_ids, filters)?
    };

    // Fetch full entries for filtered IDs
    let mut entries = Vec::new();
    for source_id in filtered_ids {
        if let Some(entry) = fetch_entry(conn, source_id)? {
            entries.push(entry);
        }
    }

    Ok(FetchResult {
        entries,
        max_id_seen,
    })
}

fn fetch_entry(conn: &Connection, source_id: i64) -> Result<Option<WorklistEntry>> {
    let row: Option<(i64, String, String, i64, i64, i64, i64)> = conn
        .query_row(
            "SELECT s.id, r.path, s.rel_path, s.root_id, s.size, s.mtime, s.basis_rev
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [source_id],
            |row| Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            )),
        )
        .ok();

    Ok(row.map(|(id, root_path, rel_path, root_id, size, mtime, basis_rev)| {
        let full_path = if rel_path.is_empty() {
            root_path
        } else {
            format!("{}/{}", root_path, rel_path)
        };

        WorklistEntry {
            source_id: id,
            path: full_path,
            root_id,
            size,
            mtime,
            basis_rev,
        }
    }))
}
