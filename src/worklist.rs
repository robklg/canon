use anyhow::Result;
use rusqlite::types::Value;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::db::{build_scope_clause, Connection, Db};
use crate::path::canonicalize_scopes;
use crate::exclude;
use crate::filter::{self, get_fact_value, Filter};

const BATCH_SIZE: i64 = 1000;

#[derive(Serialize)]
struct WorklistEntry {
    source_id: i64,
    path: String,
    root_id: i64,
    size: i64,
    mtime: i64,
    basis_rev: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    facts: Option<HashMap<String, serde_json::Value>>,
    #[serde(skip_serializing)]
    object_id: Option<i64>,
}

struct FetchResult {
    entries: Vec<WorklistEntry>,
    max_id_seen: Option<i64>,
}

pub fn run(db: &mut Db, scope_paths: &[PathBuf], filter_strs: &[String], include_archived: bool, include_excluded: bool, unique_content: bool, emit_keys: &[String]) -> Result<()> {
    // Parse filters upfront
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Check excluded count if we're skipping them
    let conn = db.conn_mut();
    let excluded_count = if !include_excluded {
        exclude::count_excluded(conn, scope_prefixes.first().map(|s| s.as_str()), include_archived)?
    } else {
        0
    };

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut last_id: i64 = 0;
    let mut seen_objects: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut skipped_unhashed: u64 = 0;
    let mut skipped_duplicate: u64 = 0;

    loop {
        let result = fetch_batch(conn, last_id, &scope_prefixes, &filters, include_archived, include_excluded, unique_content, emit_keys)?;

        // If we didn't see any source IDs, we're done
        let max_id = match result.max_id_seen {
            Some(id) => id,
            None => break,
        };

        for entry in &result.entries {
            if unique_content {
                // Skip sources without an object_id
                if entry.object_id.is_none() {
                    skipped_unhashed += 1;
                    continue;
                }
                let object_id = entry.object_id.unwrap();
                // Skip if we've already emitted a source for this object
                if seen_objects.contains(&object_id) {
                    skipped_duplicate += 1;
                    continue;
                }
                seen_objects.insert(object_id);
            }
            let json = serde_json::to_string(entry)?;
            writeln!(handle, "{}", json)?;
        }

        last_id = max_id;
    }

    // Report stats to stderr
    if include_excluded && excluded_count > 0 {
        eprintln!("Included {} excluded sources", excluded_count);
    } else if !include_excluded && excluded_count > 0 {
        eprintln!("Skipped {} excluded sources", excluded_count);
    }
    if unique_content && (skipped_unhashed > 0 || skipped_duplicate > 0) {
        eprintln!("Skipped {} unhashed, {} duplicate sources", skipped_unhashed, skipped_duplicate);
    }

    Ok(())
}

fn fetch_batch(
    conn: &mut Connection,
    after_id: i64,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
    _unique_content: bool,
    emit_keys: &[String],
) -> Result<FetchResult> {
    // Build the query based on options
    let role_clause = if include_archived {
        "r.suspended = 0" // Include all roles, but not suspended
    } else {
        "r.role = 'source' AND r.suspended = 0"
    };

    let exclude_clause = exclude::exclude_clause(include_excluded);
    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);

    // Build params: scope params + after_id + batch_size
    let mut params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();
    params.push(Value::from(after_id));
    params.push(Value::from(BATCH_SIZE));

    let source_ids: Vec<i64> = conn
        .prepare(&format!(
            "SELECT s.id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {} AND {} AND s.id > ?
             ORDER BY s.id
             LIMIT ?",
            role_clause, exclude_clause, scope_clause
        ))?
        .query_map(rusqlite::params_from_iter(params), |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

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
        if let Some(entry) = fetch_entry(conn, source_id, emit_keys)? {
            entries.push(entry);
        }
    }

    Ok(FetchResult {
        entries,
        max_id_seen,
    })
}

fn fetch_entry(conn: &Connection, source_id: i64, emit_keys: &[String]) -> Result<Option<WorklistEntry>> {
    let row: Option<(i64, String, String, i64, i64, i64, i64, Option<i64>)> = conn
        .query_row(
            "SELECT s.id, r.path, s.rel_path, s.root_id, s.size, s.mtime, s.basis_rev, s.object_id
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
                row.get(7)?,
            )),
        )
        .ok();

    let Some((id, root_path, rel_path, root_id, size, mtime, basis_rev, object_id)) = row else {
        return Ok(None);
    };

    let full_path = if rel_path.is_empty() {
        root_path
    } else {
        format!("{}/{}", root_path, rel_path)
    };

    // Fetch requested facts using the existing get_fact_value from filter module
    // Always emit requested keys (null if absent) for consistent structure
    let facts = if emit_keys.is_empty() {
        None
    } else {
        let mut map = HashMap::new();
        for key in emit_keys {
            let (entity_type, entity_id) = if key.starts_with("source.") {
                ("source", Some(id))
            } else {
                ("object", object_id)
            };
            let value = match entity_id {
                Some(eid) => get_fact_value(conn, entity_type, eid, key)?
                    .map(|v| v.into())
                    .unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            };
            map.insert(key.clone(), value);
        }
        Some(map)
    };

    Ok(Some(WorklistEntry {
        source_id: id,
        path: full_path,
        root_id,
        size,
        mtime,
        basis_rev,
        facts,
        object_id,
    }))
}
