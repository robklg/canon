use anyhow::{bail, Result};
use rusqlite::params;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::db::{canonicalize_scope, scope_param, Db, SCOPE_CLAUSE};
use crate::exclude;
use crate::filter::{self, Filter};

pub struct CompareOptions {
    pub include_excluded: bool,
    pub quiet: bool,
}

pub fn run(
    db: &mut Db,
    path_a: &Path,
    path_b: &Path,
    filter_strs: &[String],
    options: &CompareOptions,
) -> Result<bool> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Canonicalize both paths
    let scope_a = canonicalize_scope(Some(path_a))?;
    let scope_b = canonicalize_scope(Some(path_b))?;

    let Some(ref prefix_a) = scope_a else {
        bail!("Path A does not exist: {}", path_a.display());
    };
    let Some(ref prefix_b) = scope_b else {
        bail!("Path B does not exist: {}", path_b.display());
    };

    // Query sources in each scope
    let (sources_a, unhashed_a) = query_sources(conn, &scope_a, &filters, options.include_excluded)?;
    let (sources_b, unhashed_b) = query_sources(conn, &scope_b, &filters, options.include_excluded)?;

    // Build object_id sets
    let objects_a: HashSet<i64> = sources_a.keys().copied().collect();
    let objects_b: HashSet<i64> = sources_b.keys().copied().collect();

    // Compute differences
    let in_both: HashSet<i64> = objects_a.intersection(&objects_b).copied().collect();
    let only_in_a: HashSet<i64> = objects_a.difference(&objects_b).copied().collect();
    let only_in_b: HashSet<i64> = objects_b.difference(&objects_a).copied().collect();

    // Print header
    println!("Comparing:");
    println!("  A: {}", prefix_a);
    println!("  B: {}", prefix_b);
    println!();

    // Report unhashed files
    let total_unhashed = unhashed_a + unhashed_b;
    if total_unhashed > 0 {
        eprintln!(
            "Skipped {} unhashed files (use `canon worklist` to hash them)",
            total_unhashed
        );
        eprintln!();
    }

    // Check if identical
    let is_identical = only_in_a.is_empty() && only_in_b.is_empty();

    if is_identical {
        println!("Folders are identical ({} files)", in_both.len());
        return Ok(true);
    }

    // Print summary
    println!("Files in both (by content): {}", in_both.len());

    // Print only in A
    if !only_in_a.is_empty() {
        println!("Only in A: {}", only_in_a.len());
        if !options.quiet {
            let mut paths: Vec<&str> = only_in_a
                .iter()
                .filter_map(|oid| sources_a.get(oid))
                .map(|s| s.as_str())
                .collect();
            paths.sort();
            for path in paths {
                println!("  {}", path);
            }
        }
    }

    // Print only in B
    if !only_in_b.is_empty() {
        println!("Only in B: {}", only_in_b.len());
        if !options.quiet {
            let mut paths: Vec<&str> = only_in_b
                .iter()
                .filter_map(|oid| sources_b.get(oid))
                .map(|s| s.as_str())
                .collect();
            paths.sort();
            for path in paths {
                println!("  {}", path);
            }
        }
    }

    println!();
    println!("Folders are NOT identical");

    Ok(false)
}

/// Query sources in scope, returns (object_id -> path map, unhashed count)
fn query_sources(
    conn: &mut crate::db::Connection,
    scope_prefix: &Option<String>,
    filters: &[Filter],
    include_excluded: bool,
) -> Result<(HashMap<i64, String>, usize)> {
    let exclude_clause = exclude::exclude_clause(include_excluded);
    let prefix = scope_param(scope_prefix);

    // Query all sources in scope (exclude suspended roots)
    let rows: Vec<(i64, Option<i64>, String)> = {
        let mut stmt = conn.prepare(&format!(
            "SELECT s.id, s.object_id, r.path || '/' || s.rel_path as full_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND r.suspended = 0 AND {} AND {}",
            exclude_clause, SCOPE_CLAUSE
        ))?;
        let result = stmt
            .query_map(params![prefix, prefix], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        result
    };

    // Apply filters
    let source_ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    let filtered_ids: HashSet<i64> = if filters.is_empty() {
        source_ids.into_iter().collect()
    } else {
        filter::apply_filters(conn, &source_ids, filters)?
            .into_iter()
            .collect()
    };

    // Build result map and count unhashed
    let mut result: HashMap<i64, String> = HashMap::new();
    let mut unhashed = 0;

    for (id, object_id, path) in rows {
        if !filtered_ids.contains(&id) {
            continue;
        }

        match object_id {
            Some(oid) => {
                // If multiple sources have same object_id, keep first path
                result.entry(oid).or_insert(path);
            }
            None => {
                unhashed += 1;
            }
        }
    }

    Ok((result, unhashed))
}
