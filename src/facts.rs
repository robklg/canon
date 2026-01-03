use anyhow::{bail, Result};
use std::path::Path;

use crate::db::{populate_temp_sources, Connection, Db};
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

// Built-in source facts - default visible
const BUILTIN_FACTS_DEFAULT: &[&str] = &[
    "source.ext",
    "source.size",
    "source.mtime",
    "source.path",
];

// Built-in source facts - only shown with --all
const BUILTIN_FACTS_HIDDEN: &[&str] = &[
    "source.root",
    "source.rel_path",
    "source.device",
    "source.inode",
];

fn is_builtin_fact(key: &str) -> bool {
    BUILTIN_FACTS_DEFAULT.contains(&key) || BUILTIN_FACTS_HIDDEN.contains(&key)
}

pub fn run(db: &mut Db, key_arg: Option<&str>, path_arg: Option<&Path>, filter_strs: &[String], limit: usize, show_all: bool, include_archived: bool, include_excluded: bool) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Disambiguate key vs path: if key looks like a path, swap them
    let (key, scope_path): (Option<&str>, Option<&Path>) = match (key_arg, path_arg) {
        (Some(k), None) if k.starts_with('/') || k.starts_with('.') => {
            // Single arg that looks like a path
            (None, Some(Path::new(k)))
        }
        (k, p) => (k, p),
    };

    // Resolve scope path to realpath if provided
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Get excluded count for reporting
    let excluded_count = if !include_excluded {
        exclude::count_excluded(&conn, scope_prefix.as_deref(), include_archived)?
    } else {
        0
    };

    // Get all matching source IDs
    let source_ids = get_matching_sources(&conn, scope_prefix.as_deref(), &filters, include_archived, include_excluded)?;
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        if !include_excluded && excluded_count > 0 {
            println!("\n({} excluded sources hidden, use --include-excluded to show)", excluded_count);
        }
        return Ok(());
    }

    println!("Sources matching filters: {}\n", total_sources);

    if let Some(fact_key) = key {
        if is_builtin_fact(fact_key) {
            show_builtin_distribution(conn, &source_ids, fact_key, total_sources, limit)?;
        } else {
            show_value_distribution(conn, &source_ids, fact_key, total_sources, limit)?;
        }
    } else {
        show_all_keys(conn, &source_ids, total_sources, show_all)?;
    }

    // Report excluded count
    if !include_excluded && excluded_count > 0 {
        println!("\n({} excluded sources hidden, use --include-excluded to show)", excluded_count);
    }

    Ok(())
}

fn get_matching_sources(
    conn: &Connection,
    scope_prefix: Option<&str>,
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<Vec<i64>> {
    let mut all_ids = Vec::new();
    let mut last_id: i64 = 0;

    let role_clause = if include_archived {
        "1=1" // Include all roles
    } else {
        "r.role = 'source'"
    };

    let exclude_clause = exclude::exclude_clause(include_excluded);

    loop {
        // Fetch batch of source IDs
        let batch: Vec<i64> = if let Some(prefix) = scope_prefix {
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
            .query_map(rusqlite::params![last_id, prefix, BATCH_SIZE], |row| row.get(0))?
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
            .query_map(rusqlite::params![last_id, BATCH_SIZE], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        };

        if batch.is_empty() {
            break;
        }

        let max_id = *batch.last().unwrap();

        // Apply filters
        let filtered = if filters.is_empty() {
            batch
        } else {
            filter::apply_filters(conn, &batch, filters)?
        };

        all_ids.extend(filtered);
        last_id = max_id;
    }

    Ok(all_ids)
}

fn show_all_keys(conn: &mut Connection, source_ids: &[i64], total_sources: usize, show_all: bool) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Build a temp table for efficiency with large source lists
    populate_temp_sources(conn, source_ids)?;

    // Query fact keys from both source and object facts
    // Count sources (not entities) - multiple sources can share an object
    // Use UNION ALL for index efficiency, dedupe once in outer SELECT DISTINCT
    let mut results: Vec<(String, i64, bool)> = conn
        .prepare(
            "SELECT key, COUNT(*) as cnt
             FROM (
                 SELECT DISTINCT id, key FROM (
                     SELECT ts.id, f.key
                     FROM temp_sources ts
                     JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

                     UNION ALL

                     SELECT ts.id, f.key
                     FROM temp_sources ts
                     JOIN sources s ON s.id = ts.id
                     JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
                     WHERE s.object_id IS NOT NULL
                 )
             )
             GROUP BY key
             ORDER BY cnt DESC"
        )?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, false)))?
        .collect::<Result<Vec<_>, _>>()?;

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Add built-in facts at the top (they always have 100% coverage)
    let mut all_results: Vec<(String, i64, bool)> = BUILTIN_FACTS_DEFAULT
        .iter()
        .map(|&name| (name.to_string(), total_sources as i64, true))
        .collect();

    // Add hidden built-ins if --all flag is set
    if show_all {
        for &name in BUILTIN_FACTS_HIDDEN {
            all_results.push((name.to_string(), total_sources as i64, true));
        }
    }

    all_results.append(&mut results);

    // Print header
    println!("{:<30} {:>10} {:>10}", "Fact", "Count", "Coverage");
    println!("{}", "─".repeat(52));

    for (key, count, is_builtin) in &all_results {
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        let suffix = if *is_builtin { "  (built-in)" } else { "" };
        println!("{:<30} {:>10} {:>9.1}%{}", key, count, coverage, suffix);
    }

    if !show_all {
        let hidden_count = BUILTIN_FACTS_HIDDEN.len();
        println!("\n({} built-in facts hidden, use --all to show)", hidden_count);
    }

    Ok(())
}

fn show_value_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp table
    populate_temp_sources(conn, source_ids)?;

    // Query value distribution
    // Count sources (not entities) - multiple sources can share an object
    // Use COALESCE to get a displayable value from the typed columns
    // Use UNION ALL for index efficiency, dedupe once in outer SELECT DISTINCT
    let query = if limit == 0 {
        "SELECT val, COUNT(*) as cnt
         FROM (
             SELECT DISTINCT id, val FROM (
                 SELECT ts.id,
                     COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch'), f.value_json) as val
                 FROM temp_sources ts
                 JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

                 UNION ALL

                 SELECT ts.id,
                     COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch'), f.value_json) as val
                 FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
                 WHERE s.object_id IS NOT NULL
             )
         )
         GROUP BY val
         ORDER BY cnt DESC".to_string()
    } else {
        format!(
            "SELECT val, COUNT(*) as cnt
             FROM (
                 SELECT DISTINCT id, val FROM (
                     SELECT ts.id,
                         COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch'), f.value_json) as val
                     FROM temp_sources ts
                     JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

                     UNION ALL

                     SELECT ts.id,
                         COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch'), f.value_json) as val
                     FROM temp_sources ts
                     JOIN sources s ON s.id = ts.id
                     JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
                     WHERE s.object_id IS NOT NULL
                 )
             )
             GROUP BY val
             ORDER BY cnt DESC
             LIMIT {}",
            limit
        )
    };

    let results: Vec<(String, i64)> = conn
        .prepare(&query)?
        .query_map([key], |row| {
            let val: Option<String> = row.get(0)?;
            let cnt: i64 = row.get(1)?;
            Ok((val.unwrap_or_else(|| "(null)".to_string()), cnt))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Count sources that have this fact (either directly or via their object)
    // Use UNION ALL for index efficiency
    let sources_with_fact: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT id) FROM (
             SELECT ts.id
             FROM temp_sources ts
             JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

             UNION ALL

             SELECT ts.id
             FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
             WHERE s.object_id IS NOT NULL
         )",
        [key],
        |row| row.get(0),
    )?;

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Print header
    println!("{:<40} {:>10} {:>10}", key, "Count", "Coverage");
    println!("{}", "─".repeat(62));

    for (value, count) in &results {
        let display_val = if value.len() > 38 {
            format!("{}...", &value[..35])
        } else {
            value.clone()
        };
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        println!("{:<40} {:>10} {:>9.1}%", display_val, count, coverage);
    }

    // Show "(no value)" count
    let without_fact = total_sources as i64 - sources_with_fact;
    if without_fact > 0 {
        let coverage = (without_fact as f64 / total_sources as f64) * 100.0;
        println!("{:<40} {:>10} {:>9.1}%", "(no value)", without_fact, coverage);
    }

    Ok(())
}

fn show_builtin_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    use std::collections::HashMap;

    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp table
    populate_temp_sources(conn, source_ids)?;

    let label = format!("{} (built-in)", key);

    let mut counts: HashMap<String, i64> = HashMap::new();

    match key {
        "source.ext" => {
            let rows: Vec<String> = conn
                .prepare("SELECT rel_path FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for rel_path in rows {
                let ext = std::path::Path::new(&rel_path)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_lowercase())
                    .unwrap_or_default();
                *counts.entry(ext).or_insert(0) += 1;
            }
        }
        "source.size" => {
            let rows: Vec<i64> = conn
                .prepare("SELECT size FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for size in rows {
                let bucket = if size < 1024 {
                    "< 1 KB"
                } else if size < 1024 * 1024 {
                    "1 KB - 1 MB"
                } else if size < 10 * 1024 * 1024 {
                    "1 MB - 10 MB"
                } else if size < 100 * 1024 * 1024 {
                    "10 MB - 100 MB"
                } else if size < 1024 * 1024 * 1024 {
                    "100 MB - 1 GB"
                } else {
                    "> 1 GB"
                };
                *counts.entry(bucket.to_string()).or_insert(0) += 1;
            }
        }
        "source.mtime" => {
            let rows: Vec<i64> = conn
                .prepare("SELECT mtime FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for mtime in rows {
                let year = chrono::DateTime::from_timestamp(mtime, 0)
                    .map(|dt| dt.format("%Y").to_string())
                    .unwrap_or_else(|| "(unknown)".to_string());
                *counts.entry(year).or_insert(0) += 1;
            }
        }
        "source.path" => {
            let rows: Vec<(String, String)> = conn
                .prepare(
                    "SELECT r.path, s.rel_path FROM sources s
                     JOIN roots r ON s.root_id = r.id
                     WHERE s.id IN (SELECT id FROM temp_sources)"
                )?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;

            for (root_path, rel_path) in rows {
                let full_path = if rel_path.is_empty() {
                    root_path
                } else {
                    format!("{}/{}", root_path, rel_path)
                };
                *counts.entry(full_path).or_insert(0) += 1;
            }
        }
        "source.root" => {
            let rows: Vec<String> = conn
                .prepare(
                    "SELECT r.path FROM sources s
                     JOIN roots r ON s.root_id = r.id
                     WHERE s.id IN (SELECT id FROM temp_sources)"
                )?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for root_path in rows {
                *counts.entry(root_path).or_insert(0) += 1;
            }
        }
        "source.rel_path" => {
            let rows: Vec<String> = conn
                .prepare("SELECT rel_path FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for rel_path in rows {
                *counts.entry(rel_path).or_insert(0) += 1;
            }
        }
        "source.device" => {
            let rows: Vec<Option<i64>> = conn
                .prepare("SELECT device FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for device in rows {
                let val = device.map(|d| d.to_string()).unwrap_or_else(|| "(null)".to_string());
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.inode" => {
            let rows: Vec<Option<i64>> = conn
                .prepare("SELECT inode FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for inode in rows {
                let val = inode.map(|i| i.to_string()).unwrap_or_else(|| "(null)".to_string());
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        _ => return Ok(()),
    }

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Sort by count descending
    let mut results: Vec<(String, i64)> = counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1));

    // Apply limit
    if limit > 0 && results.len() > limit {
        results.truncate(limit);
    }

    // Print header
    println!("{:<40} {:>10} {:>10}", label, "Count", "Coverage");
    println!("{}", "─".repeat(62));

    for (value, count) in &results {
        let display_val = if value.is_empty() {
            "(no extension)".to_string()
        } else if value.len() > 38 {
            format!("{}...", &value[..35])
        } else {
            value.clone()
        };
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        println!("{:<40} {:>10} {:>9.1}%", display_val, count, coverage);
    }

    Ok(())
}

// ============================================================================
// Delete Facts
// ============================================================================

pub struct DeleteOptions {
    pub entity_type: String, // "source" or "object"
    pub dry_run: bool,
}

/// Check if a fact key is protected from deletion
fn is_protected_fact(key: &str) -> bool {
    key.starts_with("source.") || key.starts_with("policy.")
}

pub fn delete_facts(
    db: &mut Db,
    key: &str,
    scope_path: Option<&Path>,
    filter_strs: &[String],
    options: &DeleteOptions,
) -> Result<()> {
    // Validate key is not protected
    if is_protected_fact(key) {
        bail!(
            "Cannot delete protected fact '{}'. Facts in source.* and policy.* namespaces cannot be deleted.",
            key
        );
    }

    // Validate entity type
    if options.entity_type != "source" && options.entity_type != "object" {
        bail!(
            "Invalid entity type '{}'. Must be 'source' or 'object'.",
            options.entity_type
        );
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope path
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Get matching source IDs
    let source_ids = get_matching_sources(&conn, scope_prefix.as_deref(), &filters, true, true)?;

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // Build temp table for efficiency
    populate_temp_sources(conn, &source_ids)?;

    // Count and optionally delete based on entity type
    let (fact_count, entity_count) = if options.entity_type == "source" {
        // Delete facts on source entities
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (SELECT id FROM temp_sources)
               AND key = ?",
            [key],
            |row| row.get(0),
        )?;

        let entity_count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT entity_id) FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (SELECT id FROM temp_sources)
               AND key = ?",
            [key],
            |row| row.get(0),
        )?;

        if !options.dry_run && count > 0 {
            conn.execute(
                "DELETE FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ?",
                [key],
            )?;
        }

        (count, entity_count)
    } else {
        // Delete facts on object entities
        // First get object IDs from sources
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS temp_objects (id INTEGER PRIMARY KEY)",
            [],
        )?;
        conn.execute("DELETE FROM temp_objects", [])?;
        conn.execute(
            "INSERT OR IGNORE INTO temp_objects (id)
             SELECT DISTINCT object_id FROM sources
             WHERE id IN (SELECT id FROM temp_sources) AND object_id IS NOT NULL",
            [],
        )?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (SELECT id FROM temp_objects)
               AND key = ?",
            [key],
            |row| row.get(0),
        )?;

        let entity_count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT entity_id) FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (SELECT id FROM temp_objects)
               AND key = ?",
            [key],
            |row| row.get(0),
        )?;

        if !options.dry_run && count > 0 {
            conn.execute(
                "DELETE FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ?",
                [key],
            )?;
        }

        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;

        (count, entity_count)
    };

    // Clean up
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Report results
    let entity_label = if options.entity_type == "source" {
        "sources"
    } else {
        "objects"
    };

    if fact_count == 0 {
        println!("No '{}' facts found on matching {}.", key, entity_label);
    } else if options.dry_run {
        println!(
            "Would delete {} fact rows across {} {}",
            format_number(fact_count),
            format_number(entity_count),
            entity_label
        );
    } else {
        println!(
            "Deleted {} fact rows across {} {}",
            format_number(fact_count),
            format_number(entity_count),
            entity_label
        );
    }

    Ok(())
}

// ============================================================================
// Prune Stale Facts
// ============================================================================

pub fn prune_stale(db: &Db, dry_run: bool) -> Result<()> {
    let conn = db.conn();

    // Find stale source facts: where observed_basis_rev doesn't match current basis_rev
    let stale_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         JOIN sources s ON f.entity_type = 'source' AND f.entity_id = s.id
         WHERE f.observed_basis_rev IS NOT NULL
           AND f.observed_basis_rev != s.basis_rev",
        [],
        |row| row.get(0),
    )?;

    if stale_count == 0 {
        println!("No stale facts found.");
        return Ok(());
    }

    if dry_run {
        println!(
            "Would delete {} stale fact rows (observed_basis_rev mismatch)",
            format_number(stale_count)
        );
    } else {
        let deleted = conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (
                   SELECT f.entity_id FROM facts f
                   JOIN sources s ON f.entity_type = 'source' AND f.entity_id = s.id
                   WHERE f.observed_basis_rev IS NOT NULL
                     AND f.observed_basis_rev != s.basis_rev
               )
               AND observed_basis_rev IS NOT NULL
               AND observed_basis_rev != (
                   SELECT basis_rev FROM sources WHERE id = facts.entity_id
               )",
            [],
        )?;
        println!(
            "Deleted {} stale fact rows (observed_basis_rev mismatch)",
            format_number(deleted as i64)
        );
    }

    Ok(())
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
