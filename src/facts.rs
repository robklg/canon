use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

use crate::db;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

// Built-in facts derived from source data
const BUILTIN_FACTS: &[&str] = &["ext", "size", "mtime"];

pub fn run(db_path: &Path, key_arg: Option<&str>, path_arg: Option<&Path>, filter_strs: &[String], limit: usize) -> Result<()> {
    let conn = db::open(db_path)?;

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

    // Get all matching source IDs
    let source_ids = get_matching_sources(&conn, scope_prefix.as_deref(), &filters)?;
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        return Ok(());
    }

    println!("Sources matching filters: {}\n", total_sources);

    if let Some(fact_key) = key {
        if BUILTIN_FACTS.contains(&fact_key) {
            show_builtin_distribution(&conn, &source_ids, fact_key, total_sources, limit)?;
        } else {
            show_value_distribution(&conn, &source_ids, fact_key, total_sources, limit)?;
        }
    } else {
        show_all_keys(&conn, &source_ids, total_sources)?;
    }

    Ok(())
}

fn get_matching_sources(
    conn: &Connection,
    scope_prefix: Option<&str>,
    filters: &[Filter],
) -> Result<Vec<i64>> {
    let mut all_ids = Vec::new();
    let mut last_id: i64 = 0;

    loop {
        // Fetch batch of source IDs
        let batch: Vec<i64> = if let Some(prefix) = scope_prefix {
            // Filter by path prefix
            conn.prepare(
                "SELECT s.id
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND s.id > ?
                   AND (r.path || '/' || s.rel_path) LIKE ? || '%'
                 ORDER BY s.id
                 LIMIT ?"
            )?
            .query_map(rusqlite::params![last_id, prefix, BATCH_SIZE], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            conn.prepare(
                "SELECT s.id
                 FROM sources s
                 WHERE s.present = 1 AND s.id > ?
                 ORDER BY s.id
                 LIMIT ?"
            )?
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

fn show_all_keys(conn: &Connection, source_ids: &[i64], total_sources: usize) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Build a temp table for efficiency with large source lists
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_sources (id INTEGER PRIMARY KEY)", [])?;
    conn.execute("DELETE FROM temp_sources", [])?;

    let mut stmt = conn.prepare("INSERT INTO temp_sources (id) VALUES (?)")?;
    for id in source_ids {
        stmt.execute([id])?;
    }
    drop(stmt);

    // Get object IDs for these sources
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

    // Query fact keys from both source and object facts
    let mut results: Vec<(String, i64, bool)> = conn
        .prepare(
            "SELECT key, COUNT(DISTINCT entity_id) as cnt
             FROM facts
             WHERE (entity_type = 'source' AND entity_id IN (SELECT id FROM temp_sources))
                OR (entity_type = 'object' AND entity_id IN (SELECT id FROM temp_objects))
             GROUP BY key
             ORDER BY cnt DESC"
        )?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, false)))?
        .collect::<Result<Vec<_>, _>>()?;

    // Clean up temp tables
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
    conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;

    // Add built-in facts at the top (they always have 100% coverage)
    let mut all_results: Vec<(String, i64, bool)> = BUILTIN_FACTS
        .iter()
        .map(|&name| (name.to_string(), total_sources as i64, true))
        .collect();
    all_results.append(&mut results);

    // Print header
    println!("{:<30} {:>10} {:>10}", "Fact", "Count", "Coverage");
    println!("{}", "─".repeat(52));

    for (key, count, is_builtin) in &all_results {
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        let suffix = if *is_builtin { "  (built-in)" } else { "" };
        println!("{:<30} {:>10} {:>9.1}%{}", key, count, coverage, suffix);
    }

    Ok(())
}

fn show_value_distribution(
    conn: &Connection,
    source_ids: &[i64],
    key: &str,
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp tables
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_sources (id INTEGER PRIMARY KEY)", [])?;
    conn.execute("DELETE FROM temp_sources", [])?;

    let mut stmt = conn.prepare("INSERT INTO temp_sources (id) VALUES (?)")?;
    for id in source_ids {
        stmt.execute([id])?;
    }
    drop(stmt);

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

    // Query value distribution
    // Use COALESCE to get a displayable value from the typed columns
    let query = if limit == 0 {
        "SELECT
            COALESCE(value_text, CAST(value_num AS TEXT), datetime(value_time, 'unixepoch'), value_json) as val,
            COUNT(*) as cnt
         FROM facts
         WHERE key = ?
           AND ((entity_type = 'source' AND entity_id IN (SELECT id FROM temp_sources))
                OR (entity_type = 'object' AND entity_id IN (SELECT id FROM temp_objects)))
         GROUP BY val
         ORDER BY cnt DESC".to_string()
    } else {
        format!(
            "SELECT
                COALESCE(value_text, CAST(value_num AS TEXT), datetime(value_time, 'unixepoch'), value_json) as val,
                COUNT(*) as cnt
             FROM facts
             WHERE key = ?
               AND ((entity_type = 'source' AND entity_id IN (SELECT id FROM temp_sources))
                    OR (entity_type = 'object' AND entity_id IN (SELECT id FROM temp_objects)))
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

    // Count sources with this fact
    let sources_with_fact: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT CASE
            WHEN entity_type = 'source' THEN entity_id
            ELSE (SELECT s.id FROM sources s WHERE s.object_id = facts.entity_id LIMIT 1)
         END)
         FROM facts
         WHERE key = ?
           AND ((entity_type = 'source' AND entity_id IN (SELECT id FROM temp_sources))
                OR (entity_type = 'object' AND entity_id IN (SELECT id FROM temp_objects)))",
        [key],
        |row| row.get(0),
    )?;

    // Clean up temp tables
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
    conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;

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
    conn: &Connection,
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
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_sources (id INTEGER PRIMARY KEY)", [])?;
    conn.execute("DELETE FROM temp_sources", [])?;

    let mut stmt = conn.prepare("INSERT INTO temp_sources (id) VALUES (?)")?;
    for id in source_ids {
        stmt.execute([id])?;
    }
    drop(stmt);

    let label = format!("{} (built-in)", key);

    // For ext, we need to compute in Rust since SQLite can't easily extract extensions
    let mut counts: HashMap<String, i64> = HashMap::new();

    match key {
        "ext" => {
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
        "size" => {
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
        "mtime" => {
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
