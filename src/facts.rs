use anyhow::{bail, Result};
use rusqlite::types::Value;
use std::path::PathBuf;

use crate::db::{build_scope_clause, canonicalize_scopes, populate_temp_sources, Connection, Db};
use crate::exclude;
use crate::expr::{self, BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, FactValue, Modifier, PathAccessor};
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

fn get_fact_category(key: &str) -> BuiltinKeyCategory {
    BuiltinKey::from_str(key)
        .map(|k| k.category())
        .unwrap_or(BuiltinKeyCategory::Stored)
}

fn is_builtin_or_derived(key: &str) -> bool {
    BuiltinKey::from_str(key)
        .map(|k| k.visibility() != BuiltinKeyVisibility::NotListed)
        .unwrap_or(false)
}

/// Apply accessor and modifiers to a FactValue, returning a string for grouping
fn apply_transforms(
    value: FactValue,
    accessor: &Option<PathAccessor>,
    modifiers: &[Modifier],
    key: &str,
) -> Result<String> {
    let mut result = value;

    // Apply accessor if present
    if let Some(acc) = accessor {
        result = expr::apply_accessor(&result, acc, key)?;
    }

    // Apply modifiers
    for modifier in modifiers {
        result = expr::apply_modifier(&result, *modifier, key)?;
    }

    // Convert to string for grouping
    Ok(match result {
        FactValue::Text(t) => t,
        FactValue::Path(p) => p,
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", n as i64)
            } else {
                format!("{}", n)
            }
        }
        FactValue::Time(ts) => {
            chrono::DateTime::from_timestamp(ts, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string())
        }
    })
}

pub fn run(db: &mut Db, key_arg: Option<&str>, scope_paths: &[PathBuf], filter_strs: &[String], limit: usize, show_all: bool, include_archived: bool, include_excluded: bool) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get excluded count for reporting
    let excluded_count = if !include_excluded {
        exclude::count_excluded(conn, scope_prefixes.first().map(|s| s.as_str()), include_archived)?
    } else {
        0
    };

    // Get all matching source IDs
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, include_excluded)?;
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        if !include_excluded && excluded_count > 0 {
            println!("\n({} excluded sources hidden, use --include-excluded to show)", excluded_count);
        }
        return Ok(());
    }

    println!("Sources matching filters: {}\n", total_sources);

    if let Some(fact_key) = key_arg {
        // Parse key for accessor and modifiers
        let (base_key, accessor, modifiers) = expr::parse_key_with_modifiers(fact_key)?;
        let has_transforms = accessor.is_some() || !modifiers.is_empty();

        if is_builtin_or_derived(&base_key) {
            show_builtin_distribution(conn, &source_ids, &base_key, fact_key, &accessor, &modifiers, total_sources, limit)?;
        } else if has_transforms {
            // Stored fact with transforms - need to fetch raw values and apply transforms
            show_transformed_distribution(conn, &source_ids, &base_key, fact_key, &accessor, &modifiers, total_sources, limit)?;
        } else {
            // Stored fact without transforms - use SQL grouping
            show_value_distribution(conn, &source_ids, &base_key, total_sources, limit)?;
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
    scope_prefixes: &[String],
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
    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);

    loop {
        // Build params: scope params + last_id + batch_size
        let mut params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();
        params.push(Value::from(last_id));
        params.push(Value::from(BATCH_SIZE));

        // Fetch batch of source IDs
        let batch: Vec<i64> = conn
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
    let results: Vec<(String, i64, bool)> = conn
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

    // Add built-in and derived facts at the top (they always have 100% coverage)
    use strum::IntoEnumIterator;
    let mut all_results: Vec<(String, i64, BuiltinKeyCategory)> = Vec::new();

    for key in BuiltinKey::iter() {
        let vis = key.visibility();
        // Skip NotListed keys and Hidden keys (unless --all)
        if vis == BuiltinKeyVisibility::NotListed {
            continue;
        }
        if vis == BuiltinKeyVisibility::Hidden && !show_all {
            continue;
        }
        let name: &'static str = key.into();
        all_results.push((name.to_string(), total_sources as i64, key.category()));
    }

    // Add stored facts (with Stored category)
    let stored_results: Vec<(String, i64, BuiltinKeyCategory)> = results
        .into_iter()
        .map(|(key, count, _)| (key, count, BuiltinKeyCategory::Stored))
        .collect();
    all_results.extend(stored_results);

    // Print header
    println!("{:<30} {:>10} {:>10}", "Fact", "Count", "Coverage");
    println!("{}", "─".repeat(52));

    for (key, count, category) in &all_results {
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        let suffix = match category {
            BuiltinKeyCategory::BuiltIn => "  (built-in)",
            BuiltinKeyCategory::Derived => "  (derived)",
            BuiltinKeyCategory::Stored => "",
        };
        println!("{:<30} {:>10} {:>9.1}%{}", key, count, coverage, suffix);
    }

    if !show_all {
        let hidden_count = BuiltinKey::iter()
            .filter(|k| k.visibility() == BuiltinKeyVisibility::Hidden)
            .count();
        println!("\n({} built-in/derived facts hidden, use --all to show)", hidden_count);
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
                     COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch')) as val
                 FROM temp_sources ts
                 JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

                 UNION ALL

                 SELECT ts.id,
                     COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch')) as val
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
                         COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch')) as val
                     FROM temp_sources ts
                     JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

                     UNION ALL

                     SELECT ts.id,
                         COALESCE(f.value_text, CAST(f.value_num AS TEXT), datetime(f.value_time, 'unixepoch')) as val
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

/// Show distribution for stored facts with transforms (accessor/modifiers)
fn show_transformed_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    base_key: &str,
    display_key: &str,
    accessor: &Option<PathAccessor>,
    modifiers: &[Modifier],
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    use std::collections::HashMap;

    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp table
    populate_temp_sources(conn, source_ids)?;

    // Fetch raw values and apply transforms
    // Query both source and object facts
    let rows: Vec<(Option<String>, Option<f64>, Option<i64>)> = conn
        .prepare(
            "SELECT DISTINCT
                 COALESCE(f.value_text, NULL) as text_val,
                 COALESCE(f.value_num, NULL) as num_val,
                 COALESCE(f.value_time, NULL) as time_val
             FROM (
                 SELECT ts.id, f.value_text, f.value_num, f.value_time
                 FROM temp_sources ts
                 JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

                 UNION ALL

                 SELECT ts.id, f.value_text, f.value_num, f.value_time
                 FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
                 WHERE s.object_id IS NOT NULL
             ) f"
        )?
        .query_map([base_key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_fact: i64 = 0;

    for (text_val, num_val, time_val) in rows {
        let fact_value = if let Some(t) = text_val {
            FactValue::Text(t)
        } else if let Some(n) = num_val {
            FactValue::Num(n)
        } else if let Some(ts) = time_val {
            FactValue::Time(ts)
        } else {
            continue;
        };

        sources_with_fact += 1;
        let transformed = apply_transforms(fact_value, accessor, modifiers, display_key)?;
        *counts.entry(transformed).or_insert(0) += 1;
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
    println!("{:<40} {:>10} {:>10}", display_key, "Count", "Coverage");
    println!("{}", "─".repeat(62));

    for (value, count) in &results {
        let display_val = if value.is_empty() {
            "(empty)".to_string()
        } else if value.len() > 38 {
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
    base_key: &str,
    display_key: &str,
    accessor: &Option<PathAccessor>,
    modifiers: &[Modifier],
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    use std::collections::HashMap;

    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp table
    populate_temp_sources(conn, source_ids)?;

    let category = get_fact_category(base_key);
    let category_str = match category {
        BuiltinKeyCategory::BuiltIn => "built-in",
        BuiltinKeyCategory::Derived => "derived",
        BuiltinKeyCategory::Stored => "stored",
    };
    let label = format!("{} ({})", display_key, category_str);

    let has_transforms = accessor.is_some() || !modifiers.is_empty();
    let mut counts: HashMap<String, i64> = HashMap::new();

    match base_key {
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
                let val = if has_transforms {
                    apply_transforms(FactValue::Text(ext), accessor, modifiers, display_key)?
                } else {
                    ext
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.size" => {
            let rows: Vec<i64> = conn
                .prepare("SELECT size FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for size in rows {
                let val = if has_transforms {
                    apply_transforms(FactValue::Num(size as f64), accessor, modifiers, display_key)?
                } else {
                    // Default: size buckets
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
                    bucket.to_string()
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.mtime" => {
            let rows: Vec<i64> = conn
                .prepare("SELECT mtime FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for mtime in rows {
                let val = if has_transforms {
                    apply_transforms(FactValue::Time(mtime), accessor, modifiers, display_key)?
                } else {
                    // Default: group by year
                    chrono::DateTime::from_timestamp(mtime, 0)
                        .map(|dt| dt.format("%Y").to_string())
                        .unwrap_or_else(|| "(unknown)".to_string())
                };
                *counts.entry(val).or_insert(0) += 1;
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
                let val = if has_transforms {
                    apply_transforms(FactValue::Path(full_path), accessor, modifiers, display_key)?
                } else {
                    full_path
                };
                *counts.entry(val).or_insert(0) += 1;
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
                let val = if has_transforms {
                    apply_transforms(FactValue::Path(root_path), accessor, modifiers, display_key)?
                } else {
                    root_path
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.rel_path" => {
            let rows: Vec<String> = conn
                .prepare("SELECT rel_path FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for rel_path in rows {
                let val = if has_transforms {
                    apply_transforms(FactValue::Path(rel_path), accessor, modifiers, display_key)?
                } else {
                    rel_path
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.device" => {
            let rows: Vec<Option<i64>> = conn
                .prepare("SELECT device FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for device in rows {
                let val = match device {
                    Some(d) => {
                        if has_transforms {
                            apply_transforms(FactValue::Num(d as f64), accessor, modifiers, display_key)?
                        } else {
                            d.to_string()
                        }
                    }
                    None => "(null)".to_string(),
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "source.inode" => {
            let rows: Vec<Option<i64>> = conn
                .prepare("SELECT inode FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for inode in rows {
                let val = match inode {
                    Some(i) => {
                        if has_transforms {
                            apply_transforms(FactValue::Num(i as f64), accessor, modifiers, display_key)?
                        } else {
                            i.to_string()
                        }
                    }
                    None => "(null)".to_string(),
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        "filename" => {
            let rows: Vec<String> = conn
                .prepare("SELECT rel_path FROM sources WHERE id IN (SELECT id FROM temp_sources)")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            for rel_path in rows {
                let filename = std::path::Path::new(&rel_path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or(&rel_path)
                    .to_string();
                let val = if has_transforms {
                    apply_transforms(FactValue::Text(filename), accessor, modifiers, display_key)?
                } else {
                    filename
                };
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
    scope_paths: &[PathBuf],
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

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get matching source IDs
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, true, true)?;

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

// ============================================================================
// Show Aliases
// ============================================================================

pub fn show_aliases() {
    use crate::expr::BuiltinKey;
    use strum::IntoEnumIterator;

    println!("Pattern Aliases:");
    println!();

    for key in BuiltinKey::iter() {
        if let Some(expansion) = key.expansion() {
            let name: &'static str = key.into();
            println!("  {:<15} \u{2192} {}", name, expansion);
        }
    }

    println!();
    println!("Note: 'filename' and 'ext' also work in --where filters.");
    println!("Other aliases (stem, hash, hash_short, id) only work in manifest patterns.");
}
