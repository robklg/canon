use anyhow::{bail, Result};
use rusqlite::types::Value;
use std::path::PathBuf;

use std::collections::HashMap;

use crate::db::{build_scope_clause, populate_temp_sources, Connection, Db};
use crate::path::canonicalize_scopes;
use crate::exclude;
use crate::expr::{self, BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, FactType, FactValue, ModifierCall, PathAccessor};
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

/// Represents a parsed grouping key
struct GroupingKey {
    raw: String,                      // Original key string for display
    base_key: String,                 // Base fact key
    accessor: Option<PathAccessor>,
    modifiers: Vec<ModifierCall>,
    is_root: bool,                    // Special handling for source.root
}

impl GroupingKey {
    fn parse(key: &str) -> Result<Self> {
        let is_root = key == "source.root";
        let (base_key, accessor, modifiers) = expr::parse_key_with_modifiers(key)?;
        Ok(GroupingKey {
            raw: key.to_string(),
            base_key,
            accessor,
            modifiers,
            is_root,
        })
    }
}

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
    modifiers: &[ModifierCall],
    key: &str,
) -> Result<String> {
    let mut result = value;

    // Apply accessor if present
    if let Some(acc) = accessor {
        result = expr::apply_accessor(&result, acc, key)?;
    }

    // Apply modifiers (for_display: true since this is for facts output)
    for modifier_call in modifiers {
        result = expr::apply_modifier(&result, modifier_call, key, true)?;
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

pub fn run(db: &mut Db, key_arg: Option<&str>, scope_paths: &[PathBuf], filter_strs: &[String], limit: usize, show_all: bool, include_archived: bool, include_excluded: bool, by_root: bool, group_by: &[String]) -> Result<()> {
    // Validate grouping requires --key
    let has_grouping = by_root || !group_by.is_empty();
    if has_grouping && key_arg.is_none() {
        bail!("--by-root and --group-by require --key to be specified");
    }

    // Build grouping keys list
    let mut grouping_keys: Vec<GroupingKey> = Vec::new();
    if by_root {
        grouping_keys.push(GroupingKey::parse("source.root")?);
    }
    for key in group_by {
        grouping_keys.push(GroupingKey::parse(key)?);
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get all matching source IDs
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, include_excluded)?;
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        // Only show excluded hint if excluded sources actually match the filter
        if !include_excluded {
            let excluded_matches = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, true)?.len();
            if excluded_matches > 0 {
                println!("\n({} excluded sources hidden, use --include-excluded to show)", excluded_matches);
            }
        }
        return Ok(());
    }

    // Get excluded count for reporting (sources matching filter but excluded)
    let excluded_count = if !include_excluded {
        get_matching_sources(conn, &scope_prefixes, &filters, include_archived, true)?.len() - source_ids.len()
    } else {
        0
    };

    println!("Sources matching filters: {}\n", total_sources);

    if let Some(fact_key) = key_arg {
        // Parse key for accessor and modifiers
        let (base_key, accessor, modifiers) = expr::parse_key_with_modifiers(fact_key)?;
        let has_transforms = accessor.is_some() || !modifiers.is_empty();

        if !grouping_keys.is_empty() {
            // Grouped distribution
            show_grouped_distribution(conn, &source_ids, &base_key, fact_key, &accessor, &modifiers, &grouping_keys, total_sources, limit)?;
        } else if is_builtin_or_derived(&base_key) {
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
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<Vec<i64>> {
    let mut all_ids = Vec::new();
    let mut last_id: i64 = 0;

    let role_clause = if include_archived {
        "r.suspended = 0" // Include all roles, but not suspended
    } else {
        "r.role = 'source' AND r.suspended = 0"
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
    // Also determine fact type from which column is non-null
    let results: Vec<(String, i64, FactType)> = conn
        .prepare(
            "SELECT key, COUNT(*) as cnt,
                    MAX(CASE WHEN value_text IS NOT NULL THEN 1 ELSE 0 END) as is_text,
                    MAX(CASE WHEN value_num IS NOT NULL THEN 1 ELSE 0 END) as is_num,
                    MAX(CASE WHEN value_time IS NOT NULL THEN 1 ELSE 0 END) as is_time
             FROM (
                 SELECT DISTINCT id, key, value_text, value_num, value_time FROM (
                     SELECT ts.id, f.key, f.value_text, f.value_num, f.value_time
                     FROM temp_sources ts
                     JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

                     UNION ALL

                     SELECT ts.id, f.key, f.value_text, f.value_num, f.value_time
                     FROM temp_sources ts
                     JOIN sources s ON s.id = ts.id
                     JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
                     WHERE s.object_id IS NOT NULL
                 )
             )
             GROUP BY key
             ORDER BY cnt DESC"
        )?
        .query_map([], |row| {
            let key: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            let _is_text: i64 = row.get(2)?;
            let is_num: i64 = row.get(3)?;
            let is_time: i64 = row.get(4)?;
            let fact_type = if is_time == 1 {
                FactType::Time
            } else if is_num == 1 {
                FactType::Num
            } else {
                FactType::Text
            };
            Ok((key, count, fact_type))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Add built-in and derived facts at the top (they always have 100% coverage)
    use strum::IntoEnumIterator;
    let mut all_results: Vec<(String, i64, BuiltinKeyCategory, FactType)> = Vec::new();

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
        all_results.push((name.to_string(), total_sources as i64, key.category(), key.fact_type()));
    }

    // Add stored facts (with Stored category)
    let stored_results: Vec<(String, i64, BuiltinKeyCategory, FactType)> = results
        .into_iter()
        .map(|(key, count, fact_type)| (key, count, BuiltinKeyCategory::Stored, fact_type))
        .collect();
    all_results.extend(stored_results);

    // Print header
    println!("{:<30} {:>6} {:>10} {:>10}", "Fact", "Type", "Count", "Coverage");
    println!("{}", "─".repeat(60));

    for (key, count, category, fact_type) in &all_results {
        let coverage = (*count as f64 / total_sources as f64) * 100.0;
        let suffix = match category {
            BuiltinKeyCategory::BuiltIn => "  (built-in)",
            BuiltinKeyCategory::Derived => "  (derived)",
            BuiltinKeyCategory::Stored => "",
        };
        println!("{:<30} {:>6} {:>10} {:>9.1}%{}", key, fact_type.as_str(), count, coverage, suffix);
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
    modifiers: &[ModifierCall],
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
    // Query both source and object facts (one row per source)
    let rows: Vec<(Option<String>, Option<f64>, Option<i64>)> = conn
        .prepare(
            "SELECT
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
    let mut skipped_type_mismatch: i64 = 0;

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
        match apply_transforms(fact_value, accessor, modifiers, display_key) {
            Ok(transformed) => {
                *counts.entry(transformed).or_insert(0) += 1;
            }
            Err(_) => {
                // Type mismatch (e.g., text value when time modifier expected)
                // This can happen with malformed data - skip and count
                skipped_type_mismatch += 1;
            }
        }
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

    // Warn about skipped values due to type mismatch
    if skipped_type_mismatch > 0 {
        eprintln!(
            "Warning: skipped {} values with incompatible type for transform",
            skipped_type_mismatch
        );
    }

    Ok(())
}

fn show_builtin_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    base_key: &str,
    display_key: &str,
    accessor: &Option<PathAccessor>,
    modifiers: &[ModifierCall],
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
// Grouped Distribution
// ============================================================================

/// Data needed for a single source when computing grouped distribution
struct SourceData {
    source_id: i64,
    root_id: i64,
    root_path: String,
    rel_path: String,
    size: i64,
    mtime: i64,
    device: Option<i64>,
    inode: Option<i64>,
}

/// Fetch raw SourceData for all sources in temp_sources
fn fetch_source_data(conn: &Connection) -> Result<Vec<SourceData>> {
    let rows: Vec<SourceData> = conn
        .prepare(
            "SELECT ts.id, s.root_id, r.path, s.rel_path, s.size, s.mtime, s.device, s.inode
             FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             JOIN roots r ON s.root_id = r.id"
        )?
        .query_map([], |row| {
            Ok(SourceData {
                source_id: row.get(0)?,
                root_id: row.get(1)?,
                root_path: row.get(2)?,
                rel_path: row.get(3)?,
                size: row.get(4)?,
                mtime: row.get(5)?,
                device: row.get(6)?,
                inode: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Get a FactValue for a built-in key from source data
fn get_builtin_value(data: &SourceData, builtin: BuiltinKey) -> Option<FactValue> {
    match builtin {
        BuiltinKey::SourceExt => {
            let ext = std::path::Path::new(&data.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();
            Some(FactValue::Text(ext))
        }
        BuiltinKey::SourceSize | BuiltinKey::Size => {
            Some(FactValue::Num(data.size as f64))
        }
        BuiltinKey::SourceMtime | BuiltinKey::Mtime => {
            Some(FactValue::Time(data.mtime))
        }
        BuiltinKey::SourcePath => {
            let full_path = if data.rel_path.is_empty() {
                data.root_path.clone()
            } else {
                format!("{}/{}", data.root_path, data.rel_path)
            };
            Some(FactValue::Path(full_path))
        }
        BuiltinKey::SourceRoot => {
            Some(FactValue::Path(data.root_path.clone()))
        }
        BuiltinKey::SourceRelPath => {
            Some(FactValue::Path(data.rel_path.clone()))
        }
        BuiltinKey::SourceId | BuiltinKey::Id => {
            Some(FactValue::Num(data.source_id as f64))
        }
        BuiltinKey::SourceDevice => {
            data.device.map(|d| FactValue::Num(d as f64))
        }
        BuiltinKey::SourceInode => {
            data.inode.map(|i| FactValue::Num(i as f64))
        }
        BuiltinKey::Filename => {
            let filename = std::path::Path::new(&data.rel_path)
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or(&data.rel_path)
                .to_string();
            Some(FactValue::Text(filename))
        }
        BuiltinKey::Stem => {
            let stem = std::path::Path::new(&data.rel_path)
                .file_stem()
                .and_then(|f| f.to_str())
                .unwrap_or("")
                .to_string();
            Some(FactValue::Text(stem))
        }
        BuiltinKey::Ext => {
            let ext = std::path::Path::new(&data.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();
            Some(FactValue::Text(ext))
        }
        BuiltinKey::RootId => {
            Some(FactValue::Num(data.root_id as f64))
        }
        // Stored facts (hash, content.hash.sha256) need DB lookup
        BuiltinKey::Hash | BuiltinKey::HashShort | BuiltinKey::ContentHashSha256 => None,
    }
}

/// Fetch stored fact values for all sources, returning a map of source_id -> FactValue
fn fetch_stored_fact_values(conn: &Connection, key: &str) -> Result<HashMap<i64, FactValue>> {
    let rows: Vec<(i64, Option<String>, Option<f64>, Option<i64>)> = conn
        .prepare(
            "SELECT ts.id, f.value_text, f.value_num, f.value_time
             FROM temp_sources ts
             LEFT JOIN sources s ON s.id = ts.id
             LEFT JOIN facts f ON (
                 (f.entity_type = 'source' AND f.entity_id = ts.id)
                 OR (f.entity_type = 'object' AND f.entity_id = s.object_id)
             ) AND f.key = ?1"
        )?
        .query_map([key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut map = HashMap::new();
    for (source_id, text_val, num_val, time_val) in rows {
        let fact_value = if let Some(t) = text_val {
            Some(FactValue::Text(t))
        } else if let Some(n) = num_val {
            Some(FactValue::Num(n))
        } else if let Some(ts) = time_val {
            Some(FactValue::Time(ts))
        } else {
            None
        };
        if let Some(v) = fact_value {
            map.insert(source_id, v);
        }
    }
    Ok(map)
}

/// Format root for display: id:N ...truncated_path
fn format_root_display(root_id: i64, root_path: &str) -> String {
    const MAX_PATH_LEN: usize = 30;
    let id_prefix = format!("id:{:<2}", root_id);
    if root_path.len() <= MAX_PATH_LEN {
        format!("{} {}", id_prefix, root_path)
    } else {
        let truncated = &root_path[root_path.len() - MAX_PATH_LEN + 3..];
        format!("{} ...{}", id_prefix, truncated)
    }
}

/// Show distribution with grouping by root and/or other fact keys
fn show_grouped_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    main_base_key: &str,
    main_display_key: &str,
    main_accessor: &Option<PathAccessor>,
    main_modifiers: &[ModifierCall],
    grouping_keys: &[GroupingKey],
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Build temp table
    populate_temp_sources(conn, source_ids)?;

    // Fetch source data for built-in keys
    let source_data = fetch_source_data(conn)?;
    // Determine if main key is built-in
    let main_builtin = BuiltinKey::from_str(main_base_key);

    // Fetch stored fact values if needed for main key
    let main_stored_values: HashMap<i64, FactValue> = if main_builtin.is_none() {
        fetch_stored_fact_values(conn, main_base_key)?
    } else {
        HashMap::new()
    };

    // Fetch stored fact values for each grouping key that isn't built-in
    let mut grouping_stored_values: Vec<HashMap<i64, FactValue>> = Vec::new();
    for gk in grouping_keys {
        if BuiltinKey::from_str(&gk.base_key).is_none() {
            grouping_stored_values.push(fetch_stored_fact_values(conn, &gk.base_key)?);
        } else {
            grouping_stored_values.push(HashMap::new());
        }
    }

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    // Aggregate: (main_value, group_values...) -> count
    // Also track root_id for display when grouping by root
    #[derive(Hash, Eq, PartialEq, Clone)]
    struct GroupKey {
        main_value: String,
        group_values: Vec<String>,
    }

    struct GroupInfo {
        count: i64,
        root_id: Option<i64>,
        root_path: Option<String>,
    }

    let mut aggregated: HashMap<GroupKey, GroupInfo> = HashMap::new();
    let mut sources_with_main_value: i64 = 0;

    for data in &source_data {
        // Get main value
        let main_value_opt = if let Some(builtin) = main_builtin {
            get_builtin_value(data, builtin)
        } else {
            main_stored_values.get(&data.source_id).cloned()
        };

        let main_value = match main_value_opt {
            Some(v) => {
                sources_with_main_value += 1;
                match apply_transforms(v, main_accessor, main_modifiers, main_display_key) {
                    Ok(s) => s,
                    Err(_) => continue,
                }
            }
            None => continue,
        };

        // Get grouping values
        let mut group_values: Vec<String> = Vec::new();
        let mut root_id_for_display: Option<i64> = None;
        let mut root_path_for_display: Option<String> = None;

        for (i, gk) in grouping_keys.iter().enumerate() {
            let gk_builtin = BuiltinKey::from_str(&gk.base_key);

            let gk_value_opt = if let Some(builtin) = gk_builtin {
                get_builtin_value(data, builtin)
            } else {
                grouping_stored_values[i].get(&data.source_id).cloned()
            };

            let gk_value = match gk_value_opt {
                Some(v) => {
                    match apply_transforms(v, &gk.accessor, &gk.modifiers, &gk.raw) {
                        Ok(s) => s,
                        Err(_) => "(transform error)".to_string(),
                    }
                }
                None => "(no value)".to_string(),
            };

            // Track root info for special display
            if gk.is_root {
                root_id_for_display = Some(data.root_id);
                root_path_for_display = Some(data.root_path.clone());
            }

            group_values.push(gk_value);
        }

        let key = GroupKey { main_value, group_values };
        let entry = aggregated.entry(key).or_insert(GroupInfo {
            count: 0,
            root_id: root_id_for_display,
            root_path: root_path_for_display,
        });
        entry.count += 1;
    }

    // Build results: group by main_value, then sort sub-groups
    struct MainValueGroup {
        main_value: String,
        total_count: i64,
        sub_groups: Vec<(Vec<String>, i64, Option<i64>, Option<String>)>, // (group_values, count, root_id, root_path)
    }

    let mut by_main_value: HashMap<String, MainValueGroup> = HashMap::new();

    for (key, info) in aggregated {
        let entry = by_main_value.entry(key.main_value.clone()).or_insert(MainValueGroup {
            main_value: key.main_value,
            total_count: 0,
            sub_groups: Vec::new(),
        });
        entry.total_count += info.count;
        entry.sub_groups.push((key.group_values, info.count, info.root_id, info.root_path));
    }

    // Sort main values by total count descending
    let mut main_values: Vec<MainValueGroup> = by_main_value.into_values().collect();
    main_values.sort_by(|a, b| b.total_count.cmp(&a.total_count));

    // Apply limit to top N main values
    if limit > 0 && main_values.len() > limit {
        main_values.truncate(limit);
    }

    // Sort sub-groups within each main value by count descending
    for mv in &mut main_values {
        mv.sub_groups.sort_by(|a, b| b.1.cmp(&a.1));
    }

    // Build grouping label
    let grouping_label = if grouping_keys.len() == 1 && grouping_keys[0].is_root {
        "by root".to_string()
    } else {
        let labels: Vec<&str> = grouping_keys.iter().map(|gk| gk.raw.as_str()).collect();
        format!("grouped by {}", labels.join(", "))
    };

    // Print header
    println!("{} ({})\n", main_display_key, grouping_label);

    for mv in &main_values {
        let coverage = (mv.total_count as f64 / total_sources as f64) * 100.0;
        let main_display = if mv.main_value.is_empty() {
            "(empty)"
        } else {
            &mv.main_value
        };
        println!("{} (total: {:>6}, {:>5.1}%)", main_display, format_number(mv.total_count), coverage);

        for (group_values, count, root_id, root_path) in &mv.sub_groups {
            let sub_coverage = (*count as f64 / mv.total_count as f64) * 100.0;

            // Format group display
            let group_display = if grouping_keys.len() == 1 && grouping_keys[0].is_root {
                // Special root display
                if let (Some(rid), Some(rpath)) = (root_id, root_path) {
                    format_root_display(*rid, rpath)
                } else {
                    group_values[0].clone()
                }
            } else {
                // Multiple grouping keys or non-root
                let parts: Vec<String> = grouping_keys.iter().enumerate().map(|(i, gk)| {
                    if gk.is_root {
                        if let (Some(rid), Some(rpath)) = (root_id, root_path) {
                            format_root_display(*rid, rpath)
                        } else {
                            group_values[i].clone()
                        }
                    } else {
                        group_values[i].clone()
                    }
                }).collect();
                parts.join(" / ")
            };

            println!("  {:<40} {:>8} {:>6.1}%", group_display, format_number(*count), sub_coverage);
        }
        println!();
    }

    // Show "(no value)" count for main key
    let without_main_value = total_sources as i64 - sources_with_main_value;
    if without_main_value > 0 {
        let coverage = (without_main_value as f64 / total_sources as f64) * 100.0;
        println!("(no value) (total: {:>6}, {:>5.1}%)", format_number(without_main_value), coverage);
    }

    Ok(())
}

// ============================================================================
// Delete Facts
// ============================================================================

pub struct DeleteOptions {
    pub entity_type: String, // "source" or "object"
    pub value_type: Option<String>, // "text", "num", or "time"
    pub dry_run: bool,
}

/// Build SQL clause for value type filter
fn value_type_clause(value_type: &Option<String>) -> &'static str {
    match value_type.as_deref() {
        Some("text") => "AND value_text IS NOT NULL",
        Some("num") => "AND value_num IS NOT NULL",
        Some("time") => "AND value_time IS NOT NULL",
        _ => "",
    }
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

    // Build value type clause for filtering
    let vt_clause = value_type_clause(&options.value_type);

    // Count and optionally delete based on entity type
    let (fact_count, entity_count) = if options.entity_type == "source" {
        // Delete facts on source entities
        let count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        let entity_count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        if !options.dry_run && count > 0 {
            conn.execute(
                &format!(
                    "DELETE FROM facts
                     WHERE entity_type = 'source'
                       AND entity_id IN (SELECT id FROM temp_sources)
                       AND key = ? {}",
                    vt_clause
                ),
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
            &format!(
                "SELECT COUNT(*) FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        let entity_count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        if !options.dry_run && count > 0 {
            conn.execute(
                &format!(
                    "DELETE FROM facts
                     WHERE entity_type = 'object'
                       AND entity_id IN (SELECT id FROM temp_objects)
                       AND key = ? {}",
                    vt_clause
                ),
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
// Prune Orphaned Objects
// ============================================================================

/// Delete objects (and their facts) that have no remaining present sources.
/// Also deletes non-present sources that reference these objects.
///
/// An object is considered orphaned when no source with `present = 1` references it.
/// This can happen when:
/// - All sources for a piece of content were deleted from disk
/// - Sources were moved cross-device (old source marked not present, new source created)
/// - Manual cleanup removed sources but not objects
///
/// Note: You may want to keep orphaned objects because:
/// - They serve as a historical record of content you've processed
/// - If the content reappears (restore from backup, found on another drive),
///   all the facts (EXIF, hashes, etc.) are already available
/// - Storage cost is minimal (just metadata rows)
pub fn prune_orphaned_objects(db: &Db, dry_run: bool) -> Result<()> {
    let conn = db.conn();

    // Find orphaned objects: objects with no present sources
    let orphaned_object_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM objects o
         WHERE NOT EXISTS (
             SELECT 1 FROM sources s
             WHERE s.object_id = o.id AND s.present = 1
         )",
        [],
        |row| row.get(0),
    )?;

    if orphaned_object_count == 0 {
        println!("No orphaned objects found.");
        return Ok(());
    }

    // Count non-present sources that reference orphaned objects
    let orphaned_source_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sources s
         WHERE s.present = 0
           AND s.object_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s2
                   WHERE s2.object_id = o.id AND s2.present = 1
               )
           )",
        [],
        |row| row.get(0),
    )?;

    // Count source facts that would be deleted
    let source_fact_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         WHERE f.entity_type = 'source'
           AND f.entity_id IN (
               SELECT s.id FROM sources s
               WHERE s.present = 0
                 AND s.object_id IN (
                     SELECT o.id FROM objects o
                     WHERE NOT EXISTS (
                         SELECT 1 FROM sources s2
                         WHERE s2.object_id = o.id AND s2.present = 1
                     )
                 )
           )",
        [],
        |row| row.get(0),
    )?;

    // Count object facts that would be deleted
    let object_fact_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         WHERE f.entity_type = 'object'
           AND f.entity_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s
                   WHERE s.object_id = o.id AND s.present = 1
               )
           )",
        [],
        |row| row.get(0),
    )?;

    let total_fact_count = source_fact_count + object_fact_count;

    if dry_run {
        println!(
            "Would delete {} orphaned objects, {} non-present sources, and {} facts",
            format_number(orphaned_object_count),
            format_number(orphaned_source_count),
            format_number(total_fact_count)
        );
        println!();
        println!("Note: Orphaned objects represent content you've seen but no longer have.");
        println!("They may be useful if the content reappears (backup restore, found elsewhere).");
        println!("Object-level exclusions will also be deleted (use `exclude list-objects` to review).");
        println!("Use --yes to proceed with deletion.");
    } else {
        // Delete source facts first
        let source_facts_deleted = conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (
                   SELECT s.id FROM sources s
                   WHERE s.present = 0
                     AND s.object_id IN (
                         SELECT o.id FROM objects o
                         WHERE NOT EXISTS (
                             SELECT 1 FROM sources s2
                             WHERE s2.object_id = o.id AND s2.present = 1
                         )
                     )
               )",
            [],
        )?;

        // Delete non-present sources that reference orphaned objects
        let sources_deleted = conn.execute(
            "DELETE FROM sources
             WHERE present = 0
               AND object_id IN (
                   SELECT o.id FROM objects o
                   WHERE NOT EXISTS (
                       SELECT 1 FROM sources s
                       WHERE s.object_id = o.id AND s.present = 1
                   )
               )",
            [],
        )?;

        // Delete object facts
        let object_facts_deleted = conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (
                   SELECT o.id FROM objects o
                   WHERE NOT EXISTS (
                       SELECT 1 FROM sources s
                       WHERE s.object_id = o.id AND s.present = 1
                   )
               )",
            [],
        )?;

        // Delete orphaned objects
        let objects_deleted = conn.execute(
            "DELETE FROM objects
             WHERE NOT EXISTS (
                 SELECT 1 FROM sources s
                 WHERE s.object_id = objects.id AND s.present = 1
             )",
            [],
        )?;

        let total_facts_deleted = source_facts_deleted + object_facts_deleted;

        println!(
            "Deleted {} orphaned objects, {} non-present sources, and {} facts",
            format_number(objects_deleted as i64),
            format_number(sources_deleted as i64),
            format_number(total_facts_deleted as i64)
        );
    }

    Ok(())
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
