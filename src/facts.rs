use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::db::{populate_temp_sources, Connection, Db};
use crate::expr::{self, BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, FactType, FactValue, ModifierCall, ParsedFactKey, PathAccessor};
use crate::fact::FactEntry;
use crate::fact_repo;
use crate::fact_value;
use crate::filter::{self, Filter};
use crate::path::canonicalize_scopes;
use crate::scope::ScopeMatch;
use crate::source_repo;

/// Check if a parsed key represents source.root (for special display formatting)
fn is_root_key(key: &ParsedFactKey) -> bool {
    key.raw == "source.root" || key.raw == "root_id"
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

/// Convert a FactValue to a display string.
fn fact_value_to_display(value: &FactValue) -> String {
    match value {
        FactValue::Text(t) => t.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
            }
        }
        FactValue::Time(ts) => {
            chrono::DateTime::from_timestamp(*ts, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string())
        }
    }
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

    // Convert to string for display
    Ok(fact_value_to_display(&result))
}

pub fn run(db: &mut Db, key_arg: Option<&str>, scope_paths: &[PathBuf], filter_strs: &[String], limit: usize, show_all: bool, include_archived: bool, include_excluded: bool, by_root: bool, group_by: &[String]) -> Result<()> {
    // Validate grouping requires --key
    let has_grouping = by_root || !group_by.is_empty();
    if has_grouping && key_arg.is_none() {
        bail!("--by-root and --group-by require --key to be specified");
    }

    // Build grouping keys list
    let mut grouping_keys: Vec<ParsedFactKey> = Vec::new();
    if by_root {
        grouping_keys.push(ParsedFactKey::parse("source.root")?);
    }
    for key in group_by {
        grouping_keys.push(ParsedFactKey::parse(key)?);
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get all matching source IDs using domain predicates
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    let (source_ids, excluded_count) = get_matching_sources(conn, &scopes, &filters, include_archived, include_excluded)?;
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        // Show excluded hint if excluded sources were filtered out
        if !include_excluded && excluded_count > 0 {
            println!("\n({} excluded sources hidden, use --include-excluded to show)", excluded_count);
        }
        return Ok(());
    }

    println!("Sources matching filters: {}\n", total_sources);

    if let Some(fact_key) = key_arg {
        // Parse key for accessor and modifiers
        let main_key = ParsedFactKey::parse(fact_key)?;

        if !grouping_keys.is_empty() {
            // Grouped distribution
            show_grouped_distribution(conn, &source_ids, &main_key, &grouping_keys, total_sources, limit)?;
        } else if is_builtin_or_derived(&main_key.base_key) {
            show_builtin_distribution(conn, &source_ids, &main_key.base_key, fact_key, &main_key.accessor, &main_key.modifiers, total_sources, limit)?;
        } else if main_key.has_transforms() {
            // Stored fact with transforms - need to fetch raw values and apply transforms
            show_transformed_distribution(conn, &source_ids, &main_key.base_key, fact_key, &main_key.accessor, &main_key.modifiers, total_sources, limit)?;
        } else {
            // Stored fact without transforms - use SQL grouping
            show_value_distribution(conn, &source_ids, &main_key.base_key, total_sources, limit)?;
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

/// Fetch sources matching scope/role/exclusion criteria, then apply --where filters.
///
/// Returns (source_ids, excluded_count) where excluded_count is the number
/// of sources that matched scope/role but were excluded.
fn get_matching_sources(
    conn: &mut Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<(Vec<i64>, usize)> {
    // 1. Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 2. Fetch all present sources for those roots
    let all_sources = source_repo::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Filter using domain predicates, tracking excluded count
    let mut excluded_count = 0usize;
    let filtered: Vec<i64> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| include_archived || s.is_from_role("source"))
        .filter(|s| s.matches_scope(scopes))
        .filter(|s| {
            if s.is_excluded() {
                if !include_excluded {
                    excluded_count += 1;
                    return false;
                }
            }
            true
        })
        .map(|s| s.id)
        .collect();

    // 4. Apply --where filters if present
    if filters.is_empty() {
        return Ok((filtered, excluded_count));
    }

    let filtered_ids = filter::apply_filters(conn, &filtered, filters)?;
    let filtered_id_set: HashSet<i64> = filtered_ids.into_iter().collect();

    // Keep only IDs that passed the filter (preserving order)
    let result: Vec<i64> = filtered
        .into_iter()
        .filter(|id| filtered_id_set.contains(id))
        .collect();

    Ok((result, excluded_count))
}

fn show_all_keys(conn: &mut Connection, source_ids: &[i64], total_sources: usize, show_all: bool) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Use fact_repo to count fact keys
    let results = fact_repo::count_fact_keys(conn, source_ids)?;

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
        .map(|(key, count, fact_type)| (key, count as i64, BuiltinKeyCategory::Stored, fact_type))
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

    // Fetch values using fact_repo
    let fact_map = fact_repo::batch_fetch_key_for_sources(conn, source_ids, key)?;

    // Group values and count
    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_fact: i64 = 0;

    for (_source_id, entry_opt) in &fact_map {
        if let Some(entry) = entry_opt {
            sources_with_fact += 1;
            let display_val = fact_value_to_display(&entry.value);
            *counts.entry(display_val).or_insert(0) += 1;
        }
    }

    // Sort by count descending
    let mut results: Vec<(String, i64)> = counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1));

    // Apply limit
    if limit > 0 && results.len() > limit {
        results.truncate(limit);
    }

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
    if source_ids.is_empty() {
        return Ok(());
    }

    // Fetch values using fact_repo
    let fact_map = fact_repo::batch_fetch_key_for_sources(conn, source_ids, base_key)?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_fact: i64 = 0;
    let mut skipped_type_mismatch: i64 = 0;

    for (_source_id, entry_opt) in &fact_map {
        if let Some(entry) = entry_opt {
            sources_with_fact += 1;
            match apply_transforms(entry.value.clone(), accessor, modifiers, display_key) {
                Ok(transformed) => {
                    *counts.entry(transformed).or_insert(0) += 1;
                }
                Err(_) => {
                    // Type mismatch (e.g., text value when time modifier expected)
                    skipped_type_mismatch += 1;
                }
            }
        }
    }

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
    main_key: &ParsedFactKey,
    grouping_keys: &[ParsedFactKey],
    total_sources: usize,
    limit: usize,
) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // 1. FETCH - Use infrastructure layer
    // Fetch all sources by ID
    let sources = source_repo::batch_fetch_by_ids(conn, source_ids)?;

    // Collect all stored fact keys we need to fetch
    let mut stored_keys: Vec<&str> = Vec::new();
    if !main_key.is_builtin() {
        stored_keys.push(&main_key.base_key);
    }
    for gk in grouping_keys {
        if !gk.is_builtin() && !stored_keys.contains(&gk.base_key.as_str()) {
            stored_keys.push(&gk.base_key);
        }
    }

    // Fetch stored facts for all needed keys
    let all_facts: HashMap<i64, HashMap<String, FactEntry>> = if stored_keys.is_empty() {
        HashMap::new()
    } else {
        // Fetch each key and merge results per source
        let mut merged: HashMap<i64, HashMap<String, FactEntry>> = HashMap::new();
        for key in &stored_keys {
            let key_facts = fact_repo::batch_fetch_key_for_sources(conn, source_ids, key)?;
            for (source_id, entry_opt) in key_facts {
                if let Some(entry) = entry_opt {
                    merged.entry(source_id).or_default().insert(entry.key.clone(), entry);
                }
            }
        }
        merged
    };

    // 2. RESOLVE + AGGREGATE
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

    for source_id in source_ids {
        let source = match sources.get(source_id) {
            Some(s) => s,
            None => continue,
        };

        // Get stored facts for this source (empty map if none)
        let empty_facts: HashMap<String, FactEntry> = HashMap::new();
        let stored_facts = all_facts.get(source_id).unwrap_or(&empty_facts);

        // Resolve main value using domain layer
        let main_value = match fact_value::resolve_fact_value(source, main_key, stored_facts)? {
            Some(v) => {
                sources_with_main_value += 1;
                v
            }
            None => continue,
        };

        // Resolve grouping values
        let mut group_values: Vec<String> = Vec::new();
        let mut root_id_for_display: Option<i64> = None;
        let mut root_path_for_display: Option<String> = None;

        for gk in grouping_keys {
            let gk_value = match fact_value::resolve_fact_value(source, gk, stored_facts) {
                Ok(Some(v)) => v,
                Ok(None) => "(no value)".to_string(),
                Err(_) => "(transform error)".to_string(),
            };

            // Track root info for special display
            if is_root_key(gk) {
                root_id_for_display = Some(source.root_id);
                root_path_for_display = Some(source.root_path.clone());
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

    // 3. BUILD RESULTS - Group by main_value, then sort sub-groups
    struct MainValueGroup {
        main_value: String,
        total_count: i64,
        sub_groups: Vec<(Vec<String>, i64, Option<i64>, Option<String>)>,
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

    // 4. DISPLAY
    // Build grouping label
    let grouping_label = if grouping_keys.len() == 1 && is_root_key(&grouping_keys[0]) {
        "by root".to_string()
    } else {
        let labels: Vec<&str> = grouping_keys.iter().map(|gk| gk.raw.as_str()).collect();
        format!("grouped by {}", labels.join(", "))
    };

    // Print header
    println!("{} ({})\n", main_key.raw, grouping_label);

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
            let group_display = if grouping_keys.len() == 1 && is_root_key(&grouping_keys[0]) {
                // Special root display
                if let (Some(rid), Some(rpath)) = (root_id, root_path) {
                    format_root_display(*rid, rpath)
                } else {
                    group_values[0].clone()
                }
            } else {
                // Multiple grouping keys or non-root
                let parts: Vec<String> = grouping_keys.iter().enumerate().map(|(i, gk)| {
                    if is_root_key(gk) {
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
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Get matching source IDs (include_archived=true, include_excluded=true for delete)
    let (source_ids, _excluded_count) = get_matching_sources(conn, &scopes, &filters, true, true)?;

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
// Prune Excluded Facts
// ============================================================================

/// Delete facts for excluded sources and/or objects.
///
/// Scope options:
/// - "all": Delete facts for both excluded sources AND excluded objects (default)
/// - "source": Delete only source facts where sources.excluded = 1
/// - "object": Delete only object facts where objects.excluded = 1
///
/// This is useful when you've excluded sources/objects you're not interested in archiving,
/// and want to free up database space by removing their associated metadata.
pub fn prune_excluded_facts(db: &Db, scope: &str, dry_run: bool) -> Result<()> {
    let conn = db.conn();

    // Parse scope
    let prune_sources = scope == "all" || scope == "source";
    let prune_objects = scope == "all" || scope == "object";

    if !prune_sources && !prune_objects {
        anyhow::bail!(
            "Invalid scope '{}'. Use 'source', 'object', or omit for both.",
            scope
        );
    }

    // Count source facts for excluded sources
    let source_fact_count: i64 = if prune_sources {
        conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (SELECT id FROM sources WHERE excluded = 1)",
            [],
            |row| row.get(0),
        )?
    } else {
        0
    };

    // Count object facts for excluded objects
    let object_fact_count: i64 = if prune_objects {
        conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (SELECT id FROM objects WHERE excluded = 1)",
            [],
            |row| row.get(0),
        )?
    } else {
        0
    };

    let total_count = source_fact_count + object_fact_count;

    if total_count == 0 {
        println!("No facts found for excluded entities.");
        return Ok(());
    }

    if dry_run {
        println!("Facts for excluded entities:");
        if prune_sources {
            println!(
                "  Source facts (excluded sources): {}",
                format_number(source_fact_count)
            );
        }
        if prune_objects {
            println!(
                "  Object facts (excluded objects): {}",
                format_number(object_fact_count)
            );
        }
        println!(
            "  Total: {} facts would be deleted",
            format_number(total_count)
        );
        println!();
        if scope == "all" {
            println!("Tip: Use --excluded-facts=source or --excluded-facts=object to narrow scope.");
        }
        println!("Use --yes to proceed with deletion.");
    } else {
        let mut total_deleted = 0;

        if prune_sources && source_fact_count > 0 {
            let deleted = conn.execute(
                "DELETE FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM sources WHERE excluded = 1)",
                [],
            )?;
            total_deleted += deleted;
            println!(
                "Deleted {} source facts (from excluded sources)",
                format_number(deleted as i64)
            );
        }

        if prune_objects && object_fact_count > 0 {
            let deleted = conn.execute(
                "DELETE FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM objects WHERE excluded = 1)",
                [],
            )?;
            total_deleted += deleted;
            println!(
                "Deleted {} object facts (from excluded objects)",
                format_number(deleted as i64)
            );
        }

        if total_deleted > 0 {
            println!(
                "Total: {} facts deleted",
                format_number(total_deleted as i64)
            );
        }
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
