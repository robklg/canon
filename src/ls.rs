use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::types::Value;

use crate::db::{Connection, Db};
use crate::scope::{build_scope_clause, ScopeMatch};
use crate::path::{canonicalize_scopes, path_strip_prefix};
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

pub fn run(
    db: &mut Db,
    scope_paths: &[std::path::PathBuf],
    filter_strs: &[String],
    archived_mode: Option<&str>,
    unarchived_only: bool,
    unhashed_only: bool,
    include_archived: bool,
    include_excluded: bool,
    use_relative_paths: bool,
    long_format: bool,
    sort_by: &str,
    reverse: bool,
    null_delim: bool,
) -> Result<()> {
    let archived_only = archived_mode.is_some();
    let show_archive_paths = archived_mode == Some("show");
    let conn = db.conn_mut();

    // Validate sort option
    if !matches!(sort_by, "path" | "size" | "mtime" | "name") {
        anyhow::bail!("Invalid sort option '{}'. Valid options: path, size, mtime, name", sort_by);
    }

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    // Get all matching source IDs
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, include_excluded)?;

    if source_ids.is_empty() {
        eprintln!("No sources match the given filters.");
        // Only show excluded hint if excluded sources actually match the filter
        if !include_excluded {
            let excluded_matches = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, true)?.len();
            if excluded_matches > 0 {
                eprintln!("({} excluded sources hidden, use --include-excluded to show)", excluded_matches);
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

    // Apply archived/unarchived/unhashed filter and collect output lines
    // Each entry is (source_path, optional_archive_path, size, mtime)
    let mut output_lines: Vec<(String, Option<String>, i64, i64)> = Vec::new();
    let mut unhashed_count = 0usize;

    // Need size/mtime for long format or when sorting by those fields
    let need_details = long_format || matches!(sort_by, "size" | "mtime");

    for source_id in &source_ids {
        let (full_path, object_id, size, mtime) = if need_details {
            get_source_details(conn, *source_id)?
        } else {
            let (path, obj_id) = get_source_path(conn, *source_id)?;
            (path, obj_id, 0, 0)
        };
        let formatted_source = format_path(&full_path, cwd.as_deref());

        // Check archive status if filtering
        if archived_only {
            match object_id {
                None => {
                    // Unhashed - skip but track count (can't determine archive status)
                    unhashed_count += 1;
                }
                Some(obj_id) => {
                    if show_archive_paths {
                        // Get all archive locations for this object
                        let archive_paths = get_archive_paths(conn, obj_id)?;
                        for archive_path in archive_paths {
                            output_lines.push((formatted_source.clone(), Some(archive_path), size, mtime));
                        }
                    } else if check_archived(conn, obj_id)? {
                        output_lines.push((formatted_source, None, size, mtime));
                    }
                }
            }
        } else if unarchived_only {
            match object_id {
                None => {
                    // Unhashed - skip but track count (can't determine archive status)
                    unhashed_count += 1;
                }
                Some(obj_id) => {
                    if !check_archived(conn, obj_id)? {
                        output_lines.push((formatted_source, None, size, mtime));
                    }
                }
            }
        } else if unhashed_only {
            if object_id.is_none() {
                output_lines.push((formatted_source, None, size, mtime));
            }
        } else {
            // Default: show all
            output_lines.push((formatted_source, None, size, mtime));
        }
    }

    // Sort output (all ascending by default, -r reverses)
    match sort_by {
        "path" => output_lines.sort_by(|a, b| a.0.cmp(&b.0)),
        "size" => output_lines.sort_by(|a, b| a.2.cmp(&b.2)),
        "mtime" => output_lines.sort_by(|a, b| a.3.cmp(&b.3)),
        "name" => output_lines.sort_by(|a, b| {
            let name_a = a.0.rsplit('/').next().unwrap_or(&a.0);
            let name_b = b.0.rsplit('/').next().unwrap_or(&b.0);
            name_a.cmp(name_b)
        }),
        _ => {} // Already validated above
    }
    if reverse {
        output_lines.reverse();
    }

    // Print output (to stdout for pipe-friendliness)
    let line_end = if null_delim { "\0" } else { "\n" };
    for (source_path, archive_path, size, mtime) in &output_lines {
        if long_format {
            let size_str = format_size(*size);
            let date_str = format_date(*mtime);
            if let Some(ap) = archive_path {
                print!("{:>8}  {}  {}\t{}{}", size_str, date_str, source_path, ap, line_end);
            } else {
                print!("{:>8}  {}  {}{}", size_str, date_str, source_path, line_end);
            }
        } else if let Some(ap) = archive_path {
            print!("{}\t{}{}", source_path, ap, line_end);
        } else {
            print!("{}{}", source_path, line_end);
        }
    }

    // Print footer to stderr
    // Count unique sources (not archive locations)
    let source_count = if show_archive_paths {
        output_lines.iter().map(|(s, _, _, _)| s).collect::<std::collections::HashSet<_>>().len()
    } else {
        output_lines.len()
    };
    let mut footer_parts = vec![format!("{} sources", source_count)];
    if !include_excluded && excluded_count > 0 {
        footer_parts.push(format!("{} excluded hidden", excluded_count));
    }
    if (archived_only || unarchived_only) && unhashed_count > 0 {
        footer_parts.push(format!("{} unhashed skipped, use --unhashed to see", unhashed_count));
    }

    if footer_parts.len() > 1 {
        eprintln!("{} ({})", footer_parts[0], footer_parts[1..].join(", "));
    } else {
        eprintln!("{}", footer_parts[0]);
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
    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let (scope_clause, scope_params) = build_scope_clause(&scopes);

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
                 LEFT JOIN objects o ON s.object_id = o.id
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

fn get_source_path(conn: &Connection, source_id: i64) -> Result<(String, Option<i64>)> {
    let (root_path, rel_path, object_id): (String, String, Option<i64>) = conn.query_row(
        "SELECT r.path, s.rel_path, s.object_id
         FROM sources s
         JOIN roots r ON s.root_id = r.id
         WHERE s.id = ?",
        [source_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;

    let full_path = if rel_path.is_empty() {
        root_path
    } else {
        format!("{}/{}", root_path, rel_path)
    };

    Ok((full_path, object_id))
}

/// Get source details including size and mtime for long format
fn get_source_details(conn: &Connection, source_id: i64) -> Result<(String, Option<i64>, i64, i64)> {
    let (root_path, rel_path, object_id, size, mtime): (String, String, Option<i64>, i64, i64) = conn.query_row(
        "SELECT r.path, s.rel_path, s.object_id, s.size, s.mtime
         FROM sources s
         JOIN roots r ON s.root_id = r.id
         WHERE s.id = ?",
        [source_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;

    let full_path = if rel_path.is_empty() {
        root_path
    } else {
        format!("{}/{}", root_path, rel_path)
    };

    Ok((full_path, object_id, size, mtime))
}

fn check_archived(conn: &Connection, object_id: i64) -> Result<bool> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sources s
            JOIN roots r ON s.root_id = r.id
            WHERE s.object_id = ? AND r.role = 'archive' AND s.present = 1
        )",
        [object_id],
        |row| row.get(0),
    )?;

    Ok(exists)
}

fn get_archive_paths(conn: &Connection, object_id: i64) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT r.path, s.rel_path
         FROM sources s
         JOIN roots r ON s.root_id = r.id
         WHERE s.object_id = ? AND r.role = 'archive' AND s.present = 1
         ORDER BY r.path, s.rel_path",
    )?;

    let paths: Vec<String> = stmt
        .query_map([object_id], |row| {
            let root_path: String = row.get(0)?;
            let rel_path: String = row.get(1)?;
            if rel_path.is_empty() {
                Ok(root_path)
            } else {
                Ok(format!("{}/{}", root_path, rel_path))
            }
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(paths)
}

fn format_path(full_path: &str, cwd: Option<&str>) -> String {
    if let Some(cwd) = cwd {
        if full_path == cwd {
            ".".to_string()
        } else if let Some(rel) = path_strip_prefix(full_path, cwd) {
            rel.to_string()
        } else {
            full_path.to_string()
        }
    } else {
        full_path.to_string()
    }
}

fn format_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_date(unix_timestamp: i64) -> String {
    Utc.timestamp_opt(unix_timestamp, 0)
        .single()
        .map(|dt| dt.format("%b %e %Y").to_string())
        .unwrap_or_else(|| "???".to_string())
}

/// Show sources with duplicate content, grouped by hash
pub fn show_duplicates(
    db: &mut Db,
    scope_paths: &[std::path::PathBuf],
    filter_strs: &[String],
    include_archived: bool,
    include_excluded: bool,
    use_relative_paths: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    // Get all matching source IDs
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, include_excluded)?;

    if source_ids.is_empty() {
        eprintln!("No sources match the given filters.");
        // Only show excluded hint if excluded sources actually match the filter
        if !include_excluded {
            let excluded_matches = get_matching_sources(conn, &scope_prefixes, &filters, include_archived, true)?.len();
            if excluded_matches > 0 {
                eprintln!("({} excluded sources hidden, use --include-excluded to show)", excluded_matches);
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

    // Find duplicate groups: object_ids that appear more than once
    let duplicate_groups = find_duplicate_groups(conn, &source_ids)?;

    if duplicate_groups.is_empty() {
        println!("No duplicates found.");
        if !include_excluded && excluded_count > 0 {
            eprintln!("({} excluded sources hidden, use --include-excluded to show)", excluded_count);
        }
        return Ok(());
    }

    // Print each duplicate group
    let mut total_sources = 0usize;
    for (hash, size, sources) in &duplicate_groups {
        let short_hash = if hash.len() > 12 { &hash[..12] } else { hash };
        let size_str = format_size(*size);
        println!("[{}...] {} sources, {}:", short_hash, sources.len(), size_str);
        for (path, source_id) in sources {
            let display_path = format_path(path, cwd.as_deref());
            println!("  {} (id: {})", display_path, source_id);
        }
        println!();
        total_sources += sources.len();
    }

    // Summary
    println!("Found {} duplicate groups ({} sources)", duplicate_groups.len(), total_sources);
    if !include_excluded && excluded_count > 0 {
        eprintln!("({} excluded sources hidden, use --include-excluded to show)", excluded_count);
    }

    Ok(())
}

/// Find groups of sources that share the same object_id (content hash)
/// Returns Vec of (hash_value, size, Vec<(path, source_id)>)
fn find_duplicate_groups(
    conn: &Connection,
    source_ids: &[i64],
) -> Result<Vec<(String, i64, Vec<(String, i64)>)>> {
    use std::collections::HashMap;

    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Build a map of object_id -> (hash, size, sources)
    let mut object_map: HashMap<i64, (String, i64, Vec<(String, i64)>)> = HashMap::new();

    for &source_id in source_ids {
        // Get source info including object_id, hash, and size
        let result: Option<(i64, String, i64, String, String)> = conn
            .query_row(
                "SELECT s.object_id, o.hash_value, s.size, r.path, s.rel_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 JOIN objects o ON s.object_id = o.id
                 WHERE s.id = ? AND s.object_id IS NOT NULL",
                [source_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .ok();

        if let Some((object_id, hash, size, root_path, rel_path)) = result {
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{}/{}", root_path, rel_path)
            };

            object_map
                .entry(object_id)
                .or_insert_with(|| (hash, size, Vec::new()))
                .2
                .push((full_path, source_id));
        }
    }

    // Filter to only groups with 2+ sources
    let mut groups: Vec<(String, i64, Vec<(String, i64)>)> = object_map
        .into_values()
        .filter(|(_, _, sources)| sources.len() > 1)
        .collect();

    // Sort sources within each group by path
    for (_, _, sources) in &mut groups {
        sources.sort_by(|a, b| a.0.cmp(&b.0));
    }
    // Sort groups by first path (so related duplicates appear near each other)
    groups.sort_by(|a, b| {
        a.2.first().map(|(p, _)| p.as_str()).cmp(&b.2.first().map(|(p, _)| p.as_str()))
    });

    Ok(groups)
}
