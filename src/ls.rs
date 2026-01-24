use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::types::Value;
use std::collections::HashMap;

use crate::db::{Connection, Db};
use crate::filter::{self, Filter};
use crate::path::{canonicalize_scopes, path_strip_prefix};
use crate::scope::ScopeMatch;
use crate::source::Source;
use crate::source_repo::{self, BATCH_SIZE};

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
        anyhow::bail!(
            "Invalid sort option '{}'. Valid options: path, size, mtime, name",
            sort_by
        );
    }

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths and classify them
    let scope_prefixes = canonicalize_scopes(scope_paths)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    // Fetch all sources and filter using domain predicates
    let (sources, excluded_count) =
        get_matching_sources(conn, &scopes, &filters, include_archived, include_excluded)?;

    if sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if !include_excluded && excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include-excluded to show)",
                excluded_count
            );
        }
        return Ok(());
    }

    // Apply archived/unarchived/unhashed filter and collect output lines
    // Each entry is (source_path, optional_archive_path, size, mtime)
    let mut output_lines: Vec<(String, Option<String>, i64, i64)> = Vec::new();
    let mut unhashed_count = 0usize;

    for source in &sources {
        let formatted_source = format_path(&source.path(), cwd.as_deref());
        let object_id = source.object_id;
        let size = source.size;
        let mtime = source.mtime;

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
                            output_lines.push((
                                formatted_source.clone(),
                                Some(archive_path),
                                size,
                                mtime,
                            ));
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
                print!(
                    "{:>8}  {}  {}\t{}{}",
                    size_str, date_str, source_path, ap, line_end
                );
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
        output_lines
            .iter()
            .map(|(s, _, _, _)| s)
            .collect::<std::collections::HashSet<_>>()
            .len()
    } else {
        output_lines.len()
    };
    let mut footer_parts = vec![format!("{} sources", source_count)];
    if !include_excluded && excluded_count > 0 {
        footer_parts.push(format!("{} excluded hidden", excluded_count));
    }
    if (archived_only || unarchived_only) && unhashed_count > 0 {
        footer_parts.push(format!(
            "{} unhashed skipped, use --unhashed to see",
            unhashed_count
        ));
    }

    if footer_parts.len() > 1 {
        eprintln!("{} ({})", footer_parts[0], footer_parts[1..].join(", "));
    } else {
        eprintln!("{}", footer_parts[0]);
    }

    Ok(())
}

/// Fetch sources matching scope/role/exclusion criteria, then apply --where filters.
///
/// Returns (matching_sources, excluded_count) where excluded_count is the number
/// of sources that matched scope/role but were excluded.
///
/// This function implements the domain-predicate filtering pattern:
/// 1. Fetch all sources from all roots
/// 2. Filter using pure domain predicates (is_active, is_from_role, matches_scope, is_excluded)
/// 3. Apply --where filters (requires DB access for fact queries)
fn get_matching_sources(
    conn: &mut Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<(Vec<Source>, usize)> {
    // 1. Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 2. Fetch all present sources for those roots
    let all_sources = source_repo::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Filter using domain predicates, tracking excluded count
    let mut excluded_count = 0usize;
    let filtered: Vec<Source> = all_sources
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
        .collect();

    // 4. Apply --where filters if present
    if filters.is_empty() {
        return Ok((filtered, excluded_count));
    }

    // Extract IDs, apply filters, then map back to Source objects
    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;

    // Build a set for O(1) lookup
    let filtered_id_set: std::collections::HashSet<i64> = filtered_ids.into_iter().collect();

    // Keep only sources whose ID passed the filter
    let result: Vec<Source> = filtered
        .into_iter()
        .filter(|s| filtered_id_set.contains(&s.id))
        .collect();

    Ok((result, excluded_count))
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

    // Resolve scope paths and classify
    let scope_prefixes = canonicalize_scopes(scope_paths)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    // Get all matching sources using domain predicates
    let (sources, excluded_count) =
        get_matching_sources(conn, &scopes, &filters, include_archived, include_excluded)?;

    if sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if !include_excluded && excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include-excluded to show)",
                excluded_count
            );
        }
        return Ok(());
    }

    // Extract source IDs for duplicate finding
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();

    // Find duplicate groups: object_ids that appear more than once
    let duplicate_groups = find_duplicate_groups(conn, &source_ids)?;

    if duplicate_groups.is_empty() {
        println!("No duplicates found.");
        if !include_excluded && excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include-excluded to show)",
                excluded_count
            );
        }
        return Ok(());
    }

    // Print each duplicate group
    let mut total_sources = 0usize;
    for (hash, size, dup_sources) in &duplicate_groups {
        let short_hash = if hash.len() > 12 { &hash[..12] } else { hash };
        let size_str = format_size(*size);
        println!(
            "[{}...] {} sources, {}:",
            short_hash,
            dup_sources.len(),
            size_str
        );
        for (path, source_id) in dup_sources {
            let display_path = format_path(path, cwd.as_deref());
            println!("  {} (id: {})", display_path, source_id);
        }
        println!();
        total_sources += dup_sources.len();
    }

    // Summary
    println!(
        "Found {} duplicate groups ({} sources)",
        duplicate_groups.len(),
        total_sources
    );
    if !include_excluded && excluded_count > 0 {
        eprintln!(
            "({} excluded sources hidden, use --include-excluded to show)",
            excluded_count
        );
    }

    Ok(())
}

/// Find groups of sources that share the same object_id (content hash)
/// Returns Vec of (hash_value, size, Vec<(path, source_id)>)
fn find_duplicate_groups(
    conn: &Connection,
    source_ids: &[i64],
) -> Result<Vec<(String, i64, Vec<(String, i64)>)>> {
    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Build a map of object_id -> (hash, size, sources)
    let mut object_map: HashMap<i64, (String, i64, Vec<(String, i64)>)> = HashMap::new();

    // Batch fetch source info including object_id, hash, and size
    // Note: This query needs hash_value from objects table, which isn't in Source struct
    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT s.id, s.object_id, o.hash_value, s.size, r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE s.id IN ({}) AND s.object_id IS NOT NULL",
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok((
                row.get::<_, i64>(0)?,    // source_id
                row.get::<_, i64>(1)?,    // object_id
                row.get::<_, String>(2)?, // hash_value
                row.get::<_, i64>(3)?,    // size
                row.get::<_, String>(4)?, // root_path
                row.get::<_, String>(5)?, // rel_path
            ))
        })?;

        for row in rows {
            let (source_id, object_id, hash, size, root_path, rel_path) = row?;
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
        a.2.first()
            .map(|(p, _)| p.as_str())
            .cmp(&b.2.first().map(|(p, _)| p.as_str()))
    });

    Ok(groups)
}
