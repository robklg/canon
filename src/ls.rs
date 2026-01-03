use anyhow::Result;
use rusqlite::params;
use std::path::Path;

use crate::db::{Connection, Db};
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

pub fn run(
    db: &Db,
    scope_path: Option<&Path>,
    filter_strs: &[String],
    archived_mode: Option<&str>,
    unarchived_only: bool,
    unhashed_only: bool,
    include_archived: bool,
    include_excluded: bool,
    use_relative_paths: bool,
) -> Result<()> {
    let archived_only = archived_mode.is_some();
    let show_archive_paths = archived_mode == Some("show");
    let conn = db.conn();

    // Parse filters
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

    // Get cwd for relative path display
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    // Get excluded count for reporting
    let excluded_count = if !include_excluded {
        exclude::count_excluded(conn, scope_prefix.as_deref(), include_archived)?
    } else {
        0
    };

    // Get all matching source IDs
    let source_ids = get_matching_sources(conn, scope_prefix.as_deref(), &filters, include_archived, include_excluded)?;

    if source_ids.is_empty() {
        eprintln!("No sources match the given filters.");
        if !include_excluded && excluded_count > 0 {
            eprintln!("({} excluded sources hidden, use --include-excluded to show)", excluded_count);
        }
        return Ok(());
    }

    // Apply archived/unarchived/unhashed filter and collect output lines
    // Each entry is (source_path, optional_archive_path)
    let mut output_lines: Vec<(String, Option<String>)> = Vec::new();
    let mut unhashed_count = 0usize;

    for source_id in &source_ids {
        let (full_path, object_id) = get_source_path(conn, *source_id)?;
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
                            output_lines.push((formatted_source.clone(), Some(archive_path)));
                        }
                    } else if check_archived(conn, obj_id)? {
                        output_lines.push((formatted_source, None));
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
                        output_lines.push((formatted_source, None));
                    }
                }
            }
        } else if unhashed_only {
            if object_id.is_none() {
                output_lines.push((formatted_source, None));
            }
        } else {
            // Default: show all
            output_lines.push((formatted_source, None));
        }
    }

    // Print output (to stdout for pipe-friendliness)
    for (source_path, archive_path) in &output_lines {
        if let Some(ap) = archive_path {
            println!("{}\t{}", source_path, ap);
        } else {
            println!("{}", source_path);
        }
    }

    // Print footer to stderr
    // Count unique sources (not archive locations)
    let source_count = if show_archive_paths {
        output_lines.iter().map(|(s, _)| s).collect::<std::collections::HashSet<_>>().len()
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
            .query_map(params![last_id, prefix, BATCH_SIZE], |row| row.get(0))?
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
            .query_map(params![last_id, BATCH_SIZE], |row| row.get(0))?
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
        } else if let Some(rel) = full_path.strip_prefix(&format!("{}/", cwd)) {
            rel.to_string()
        } else {
            full_path.to_string()
        }
    } else {
        full_path.to_string()
    }
}
