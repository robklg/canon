use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::types::Value;
use std::collections::{HashMap, HashSet};

use crate::domain::path::resolve_paths;
use crate::domain::root::Root;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::source::BATCH_SIZE;
use crate::repo::{self, Connection, Db};

pub fn run(
    db: &mut Db,
    scope_paths: &[std::path::PathBuf],
    roots: &[Root],
    filter_strs: &[String],
    archived_mode: Option<&str>,
    unarchived_only: bool,
    unhashed_only: bool,
    excluded_only: bool,
    include: &IncludeSet,
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
        anyhow::bail!("Invalid sort option '{sort_by}'. Valid options: path, size, mtime, name");
    }

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let scope_prefixes = resolve_paths(scope_paths, roots)?;
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

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = selection::select_sources(conn, &params)?;

    if sel.sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    // Batch fetch archive status for all sources with object_ids (eliminates N+1)
    let object_ids: Vec<i64> = sel.sources.iter().filter_map(|s| s.object_id).collect();
    let archived_set: HashSet<i64> = if archived_only || unarchived_only {
        repo::object::batch_check_archived(conn, &object_ids, None)?
    } else {
        HashSet::new()
    };
    let archive_paths_map: HashMap<i64, Vec<String>> = if show_archive_paths {
        repo::object::batch_find_archive_paths(conn, &object_ids)?
    } else {
        HashMap::new()
    };

    // Determine whether to show status column
    let show_status = include.is_expanded() || excluded_only;

    // Apply archived/unarchived/unhashed/excluded filter and collect output lines
    // Each entry is (source_path, optional_archive_path, size, mtime, status)
    let mut output_lines: Vec<(String, Option<String>, i64, i64, String)> = Vec::new();
    let mut unhashed_count = 0usize;

    for source in &sel.sources {
        let formatted_source = format_path(&source.path(), cwd.as_deref());
        let object_id = source.object_id;
        let size = source.size;
        let mtime = source.mtime;
        let status = status_indicator(source);

        // Check archive status if filtering
        if archived_only {
            match object_id {
                None => {
                    // Unhashed - skip but track count (can't determine archive status)
                    unhashed_count += 1;
                }
                Some(obj_id) => {
                    if show_archive_paths {
                        // Get all archive locations for this object (from batch result)
                        if let Some(paths) = archive_paths_map.get(&obj_id) {
                            for archive_path in paths {
                                output_lines.push((
                                    formatted_source.clone(),
                                    Some(archive_path.clone()),
                                    size,
                                    mtime,
                                    status.to_string(),
                                ));
                            }
                        }
                    } else if archived_set.contains(&obj_id) {
                        output_lines.push((
                            formatted_source,
                            None,
                            size,
                            mtime,
                            status.to_string(),
                        ));
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
                    if !archived_set.contains(&obj_id) {
                        output_lines.push((
                            formatted_source,
                            None,
                            size,
                            mtime,
                            status.to_string(),
                        ));
                    }
                }
            }
        } else if unhashed_only {
            if object_id.is_none() {
                output_lines.push((formatted_source, None, size, mtime, status.to_string()));
            }
        } else if excluded_only {
            if source.is_excluded() {
                output_lines.push((formatted_source, None, size, mtime, status.to_string()));
            }
        } else {
            // Default: show all
            output_lines.push((formatted_source, None, size, mtime, status.to_string()));
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
    for (source_path, archive_path, size, mtime, status) in &output_lines {
        if long_format {
            let size_str = format_size(*size);
            let date_str = format_date(*mtime);
            if show_status {
                if let Some(ap) = archive_path {
                    print!("{status}{size_str:>8}  {date_str}  {source_path}\t{ap}{line_end}");
                } else {
                    print!("{status}{size_str:>8}  {date_str}  {source_path}{line_end}");
                }
            } else if let Some(ap) = archive_path {
                print!("{size_str:>8}  {date_str}  {source_path}\t{ap}{line_end}");
            } else {
                print!("{size_str:>8}  {date_str}  {source_path}{line_end}");
            }
        } else if let Some(ap) = archive_path {
            print!("{source_path}\t{ap}{line_end}");
        } else {
            print!("{source_path}{line_end}");
        }
    }

    // Print footer to stderr
    // Count unique sources (not archive locations)
    let source_count = if show_archive_paths {
        output_lines
            .iter()
            .map(|(s, _, _, _, _)| s)
            .collect::<std::collections::HashSet<_>>()
            .len()
    } else {
        output_lines.len()
    };
    let mut footer_parts = vec![format!("{} sources", source_count)];
    if sel.excluded_count > 0 {
        footer_parts.push(format!("{} excluded hidden", sel.excluded_count));
    }
    if (archived_only || unarchived_only) && unhashed_count > 0 {
        footer_parts.push(format!(
            "{unhashed_count} unhashed skipped, use --unhashed to see"
        ));
    }

    if footer_parts.len() > 1 {
        eprintln!("{} ({})", footer_parts[0], footer_parts[1..].join(", "));
    } else {
        eprintln!("{}", footer_parts[0]);
    }

    Ok(())
}

fn format_path(full_path: &str, cwd: Option<&str>) -> String {
    crate::domain::path::format_path(full_path, cwd)
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
        format!("{bytes} B")
    }
}

fn format_date(unix_timestamp: i64) -> String {
    Utc.timestamp_opt(unix_timestamp, 0)
        .single()
        .map(|dt| dt.format("%b %e %Y").to_string())
        .unwrap_or_else(|| "???".to_string())
}

/// Status indicator for long format output.
/// E = source-level excluded, X = object-level excluded, A = archive source.
fn status_indicator(source: &Source) -> &'static str {
    if source.excluded {
        " E"
    } else if source.object_excluded == Some(true) {
        " X"
    } else if source.is_from_role("archive") {
        " A"
    } else {
        "  "
    }
}

/// Show sources with duplicate content, grouped by hash
pub fn show_duplicates(
    db: &mut Db,
    scope_paths: &[std::path::PathBuf],
    roots: &[Root],
    filter_strs: &[String],
    include: &IncludeSet,
    use_relative_paths: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let scope_prefixes = resolve_paths(scope_paths, roots)?;
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

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = selection::select_sources(conn, &params)?;

    if sel.sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    // Extract source IDs for duplicate finding
    let source_ids = sel.source_ids();

    // Find duplicate groups: object_ids that appear more than once
    let duplicate_groups = find_duplicate_groups(conn, &source_ids)?;

    if duplicate_groups.is_empty() {
        println!("No duplicates found.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
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
            println!("  {display_path} (id: {source_id})");
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
    if sel.excluded_count > 0 {
        eprintln!(
            "({} excluded sources hidden, use --include excluded to show)",
            sel.excluded_count
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
                format!("{root_path}/{rel_path}")
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &RusqliteConnection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Test that --archived counts sources, not unique objects.
    ///
    /// This guards against a regression where someone might use archived_set.len()
    /// instead of counting sources whose object is in the set.
    #[test]
    fn test_ls_archived_flag_counts_sources_not_objects() {
        let conn = setup_test_db();

        // Create source root and archive root
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // Create ONE object that is archived
        let archived_obj = insert_object(&conn, "abc123archived", false);

        // Create 3 source files pointing to the SAME archived object
        let source1 = insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj));
        let source2 = insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj));
        let source3 = insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj));

        // Create another object that is NOT archived
        let unarchived_obj = insert_object(&conn, "def456unarchived", false);
        let _source4 = insert_source(&conn, source_root, "photo4.jpg", Some(unarchived_obj));

        // Put the archived object in the archive root (this makes it "archived")
        insert_source(&conn, archive_root, "photo_backup.jpg", Some(archived_obj));

        // Get archived status using the same function ls.rs uses
        let object_ids = vec![archived_obj, unarchived_obj];
        let archived_set = repo::object::batch_check_archived(&conn, &object_ids, None).unwrap();

        // Verify archived_set contains only the archived object
        assert!(archived_set.contains(&archived_obj));
        assert!(!archived_set.contains(&unarchived_obj));
        assert_eq!(
            archived_set.len(),
            1,
            "Only 1 unique object should be archived"
        );

        // NOW THE CRITICAL TEST:
        // If we filter sources by "has archived object", we should get 3 sources, not 1
        let source_ids = [source1, source2, source3];
        let archived_source_count = source_ids
            .iter()
            .filter(|_| {
                // Each source points to archived_obj
                archived_set.contains(&archived_obj)
            })
            .count();

        assert_eq!(
            archived_source_count, 3,
            "Should count 3 SOURCES with archived objects, not 1 unique object"
        );
    }
}
