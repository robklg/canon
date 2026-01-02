use anyhow::{bail, Result};
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::Path;

use crate::db;
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

/// Statistics for a single root or overall
struct CoverageStats {
    root_path: Option<String>,
    root_role: Option<String>,
    total_sources: i64,
    excluded_sources: i64,
    hashed_sources: i64,
    archived_sources: i64,
}

impl CoverageStats {
    fn new() -> Self {
        CoverageStats {
            root_path: None,
            root_role: None,
            total_sources: 0,
            excluded_sources: 0,
            hashed_sources: 0,
            archived_sources: 0,
        }
    }

    fn included_sources(&self) -> i64 {
        self.total_sources - self.excluded_sources
    }

    fn excluded_pct(&self) -> f64 {
        if self.total_sources == 0 {
            0.0
        } else {
            (self.excluded_sources as f64 / self.total_sources as f64) * 100.0
        }
    }

    fn hashed_pct(&self) -> f64 {
        let included = self.included_sources();
        if included == 0 {
            0.0
        } else {
            (self.hashed_sources as f64 / included as f64) * 100.0
        }
    }

    fn archived_pct(&self) -> f64 {
        if self.hashed_sources == 0 {
            0.0
        } else {
            (self.archived_sources as f64 / self.hashed_sources as f64) * 100.0
        }
    }

    fn unarchived(&self) -> i64 {
        self.hashed_sources - self.archived_sources
    }
}

pub fn run(
    db_path: &Path,
    scope_path: Option<&Path>,
    filter_strs: &[String],
    archive_path: Option<&Path>,
    include_archived: bool,
    include_excluded: bool,
) -> Result<()> {
    let conn = db::open(db_path)?;

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

    // Resolve and validate archive path, find containing archive root
    let archive_info = if let Some(p) = archive_path {
        let resolved = std::fs::canonicalize(p)?.to_string_lossy().to_string();
        let (root_id, root_path) = find_archive_root_for_path(&conn, &resolved)?;
        Some((root_id, root_path, resolved))
    } else {
        None
    };

    // Build the set of archived hashes
    let archived_hashes = build_archived_hash_set(&conn, archive_info.as_ref())?;

    // Compute and display stats
    if scope_prefix.is_some() {
        // Single scope mode
        let stats = compute_scoped_stats(
            &conn,
            scope_prefix.as_deref(),
            &filters,
            &archived_hashes,
            include_archived,
            include_excluded,
        )?;
        display_scoped_stats(&stats, scope_prefix.as_deref(), archive_info.as_ref().map(|(_, _, p)| p.as_str()), include_excluded);
    } else {
        // Per-root breakdown mode
        let (per_root_stats, overall) = compute_per_root_stats(
            &conn,
            &filters,
            &archived_hashes,
            include_archived,
            include_excluded,
        )?;
        display_per_root_stats(&per_root_stats, &overall, archive_info.as_ref().map(|(_, _, p)| p.as_str()), include_excluded);
    }

    Ok(())
}

/// Find the archive root that contains the given path
fn find_archive_root_for_path(conn: &Connection, path: &str) -> Result<(i64, String)> {
    let mut stmt = conn.prepare("SELECT id, path FROM roots WHERE role = 'archive'")?;
    let archives: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (id, archive_path) in archives {
        if path.starts_with(&archive_path) {
            return Ok((id, archive_path));
        }
    }

    bail!("Path '{}' is not within any registered archive root", path);
}

/// Build a set of all hashes that exist in the specified archive(s)
fn build_archived_hash_set(
    conn: &Connection,
    archive_info: Option<&(i64, String, String)>,
) -> Result<HashSet<String>> {
    let hashes = if let Some((root_id, _root_path, resolved_path)) = archive_info {
        // Specific archive with optional sub-path scope
        conn.prepare(
            "SELECT DISTINCT o.hash_value
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.id = ? AND s.present = 1
               AND (r.path || '/' || s.rel_path) LIKE ? || '%'"
        )?
        .query_map([root_id.to_string(), resolved_path.clone()], |row| row.get::<_, String>(0))?
        .collect::<Result<HashSet<_>, _>>()?
    } else {
        // All archive roots
        conn.prepare(
            "SELECT DISTINCT o.hash_value
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.role = 'archive' AND s.present = 1"
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<HashSet<_>, _>>()?
    };

    Ok(hashes)
}

/// Compute coverage stats for sources under a specific path scope
fn compute_scoped_stats(
    conn: &Connection,
    scope_prefix: Option<&str>,
    filters: &[Filter],
    archived_hashes: &HashSet<String>,
    include_archived: bool,
    include_excluded: bool,
) -> Result<CoverageStats> {
    let mut stats = CoverageStats::new();

    // Build role clause
    let role_clause = if include_archived {
        "1=1"
    } else {
        "r.role = 'source'"
    };

    // Build path clause
    let path_clause = if scope_prefix.is_some() {
        "(r.path || '/' || s.rel_path) LIKE ? || '%'"
    } else {
        "1=1"
    };

    // Build exclude clause - always query all sources, track excluded separately
    let exclude_clause = exclude::exclude_clause(true); // Always include all, we track separately

    // Get source IDs with batched processing
    let mut last_id: i64 = 0;
    loop {
        let batch_query = format!(
            "SELECT s.id FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {} AND {} AND s.id > ?
             ORDER BY s.id LIMIT ?",
            role_clause, path_clause, exclude_clause
        );

        let source_ids: Vec<i64> = if let Some(prefix) = scope_prefix {
            conn.prepare(&batch_query)?
                .query_map(rusqlite::params![prefix, last_id, BATCH_SIZE], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            conn.prepare(&batch_query)?
                .query_map(rusqlite::params![last_id, BATCH_SIZE], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        if source_ids.is_empty() {
            break;
        }

        last_id = *source_ids.last().unwrap();

        // Apply filters
        let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;

        // Count stats for this batch
        for source_id in filtered_ids {
            stats.total_sources += 1;

            // Check if excluded
            if exclude::is_excluded(conn, source_id)? {
                stats.excluded_sources += 1;
                // Skip further processing for excluded sources unless include_excluded
                if !include_excluded {
                    continue;
                }
            }

            // Check if source has a hash (only for included sources)
            let hash: Option<String> = conn
                .query_row(
                    "SELECT o.hash_value FROM sources s
                     JOIN objects o ON s.object_id = o.id
                     WHERE s.id = ?",
                    [source_id],
                    |row| row.get(0),
                )
                .ok();

            if let Some(h) = hash {
                stats.hashed_sources += 1;
                if archived_hashes.contains(&h) {
                    stats.archived_sources += 1;
                }
            }
        }
    }

    Ok(stats)
}

/// Compute coverage stats per root, plus overall totals
fn compute_per_root_stats(
    conn: &Connection,
    filters: &[Filter],
    archived_hashes: &HashSet<String>,
    include_archived: bool,
    include_excluded: bool,
) -> Result<(Vec<CoverageStats>, CoverageStats)> {
    // Get list of roots
    let role_clause = if include_archived {
        "1=1"
    } else {
        "role = 'source'"
    };

    let roots: Vec<(i64, String, String)> = conn
        .prepare(&format!(
            "SELECT id, path, role FROM roots WHERE {} ORDER BY path",
            role_clause
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for (root_id, root_path, root_role) in roots {
        let mut stats = CoverageStats {
            root_path: Some(root_path.clone()),
            root_role: Some(root_role),
            total_sources: 0,
            excluded_sources: 0,
            hashed_sources: 0,
            archived_sources: 0,
        };

        // Get sources for this root with batched processing
        let mut last_id: i64 = 0;
        loop {
            let source_ids: Vec<i64> = conn
                .prepare(
                    "SELECT id FROM sources
                     WHERE root_id = ? AND present = 1 AND id > ?
                     ORDER BY id LIMIT ?"
                )?
                .query_map(rusqlite::params![root_id, last_id, BATCH_SIZE], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;

            if source_ids.is_empty() {
                break;
            }

            last_id = *source_ids.last().unwrap();

            // Apply filters
            let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;

            // Count stats for this batch
            for source_id in filtered_ids {
                stats.total_sources += 1;

                // Check if excluded
                if exclude::is_excluded(conn, source_id)? {
                    stats.excluded_sources += 1;
                    // Skip further processing for excluded sources unless include_excluded
                    if !include_excluded {
                        continue;
                    }
                }

                // Check if source has a hash
                let hash: Option<String> = conn
                    .query_row(
                        "SELECT o.hash_value FROM sources s
                         JOIN objects o ON s.object_id = o.id
                         WHERE s.id = ?",
                        [source_id],
                        |row| row.get(0),
                    )
                    .ok();

                if let Some(h) = hash {
                    stats.hashed_sources += 1;
                    if archived_hashes.contains(&h) {
                        stats.archived_sources += 1;
                    }
                }
            }
        }

        // Add to overall totals
        overall.total_sources += stats.total_sources;
        overall.excluded_sources += stats.excluded_sources;
        overall.hashed_sources += stats.hashed_sources;
        overall.archived_sources += stats.archived_sources;

        per_root_stats.push(stats);
    }

    Ok((per_root_stats, overall))
}

fn display_scoped_stats(stats: &CoverageStats, scope: Option<&str>, archive: Option<&str>, include_excluded: bool) {
    if let Some(arch) = archive {
        println!("Archive Coverage (relative to {})", arch);
    } else {
        println!("Archive Coverage");
    }

    if let Some(s) = scope {
        println!("Scope: {}\n", s);
    } else {
        println!();
    }

    if stats.total_sources == 0 {
        println!("No sources match the given filters.");
        return;
    }

    if include_excluded && stats.excluded_sources > 0 {
        // Show full breakdown with excluded
        println!("  Total sources:   {:>8}", format_number(stats.total_sources));
        println!(
            "  Excluded:        {:>8} ({:.1}%)",
            format_number(stats.excluded_sources),
            stats.excluded_pct()
        );
        println!("  Included:        {:>8}", format_number(stats.included_sources()));
        println!(
            "  Hashed:          {:>8} ({:.1}% of included)",
            format_number(stats.hashed_sources),
            stats.hashed_pct()
        );
    } else {
        // Default view: show included sources as total
        println!("  Total sources:   {:>8}", format_number(stats.included_sources()));
        println!(
            "  Hashed:          {:>8} ({:.1}%)",
            format_number(stats.hashed_sources),
            stats.hashed_pct()
        );
    }

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!("  Not in archive:  {:>8}", format_number(stats.unarchived()));
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!("  Unarchived:      {:>8}", format_number(stats.unarchived()));
    }
}

fn display_per_root_stats(per_root: &[CoverageStats], overall: &CoverageStats, archive: Option<&str>, include_excluded: bool) {
    if let Some(arch) = archive {
        println!("Archive Coverage Report (relative to {})\n", arch);
    } else {
        println!("Archive Coverage Report\n");
    }

    if per_root.is_empty() || overall.total_sources == 0 {
        println!("No sources match the given filters.");
        return;
    }

    for stats in per_root {
        if stats.total_sources == 0 {
            continue;
        }

        let root_path = stats.root_path.as_deref().unwrap_or("unknown");
        let root_role = stats.root_role.as_deref().unwrap_or("unknown");
        println!("Root: {} ({})", root_path, root_role);

        if include_excluded && stats.excluded_sources > 0 {
            println!("  Total sources:   {:>8}", format_number(stats.total_sources));
            println!(
                "  Excluded:        {:>8} ({:.1}%)",
                format_number(stats.excluded_sources),
                stats.excluded_pct()
            );
            println!("  Included:        {:>8}", format_number(stats.included_sources()));
            println!(
                "  Hashed:          {:>8} ({:.1}% of included)",
                format_number(stats.hashed_sources),
                stats.hashed_pct()
            );
        } else {
            println!("  Total sources:   {:>8}", format_number(stats.included_sources()));
            println!(
                "  Hashed:          {:>8} ({:.1}%)",
                format_number(stats.hashed_sources),
                stats.hashed_pct()
            );
        }

        if archive.is_some() {
            println!(
                "  In this archive: {:>8} ({:.1}% of hashed)",
                format_number(stats.archived_sources),
                stats.archived_pct()
            );
            println!("  Not in archive:  {:>8}", format_number(stats.unarchived()));
        } else {
            println!(
                "  Archived:        {:>8} ({:.1}% of hashed)",
                format_number(stats.archived_sources),
                stats.archived_pct()
            );
            println!("  Unarchived:      {:>8}", format_number(stats.unarchived()));
        }
        println!();
    }

    // Overall summary
    println!("{}", "─".repeat(40));
    println!("Overall:");

    if include_excluded && overall.excluded_sources > 0 {
        println!("  Total sources:   {:>8}", format_number(overall.total_sources));
        println!(
            "  Excluded:        {:>8} ({:.1}%)",
            format_number(overall.excluded_sources),
            overall.excluded_pct()
        );
        println!("  Included:        {:>8}", format_number(overall.included_sources()));
        println!(
            "  Hashed:          {:>8} ({:.1}% of included)",
            format_number(overall.hashed_sources),
            overall.hashed_pct()
        );
    } else {
        println!("  Total sources:   {:>8}", format_number(overall.included_sources()));
        println!(
            "  Hashed:          {:>8} ({:.1}%)",
            format_number(overall.hashed_sources),
            overall.hashed_pct()
        );
    }

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!("  Not in archive:  {:>8}", format_number(overall.unarchived()));
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!("  Unarchived:      {:>8}", format_number(overall.unarchived()));
    }
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
