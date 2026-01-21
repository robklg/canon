use anyhow::Result;
use rusqlite::types::Value;
use std::path::PathBuf;

use crate::db::{build_scope_clause, parse_root_spec, populate_temp_sources, Db};
use crate::path::canonicalize_scopes;
use crate::exclude;
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

/// Statistics for a single root or overall
struct CoverageStats {
    root_id: Option<i64>,
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
            root_id: None,
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
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    archive_spec: Option<&str>,
    include_archived: bool,
    include_excluded: bool,
    compact: bool,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Parse and validate archive spec (must be archive role)
    let archive_root_id = if let Some(spec) = archive_spec {
        Some(parse_root_spec(conn, spec, Some("archive"))?)
    } else {
        None
    };

    // Get mutable reference for temp table operations
    let conn = db.conn_mut();

    // Compute and display stats
    if !scope_prefixes.is_empty() {
        // Scoped mode
        let stats = compute_scoped_stats(
            conn,
            &scope_prefixes,
            &filters,
            archive_root_id,
            include_archived,
        )?;
        let scope_display = if scope_prefixes.len() == 1 {
            Some(scope_prefixes[0].as_str())
        } else {
            None
        };
        if compact {
            display_compact_scoped(&stats, scope_display);
        } else {
            display_scoped_stats(&stats, scope_display, archive_spec, include_excluded);
        }
    } else {
        // Per-root breakdown mode
        let (per_root_stats, overall) = compute_per_root_stats(
            conn,
            &filters,
            archive_root_id,
            include_archived,
        )?;
        if compact {
            display_compact_per_root(&per_root_stats, &overall);
        } else {
            display_per_root_stats(&per_root_stats, &overall, archive_spec, include_excluded);
        }
    }

    Ok(())
}

/// Compute coverage stats for sources under a specific path scope using pure SQL aggregates
fn compute_scoped_stats(
    conn: &mut rusqlite::Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include_archived: bool,
) -> Result<CoverageStats> {
    // Build role clause
    let role_clause = if include_archived {
        "r.suspended = 0" // Include all roles, but not suspended
    } else {
        "r.role = 'source' AND r.suspended = 0"
    };

    // Always query all sources (no exclude filtering at query level)
    let exclude_clause = exclude::exclude_clause(true);
    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);

    // Collect all filtered source IDs
    let mut all_filtered_ids: Vec<i64> = Vec::new();
    let mut last_id: i64 = 0;

    loop {
        // Build params: scope params + last_id + batch_size
        let mut params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();
        params.push(Value::from(last_id));
        params.push(Value::from(BATCH_SIZE));

        let batch_query = format!(
            "SELECT s.id FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {} AND {} AND s.id > ?
             ORDER BY s.id LIMIT ?",
            role_clause, exclude_clause, scope_clause
        );

        let source_ids: Vec<i64> = conn
            .prepare(&batch_query)?
            .query_map(rusqlite::params_from_iter(params), |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if source_ids.is_empty() {
            break;
        }

        last_id = *source_ids.last().unwrap();

        // Apply filters
        let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;
        all_filtered_ids.extend(filtered_ids);
    }

    // Populate temp table with all filtered source IDs
    populate_temp_sources(conn, &all_filtered_ids)?;

    // Now compute all stats with aggregate queries
    compute_stats_from_temp_table(conn, archive_root_id)
}

/// Compute coverage stats per root, plus overall totals using pure SQL aggregates
fn compute_per_root_stats(
    conn: &mut rusqlite::Connection,
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include_archived: bool,
) -> Result<(Vec<CoverageStats>, CoverageStats)> {
    // Get list of roots
    let role_clause = if include_archived {
        "suspended = 0" // Include all roles, but not suspended
    } else {
        "role = 'source' AND suspended = 0"
    };

    let roots: Vec<(i64, String, String)> = conn
        .prepare(&format!(
            "SELECT id, path, role FROM roots WHERE {} ORDER BY id",
            role_clause
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for (root_id, root_path, root_role) in roots {
        // Collect all filtered source IDs for this root
        let mut all_filtered_ids: Vec<i64> = Vec::new();
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
            all_filtered_ids.extend(filtered_ids);
        }

        // Populate temp table with all filtered source IDs for this root
        populate_temp_sources(conn, &all_filtered_ids)?;

        // Compute stats from temp table
        let mut stats = compute_stats_from_temp_table(conn, archive_root_id)?;
        stats.root_id = Some(root_id);
        stats.root_path = Some(root_path);
        stats.root_role = Some(root_role);

        // Add to overall totals
        overall.total_sources += stats.total_sources;
        overall.excluded_sources += stats.excluded_sources;
        overall.hashed_sources += stats.hashed_sources;
        overall.archived_sources += stats.archived_sources;

        per_root_stats.push(stats);
    }

    Ok((per_root_stats, overall))
}

/// Compute all coverage stats from temp_sources using pure SQL aggregates
fn compute_stats_from_temp_table(
    conn: &rusqlite::Connection,
    archive_root_id: Option<i64>,
) -> Result<CoverageStats> {
    let mut stats = CoverageStats::new();

    // Total sources
    stats.total_sources = conn.query_row(
        "SELECT COUNT(*) FROM temp_sources",
        [],
        |row| row.get(0),
    )?;

    // Excluded sources (presence of policy.exclude key)
    stats.excluded_sources = conn.query_row(
        "SELECT COUNT(*) FROM temp_sources ts
         WHERE EXISTS (
             SELECT 1 FROM facts f
             WHERE f.entity_type = 'source' AND f.entity_id = ts.id
               AND f.key = 'policy.exclude'
         )",
        [],
        |row| row.get(0),
    )?;

    // Hashed sources (have an object_id, excluding excluded sources)
    stats.hashed_sources = conn.query_row(
        "SELECT COUNT(*) FROM temp_sources ts
         JOIN sources s ON s.id = ts.id
         WHERE s.object_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM facts f
               WHERE f.entity_type = 'source' AND f.entity_id = ts.id
                 AND f.key = 'policy.exclude'
           )",
        [],
        |row| row.get(0),
    )?;

    // Archived sources (excluding excluded sources)
    if let Some(root_id) = archive_root_id {
        // Specific archive root
        stats.archived_sources = conn.query_row(
            "SELECT COUNT(*) FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             WHERE s.object_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM facts f
                   WHERE f.entity_type = 'source' AND f.entity_id = ts.id
                     AND f.key = 'policy.exclude'
               )
               AND EXISTS (
                   SELECT 1 FROM sources arch_s
                   WHERE arch_s.root_id = ?1 AND arch_s.present = 1
                     AND arch_s.object_id = s.object_id
               )",
            [root_id],
            |row| row.get(0),
        )?;
    } else {
        // Any archive root
        stats.archived_sources = conn.query_row(
            "SELECT COUNT(*) FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             WHERE s.object_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM facts f
                   WHERE f.entity_type = 'source' AND f.entity_id = ts.id
                     AND f.key = 'policy.exclude'
               )
               AND EXISTS (
                   SELECT 1 FROM sources arch_s
                   JOIN roots r ON arch_s.root_id = r.id
                   WHERE r.role = 'archive' AND arch_s.present = 1
                     AND arch_s.object_id = s.object_id
               )",
            [],
            |row| row.get(0),
        )?;
    }

    Ok(stats)
}

fn display_compact_scoped(stats: &CoverageStats, scope: Option<&str>) {
    let label = scope.unwrap_or("(all)");
    print_compact_line(label, stats, true);
}

fn display_compact_per_root(per_root: &[CoverageStats], overall: &CoverageStats) {
    let mut first = true;
    for stats in per_root {
        if stats.total_sources == 0 {
            continue;
        }
        let id = stats.root_id.map(|i| i.to_string()).unwrap_or_else(|| "?".to_string());
        let path = stats.root_path.as_deref().unwrap_or("unknown");
        let label = format_compact_label(&id, path);
        print_compact_line(&label, stats, first);
        first = false;
    }

    // Overall summary if multiple roots
    if per_root.len() > 1 && overall.total_sources > 0 {
        print_compact_line("(total)", overall, false);
    }
}

fn format_compact_label(id: &str, path: &str) -> String {
    const MAX_PATH_LEN: usize = 35;
    let id_prefix = format!("id:{:<2}", id);

    if path.len() <= MAX_PATH_LEN {
        format!("{} {}", id_prefix, path)
    } else {
        // Show ...last_n_chars
        let truncated = &path[path.len() - MAX_PATH_LEN + 3..];
        format!("{} ...{}", id_prefix, truncated)
    }
}

fn print_compact_line(label: &str, stats: &CoverageStats, show_legend: bool) {
    let sources = stats.included_sources();
    let hashed_pct = stats.hashed_pct();
    let archived_pct = stats.archived_pct();

    let legend = if show_legend {
        "  (sources/hashed/archived)"
    } else {
        ""
    };

    println!(
        "{:<42} {:>10}/{:>5.1}%/{:>5.1}%{}",
        label,
        format_number(sources),
        hashed_pct,
        archived_pct,
        legend
    );
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

        let root_id = stats.root_id.map(|id| id.to_string()).unwrap_or_else(|| "?".to_string());
        let root_path = stats.root_path.as_deref().unwrap_or("unknown");
        let root_role = stats.root_role.as_deref().unwrap_or("unknown");
        println!("Root {}: {} ({})", root_id, root_path, root_role);

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
