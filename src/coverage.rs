use anyhow::Result;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::repo::{self, Db};
use crate::expr::filter::{self, Filter};
use crate::domain::path::canonicalize_scopes;
use crate::domain::root::parse_root_spec;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;

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
    _include_excluded: bool,
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
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Parse and validate archive spec (must be archive role)
    let archive_root_id = if let Some(spec) = archive_spec {
        Some(parse_root_spec(conn, spec, Some("archive"))?)
    } else {
        None
    };

    // Get mutable reference for operations
    let conn = db.conn_mut();

    // Compute and display stats
    if !scope_prefixes.is_empty() {
        // Scoped mode
        let stats = compute_scoped_stats(
            conn,
            &scopes,
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
            display_scoped_stats(&stats, scope_display, archive_spec);
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
            display_per_root_stats(&per_root_stats, &overall, archive_spec);
        }
    }

    Ok(())
}

/// Get all sources matching scope/role criteria, then apply filters.
fn get_matching_sources(
    conn: &mut rusqlite::Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    include_archived: bool,
) -> Result<Vec<Source>> {
    // Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Fetch all present sources
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Filter using domain predicates
    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| include_archived || s.is_from_role("source"))
        .filter(|s| s.matches_scope(scopes))
        .collect();

    // Apply --where filters if present
    if filters.is_empty() {
        return Ok(filtered);
    }

    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let filtered_ids: HashSet<i64> = filter::apply_filters(conn, &source_ids, filters)?
        .into_iter()
        .collect();

    Ok(filtered
        .into_iter()
        .filter(|s| filtered_ids.contains(&s.id))
        .collect())
}

/// Compute coverage stats for sources under a specific path scope
fn compute_scoped_stats(
    conn: &mut rusqlite::Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include_archived: bool,
) -> Result<CoverageStats> {
    let sources = get_matching_sources(conn, scopes, filters, include_archived)?;
    compute_stats_from_sources(conn, &sources, archive_root_id)
}

/// Compute coverage stats per root, plus overall totals
fn compute_per_root_stats(
    conn: &mut rusqlite::Connection,
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include_archived: bool,
) -> Result<(Vec<CoverageStats>, CoverageStats)> {
    // Get list of roots
    let role_clause = if include_archived {
        "suspended = 0"
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

    // Get all matching sources (unscoped)
    let all_sources = get_matching_sources(conn, &[], filters, include_archived)?;

    // Group by root_id
    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for (root_id, root_path, root_role) in roots {
        // Filter sources for this root
        let root_sources: Vec<&Source> = all_sources
            .iter()
            .filter(|s| s.root_id == root_id)
            .collect();

        let mut stats = compute_stats_from_source_refs(conn, &root_sources, archive_root_id)?;
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

/// Compute stats from a list of sources using domain predicates.
/// Uses is_excluded() which checks BOTH source-level and object-level exclusion.
fn compute_stats_from_sources(
    conn: &mut rusqlite::Connection,
    sources: &[Source],
    archive_root_id: Option<i64>,
) -> Result<CoverageStats> {
    let refs: Vec<&Source> = sources.iter().collect();
    compute_stats_from_source_refs(conn, &refs, archive_root_id)
}

/// Compute stats from source references.
fn compute_stats_from_source_refs(
    conn: &mut rusqlite::Connection,
    sources: &[&Source],
    archive_root_id: Option<i64>,
) -> Result<CoverageStats> {
    let mut stats = CoverageStats::new();

    // Total sources
    stats.total_sources = sources.len() as i64;

    // Excluded sources - uses is_excluded() which checks BOTH source and object level
    stats.excluded_sources = sources.iter().filter(|s| s.is_excluded()).count() as i64;

    // Hashed sources (have object_id AND not excluded)
    let hashed_sources: Vec<&&Source> = sources
        .iter()
        .filter(|s| s.object_id.is_some() && !s.is_excluded())
        .collect();
    stats.hashed_sources = hashed_sources.len() as i64;

    // Archived sources - use batch archive detection
    if stats.hashed_sources > 0 {
        // Collect object IDs from hashed sources
        let object_ids: Vec<i64> = hashed_sources
            .iter()
            .filter_map(|s| s.object_id)
            .collect();

        // Batch check which objects are archived
        let archived_set = repo::object::batch_check_archived(conn, &object_ids, archive_root_id)?;

        // Count sources whose object_id is in the archived set
        // (not unique objects — multiple sources can have the same object)
        stats.archived_sources = hashed_sources
            .iter()
            .filter(|s| s.object_id.map_or(false, |oid| archived_set.contains(&oid)))
            .count() as i64;
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
        let id = stats
            .root_id
            .map(|i| i.to_string())
            .unwrap_or_else(|| "?".to_string());
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

fn display_scoped_stats(stats: &CoverageStats, scope: Option<&str>, archive: Option<&str>) {
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

    // Always show included sources as total (excluded are filtered out conceptually)
    println!(
        "  Total sources:   {:>8}",
        format_number(stats.included_sources())
    );
    println!(
        "  Hashed:          {:>8} ({:.1}%)",
        format_number(stats.hashed_sources),
        stats.hashed_pct()
    );

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!(
            "  Not in archive:  {:>8}",
            format_number(stats.unarchived())
        );
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!("  Unarchived:      {:>8}", format_number(stats.unarchived()));
    }
}

fn display_per_root_stats(
    per_root: &[CoverageStats],
    overall: &CoverageStats,
    archive: Option<&str>,
) {
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

        let root_id = stats
            .root_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "?".to_string());
        let root_path = stats.root_path.as_deref().unwrap_or("unknown");
        let root_role = stats.root_role.as_deref().unwrap_or("unknown");
        println!("Root {}: {} ({})", root_id, root_path, root_role);

        println!(
            "  Total sources:   {:>8}",
            format_number(stats.included_sources())
        );
        println!(
            "  Hashed:          {:>8} ({:.1}%)",
            format_number(stats.hashed_sources),
            stats.hashed_pct()
        );

        if archive.is_some() {
            println!(
                "  In this archive: {:>8} ({:.1}% of hashed)",
                format_number(stats.archived_sources),
                stats.archived_pct()
            );
            println!(
                "  Not in archive:  {:>8}",
                format_number(stats.unarchived())
            );
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

    println!(
        "  Total sources:   {:>8}",
        format_number(overall.included_sources())
    );
    println!(
        "  Hashed:          {:>8} ({:.1}%)",
        format_number(overall.hashed_sources),
        overall.hashed_pct()
    );

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!(
            "  Not in archive:  {:>8}",
            format_number(overall.unarchived())
        );
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!(
            "  Unarchived:      {:>8}",
            format_number(overall.unarchived())
        );
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        let conn = RusqliteConnection::open_in_memory().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE roots (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL DEFAULT 'source',
                comment TEXT,
                last_scanned_at INTEGER,
                suspended INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE objects (
                id INTEGER PRIMARY KEY,
                hash_type TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE sources (
                id INTEGER PRIMARY KEY,
                root_id INTEGER NOT NULL REFERENCES roots(id),
                rel_path TEXT NOT NULL,
                object_id INTEGER REFERENCES objects(id),
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                device INTEGER NOT NULL DEFAULT 0,
                inode INTEGER NOT NULL DEFAULT 0,
                partial_hash TEXT NOT NULL DEFAULT '',
                basis_rev INTEGER NOT NULL DEFAULT 0,
                present INTEGER NOT NULL DEFAULT 1,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE facts (
                id INTEGER PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value_type TEXT NOT NULL,
                value_text TEXT,
                value_num REAL,
                value_time INTEGER
            );
            "#,
        )
        .unwrap();

        conn
    }

    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str, _suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 0)",
            rusqlite::params![path, role],
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
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime)
             VALUES (?, ?, ?, 1000, 1704067200)",
            rusqlite::params![root_id, rel_path, object_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Test that archived_sources counts sources, not unique objects.
    ///
    /// This guards against the Object Infrastructure bug pattern where
    /// archived_set.len() was used instead of counting sources.
    #[test]
    fn test_coverage_archived_counts_sources_not_objects() {
        let mut conn = setup_test_db();

        // Create source root and archive root
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // Create ONE object that will be archived
        let archived_obj = insert_object(&conn, "abc123archived", false);

        // Create 3 source files pointing to the SAME object
        insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj));

        // Create another object that is NOT archived
        let unarchived_obj = insert_object(&conn, "def456unarchived", false);
        insert_source(&conn, source_root, "photo4.jpg", Some(unarchived_obj));

        // Put the archived object in the archive root
        insert_source(&conn, archive_root, "backup.jpg", Some(archived_obj));

        // Fetch sources from source root only (simulating what coverage does)
        let sources = repo::source::batch_fetch_by_roots(&conn, &[source_root]).unwrap();
        assert_eq!(sources.len(), 4, "Should have 4 sources in source root");

        // Compute stats using the actual function
        let stats = compute_stats_from_sources(&mut conn, &sources, None).unwrap();

        // The critical assertion: archived_sources should be 3 (sources), not 1 (object)
        assert_eq!(
            stats.archived_sources, 3,
            "Should count 3 SOURCES with archived objects, not 1 unique object"
        );

        // Verify other stats are correct
        assert_eq!(stats.total_sources, 4);
        assert_eq!(stats.hashed_sources, 4);
    }
}
