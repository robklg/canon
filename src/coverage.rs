use anyhow::Result;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::domain::path::resolve_paths;
use crate::domain::root::{parse_root_spec, Root};
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::{self, Filter};
use crate::repo::{self, Db};

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
    include: &IncludeSet,
    compact: bool,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Fetch all roots for path and spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let scope_prefixes = resolve_paths(scope_paths, &roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Parse and validate archive spec (must be archive role)
    let archive_root_id = if let Some(spec) = archive_spec {
        Some(parse_root_spec(&roots, spec, Some("archive"))?)
    } else {
        None
    };

    // Get mutable reference for operations
    let conn = db.conn_mut();

    // Show annotation when include changes the source set
    if include.is_expanded() {
        let mut parts = Vec::new();
        if include.includes_excluded() {
            parts.push("excluded");
        }
        if include.includes_archived() {
            parts.push("archived");
        }
        eprintln!("[including {}]", parts.join(", "));
    }

    // Compute and display stats
    if !scope_prefixes.is_empty() {
        // Scoped mode
        let stats =
            compute_scoped_stats(conn, &scopes, &filters, archive_root_id, include)?;
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
        let (per_root_stats, overall) =
            compute_per_root_stats(conn, &filters, archive_root_id, include)?;
        if compact {
            display_compact_per_root(&per_root_stats, &overall);
        } else {
            display_per_root_stats(&per_root_stats, &overall, archive_spec);
        }
    }

    Ok(())
}

/// Get all sources matching scope/role/exclusion criteria, then apply filters.
fn get_matching_sources(
    conn: &mut rusqlite::Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    include: &IncludeSet,
) -> Result<Vec<Source>> {
    // Get all roots and extract IDs
    let roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();

    // Fetch all present sources
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Filter using domain predicates
    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| include.includes_archived() || s.is_from_role("source"))
        .filter(|s| s.matches_scope(scopes))
        .filter(|s| include.includes_excluded() || !s.is_excluded())
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
    include: &IncludeSet,
) -> Result<CoverageStats> {
    let sources = get_matching_sources(conn, scopes, filters, include)?;
    compute_stats_from_sources(conn, &sources, archive_root_id)
}

/// Compute coverage stats per root, plus overall totals
fn compute_per_root_stats(
    conn: &mut rusqlite::Connection,
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include: &IncludeSet,
) -> Result<(Vec<CoverageStats>, CoverageStats)> {
    // Get list of roots, filtered by role and suspension status
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: Vec<&Root> = all_roots
        .iter()
        .filter(|r| r.is_active())
        .filter(|r| include.includes_archived() || r.is_source())
        .collect();

    // Get all matching sources (unscoped)
    let all_sources = get_matching_sources(conn, &[], filters, include)?;

    // Group by root_id
    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for root in &roots {
        // Filter sources for this root
        let root_sources: Vec<&Source> = all_sources
            .iter()
            .filter(|s| s.root_id == root.id)
            .collect();

        let mut stats = compute_stats_from_source_refs(conn, &root_sources, archive_root_id)?;
        stats.root_id = Some(root.id);
        stats.root_path = Some(root.path.clone());
        stats.root_role = Some(root.role.clone());

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
        let object_ids: Vec<i64> = hashed_sources.iter().filter_map(|s| s.object_id).collect();

        // Batch check which objects are archived
        let archived_set = repo::object::batch_check_archived(conn, &object_ids, archive_root_id)?;

        // Count sources whose object_id is in the archived set
        // (not unique objects — multiple sources can have the same object)
        stats.archived_sources = hashed_sources
            .iter()
            .filter(|s| s.object_id.is_some_and(|oid| archived_set.contains(&oid)))
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
    let id_prefix = format!("id:{id:<2}");

    if path.len() <= MAX_PATH_LEN {
        format!("{id_prefix} {path}")
    } else {
        // Show ...last_n_chars
        let truncated = &path[path.len() - MAX_PATH_LEN + 3..];
        format!("{id_prefix} ...{truncated}")
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
        println!("Archive Coverage (relative to {arch})");
    } else {
        println!("Archive Coverage");
    }

    if let Some(s) = scope {
        println!("Scope: {s}\n");
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
        println!(
            "  Unarchived:      {:>8}",
            format_number(stats.unarchived())
        );
    }
}

fn display_per_root_stats(
    per_root: &[CoverageStats],
    overall: &CoverageStats,
    archive: Option<&str>,
) {
    if let Some(arch) = archive {
        println!("Archive Coverage Report (relative to {arch})\n");
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
        println!("Root {root_id}: {root_path} ({root_role})");

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
            println!(
                "  Unarchived:      {:>8}",
                format_number(stats.unarchived())
            );
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

    /// Test that get_matching_sources respects scope filtering.
    ///
    /// This validates the full scope-filtering pipeline:
    /// - Sources are filtered by path using Source::matches_scope()
    /// - Empty scopes returns all sources
    #[test]
    fn test_get_matching_sources_respects_scope() {
        use crate::domain::scope::ScopeMatch;

        let mut conn = setup_test_db();

        // Create two roots at different paths
        let photos_root = insert_root(&conn, "/photos", "source", false);
        let videos_root = insert_root(&conn, "/videos", "source", false);

        // Add sources to each root
        let photo1_id = insert_source(&conn, photos_root, "photo1.jpg", None);
        let photo2_id = insert_source(&conn, photos_root, "photo2.jpg", None);
        let video1_id = insert_source(&conn, videos_root, "video1.mp4", None);

        // Test 1: Scoped to /photos - should return only photo sources
        let scopes = vec![ScopeMatch::UnderDirectory("/photos".to_string())];
        let include = IncludeSet::default();
        let sources = get_matching_sources(&mut conn, &scopes, &[], &include).unwrap();
        let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();

        assert_eq!(source_ids.len(), 2, "Should return 2 photo sources");
        assert!(source_ids.contains(&photo1_id), "Should contain photo1");
        assert!(source_ids.contains(&photo2_id), "Should contain photo2");
        assert!(
            !source_ids.contains(&video1_id),
            "Should NOT contain video1"
        );

        // Test 2: Unscoped (empty scopes = all) - should return all sources
        let sources = get_matching_sources(&mut conn, &[], &[], &include).unwrap();
        let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();

        assert_eq!(source_ids.len(), 3, "Should return all 3 sources");
        assert!(source_ids.contains(&photo1_id), "Should contain photo1");
        assert!(source_ids.contains(&photo2_id), "Should contain photo2");
        assert!(source_ids.contains(&video1_id), "Should contain video1");
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

    /// Test that excluded sources are filtered out when include.excluded is false.
    /// This verifies the bug fix where _include_excluded was previously unused.
    #[test]
    fn test_coverage_excludes_excluded_sources() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Create an excluded object and a normal object
        let excluded_obj = insert_object(&conn, "excluded_hash", true);
        let normal_obj = insert_object(&conn, "normal_hash", false);

        // Insert sources
        insert_source(&conn, root, "excluded.jpg", Some(excluded_obj));
        insert_source(&conn, root, "normal.jpg", Some(normal_obj));

        // Also add a source-level excluded source (excluded on source, not object)
        let normal_obj2 = insert_object(&conn, "normal_hash2", false);
        let src_id = insert_source(&conn, root, "src_excluded.jpg", Some(normal_obj2));
        conn.execute(
            "UPDATE sources SET excluded = 1 WHERE id = ?",
            rusqlite::params![src_id],
        )
        .unwrap();

        // Default include (excluded=false) should filter out excluded sources
        let include = IncludeSet::default();
        let sources = get_matching_sources(&mut conn, &[], &[], &include).unwrap();

        // Only the non-excluded source should remain
        assert_eq!(sources.len(), 1, "Should only return non-excluded source");
        assert_eq!(sources[0].rel_path, "normal.jpg");
    }

    /// Test that excluded sources are included when include.excluded is true.
    #[test]
    fn test_coverage_includes_excluded_when_requested() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Create an excluded object and a normal object
        let excluded_obj = insert_object(&conn, "excluded_hash", true);
        let normal_obj = insert_object(&conn, "normal_hash", false);

        // Insert sources
        insert_source(&conn, root, "excluded.jpg", Some(excluded_obj));
        insert_source(&conn, root, "normal.jpg", Some(normal_obj));

        // Include excluded sources
        let include = IncludeSet { excluded: true, archived: false };
        let sources = get_matching_sources(&mut conn, &[], &[], &include).unwrap();

        // Both sources should be returned
        assert_eq!(sources.len(), 2, "Should return all sources including excluded");
    }
}
