use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::ceremony::format_count;
use crate::domain;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::repo;

struct SurveyResult {
    scope_prefixes: Vec<String>,
    total_count: usize,
    unhashed_count: usize,
    total_hashed: usize,
    archived_source_count: usize,
    archive_scopes: Vec<(String, usize)>,
    location_results: Vec<LocationResult>,
    unique_count: usize,
}

struct LocationResult {
    path: String,
    shared_count: usize,
}

/// Outcome of compute_survey: either a result to display or an early exit.
enum SurveyOutcome {
    /// Normal result with all computed data.
    Result(SurveyResult),
    /// Empty selection — display header and stop.
    Empty { scope_prefixes: Vec<String> },
    /// All unhashed — display header and hashing guidance.
    AllUnhashed {
        scope_prefixes: Vec<String>,
        total_count: usize,
    },
}

pub fn run(
    db: &mut repo::Db,
    paths: &[PathBuf],
    include: &IncludeSet,
) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots and sources upfront
    let all_roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    match compute_survey(paths, include, &all_sources, &all_roots)? {
        SurveyOutcome::Empty { scope_prefixes } => {
            print_selection_header(&scope_prefixes, 0, 0, 0);
        }
        SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        } => {
            print_selection_header(&scope_prefixes, total_count, total_count, 0);
            println!();
            println!("No hashed sources in selection. Content comparison requires hashing.");
            println!("Use `canon worklist` to generate a hashing worklist.");
        }
        SurveyOutcome::Result(result) => {
            print_selection_header(
                &result.scope_prefixes,
                result.total_count,
                result.unhashed_count,
                result.total_hashed,
            );
            println!();
            print_archive_section(
                result.archived_source_count,
                result.total_hashed,
                &result.archive_scopes,
            );
            println!();
            print_related_locations(&result.location_results, result.total_hashed);
            println!();
            println!("{} unique to this scope", format_count(result.unique_count));
        }
    }

    Ok(())
}

fn compute_survey(
    paths: &[PathBuf],
    include: &IncludeSet,
    all_sources: &[Source],
    all_roots: &[domain::Root],
) -> Result<SurveyOutcome> {
    // Default to cwd
    let scope_paths = if paths.is_empty() {
        vec![std::env::current_dir()?]
    } else {
        paths.to_vec()
    };

    // Resolve scope paths (soft resolution — offline-capable)
    let scope_prefixes = domain::path::resolve_paths(&scope_paths, all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Build selection: active, source role, in scope, visibility rules
    let selection: Vec<&Source> = all_sources
        .iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| include.includes_excluded() || !s.is_excluded())
        .collect();

    // Partition: unhashed vs hashed
    let total_count = selection.len();
    let hashed: Vec<&Source> = selection
        .iter()
        .filter(|s| s.object_id.is_some())
        .copied()
        .collect();
    let unhashed_count = total_count - hashed.len();
    let total_hashed = hashed.len();

    // Early exit: empty selection
    if total_count == 0 {
        return Ok(SurveyOutcome::Empty { scope_prefixes });
    }

    // Early exit: all unhashed
    if total_hashed == 0 {
        return Ok(SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        });
    }

    // Collect selection identity
    let sel_object_ids: HashSet<i64> = hashed.iter().filter_map(|s| s.object_id).collect();
    let sel_source_ids: HashSet<i64> = selection.iter().map(|s| s.id).collect();

    // Build object index from ALL active, non-excluded, hashed sources
    let mut by_object_id: HashMap<i64, Vec<&Source>> = HashMap::new();
    for s in all_sources {
        if s.is_active() && !s.is_excluded() {
            if let Some(oid) = s.object_id {
                by_object_id.entry(oid).or_default().push(s);
            }
        }
    }

    // --- Phase 3: Archive status ---
    let mut archive_sources: Vec<&Source> = Vec::new();
    let mut archived_object_ids: HashSet<i64> = HashSet::new();

    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            let mut found_archive = false;
            for sib in siblings {
                if sib.is_from_role("archive") {
                    if !found_archive {
                        archived_object_ids.insert(oid);
                        found_archive = true;
                    }
                    archive_sources.push(sib);
                }
            }
        }
    }

    // Count selection sources that are archived (source-based counting)
    let archived_source_count = hashed
        .iter()
        .filter(|s| archived_object_ids.contains(&s.object_id.unwrap()))
        .count();

    // Scope discovery on archive sources for grouped display
    let mut archive_scopes = domain::survey::discover_scopes_by_root(&archive_sources);
    archive_scopes.sort_by(|a, b| a.0.cmp(&b.0));

    // --- Phase 4: Overlap and related locations ---
    let mut overlap_sources: Vec<&Source> = Vec::new();
    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            for sib in siblings {
                if !sel_source_ids.contains(&sib.id) && sib.is_from_role("source") {
                    overlap_sources.push(sib);
                }
            }
        }
    }

    // Scope discovery on overlap sources → related locations
    let location_scopes = domain::survey::discover_scopes_by_root(&overlap_sources);

    // Per-location shared count
    let mut location_results: Vec<LocationResult> = Vec::new();

    for (scope_path, _overlap_count) in &location_scopes {
        let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

        // Object IDs present at this location (from overlap sources)
        let loc_object_ids: HashSet<i64> = overlap_sources
            .iter()
            .filter(|s| s.matches_scope(&loc_scope))
            .filter_map(|s| s.object_id)
            .collect();

        // Count selection sources whose content appears at this location
        let shared_count = hashed
            .iter()
            .filter(|s| loc_object_ids.contains(&s.object_id.unwrap()))
            .count();

        location_results.push(LocationResult {
            path: scope_path.clone(),
            shared_count,
        });
    }

    // Sort by shared count descending (no classification yet — Story 3)
    location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));

    // --- Phase 5: Unique count ---
    let unique_count = domain::survey::count_unique_to_selection(
        &sel_object_ids,
        &sel_source_ids,
        &by_object_id,
    );

    Ok(SurveyOutcome::Result(SurveyResult {
        scope_prefixes,
        total_count,
        unhashed_count,
        total_hashed,
        archived_source_count,
        archive_scopes,
        location_results,
        unique_count,
    }))
}

// =============================================================================
// Output formatting
// =============================================================================

fn print_selection_header(
    scope_prefixes: &[String],
    total: usize,
    unhashed: usize,
    hashed: usize,
) {
    if scope_prefixes.len() == 1 {
        println!("Selection: {}", scope_prefixes[0]);
    } else {
        println!("Selection:");
        for p in scope_prefixes {
            println!("  {p}");
        }
    }

    println!(
        "  {} sources ({} unhashed, {} hashed)",
        format_count(total),
        format_count(unhashed),
        format_count(hashed),
    );
}

fn print_archive_section(
    archived_count: usize,
    total_hashed: usize,
    archive_scopes: &[(String, usize)],
) {
    if archived_count == 0 {
        println!("Archived: 0 of {}", format_count(total_hashed));
        return;
    }

    let pct = 100.0 * archived_count as f64 / total_hashed as f64;
    println!(
        "Archived: {} of {} ({:.1}%)",
        format_count(archived_count),
        format_count(total_hashed),
        pct,
    );

    // Scope-grouped archive paths with right-aligned counts
    let max_path_len = archive_scopes
        .iter()
        .map(|(p, _)| p.len())
        .max()
        .unwrap_or(0);
    let max_count_len = archive_scopes
        .iter()
        .map(|(_, c)| format_count(*c).len())
        .max()
        .unwrap_or(0);

    for (path, count) in archive_scopes {
        println!(
            "  {:path_w$}  {:>count_w$}",
            path,
            format_count(*count),
            path_w = max_path_len,
            count_w = max_count_len,
        );
    }
}

fn print_related_locations(locations: &[LocationResult], total_hashed: usize) {
    if locations.is_empty() {
        println!("No related locations found.");
        return;
    }

    println!("Related locations:");

    let max_path_len = locations.iter().map(|l| l.path.len()).max().unwrap_or(0);
    let max_shared_len = locations
        .iter()
        .map(|l| format_count(l.shared_count).len())
        .max()
        .unwrap_or(0);
    let m_str = format_count(total_hashed);

    for loc in locations {
        println!(
            "  {:path_w$}  {:>count_w$} of {} shared",
            loc.path,
            format_count(loc.shared_count),
            m_str,
            path_w = max_path_len,
            count_w = max_shared_len,
        );
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection;

    fn insert_root(conn: &Connection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 0)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_root_suspended(conn: &Connection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 1)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &Connection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, 0)",
            rusqlite::params![hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        // Use a static counter via last_insert_rowid + 1000 for unique inode
        let inode = conn.last_insert_rowid() + 1000;
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, \
             partial_hash, scanned_at, last_seen_at, device, inode) \
             VALUES (?, ?, ?, 1000, 1704067200, 'ph', 0, 0, 1, ?)",
            rusqlite::params![root_id, rel_path, object_id, inode],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source_excluded(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        let inode = conn.last_insert_rowid() + 1000;
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, \
             partial_hash, scanned_at, last_seen_at, device, inode, excluded) \
             VALUES (?, ?, ?, 1000, 1704067200, 'ph', 0, 0, 1, ?, 1)",
            rusqlite::params![root_id, rel_path, object_id, inode],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Helper to run compute_survey with test data.
    fn run_compute(
        conn: &Connection,
        scope_paths: &[&str],
        include: &IncludeSet,
    ) -> SurveyOutcome {
        let all_roots = repo::root::fetch_all(conn).unwrap();
        let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
        let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids).unwrap();

        let paths: Vec<PathBuf> = scope_paths.iter().map(|p| PathBuf::from(p)).collect();
        compute_survey(&paths, include, &all_sources, &all_roots).unwrap()
    }

    // =========================================================================
    // Test 1: Basic summary end-to-end
    // =========================================================================

    #[test]
    fn test_basic_summary() {
        let conn = open_in_memory_for_test();

        // Source root A (/mnt/drive-a)
        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        // Source root B (/mnt/backup)
        let root_b = insert_root(&conn, "/mnt/backup", "source");
        // Archive root
        let archive = insert_root(&conn, "/archive/photos", "archive");

        // Objects
        let obj1 = insert_object(&conn, "hash_001"); // on A, B, and archive
        let obj2 = insert_object(&conn, "hash_002"); // on A and B
        let obj3 = insert_object(&conn, "hash_003"); // on A and archive
        let obj4 = insert_object(&conn, "hash_004"); // unique to A

        // Source root A: 5 sources (1 unhashed)
        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_a, "photos/IMG_005.jpg", None);

        // Source root B: overlap
        insert_source(&conn, root_b, "vacation/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "vacation/IMG_002.jpg", Some(obj2));

        // Archive
        insert_source(&conn, archive, "2024/IMG_001.jpg", Some(obj1));
        insert_source(&conn, archive, "2024/IMG_003.jpg", Some(obj3));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive-a"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 5);
                assert_eq!(result.unhashed_count, 1);
                assert_eq!(result.total_hashed, 4);
                assert_eq!(result.archived_source_count, 2); // obj1 and obj3
                assert_eq!(result.archive_scopes.len(), 1);
                assert_eq!(result.archive_scopes[0].0, "/archive/photos/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/backup/vacation");
                assert_eq!(result.location_results[0].shared_count, 2); // obj1, obj2
                assert_eq!(result.unique_count, 1); // obj4
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 2: Empty selection
    // =========================================================================

    #[test]
    fn test_empty_selection() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        // Sources exist, but not under the scoped subdirectory
        let obj = insert_object(&conn, "hash_001");
        insert_source(&conn, root, "photos/a.jpg", Some(obj));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive/other"], &include);

        match outcome {
            SurveyOutcome::Empty { scope_prefixes } => {
                assert_eq!(scope_prefixes, vec!["/mnt/drive/other"]);
            }
            _ => panic!("Expected SurveyOutcome::Empty"),
        }
    }

    // =========================================================================
    // Test 3: All unhashed
    // =========================================================================

    #[test]
    fn test_all_unhashed() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        insert_source(&conn, root, "a.jpg", None);
        insert_source(&conn, root, "b.jpg", None);

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::AllUnhashed {
                scope_prefixes,
                total_count,
            } => {
                assert_eq!(scope_prefixes, vec!["/mnt/drive"]);
                assert_eq!(total_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::AllUnhashed"),
        }
    }

    // =========================================================================
    // Test 4: No related locations
    // =========================================================================

    #[test]
    fn test_no_related_locations() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive = insert_root(&conn, "/archive", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, root, "b.jpg", Some(obj2));

        // Archive copies but no overlap on source roots
        insert_source(&conn, archive, "a.jpg", Some(obj1));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1); // obj2 — not in archive
                assert_eq!(result.archived_source_count, 1); // obj1
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 5: No archived sources
    // =========================================================================

    #[test]
    fn test_no_archived() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 0);
                assert!(result.archive_scopes.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 6: Multiple scope paths
    // =========================================================================

    #[test]
    fn test_multiple_scope_paths() {
        let conn = open_in_memory_for_test();
        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/drive-b", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive-a", "/mnt/drive-b"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.scope_prefixes.len(), 2);
                assert_eq!(result.total_count, 2); // union of both scopes
                assert_eq!(result.total_hashed, 2);
                assert_eq!(result.unique_count, 2); // both unique
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 7: Suspended root excluded
    // =========================================================================

    #[test]
    fn test_suspended_root_excluded() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let suspended = insert_root_suspended(&conn, "/mnt/suspended", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        // This source is on a suspended root — should not appear anywhere
        insert_source(&conn, suspended, "b.jpg", Some(obj1));
        insert_source(&conn, suspended, "c.jpg", Some(obj2));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1);
                // No overlap — suspended root's sources invisible
                assert!(result.location_results.is_empty());
                // Unique: obj1 has a copy on suspended root, but suspended sources
                // are not in the object index, so obj1 is unique to selection
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 8: Excluded sources hidden by default
    // =========================================================================

    #[test]
    fn test_excluded_sources_hidden() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let other = insert_root(&conn, "/mnt/other", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source_excluded(&conn, root, "excluded.jpg", Some(obj2));

        // Overlap source also excluded
        insert_source_excluded(&conn, other, "b.jpg", Some(obj1));

        // Default: excluded hidden
        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1); // excluded.jpg hidden
                assert_eq!(result.total_hashed, 1);
                // Excluded overlap source is not in object index, so no overlap
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }

        // With --include excluded: excluded appears in selection
        let include = IncludeSet {
            excluded: true,
            archived: false,
        };
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2); // excluded.jpg now in selection
                assert_eq!(result.total_hashed, 2);
                // Object index still excludes excluded sources (outward side
                // never shows excluded), so no overlap and both unique
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 9: Archive scope grouping
    // =========================================================================

    #[test]
    fn test_archive_scope_grouping() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive1 = insert_root(&conn, "/archive/a", "archive");
        let archive2 = insert_root(&conn, "/archive/b", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));
        insert_source(&conn, root, "z.jpg", Some(obj3));

        // Archive sources across two roots, multiple directories
        insert_source(&conn, archive1, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive1, "2024/y.jpg", Some(obj2));
        insert_source(&conn, archive2, "backup/z.jpg", Some(obj3));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 3);
                assert_eq!(result.archive_scopes.len(), 2);
                // Sorted alphabetically
                assert_eq!(result.archive_scopes[0].0, "/archive/a/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.archive_scopes[1].0, "/archive/b/backup");
                assert_eq!(result.archive_scopes[1].1, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Test 10: Same root, different scope — overlap from own root
    // =========================================================================

    #[test]
    fn test_same_root_different_scope() {
        let conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        // Selection: scoped to /mnt/drive/photos
        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));

        // Same root, but outside scope — overlap
        insert_source(&conn, root, "documents/a_copy.jpg", Some(obj1));

        let include = IncludeSet::default();
        let outcome = run_compute(&conn, &["/mnt/drive/photos"], &include);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/drive/documents");
                assert_eq!(result.location_results[0].shared_count, 1); // obj1
                assert_eq!(result.unique_count, 1); // obj2
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }
}
