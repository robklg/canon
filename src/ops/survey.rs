//! Survey operations — outward-looking comparison from a shaped selection.
//!
//! Computes archive status, overlap detection, scope discovery, location
//! classification, and detail views (complement, unique, overlap, residual).
//! Uses custom selection logic (not `select_sources()`) to support the
//! asymmetric visibility model.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::domain;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::{self, Filter};

const SUPERSET_THRESHOLD: f64 = 0.8;

/// Parameters controlling survey computation.
pub struct SurveyParams {
    /// Visibility control (--include excluded).
    pub include: IncludeSet,
    /// Whether to compute affinity data (complementary counts, classification).
    pub compute_affinity: bool,
    /// Whether to compute overlap pairs per location.
    pub compute_overlap_pairs: bool,
    /// Whether to compute residual paths per location.
    pub compute_residual: bool,
}

/// Outcome of compute_survey: either a result to display or an early exit.
pub enum SurveyOutcome {
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

pub struct SurveyResult {
    pub scope_prefixes: Vec<String>,
    pub total_count: usize,
    pub unhashed_count: usize,
    pub total_hashed: usize,
    pub archived_source_count: usize,
    pub archive_scopes: Vec<(String, usize)>,
    pub location_results: Vec<LocationResult>,
    pub unique_count: usize,
    pub unique_paths: Vec<String>,
    pub is_other_mode: bool,
    /// Display label when --archive is specified (e.g., "in /archive/photos").
    /// Set by the interface after return — ops always returns None.
    pub archive_label: Option<String>,
}

/// A selection-side path paired with its counterpart paths at a location.
pub struct OverlapPair {
    /// Path of the selection-side source (absolute).
    pub selection_path: String,
    /// Paths at the other location with matching content (relative to location).
    pub counterpart_paths: Vec<String>,
}

pub struct LocationResult {
    pub path: String,
    pub shared_count: usize,
    /// Total hashed sources at this location (always computed).
    pub total_count: usize,
    pub complementary_count: Option<usize>,
    pub only_here_count: Option<usize>,
    pub kind: Option<domain::survey::LocationKind>,
    /// Complementary source paths relative to location, sorted.
    /// None when affinity not computed; Some(vec![]) when computed but empty.
    pub complementary_paths: Option<Vec<String>>,
    /// Selection sources overlapping with this location, with counterpart paths.
    pub overlap_pairs: Option<Vec<OverlapPair>>,
    /// Selection source paths NOT shared with this location.
    pub residual_paths: Option<Vec<String>>,
}

/// Compute survey data for a selection of sources.
///
/// Takes pre-resolved scope paths (no cwd defaulting) and pre-resolved
/// other paths. Returns a typed outcome for the interface to format.
pub fn compute_survey(
    conn: &mut Connection,
    paths: &[PathBuf],
    filters: &[Filter],
    params: &SurveyParams,
    all_sources: &[Source],
    all_roots: &[domain::Root],
    other_paths: &[String],
    archive_root_id: Option<i64>,
) -> Result<SurveyOutcome> {
    // Resolve scope paths (soft resolution — offline-capable)
    let scope_prefixes = domain::path::resolve_paths(paths, all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Build selection from domain predicates (asymmetric visibility model)
    let selection: Vec<&Source> = all_sources
        .iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| params.include.includes_excluded() || !s.is_excluded())
        .collect();

    // Apply --where filters to selection
    let selection = if filters.is_empty() {
        selection
    } else {
        let ids: Vec<i64> = selection.iter().map(|s| s.id).collect();
        let passed: HashSet<i64> = filter::apply_filters(conn, &ids, filters)?
            .into_iter()
            .collect();
        selection
            .into_iter()
            .filter(|s| passed.contains(&s.id))
            .collect()
    };

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

    // Archive status: find selection content that exists on archive roots
    let mut archive_sources: Vec<&Source> = Vec::new();
    let mut archived_object_ids: HashSet<i64> = HashSet::new();

    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            let mut found_archive = false;
            for sib in siblings {
                if sib.is_from_role("archive") {
                    // When --archive is specified, only count that archive
                    if let Some(target_id) = archive_root_id {
                        if sib.root_id != target_id {
                            continue;
                        }
                    }
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

    // Overlap: find selection content that exists on other source roots
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

    // Location discovery
    let is_other_mode = !other_paths.is_empty();
    let location_scopes: Vec<(String, usize)> = if is_other_mode {
        // --other: user paths directly
        other_paths.iter().map(|p| (p.clone(), 0)).collect()
    } else {
        // Default: scope discovery from overlap sources
        domain::survey::discover_scopes_by_root(&overlap_sources)
    };

    // Per-location: shared count + affinity
    let mut location_results: Vec<LocationResult> = Vec::new();

    for (scope_path, _overlap_count) in &location_scopes {
        let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

        // Object IDs at this location — used for shared_count and detail views.
        // --other mode: all roles. Default mode: source role only.
        let loc_oids: HashSet<i64> = if is_other_mode {
            all_sources
                .iter()
                .filter(|s| s.is_active())
                .filter(|s| !s.is_excluded())
                .filter(|s| s.matches_scope(&loc_scope))
                .filter(|s| !sel_source_ids.contains(&s.id))
                .filter_map(|s| s.object_id)
                .collect()
        } else {
            overlap_sources
                .iter()
                .filter(|s| s.matches_scope(&loc_scope))
                .filter_map(|s| s.object_id)
                .collect()
        };

        // Shared count: how many selection sources have content at this location
        let shared_count = hashed
            .iter()
            .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
            .count();

        // Total hashed sources at location (not excluding selection sources).
        // Same visibility rules as shared_count: source-role only in default mode,
        // all roles in --other mode.
        let total_count: usize = all_sources
            .iter()
            .filter(|s| s.is_active())
            .filter(|s| !s.is_excluded())
            .filter(|s| s.object_id.is_some())
            .filter(|s| s.matches_scope(&loc_scope))
            .filter(|s| is_other_mode || s.is_from_role("source"))
            .count();

        // Complementary content and classification (only with affinity enabled)
        let (complementary_count, only_here_count, kind, complementary_paths) =
            if params.compute_affinity {
                // Step 1: Get ALL sources within this location
                // Active, non-excluded, not in selection
                let loc_sources: Vec<&Source> = all_sources
                    .iter()
                    .filter(|s| s.is_active())
                    .filter(|s| !s.is_excluded())
                    .filter(|s| s.matches_scope(&loc_scope))
                    .filter(|s| !sel_source_ids.contains(&s.id))
                    .collect();

                // Step 2: Apply --where filters to location sources
                let loc_ids: Vec<i64> = loc_sources.iter().map(|s| s.id).collect();
                let passed: HashSet<i64> = filter::apply_filters(conn, &loc_ids, filters)?
                    .into_iter()
                    .collect();

                // Step 3: Partition into overlap vs complementary
                // CRITICAL: filter to hashed-only BEFORE partitioning.
                // Unhashed sources can't participate in content comparison.
                // Without this guard, unhashed sources leak into complementary
                // count (their object_id is None, which is never in sel_object_ids,
                // so they'd always be classified as "complementary").
                let matching_hashed: Vec<&Source> = loc_sources
                    .iter()
                    .filter(|s| passed.contains(&s.id))
                    .filter(|s| s.object_id.is_some())
                    .copied()
                    .collect();

                let complementary: Vec<&Source> = matching_hashed
                    .iter()
                    .filter(|s| !sel_object_ids.contains(&s.object_id.unwrap()))
                    .copied()
                    .collect();

                let comp_count = complementary.len();

                // Step 4: Collect complementary paths relative to location
                let mut comp_paths: Vec<String> = complementary
                    .iter()
                    .filter_map(|s| {
                        domain::path::path_strip_prefix(&s.path(), scope_path)
                            .map(|p| p.to_string())
                    })
                    .collect();
                comp_paths.sort_unstable();

                // Step 5: "Only here" — unique object_ids among complementary
                let comp_oids: HashSet<i64> =
                    complementary.iter().filter_map(|s| s.object_id).collect();
                let only_here =
                    domain::survey::count_only_here(&comp_oids, scope_path, &by_object_id);

                // Step 6: Classify
                let kind = domain::survey::classify_location(
                    shared_count,
                    total_hashed,
                    comp_count,
                    SUPERSET_THRESHOLD,
                    total_count,
                );

                (
                    Some(comp_count),
                    Some(only_here),
                    Some(kind),
                    Some(comp_paths),
                )
            } else {
                (None, None, None, None)
            };

        // Overlap pairs: which selection files have copies at this location,
        // paired with their counterpart paths at the location.
        let overlap_pairs = if params.compute_overlap_pairs {
            let mut pairs: Vec<OverlapPair> = hashed
                .iter()
                .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
                .map(|s| {
                    let oid = s.object_id.unwrap();
                    let mut counterpart_paths: Vec<String> = by_object_id
                        .get(&oid)
                        .map(|sources| {
                            sources
                                .iter()
                                .filter(|cs| cs.matches_scope(&loc_scope))
                                .filter(|cs| !sel_source_ids.contains(&cs.id))
                                .map(|cs| {
                                    domain::path::path_strip_prefix(&cs.path(), scope_path)
                                        .map(|p| p.to_string())
                                        .unwrap_or_else(|| cs.path())
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    counterpart_paths.sort_unstable();
                    OverlapPair {
                        selection_path: s.path(),
                        counterpart_paths,
                    }
                })
                .collect();
            pairs.sort_by(|a, b| a.selection_path.cmp(&b.selection_path));
            Some(pairs)
        } else {
            None
        };

        // Residual paths: selection files NOT shared with this location.
        // Uses full selection (not just hashed) — unhashed sources are always residual.
        let residual_paths = if params.compute_residual {
            let mut paths: Vec<String> = selection
                .iter()
                .filter(|s| match s.object_id {
                    Some(oid) => !loc_oids.contains(&oid),
                    None => true, // unhashed always residual
                })
                .map(|s| s.path())
                .collect();
            paths.sort_unstable();
            Some(paths)
        } else {
            None
        };

        location_results.push(LocationResult {
            path: scope_path.clone(),
            shared_count,
            total_count,
            complementary_count,
            only_here_count,
            kind,
            complementary_paths,
            overlap_pairs,
            residual_paths,
        });
    }

    // Sort locations
    if is_other_mode {
        // --other: preserve user-specified order (no sort)
    } else if params.compute_affinity {
        // Classification: supersets first, then leads, then mirrors
        // Within each group: complementary desc, then shared desc
        location_results.sort_by(|a, b| {
            let kind_a = a.kind.as_ref().unwrap();
            let kind_b = b.kind.as_ref().unwrap();
            kind_a
                .cmp(kind_b)
                .then(b.complementary_count.cmp(&a.complementary_count))
                .then(b.shared_count.cmp(&a.shared_count))
        });
    } else {
        // No affinity data: sort by shared count descending
        location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));
    }

    // Unique: selection content that exists nowhere else
    let unique_oids =
        domain::survey::find_unique_object_ids(&sel_object_ids, &sel_source_ids, &by_object_id);
    let unique_count = unique_oids.len();
    let mut unique_paths: Vec<String> = hashed
        .iter()
        .filter(|s| unique_oids.contains(&s.object_id.unwrap()))
        .map(|s| s.path())
        .collect();
    unique_paths.sort_unstable();

    Ok(SurveyOutcome::Result(SurveyResult {
        scope_prefixes,
        total_count,
        unhashed_count,
        total_hashed,
        archived_source_count,
        archive_scopes,
        location_results,
        unique_count,
        unique_paths,
        is_other_mode,
        archive_label: None, // set by run() after return
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, insert_source_excluded, setup_test_db,
    };

    fn test_params() -> SurveyParams {
        SurveyParams {
            include: IncludeSet::default(),
            compute_affinity: false,
            compute_overlap_pairs: false,
            compute_residual: false,
        }
    }

    /// Helper to run compute_survey with test data.
    fn run_compute(
        conn: &mut Connection,
        scope_paths: &[&str],
        params: &SurveyParams,
        filters: &[Filter],
        other_paths: &[PathBuf],
        archive_root_id: Option<i64>,
    ) -> SurveyOutcome {
        let all_roots = crate::repo::root::fetch_all(conn).unwrap();
        let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
        let all_sources = crate::repo::source::batch_fetch_by_roots(conn, &root_ids).unwrap();

        let paths: Vec<PathBuf> = scope_paths.iter().map(PathBuf::from).collect();
        let other: Vec<String> = other_paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        compute_survey(
            conn,
            &paths,
            filters,
            params,
            &all_sources,
            &all_roots,
            &other,
            archive_root_id,
        )
        .unwrap()
    }

    // =========================================================================
    // Basic summary end-to-end
    // =========================================================================

    #[test]
    fn test_basic_summary() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let archive = insert_root(&conn, "/archive/photos", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_a, "photos/IMG_005.jpg", None);

        insert_source(&conn, root_b, "vacation/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "vacation/IMG_002.jpg", Some(obj2));

        insert_source(&conn, archive, "2024/IMG_001.jpg", Some(obj1));
        insert_source(&conn, archive, "2024/IMG_003.jpg", Some(obj3));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 5);
                assert_eq!(result.unhashed_count, 1);
                assert_eq!(result.total_hashed, 4);
                assert_eq!(result.archived_source_count, 2);
                assert_eq!(result.archive_scopes.len(), 1);
                assert_eq!(result.archive_scopes[0].0, "/archive/photos/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/backup/vacation");
                assert_eq!(result.location_results[0].shared_count, 2);
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_empty_selection() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let obj = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "photos/a.jpg", Some(obj));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive/other"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Empty { scope_prefixes } => {
                assert_eq!(scope_prefixes, vec!["/mnt/drive/other"]);
            }
            _ => panic!("Expected SurveyOutcome::Empty"),
        }
    }

    #[test]
    fn test_all_unhashed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        insert_source(&conn, root, "a.jpg", None);
        insert_source(&conn, root, "b.jpg", None);

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

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

    #[test]
    fn test_no_related_locations() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, root, "b.jpg", Some(obj2));
        insert_source(&conn, archive, "a.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1);
                assert_eq!(result.archived_source_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_no_archived() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 0);
                assert!(result.archive_scopes.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_multiple_scope_paths() {
        let mut conn = setup_test_db();
        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/drive-b", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let params = test_params();
        let outcome = run_compute(
            &mut conn,
            &["/mnt/drive-a", "/mnt/drive-b"],
            &params,
            &[],
            &[],
            None,
        );

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.scope_prefixes.len(), 2);
                assert_eq!(result.total_count, 2);
                assert_eq!(result.total_hashed, 2);
                assert_eq!(result.unique_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_suspended_root_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let suspended = insert_root(&conn, "/mnt/suspended", "source", true);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, suspended, "b.jpg", Some(obj1));
        insert_source(&conn, suspended, "c.jpg", Some(obj2));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1);
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_excluded_sources_hidden() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let other = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source_excluded(&conn, root, "excluded.jpg", Some(obj2));
        insert_source_excluded(&conn, other, "b.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1);
                assert_eq!(result.total_hashed, 1);
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }

        // With --include excluded
        let params = SurveyParams {
            include: IncludeSet {
                excluded: true,
                archived: false,
            },
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.total_hashed, 2);
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_archive_scope_grouping() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let archive1 = insert_root(&conn, "/archive/a", "archive", false);
        let archive2 = insert_root(&conn, "/archive/b", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));
        insert_source(&conn, root, "z.jpg", Some(obj3));

        insert_source(&conn, archive1, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive1, "2024/y.jpg", Some(obj2));
        insert_source(&conn, archive2, "backup/z.jpg", Some(obj3));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 3);
                assert_eq!(result.archive_scopes.len(), 2);
                assert_eq!(result.archive_scopes[0].0, "/archive/a/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.archive_scopes[1].0, "/archive/b/backup");
                assert_eq!(result.archive_scopes[1].1, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_same_root_different_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root, "documents/a_copy.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/drive/documents");
                assert_eq!(result.location_results[0].shared_count, 1);
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Affinity and classification
    // =========================================================================

    #[test]
    fn test_affinity_basic() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);
        let obj6 = insert_object(&conn, "hash_006", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        insert_source(&conn, root_b, "trip/notes.txt", Some(obj6));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 3);
                assert_eq!(result.total_hashed, 3);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.complementary_count, Some(2));
                assert_eq!(loc.only_here_count, Some(2));
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Lead));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_affinity_only_here_reduced() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let root_c = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));

        insert_source(&conn, root_c, "misc/copy.jpg", Some(obj4));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let loc_b = result
                    .location_results
                    .iter()
                    .find(|l| l.path.contains("backup"))
                    .expect("Should find backup location");
                assert_eq!(loc_b.complementary_count, Some(2));
                assert_eq!(loc_b.only_here_count, Some(1));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_affinity_unhashed_excluded() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        insert_source(&conn, root_b, "trip/IMG_006.jpg", None);

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let loc = &result.location_results[0];
                // CRITICAL: complementary must be 2 (obj4, obj5), NOT 3
                assert_eq!(loc.complementary_count, Some(2));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_no_filters_no_affinity() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_classification_sort() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup-main", "source", false);
        let root_c = insert_root(&conn, "/mnt/partner", "source", false);
        let root_d = insert_root(&conn, "/mnt/old-copy", "source", false);

        let mut sel_objs = Vec::new();
        for i in 1..=10 {
            let obj = insert_object(&conn, &format!("hash_{i:03}"), false);
            insert_source(&conn, root_a, &format!("photos/IMG_{i:03}.jpg"), Some(obj));
            sel_objs.push(obj);
        }

        for i in 1..=9 {
            insert_source(
                &conn,
                root_b,
                &format!("backup/IMG_{i:03}.jpg"),
                Some(sel_objs[i - 1]),
            );
        }
        for i in 11..=15 {
            let obj = insert_object(&conn, &format!("hash_{i:03}"), false);
            insert_source(
                &conn,
                root_b,
                &format!("backup/EXTRA_{i:03}.jpg"),
                Some(obj),
            );
        }

        insert_source(&conn, root_c, "photos/IMG_001.jpg", Some(sel_objs[0]));
        insert_source(&conn, root_c, "photos/IMG_002.jpg", Some(sel_objs[1]));
        for i in 16..=35 {
            let obj = insert_object(&conn, &format!("hash_{i:03}"), false);
            insert_source(&conn, root_c, &format!("photos/COMP_{i:03}.jpg"), Some(obj));
        }

        insert_source(&conn, root_d, "copy/IMG_001.jpg", Some(sel_objs[0]));
        insert_source(&conn, root_d, "copy/IMG_002.jpg", Some(sel_objs[1]));
        insert_source(&conn, root_d, "copy/IMG_003.jpg", Some(sel_objs[2]));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 3);
                assert!(result.location_results[0].path.contains("backup-main"));
                assert_eq!(
                    result.location_results[0].kind,
                    Some(domain::survey::LocationKind::Superset)
                );
                assert_eq!(result.location_results[0].shared_count, 9);
                assert_eq!(result.location_results[0].complementary_count, Some(5));
                assert!(result.location_results[1].path.contains("partner"));
                assert_eq!(
                    result.location_results[1].kind,
                    Some(domain::survey::LocationKind::Lead)
                );
                assert_eq!(result.location_results[1].complementary_count, Some(20));
                assert!(result.location_results[2].path.contains("old-copy"));
                assert_eq!(
                    result.location_results[2].kind,
                    Some(domain::survey::LocationKind::Subset)
                );
                assert_eq!(result.location_results[2].complementary_count, Some(0));
                assert_eq!(result.location_results[2].only_here_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_selection_narrowed_by_filter() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root, "photos/c.txt", Some(obj3));
        insert_source(&conn, root, "photos/d.txt", Some(obj4));
        insert_source(&conn, root, "photos/e.jpg", Some(obj5));

        let params = test_params();
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 3);
                assert_eq!(result.total_hashed, 3);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_same_root_complementary() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root, "documents/a.jpg", Some(obj1));
        insert_source(&conn, root, "documents/c.jpg", Some(obj3));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/drive/documents");
                assert_eq!(result.location_results[0].shared_count, 1);
                assert_eq!(result.location_results[0].complementary_count, Some(1));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_mirror_with_filters() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/mirror", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "backup/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "backup/b.jpg", Some(obj2));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Subset));
                assert_eq!(loc.complementary_count, Some(0));
                assert_eq!(loc.only_here_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other tests
    // =========================================================================

    #[test]
    fn test_other_basic() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);
        let obj6 = insert_object(&conn, "hash_006", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        insert_source(&conn, root_b, "trip/notes.txt", Some(obj6));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/mnt/backup/trip")];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip");
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.complementary_count, Some(2));
                assert_eq!(loc.only_here_count, Some(2));
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Lead));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_other_zero_overlap() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "docs/c.jpg", Some(obj3));
        insert_source(&conn, root_b, "docs/d.jpg", Some(obj4));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 0);
                assert_eq!(loc.complementary_count, Some(2));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_other_preserves_order() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/root-b", "source", false);
        let root_c = insert_root(&conn, "/mnt/root-c", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        insert_source(&conn, root_c, "backup/a.jpg", Some(obj1));

        let other = vec![PathBuf::from("/mnt/root-c"), PathBuf::from("/mnt/root-b")];
        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 2);
                assert!(result.location_results[0].path.contains("root-c"));
                assert!(result.location_results[1].path.contains("root-b"));
                assert_eq!(result.location_results[0].shared_count, 1);
                assert_eq!(result.location_results[1].shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_other_archive_root() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, archive, "2024/a.jpg", Some(obj1));
        insert_source(&conn, archive, "2024/c.jpg", Some(obj3));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/archive")];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 1);
                assert_eq!(loc.complementary_count, Some(1));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --brief tests
    // =========================================================================

    #[test]
    fn test_brief_suppresses_affinity() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        // affinity: true + brief: true → compute_affinity: false
        let params = test_params(); // compute_affinity defaults to false
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(!result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_brief_without_filters_noop() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        let params_normal = test_params();
        let outcome_normal = run_compute(&mut conn, &["/mnt/drive-a"], &params_normal, &[], &[], None);
        let params_brief = test_params(); // same — both have compute_affinity: false
        let outcome_brief = run_compute(&mut conn, &["/mnt/drive-a"], &params_brief, &[], &[], None);

        match (outcome_normal, outcome_brief) {
            (SurveyOutcome::Result(normal), SurveyOutcome::Result(brief)) => {
                assert_eq!(normal.location_results.len(), brief.location_results.len());
                assert_eq!(
                    normal.location_results[0].shared_count,
                    brief.location_results[0].shared_count
                );
                assert_eq!(
                    normal.location_results[0].complementary_count,
                    brief.location_results[0].complementary_count
                );
                assert_eq!(normal.unique_count, brief.unique_count);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_other_with_brief() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        // affinity: true + brief: true → compute_affinity: false
        let params = test_params();
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/mnt/backup/trip")];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 1);
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_other_same_root_cross_scope() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root, "documents/a.jpg", Some(obj1));
        insert_source(&conn, root, "documents/c.jpg", Some(obj3));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/mnt/drive/documents")];
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/drive/documents");
                assert_eq!(loc.shared_count, 1);
                assert_eq!(loc.complementary_count, Some(1));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Detail views
    // =========================================================================

    #[test]
    fn test_detail_complement_paths() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);
        let obj5 = insert_object(&conn, "hash_005", false);

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip");
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], "IMG_004.jpg");
                assert_eq!(paths[1], "IMG_005.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_detail_complement_mirror_has_empty_paths() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/mirror", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "backup/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_paths, Some(vec![]));
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_detail_complement_no_affinity_has_none_paths() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_paths, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_unique_paths_populated() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/other", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "backup/b.jpg", Some(obj2));
        insert_source(&conn, archive, "2024/c.jpg", Some(obj3));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 1);
                assert_eq!(result.unique_paths, vec!["/mnt/drive/photos/a.jpg"]);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_unique_paths_empty_when_none_unique() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));

        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 0);
                assert!(result.unique_paths.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_unique_paths_duplicates_within_selection() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/a_copy.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 1);
                assert_eq!(result.unique_paths.len(), 2);
                assert_eq!(result.unique_paths[0], "/mnt/drive/photos/a.jpg");
                assert_eq!(result.unique_paths[1], "/mnt/drive/photos/a_copy.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_complement_paths_relative_to_location() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        insert_source(&conn, root_b, "trip/week1/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/week1/sub/deep.jpg", Some(obj2));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip/week1");
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], "sub/deep.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_complement_other_mode_zero_overlap_has_paths() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        insert_source(&conn, root_b, "docs/x.jpg", Some(obj2));
        insert_source(&conn, root_b, "docs/y.jpg", Some(obj3));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &params, &filters, &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 0);
                assert_eq!(loc.complementary_count, Some(2));
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], "docs/x.jpg");
                assert_eq!(paths[1], "docs/y.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --archive filter tests
    // =========================================================================

    #[test]
    fn test_archive_filter_specific_root() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let archive_a = insert_root(&conn, "/archive/a", "archive", false);
        let archive_b = insert_root(&conn, "/archive/b", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));
        insert_source(&conn, root, "z.jpg", Some(obj3));

        insert_source(&conn, archive_a, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive_a, "2024/y.jpg", Some(obj2));
        insert_source(&conn, archive_b, "backup/z.jpg", Some(obj3));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);
        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 3);
                assert_eq!(result.archive_scopes.len(), 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }

        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], Some(archive_a));
        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 2);
                assert_eq!(result.archive_scopes.len(), 1);
                assert_eq!(result.archive_scopes[0].0, "/archive/a/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_archive_filter_no_matches() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let archive_a = insert_root(&conn, "/archive/a", "archive", false);
        let archive_b = insert_root(&conn, "/archive/b", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));

        insert_source(&conn, archive_b, "backup/x.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], Some(archive_a));

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 0);
                assert!(result.archive_scopes.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_archive_filter_does_not_affect_other_sections() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let archive_a = insert_root(&conn, "/archive/a", "archive", false);
        let archive_b = insert_root(&conn, "/archive/b", "archive", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "x.jpg", Some(obj1));
        insert_source(&conn, root_a, "y.jpg", Some(obj2));
        insert_source(&conn, root_a, "z.jpg", Some(obj3));

        insert_source(&conn, root_b, "copy/x.jpg", Some(obj1));

        insert_source(&conn, archive_a, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive_b, "backup/y.jpg", Some(obj2));

        let params = test_params();
        let outcome_all = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);
        let outcome_filtered =
            run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], Some(archive_a));

        match (outcome_all, outcome_filtered) {
            (SurveyOutcome::Result(all), SurveyOutcome::Result(filtered)) => {
                assert_eq!(all.archived_source_count, 2);
                assert_eq!(filtered.archived_source_count, 1);

                assert_eq!(all.location_results.len(), filtered.location_results.len());
                assert_eq!(
                    all.location_results[0].shared_count,
                    filtered.location_results[0].shared_count,
                );
                assert_eq!(all.unique_count, filtered.unique_count);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_many_locations_all_computed() {
        let mut conn = setup_test_db();

        let root_sel = insert_root(&conn, "/mnt/selection", "source", false);
        let obj_shared = insert_object(&conn, "hash_shared", false);

        insert_source(&conn, root_sel, "a.jpg", Some(obj_shared));

        for i in 0..15 {
            let root = insert_root(&conn, &format!("/mnt/other-{i:02}"), "source", false);
            insert_source(
                &conn,
                root,
                &format!("dir/copy_{i:02}.jpg"),
                Some(obj_shared),
            );
        }

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/selection"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 15);
                for loc in &result.location_results {
                    assert_eq!(loc.shared_count, 1);
                }
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Orientation and affinity gate tests
    // =========================================================================

    #[test]
    fn test_orientation_default_no_filters() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let root_c = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/d.jpg", Some(obj4));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        insert_source(&conn, root_c, "misc/a.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_hashed, 4);
                assert_eq!(result.unique_count, 2);

                assert_eq!(result.location_results.len(), 2);
                for loc in &result.location_results {
                    assert_eq!(loc.complementary_count, None);
                    assert_eq!(loc.only_here_count, None);
                    assert_eq!(loc.kind, None);
                }

                assert!(result.location_results[0].path.contains("backup"));
                assert_eq!(result.location_results[0].shared_count, 2);
                assert!(result.location_results[1].path.contains("other"));
                assert_eq!(result.location_results[1].shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_orientation_with_filters() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.txt", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = test_params();
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.total_hashed, 2);

                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_zero_unique_shown() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));

        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 0);
                assert!(result.unique_paths.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_affinity_brief_noop() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        // affinity: true + brief: true → compute_affinity: false
        let params = test_params();
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_brief_without_affinity_noop() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));
        insert_source(&conn, root_b, "a.jpg", Some(obj1));

        let params_plain = test_params();
        let outcome_plain = run_compute(&mut conn, &["/mnt/drive"], &params_plain, &[], &[], None);

        let params_brief = test_params();
        let outcome_brief = run_compute(&mut conn, &["/mnt/drive"], &params_brief, &[], &[], None);

        match (outcome_plain, outcome_brief) {
            (SurveyOutcome::Result(plain), SurveyOutcome::Result(brief)) => {
                assert_eq!(plain.location_results.len(), brief.location_results.len());
                assert_eq!(
                    plain.location_results[0].shared_count,
                    brief.location_results[0].shared_count
                );
                assert_eq!(plain.unique_count, brief.unique_count);
                assert_eq!(plain.location_results[0].kind, None);
                assert_eq!(brief.location_results[0].kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Subset classification tests
    // =========================================================================

    #[test]
    fn test_subset_classification() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Subset));
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.total_count, 2);
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_subset_vs_mirror() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        let obj_other1 = insert_object(&conn, "hash_other_1", false);
        let obj_other2 = insert_object(&conn, "hash_other_2", false);
        let obj_other3 = insert_object(&conn, "hash_other_3", false);
        let obj_other4 = insert_object(&conn, "hash_other_4", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/x1.txt", Some(obj_other1));
        insert_source(&conn, root_b, "trip/x2.txt", Some(obj_other2));
        insert_source(&conn, root_b, "trip/x3.txt", Some(obj_other3));
        insert_source(&conn, root_b, "trip/x4.txt", Some(obj_other4));

        let params = SurveyParams {
            compute_affinity: true,
            ..test_params()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &filters, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Mirror));
                assert_eq!(loc.shared_count, 1);
                assert_eq!(loc.total_count, 5);
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_total_count_in_summary() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj4));

        insert_source(&conn, root_b, "copy/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "copy/x.jpg", Some(obj2));
        insert_source(&conn, root_b, "copy/y.jpg", Some(obj3));
        insert_source(&conn, root_b, "copy/pending.raw", None);

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.total_count, 3);
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --detail overlap tests
    // =========================================================================

    #[test]
    fn test_overlap_detail_basic() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let root_c = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);
        let obj4 = insert_object(&conn, "hash_004", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/d.jpg", Some(obj4));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        insert_source(&conn, root_c, "misc/b.jpg", Some(obj2));
        insert_source(&conn, root_c, "misc/c.jpg", Some(obj3));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 2);

                let loc_b = result
                    .location_results
                    .iter()
                    .find(|l| l.path.contains("backup"))
                    .unwrap();
                let loc_c = result
                    .location_results
                    .iter()
                    .find(|l| l.path.contains("other"))
                    .unwrap();

                let pairs_b = loc_b.overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs_b.len(), 2);
                assert_eq!(pairs_b[0].selection_path, "/mnt/drive/photos/a.jpg");
                assert_eq!(pairs_b[1].selection_path, "/mnt/drive/photos/b.jpg");

                let pairs_c = loc_c.overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs_c.len(), 2);
                assert_eq!(pairs_c[0].selection_path, "/mnt/drive/photos/b.jpg");
                assert_eq!(pairs_c[1].selection_path, "/mnt/drive/photos/c.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_detail_with_other() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup/trip")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip");
                let pairs = loc.overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].selection_path, "/mnt/drive/photos/a.jpg");
                assert_eq!(pairs[1].selection_path, "/mnt/drive/photos/b.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_detail_null() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let pairs = result.location_results[0].overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].selection_path, "/mnt/drive/photos/a.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_detail_no_overlap() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 0);
                let pairs = loc.overlap_pairs.as_ref().unwrap();
                assert!(pairs.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_multi_location_dedup() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);
        let root_c = insert_root(&conn, "/mnt/other", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_c, "misc/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 2);
                for loc in &result.location_results {
                    let pairs = loc.overlap_pairs.as_ref().unwrap();
                    assert_eq!(pairs.len(), 1);
                    assert_eq!(pairs[0].selection_path, "/mnt/drive/photos/a.jpg");
                }

                let all_paths: std::collections::BTreeSet<&str> = result
                    .location_results
                    .iter()
                    .filter_map(|l| l.overlap_pairs.as_ref())
                    .flat_map(|p| p.iter().map(|s| s.selection_path.as_str()))
                    .collect();
                assert_eq!(all_paths.len(), 1);
                assert!(all_paths.contains("/mnt/drive/photos/a.jpg"));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_single_counterpart() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/photo.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let pairs = result.location_results[0].overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].selection_path, "/mnt/drive/photos/a.jpg");
                assert_eq!(pairs[0].counterpart_paths.len(), 1);
                assert_eq!(pairs[0].counterpart_paths[0], "photo.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_multiple_counterparts() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a 2.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let pairs = result.location_results[0].overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].counterpart_paths.len(), 2);
                assert_eq!(pairs[0].counterpart_paths[0], "a 2.jpg");
                assert_eq!(pairs[0].counterpart_paths[1], "a.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_counterpart_relative_to_location() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/week1/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup/trip")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let pairs = result.location_results[0].overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].counterpart_paths[0], "week1/a.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_overlap_null_delim_no_counterparts() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_overlap_pairs: true,
            ..test_params()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let pairs = result.location_results[0].overlap_pairs.as_ref().unwrap();
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].counterpart_paths.len(), 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --detail residual tests
    // =========================================================================

    #[test]
    fn test_residual_detail_basic() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_residual: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                let paths = loc.residual_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], "/mnt/drive/photos/b.jpg");
                assert_eq!(paths[1], "/mnt/drive/photos/c.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_residual_includes_unhashed() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/unhashed.raw", None);

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_residual: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let paths = result.location_results[0].residual_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], "/mnt/drive/photos/unhashed.raw");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_residual_zero() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));

        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let params = SurveyParams {
            compute_residual: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let paths = result.location_results[0].residual_paths.as_ref().unwrap();
                assert!(paths.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_residual_null() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_residual: true,
            ..test_params()
        };
        let other = vec![PathBuf::from("/mnt/backup")];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let paths = result.location_results[0].residual_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], "/mnt/drive/photos/b.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    #[test]
    fn test_residual_multiple_other() {
        let mut conn = setup_test_db();

        let root_a = insert_root(&conn, "/mnt/drive", "source", false);
        let root_b = insert_root(&conn, "/mnt/backup-1", "source", false);
        let root_c = insert_root(&conn, "/mnt/backup-2", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);
        let obj2 = insert_object(&conn, "hash_002", false);
        let obj3 = insert_object(&conn, "hash_003", false);

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));
        insert_source(&conn, root_a, "c.jpg", Some(obj3));

        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        insert_source(&conn, root_c, "a.jpg", Some(obj1));

        let params = SurveyParams {
            compute_residual: true,
            ..test_params()
        };
        let other = vec![
            PathBuf::from("/mnt/backup-1"),
            PathBuf::from("/mnt/backup-2"),
        ];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &other, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 2);

                let loc_b = &result.location_results[0];
                assert!(loc_b.path.contains("backup-1"));
                let paths_b = loc_b.residual_paths.as_ref().unwrap();
                assert_eq!(paths_b.len(), 1);
                assert_eq!(paths_b[0], "/mnt/drive/c.jpg");

                let loc_c = &result.location_results[1];
                assert!(loc_c.path.contains("backup-2"));
                let paths_c = loc_c.residual_paths.as_ref().unwrap();
                assert_eq!(paths_c.len(), 2);
                assert_eq!(paths_c[0], "/mnt/drive/b.jpg");
                assert_eq!(paths_c[1], "/mnt/drive/c.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Unique detail paths
    // =========================================================================

    #[test]
    fn test_unique_detail_relative_paths() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/mnt/drive", "source", false);

        let obj1 = insert_object(&conn, "hash_001", false);

        insert_source(&conn, root, "photos/sub/unique.jpg", Some(obj1));

        let params = test_params();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &params, &[], &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 1);
                assert_eq!(result.unique_paths[0], "/mnt/drive/photos/sub/unique.jpg");
                let relative =
                    crate::domain::path::format_path(&result.unique_paths[0], Some("/mnt/drive"));
                assert_eq!(relative, "photos/sub/unique.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }
}
