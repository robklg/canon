//! The pure computation half of survey: archive status, overlap detection,
//! scope discovery, location classification, and detail views (complement,
//! unique, overlap, residual). Uses custom selection logic (not
//! `select_sources()`) to support the asymmetric visibility model.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

use crate::core::domain;
use crate::core::domain::scope::ScopeMatch;
use crate::core::domain::source::Source;
use crate::core::domain::IncludeSet;
use crate::expr::filter::{self, Filter};
use crate::ops::scope::classify_all;
use crate::survey::domain::analysis::{
    classify_location, count_only_here, discover_scopes_by_root, find_unique_object_ids,
    LocationKind,
};
use crate::survey::domain::object_index::{ArchivePresence, ObjectIndex};

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
    /// Whether to compute archived file pairs (selection path → archive counterpart).
    pub compute_archived_pairs: bool,
}

/// Outcome of compute_survey: either a result to display or an early exit.
pub enum SurveyOutcome {
    /// Normal result with all computed data.
    Result(SurveyResult),
    /// Empty selection — display header and stop.
    Empty,
    /// All unhashed — display header and hashing guidance.
    AllUnhashed { total_count: usize },
}

pub struct SurveyResult {
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
    /// Set by `run_survey` (compute_survey always returns None — it has no
    /// access to root paths, only the pre-resolved `archive_root_id`).
    pub archive_label: Option<String>,
    /// File-level archive detail: selection sources grouped by archive location
    /// with counterpart paths. Only populated when compute_archived_pairs is true.
    pub archived_details: Vec<ArchivedLocationDetail>,
    /// Which status predicates appeared in filter expressions.
    pub used_status: filter::UsedStatus,
    /// Count of excluded sources hidden from selection (for visibility hints).
    pub excluded_count: usize,
    /// Selection-side empty files, set aside from every comparison (the
    /// contentless law: the index refuses them, so they can create no
    /// overlap, coverage, or uniqueness) — counted here so the summary can
    /// state them, never silent.
    pub contentless_count: usize,
}

/// A selection-side path paired with its counterpart paths at a location.
pub struct OverlapPair {
    /// Path of the selection-side source (absolute).
    pub selection_path: String,
    /// Paths at the other location with matching content (relative to location).
    pub counterpart_paths: Vec<String>,
}

/// An archive location with its paired selection/archive files.
pub struct ArchivedLocationDetail {
    /// Archive scope path (absolute).
    pub path: String,
    /// Selection-side paths paired with archive-side counterparts.
    pub pairs: Vec<OverlapPair>,
}

pub struct LocationResult {
    pub path: String,
    pub shared_count: usize,
    /// Total hashed sources at this location (always computed).
    pub total_count: usize,
    pub complementary_count: Option<usize>,
    pub only_here_count: Option<usize>,
    pub kind: Option<LocationKind>,
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
    scope_prefixes: &[String],
    filters: &[Filter],
    params: &SurveyParams,
    all_sources: &[Source],
    other_paths: &[String],
    archive_root_id: Option<i64>,
) -> Result<SurveyOutcome> {
    let scopes = classify_all(scope_prefixes);

    // Build selection from domain predicates (asymmetric visibility model)
    let selection: Vec<&Source> = all_sources
        .iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| params.include.includes_excluded() || !s.is_excluded())
        .collect();

    // Count excluded sources hidden from selection (for visibility hints)
    let excluded_count = if !params.include.includes_excluded() {
        all_sources
            .iter()
            .filter(|s| s.is_active())
            .filter(|s| s.is_from_role("source"))
            .filter(|s| s.matches_scope(&scopes))
            .filter(|s| s.is_excluded())
            .count()
    } else {
        0
    };

    // Apply --where filters to selection
    let (selection, used_status) = if filters.is_empty() {
        (selection, filter::UsedStatus::default())
    } else {
        let ids: Vec<i64> = selection.iter().map(|s| s.id).collect();
        let filter_result = filter::apply_filters(conn, &ids, filters)?;
        let passed: HashSet<i64> = filter_result.source_ids.into_iter().collect();
        let filtered = selection
            .into_iter()
            .filter(|s| passed.contains(&s.id))
            .collect();
        (filtered, filter_result.used_status)
    };

    // Partition: unhashed vs hashed
    let total_count = selection.len();
    let contentless_count = selection.iter().filter(|s| s.is_contentless()).count();
    let hashed: Vec<&Source> = selection
        .iter()
        .filter(|s| s.object_id.is_some())
        .copied()
        .collect();
    let unhashed_count = total_count - hashed.len();
    let total_hashed = hashed.len();

    // Early exit: empty selection
    if total_count == 0 {
        return Ok(SurveyOutcome::Empty);
    }

    // Early exit: all unhashed
    if total_hashed == 0 {
        return Ok(SurveyOutcome::AllUnhashed { total_count });
    }

    // Collect selection identity. Contentless sources contribute none (the
    // law: identity claims about empty content are vacuous) — the one
    // empty-content object is absent from the index, so left in this set it
    // would read as vacuously unique; and in --other mode it would read as
    // shared with any location holding an empty file. Counted in
    // contentless_count, never compared.
    let sel_object_ids: HashSet<i64> = hashed
        .iter()
        .filter(|s| !s.is_contentless())
        .filter_map(|s| s.object_id)
        .collect();
    let sel_source_ids: HashSet<i64> = selection.iter().map(|s| s.id).collect();

    // Build object index from ALL active, non-excluded sources.
    // The outward side never widens with --include excluded: what a survey
    // compares against is the visible world, so an excluded copy elsewhere is
    // not evidence that selection content exists elsewhere.
    let index = ObjectIndex::build(
        all_sources
            .iter()
            .filter(|s| s.is_active() && !s.is_excluded()),
    );

    // Archive status: find selection content that exists on archive roots
    // (--archive restricts to that archive root)
    let ArchivePresence {
        archived_object_ids,
        archive_sources,
    } = index.archive_presence(&sel_object_ids, archive_root_id);

    // Count selection sources that are archived (source-based counting)
    let archived_source_count = hashed
        .iter()
        .filter(|s| archived_object_ids.contains(&s.object_id.unwrap()))
        .count();

    // Scope discovery on archive sources for grouped display
    let mut archive_scopes = discover_scopes_by_root(&archive_sources);
    archive_scopes.sort_by(|a, b| a.0.cmp(&b.0));

    // Overlap: find selection content that exists on other source roots
    let mut overlap_sources: Vec<&Source> = Vec::new();
    for &oid in &sel_object_ids {
        for sib in index.locations_of(oid) {
            if !sel_source_ids.contains(&sib.id) && sib.is_from_role("source") {
                overlap_sources.push(sib);
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
        discover_scopes_by_root(&overlap_sources)
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
                // Direct read past the index — re-apply its contentless
                // refusal, or an empty file here reads as shared content.
                .filter(|s| !s.is_contentless())
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
        // all roles in --other mode. Deliberately unfiltered by --where: "is
        // this location a subset of my selection?" must be measured against
        // everything that stands there, not just the content that matched the
        // filter. Contentless sources are out (coverage's own denominator
        // precedent): a subset ratio is an identity claim, and empty files
        // can neither be shared nor make a location less of a subset.
        let total_count: usize = all_sources
            .iter()
            .filter(|s| s.is_active())
            .filter(|s| !s.is_excluded())
            .filter(|s| !s.is_contentless())
            .filter(|s| s.object_id.is_some())
            .filter(|s| s.matches_scope(&loc_scope))
            .filter(|s| is_other_mode || s.is_from_role("source"))
            .count();

        // Complementary content and classification (only with affinity enabled)
        let (complementary_count, only_here_count, kind, complementary_paths) = if params
            .compute_affinity
        {
            // Step 1: Get ALL sources within this location
            // Active, non-excluded, not in selection. Direct read past the
            // index — re-apply its contentless refusal, or an empty file
            // here reads as complementary content (and, absent from the
            // index, as vacuously "only here").
            let loc_sources: Vec<&Source> = all_sources
                .iter()
                .filter(|s| s.is_active())
                .filter(|s| !s.is_excluded())
                .filter(|s| !s.is_contentless())
                .filter(|s| s.matches_scope(&loc_scope))
                .filter(|s| !sel_source_ids.contains(&s.id))
                .collect();

            // Step 2: Apply --where filters to location sources
            let loc_ids: Vec<i64> = loc_sources.iter().map(|s| s.id).collect();
            let passed: HashSet<i64> = filter::apply_filters(conn, &loc_ids, filters)?
                .source_ids
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
                    domain::path::path_strip_prefix(&s.path(), scope_path).map(|p| p.to_string())
                })
                .collect();
            comp_paths.sort_unstable();

            // Step 5: "Only here" — unique object_ids among complementary
            let comp_oids: HashSet<i64> =
                complementary.iter().filter_map(|s| s.object_id).collect();
            let only_here = count_only_here(&comp_oids, scope_path, index.as_map());

            // Step 6: Classify
            let kind = classify_location(
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
                    let mut counterpart_paths: Vec<String> = index
                        .locations_of(oid)
                        .iter()
                        .filter(|cs| cs.matches_scope(&loc_scope))
                        .filter(|cs| !sel_source_ids.contains(&cs.id))
                        .map(|cs| {
                            domain::path::path_strip_prefix(&cs.path(), scope_path)
                                .map(|p| p.to_string())
                                .unwrap_or_else(|| cs.path())
                        })
                        .collect();
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
    let unique_oids = find_unique_object_ids(&sel_object_ids, &sel_source_ids, index.as_map());
    let unique_count = unique_oids.len();
    let mut unique_paths: Vec<String> = hashed
        .iter()
        .filter(|s| unique_oids.contains(&s.object_id.unwrap()))
        .map(|s| s.path())
        .collect();
    unique_paths.sort_unstable();

    // Archived detail: pair selection sources with archive counterparts by object_id
    let archived_details = if params.compute_archived_pairs && !archive_sources.is_empty() {
        // Build archive source index: object_id -> Vec<archive source path>
        let mut archive_by_oid: HashMap<i64, Vec<String>> = HashMap::new();
        for s in &archive_sources {
            if let Some(oid) = s.object_id {
                archive_by_oid.entry(oid).or_default().push(s.path());
            }
        }

        // Group archive sources by their scope path for display grouping
        // Reuse the already-computed archive_scopes for the location paths
        let mut details: Vec<ArchivedLocationDetail> = Vec::new();

        // For each archive scope, find the selection sources whose content is archived there
        for (scope_path, _count) in &archive_scopes {
            let mut pairs: Vec<OverlapPair> = Vec::new();

            // Find archive sources under this scope
            let scope_archive_oids: HashSet<i64> = archive_sources
                .iter()
                .filter(|s| domain::path::path_is_under(&s.path(), scope_path))
                .filter_map(|s| s.object_id)
                .collect();

            // Find selection sources matching these object_ids
            for sel_source in &hashed {
                if let Some(oid) = sel_source.object_id {
                    if scope_archive_oids.contains(&oid) {
                        // Find counterpart paths at this archive scope
                        let counterparts: Vec<String> = archive_sources
                            .iter()
                            .filter(|s| {
                                s.object_id == Some(oid)
                                    && domain::path::path_is_under(&s.path(), scope_path)
                            })
                            .map(|s| {
                                domain::path::path_strip_prefix(&s.path(), scope_path)
                                    .unwrap_or("")
                                    .to_string()
                            })
                            .collect();

                        if !counterparts.is_empty() {
                            pairs.push(OverlapPair {
                                selection_path: sel_source.path(),
                                counterpart_paths: counterparts,
                            });
                        }
                    }
                }
            }

            pairs.sort_by(|a, b| a.selection_path.cmp(&b.selection_path));

            if !pairs.is_empty() {
                details.push(ArchivedLocationDetail {
                    path: scope_path.clone(),
                    pairs,
                });
            }
        }

        // Sort by file count descending
        details.sort_by(|a, b| b.pairs.len().cmp(&a.pairs.len()));
        details
    } else {
        Vec::new()
    };

    Ok(SurveyOutcome::Result(SurveyResult {
        total_count,
        unhashed_count,
        total_hashed,
        archived_source_count,
        archive_scopes,
        location_results,
        unique_count,
        unique_paths,
        is_other_mode,
        archive_label: None, // set by run_survey() after return
        archived_details,
        used_status,
        excluded_count,
        contentless_count,
    }))
}
