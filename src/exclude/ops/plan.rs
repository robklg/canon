//! Plan functions for exclude: `plan_set`, `plan_clear`, `plan_duplicates`,
//! `plan_set_objects`. Each computes what a command would do — no side
//! effects — returning a typed plan struct with all data the interface needs
//! for dry-run display and confirmation.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::core::domain::include::IncludeSet;
use crate::core::domain::path::path_is_under;
use crate::core::domain::source::Source;
use crate::core::repo::{self, Connection};
use crate::exclude::domain::find_excludable_duplicates;
use crate::expr::apply_filters;
use crate::expr::{select_sources, RolePolicy, SelectionParams};

use super::types::{
    object_source_info, DuplicateGroupData, ExcludeClearParams, ExcludeClearPlan,
    ExcludeDuplicatesParams, ExcludeDuplicatesPlan, ExcludeItemData, ExcludeSetObjectsParams,
    ExcludeSetObjectsPlan, ExcludeSetParams, ExcludeSetPlan, ObjectPlanEntry, ObjectSourceInfo,
};

/// Compute what `exclude set` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters via `select_sources()`,
/// then computes confirmation data (root count, archive coverage).
pub fn plan_set(conn: &mut Connection, params: &ExcludeSetParams) -> Result<ExcludeSetPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet::default(),
        filters: params.filters.clone(),
        // Source roots only: exclusion is triage's letting-go of source-side
        // copies, and a scope selection must never offer the archive itself
        // for dismissal. Explicit single-target set (by id or path) is the
        // deliberate escape hatch past this policy — which is why plan_clear
        // below reaches every role: whatever set can reach, clear can undo.
        // The same policy guards plan_duplicates and plan_set_objects.
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = select_sources(conn, &sel_params)?;

    // select_sources() with default IncludeSet already filters out excluded sources
    let sources = selection.sources;

    if sources.is_empty() {
        return Ok(ExcludeSetPlan {
            items: Vec::new(),
            root_count: 0,
            not_archived_count: 0,
        });
    }

    // Compute archive coverage
    let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
    let archived_set = repo::object::batch_check_archived(conn, &object_ids, None)?;

    let not_archived_count = sources
        .iter()
        .filter(|s| match s.object_id {
            None => true, // unhashed counts as not archived
            Some(oid) => !archived_set.contains(&oid),
        })
        .count();

    let root_ids: HashSet<i64> = sources.iter().map(|s| s.root_id).collect();

    // Resolve hashes for receipt-capable items (one batch fetch).
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
    let items: Vec<ExcludeItemData> = sources
        .iter()
        .map(|s| ExcludeItemData::from_source(s, &objects))
        .collect();

    Ok(ExcludeSetPlan {
        items,
        root_count: root_ids.len(),
        not_archived_count,
    })
}

/// Compute what `exclude clear` would do — no side effects.
///
/// Finds source-level excluded sources (`s.excluded == true`, NOT object-level)
/// matching scope and filters. Uses its own selection logic rather than
/// `select_sources()` — finding sources marked for clearing is a different
/// contract from finding visible sources.
pub fn plan_clear(conn: &mut Connection, params: &ExcludeClearParams) -> Result<ExcludeClearPlan> {
    let roots = repo::root::fetch_all(conn)?;
    // Every active root, archive role included: single-target set accepts an
    // archive-role source, so an exclusion can stand there — and whatever set
    // can reach, clear must be able to undo. A role filter here would strand
    // those exclusions permanently.
    let root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active())
        .map(|r| r.id)
        .collect();

    if root_ids.is_empty() {
        return Ok(ExcludeClearPlan {
            items: Vec::new(),
            root_count: 0,
        });
    }

    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Filter for scope match and source-level exclusion only.
    // Uses s.excluded (source-level flag), NOT s.is_excluded() which includes
    // object-level. clear() must only clear source-level exclusions.
    let filtered: Vec<_> = all_sources
        .into_iter()
        .filter(|s| params.scopes.is_empty() || s.matches_scope(&params.scopes))
        .filter(|s| s.excluded)
        .collect();

    // Apply --where filters if present
    let filtered = if params.filters.is_empty() {
        filtered
    } else {
        let ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
        let filtered_ids: HashSet<i64> = apply_filters(conn, &ids, &params.filters)?
            .source_ids
            .into_iter()
            .collect();
        filtered
            .into_iter()
            .filter(|s| filtered_ids.contains(&s.id))
            .collect()
    };

    let root_ids: HashSet<i64> = filtered.iter().map(|s| s.root_id).collect();

    // Resolve hashes for receipt-capable items (one batch fetch).
    let object_ids: Vec<i64> = filtered.iter().filter_map(|s| s.object_id).collect();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
    let items: Vec<ExcludeItemData> = filtered
        .iter()
        .map(|s| ExcludeItemData::from_source(s, &objects))
        .collect();

    Ok(ExcludeClearPlan {
        items,
        root_count: root_ids.len(),
    })
}

/// Compute what `exclude duplicates` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters, runs duplicate
/// analysis via `find_excludable_duplicates()`, and computes confirmation
/// data (group count, skip statistics).
pub fn plan_duplicates(
    conn: &mut Connection,
    params: &ExcludeDuplicatesParams,
) -> Result<ExcludeDuplicatesPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet::default(),
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = select_sources(conn, &sel_params)?;
    let scope_count = selection.sources.len();

    if selection.sources.is_empty() {
        return Ok(ExcludeDuplicatesPlan {
            groups: Vec::new(),
            group_count: 0,
            prefer_prefix: params.prefer_prefix.clone(),
            scope_count: 0,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        });
    }

    // Build lookup map for source objects
    let source_map: HashMap<i64, &Source> = selection.sources.iter().map(|s| (s.id, s)).collect();

    // Collect unique object_ids for duplicate lookup
    let object_ids: Vec<i64> = selection
        .sources
        .iter()
        .filter_map(|s| s.object_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Fetch all sources that share these objects (potential duplicates)
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids)?;

    // Use pure domain function to determine what to exclude
    let result = find_excludable_duplicates(
        &selection.sources,
        &sources_by_object,
        &params.prefer_prefix,
    );

    // Resolve hashes for receipt-capable items (one batch fetch).
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;

    // Reconstruct duplicate groups: group the excluded sources by object_id
    // (preserving to_exclude order), and pull the kept copy/copies — the
    // non-excluded sources under the prefer prefix — from sources_by_object.
    let mut group_order: Vec<i64> = Vec::new();
    let mut excluded_by_obj: HashMap<i64, Vec<ExcludeItemData>> = HashMap::new();
    for &id in &result.to_exclude {
        let Some(source) = source_map.get(&id) else {
            continue;
        };
        let Some(oid) = source.object_id else {
            continue;
        };
        if !excluded_by_obj.contains_key(&oid) {
            group_order.push(oid);
        }
        excluded_by_obj
            .entry(oid)
            .or_default()
            .push(ExcludeItemData::from_source(source, &objects));
    }

    let mut groups = Vec::with_capacity(group_order.len());
    for oid in group_order {
        let excluded = excluded_by_obj.remove(&oid).unwrap_or_default();
        let hash = objects
            .get(&oid)
            .map(|o| format!("{}:{}", o.hash_type, o.hash_value))
            .unwrap_or_default();
        // The kept copies are re-derived here with the same test the duplicate
        // rule uses to count them. If that rule changes, this must change with
        // it — the receipt states which copies survived, and a stale rule here
        // misstates it.
        let mut kept: Vec<ExcludeItemData> = sources_by_object
            .get(&oid)
            .map(|ss| {
                ss.iter()
                    .filter(|s| !s.is_excluded() && path_is_under(&s.path(), &params.prefer_prefix))
                    .map(|s| ExcludeItemData::from_source(s, &objects))
                    .collect()
            })
            .unwrap_or_default();
        // Deterministic order for a diff-able receipt (DB row order is not stable).
        kept.sort_by(|a, b| {
            a.root
                .cmp(&b.root)
                .then_with(|| a.rel_path.cmp(&b.rel_path))
        });
        groups.push(DuplicateGroupData {
            hash,
            kept,
            excluded,
        });
    }

    let group_count = groups.len();

    Ok(ExcludeDuplicatesPlan {
        groups,
        group_count,
        prefer_prefix: params.prefer_prefix.clone(),
        scope_count,
        skipped_no_hash: result.skipped_no_hash,
        skipped_in_prefer: result.skipped_in_prefer,
        skipped_not_covered: result.skipped_not_covered,
        skipped_multiple: result.skipped_multiple,
    })
}

/// Compute what `exclude set --objects` would do — no side effects.
///
/// Selects sources matching scope and filters (including already-excluded
/// sources), collects their objects, filters out unhashed/empty/already-excluded,
/// and computes display data per object.
pub fn plan_set_objects(
    conn: &mut Connection,
    params: &ExcludeSetObjectsParams,
) -> Result<ExcludeSetObjectsPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet {
            excluded: true,
            archived: false,
        },
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = select_sources(conn, &sel_params)?;

    if selection.sources.is_empty() {
        return Ok(ExcludeSetObjectsPlan {
            objects: vec![],
            total_source_count: 0,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        });
    }

    // Collect unique object_ids from selected sources, counting skips
    let mut seen_objects: HashSet<i64> = HashSet::new();
    let mut object_ids_to_check: Vec<i64> = Vec::new();
    let mut skipped_no_hash = 0;
    let mut skipped_empty = 0;

    for source in &selection.sources {
        let Some(object_id) = source.object_id else {
            skipped_no_hash += 1;
            continue;
        };
        // The contentless law: every empty file shares the one empty-content
        // object, so an identity-keyed exclusion here would dismiss every
        // empty file in the universe — set aside, counted, never silent.
        // Checked before the object dedupe: the count is reported as files
        // ("N empty files skipped"), so every set-aside source must count.
        if source.is_contentless() {
            skipped_empty += 1;
            continue;
        }
        if !seen_objects.insert(object_id) {
            continue;
        }
        object_ids_to_check.push(object_id);
    }

    if object_ids_to_check.is_empty() {
        return Ok(ExcludeSetObjectsPlan {
            objects: vec![],
            total_source_count: 0,
            total_archive_count: 0,
            skipped_no_hash,
            skipped_empty,
            skipped_already_excluded: 0,
        });
    }

    // Batch fetch objects to check exclusion status
    let objects_map = repo::object::batch_fetch_by_ids(conn, &object_ids_to_check)?;

    // Batch fetch all sources per object for display
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids_to_check)?;

    // Build plan entries, filtering out already-excluded objects
    let mut objects: Vec<ObjectPlanEntry> = Vec::new();
    let mut total_source_count = 0;
    let mut total_archive_count = 0;
    let mut skipped_already_excluded = 0;

    for &object_id in &object_ids_to_check {
        let Some(object) = objects_map.get(&object_id) else {
            continue;
        };

        if object.is_excluded() {
            skipped_already_excluded += 1;
            continue;
        }

        let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();
        let hash = format!("{}:{}", object.hash_type, object.hash_value);

        // Get and sort sources for this object
        let mut obj_sources: Vec<_> = sources_by_object
            .get(&object_id)
            .cloned()
            .unwrap_or_default();

        // Sort: role DESC (source before archive), root_path, rel_path
        obj_sources.sort_by(|a, b| {
            b.root_role
                .cmp(&a.root_role)
                .then_with(|| a.root_path.cmp(&b.root_path))
                .then_with(|| a.rel_path.cmp(&b.rel_path))
        });

        let sources: Vec<ObjectSourceInfo> = obj_sources.iter().map(object_source_info).collect();

        let archive_count = sources.iter().filter(|s| s.is_archive).count();
        total_archive_count += archive_count;
        total_source_count += sources.len();

        objects.push(ObjectPlanEntry {
            object_id,
            hash_prefix,
            hash,
            sources,
        });
    }

    Ok(ExcludeSetObjectsPlan {
        objects,
        total_source_count,
        total_archive_count,
        skipped_no_hash,
        skipped_empty,
        skipped_already_excluded,
    })
}
