//! Exclude operations — plan/execute for exclusion management.
//!
//! Provides plan/execute functions for `exclude set`, `exclude clear`,
//! `exclude duplicates`, and `exclude set --objects`. Plan functions compute
//! what would happen (no side effects), returning typed plan structs with all
//! data needed for display and confirmation. Execute functions perform the writes.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::domain::exclusion::find_excludable_duplicates;
use crate::domain::include::IncludeSet;
use crate::domain::scope::ScopeMatch;
use crate::expr::filter::{self, Filter};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

/// Parameters for planning a source exclusion set operation.
pub struct ExcludeSetParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding sources. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetPlan {
    /// Source IDs to exclude.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct root count across sources to exclude.
    pub root_count: usize,
    /// Sources with no archived copy (unhashed or not in any archive root).
    pub not_archived_count: usize,
}

/// Parameters for planning a source exclusion clear operation.
pub struct ExcludeClearParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for clearing source-level exclusions.
pub struct ExcludeClearPlan {
    /// Source IDs to clear exclusion from.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct root count across sources to clear.
    pub root_count: usize,
}

/// Parameters for planning a duplicate exclusion operation.
pub struct ExcludeDuplicatesParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
    pub prefer_prefix: String,
}

/// Computed plan for excluding duplicate sources. Contains all data the
/// interface needs for dry-run display and confirmation — no further
/// queries needed.
pub struct ExcludeDuplicatesPlan {
    /// Source IDs to exclude.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct object groups being excluded (count of unique object_ids).
    pub group_count: usize,
    /// The prefer path used for duplicate resolution.
    pub prefer_prefix: String,
    /// Total sources in scope (before duplicate analysis).
    pub scope_count: usize,
    /// Sources skipped because they have no object_id (unhashed).
    pub skipped_no_hash: usize,
    /// Sources skipped because they're already in the prefer path.
    pub skipped_in_prefer: usize,
    /// Sources skipped because no copy exists in prefer path.
    pub skipped_not_covered: usize,
    /// Sources skipped because multiple copies exist in prefer path.
    pub skipped_multiple: usize,
}

/// Parameters for planning an object exclusion operation.
pub struct ExcludeSetObjectsParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding objects. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetObjectsPlan {
    /// Objects to exclude, with display data.
    pub objects: Vec<ObjectPlanEntry>,
    /// Total source count across all objects.
    pub total_source_count: usize,
    /// Total archive source count across all objects.
    pub total_archive_count: usize,
    /// Sources skipped because they have no hash.
    pub skipped_no_hash: usize,
    /// Empty files skipped (size = 0).
    pub skipped_empty: usize,
    /// Objects already excluded.
    pub skipped_already_excluded: usize,
}

/// A single object entry in the exclusion plan.
pub struct ObjectPlanEntry {
    pub object_id: i64,
    /// Hash prefix for display (first 16 chars).
    pub hash_prefix: String,
    /// Sources linked to this object (sorted: role DESC, root_path, rel_path).
    pub sources: Vec<ObjectSourceInfo>,
}

/// Source info for object exclusion display.
pub struct ObjectSourceInfo {
    pub path: String,
    pub is_archive: bool,
}

// ============================================================================
// Plan functions
// ============================================================================

/// Compute what `exclude set` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters via `select_sources()`,
/// then computes confirmation data (root count, archive coverage).
pub fn plan_set(conn: &mut Connection, params: &ExcludeSetParams) -> Result<ExcludeSetPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet::default(),
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = selection::select_sources(conn, &sel_params)?;

    // select_sources() with default IncludeSet already filters out excluded sources
    let sources = selection.sources;

    if sources.is_empty() {
        return Ok(ExcludeSetPlan {
            source_ids: Vec::new(),
            paths: Vec::new(),
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
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();
    let paths: Vec<String> = sources.iter().map(|s| s.path()).collect();

    Ok(ExcludeSetPlan {
        source_ids,
        paths,
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
    let source_root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    if source_root_ids.is_empty() {
        return Ok(ExcludeClearPlan {
            source_ids: Vec::new(),
            paths: Vec::new(),
            root_count: 0,
        });
    }

    let all_sources = repo::source::batch_fetch_by_roots(conn, &source_root_ids)?;

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
        let filtered_ids: HashSet<i64> = filter::apply_filters(conn, &ids, &params.filters)?
            .into_iter()
            .collect();
        filtered
            .into_iter()
            .filter(|s| filtered_ids.contains(&s.id))
            .collect()
    };

    let root_ids: HashSet<i64> = filtered.iter().map(|s| s.root_id).collect();
    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let paths: Vec<String> = filtered.iter().map(|s| s.path()).collect();

    Ok(ExcludeClearPlan {
        source_ids,
        paths,
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
    let selection = selection::select_sources(conn, &sel_params)?;
    let scope_count = selection.sources.len();

    if selection.sources.is_empty() {
        return Ok(ExcludeDuplicatesPlan {
            source_ids: Vec::new(),
            paths: Vec::new(),
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
    let source_map: HashMap<i64, &_> = selection.sources.iter().map(|s| (s.id, s)).collect();

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
    let result =
        find_excludable_duplicates(&selection.sources, &sources_by_object, &params.prefer_prefix);

    // Build plan from domain result
    let mut source_ids = Vec::new();
    let mut paths = Vec::new();
    for &id in &result.to_exclude {
        if let Some(source) = source_map.get(&id) {
            source_ids.push(id);
            paths.push(source.path());
        }
    }

    // Compute group_count: distinct object_ids among sources to exclude
    let group_count = source_ids
        .iter()
        .filter_map(|id| source_map.get(id).and_then(|s| s.object_id))
        .collect::<HashSet<_>>()
        .len();

    Ok(ExcludeDuplicatesPlan {
        source_ids,
        paths,
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
    let selection = selection::select_sources(conn, &sel_params)?;

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
        if !seen_objects.insert(object_id) {
            continue;
        }
        // Empty files all share the same hash — skip to prevent excluding all empty files
        if source.size == 0 {
            skipped_empty += 1;
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
    let sources_by_object =
        repo::source::fetch_sources_by_object_ids(conn, &object_ids_to_check)?;

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

        let hash_prefix =
            object.hash_value[..16.min(object.hash_value.len())].to_string();

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

        let sources: Vec<ObjectSourceInfo> = obj_sources
            .iter()
            .map(|s| ObjectSourceInfo {
                path: s.path(),
                is_archive: s.is_from_role("archive"),
            })
            .collect();

        let archive_count = sources.iter().filter(|s| s.is_archive).count();
        total_archive_count += archive_count;
        total_source_count += sources.len();

        objects.push(ObjectPlanEntry {
            object_id,
            hash_prefix,
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

// ============================================================================
// Execute functions
// ============================================================================

/// Execute an exclude-set plan — marks sources as excluded.
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, true)?;
    }
    Ok(plan.source_ids.len())
}

/// Execute an exclude-clear plan — clears source-level exclusion.
pub fn execute_clear(conn: &Connection, plan: &ExcludeClearPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, false)?;
    }
    Ok(plan.source_ids.len())
}

/// Execute a duplicate exclusion plan — marks sources as excluded.
pub fn execute_duplicates(conn: &Connection, plan: &ExcludeDuplicatesPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, true)?;
    }
    Ok(plan.source_ids.len())
}

/// Execute an object exclusion plan — marks objects as excluded.
pub fn execute_set_objects(conn: &Connection, plan: &ExcludeSetObjectsPlan) -> Result<usize> {
    for entry in &plan.objects {
        repo::object::set_excluded(conn, entry.object_id, true)?;
    }
    Ok(plan.objects.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &Connection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
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
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id],
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
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0, 1)",
            rusqlite::params![root_id, rel_path, object_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn is_source_excluded(conn: &Connection, source_id: i64) -> bool {
        conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            [source_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v == 1)
        .unwrap_or(false)
    }

    fn make_set_params(scopes: Vec<ScopeMatch>) -> ExcludeSetParams {
        ExcludeSetParams {
            scopes,
            filters: vec![],
        }
    }

    fn make_clear_params(scopes: Vec<ScopeMatch>) -> ExcludeClearParams {
        ExcludeClearParams {
            scopes,
            filters: vec![],
        }
    }

    // =========================================================================
    // plan_set() tests
    // =========================================================================

    #[test]
    fn test_plan_set_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.root_count, 0);
        assert_eq!(plan.not_archived_count, 0);
    }

    #[test]
    fn test_plan_set_excludes_already_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let _id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.source_ids, vec![id1]);
    }

    #[test]
    fn test_plan_set_skips_object_level_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_obj = insert_object(&conn, "abc123", true);
        let _id = insert_source(&conn, root, "a.jpg", Some(excluded_obj));

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        // Object-level excluded sources are filtered out by select_sources()
        assert!(plan.source_ids.is_empty());
    }

    #[test]
    fn test_plan_set_counts_roots() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/root1", "source", false);
        let root2 = insert_root(&conn, "/root2", "source", false);
        insert_source(&conn, root1, "a.jpg", None);
        insert_source(&conn, root2, "b.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.root_count, 2);
    }

    #[test]
    fn test_plan_set_archive_coverage() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // Object that IS archived
        let archived_obj = insert_object(&conn, "archived_hash", false);
        insert_source(&conn, archive_root, "copy.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "file1.jpg", Some(archived_obj));

        // Object that is NOT archived
        let unarchived_obj = insert_object(&conn, "unarchived_hash", false);
        insert_source(&conn, source_root, "file2.jpg", Some(unarchived_obj));

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.source_ids.len(), 2);
        assert_eq!(plan.not_archived_count, 1, "Only the unarchived source");
    }

    #[test]
    fn test_plan_set_unhashed_not_archived() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        insert_source(&conn, root, "unhashed.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.not_archived_count, 1, "Unhashed counts as not archived");
    }

    #[test]
    fn test_plan_set_includes_paths() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "subdir/a.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.paths, vec!["/photos/subdir/a.jpg"]);
    }

    #[test]
    fn test_plan_set_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope = insert_source(&conn, root, "2024/a.jpg", None);
        let _out_scope = insert_source(&conn, root, "2023/b.jpg", None);

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_set(&mut conn, &make_set_params(scopes)).unwrap();

        assert_eq!(plan.source_ids, vec![in_scope]);
    }

    // =========================================================================
    // plan_clear() tests
    // =========================================================================

    #[test]
    fn test_plan_clear_returns_source_level_only() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_id = insert_source_excluded(&conn, root, "excluded.jpg", None);
        let _normal_id = insert_source(&conn, root, "normal.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.source_ids, vec![excluded_id]);
    }

    #[test]
    fn test_plan_clear_ignores_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);

        // Source NOT excluded, but object IS excluded
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj));

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(
            plan.source_ids.is_empty(),
            "Object-level excluded sources should NOT appear"
        );
    }

    #[test]
    fn test_plan_clear_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope = insert_source_excluded(&conn, root, "2024/excluded.jpg", None);
        let _out_scope = insert_source_excluded(&conn, root, "2023/excluded.jpg", None);

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_clear(&mut conn, &make_clear_params(scopes)).unwrap();

        assert_eq!(plan.source_ids, vec![in_scope]);
    }

    #[test]
    fn test_plan_clear_returns_paths() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source_excluded(&conn, root, "subdir/excluded.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.paths, vec!["/photos/subdir/excluded.jpg"]);
    }

    #[test]
    fn test_plan_clear_counts_roots() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/root1", "source", false);
        let root2 = insert_root(&conn, "/root2", "source", false);
        insert_source_excluded(&conn, root1, "a.jpg", None);
        insert_source_excluded(&conn, root2, "b.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.root_count, 2);
    }

    #[test]
    fn test_plan_clear_empty_when_none_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "normal.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.root_count, 0);
    }

    #[test]
    fn test_plan_clear_ignores_suspended_roots() {
        let mut conn = setup_test_db();
        let _suspended = insert_root(&conn, "/suspended", "source", true);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, 'excluded.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 1)",
            rusqlite::params![_suspended],
        )
        .unwrap();

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids.is_empty());
    }

    #[test]
    fn test_plan_clear_ignores_archive_roots() {
        let mut conn = setup_test_db();
        let archive = insert_root(&conn, "/archive", "archive", false);
        insert_source_excluded(&conn, archive, "archived.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids.is_empty());
    }

    // =========================================================================
    // execute tests
    // =========================================================================

    #[test]
    fn test_execute_set_marks_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "b.jpg", None);

        let plan = ExcludeSetPlan {
            source_ids: vec![id1, id2],
            paths: vec!["/photos/a.jpg".to_string(), "/photos/b.jpg".to_string()],
            root_count: 1,
            not_archived_count: 2,
        };

        execute_set(&conn, &plan).unwrap();

        assert!(is_source_excluded(&conn, id1));
        assert!(is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_clear_clears_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
        let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = ExcludeClearPlan {
            source_ids: vec![id1, id2],
            paths: vec!["/photos/a.jpg".to_string(), "/photos/b.jpg".to_string()],
            root_count: 1,
        };

        execute_clear(&conn, &plan).unwrap();

        assert!(!is_source_excluded(&conn, id1));
        assert!(!is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_set_returns_count() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);

        let plan = ExcludeSetPlan {
            source_ids: vec![id1],
            paths: vec!["/photos/a.jpg".to_string()],
            root_count: 1,
            not_archived_count: 1,
        };

        let count = execute_set(&conn, &plan).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_execute_clear_returns_count() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
        let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = ExcludeClearPlan {
            source_ids: vec![id1, id2],
            paths: vec!["/photos/a.jpg".to_string(), "/photos/b.jpg".to_string()],
            root_count: 1,
        };

        let count = execute_clear(&conn, &plan).unwrap();
        assert_eq!(count, 2);
    }

    // =========================================================================
    // plan_duplicates() tests
    // =========================================================================

    fn make_duplicates_params(
        scopes: Vec<ScopeMatch>,
        prefer_prefix: &str,
    ) -> ExcludeDuplicatesParams {
        ExcludeDuplicatesParams {
            scopes,
            filters: vec![],
            prefer_prefix: prefer_prefix.to_string(),
        }
    }

    #[test]
    fn test_plan_duplicates_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/source", "source", false);

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.scope_count, 0);
        assert_eq!(plan.group_count, 0);
    }

    #[test]
    fn test_plan_duplicates_excludes_with_prefer_copy() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "same_hash", false);
        let source_id = insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.source_ids, vec![source_id]);
        assert_eq!(plan.scope_count, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_no_copy() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let _archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "unique_hash", false);
        insert_source(&conn, source_root, "unique.jpg", Some(obj));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.skipped_not_covered, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_multiple_copies() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "multi_hash", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "copy1.jpg", Some(obj));
        insert_source(&conn, archive_root, "copy2.jpg", Some(obj));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.skipped_multiple, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_unhashed() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);

        insert_source(&conn, source_root, "unhashed.jpg", None);

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.skipped_no_hash, 1);
        assert_eq!(plan.scope_count, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_in_prefer() {
        let mut conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "source", false);

        let obj = insert_object(&conn, "prefer_hash", false);
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        // Source is in the prefer path itself — should be skipped
        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids.is_empty());
        assert_eq!(plan.skipped_in_prefer, 1);
    }

    #[test]
    fn test_plan_duplicates_computes_group_count() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "group1_hash", false);
        let obj2 = insert_object(&conn, "group2_hash", false);

        // 2 sources for obj1, 2 sources for obj2
        insert_source(&conn, source_root, "a/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "b/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "a/photo2.jpg", Some(obj2));
        insert_source(&conn, source_root, "b/photo2.jpg", Some(obj2));

        // 1 copy each in archive
        insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.source_ids.len(), 4);
        assert_eq!(plan.group_count, 2, "2 distinct object groups");
    }

    #[test]
    fn test_plan_duplicates_includes_paths() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "path_hash", false);
        insert_source(&conn, source_root, "subdir/photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.paths, vec!["/source/subdir/photo.jpg"]);
    }

    #[test]
    fn test_plan_duplicates_scope_count() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "scope_hash", false);
        insert_source(&conn, source_root, "a.jpg", Some(obj));
        insert_source(&conn, source_root, "b.jpg", None); // unhashed
        insert_source(&conn, archive_root, "a.jpg", Some(obj));

        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.scope_count, 2, "Both sources in scope (before analysis)");
        assert_eq!(plan.source_ids.len(), 1, "Only hashed with prefer copy excluded");
    }

    #[test]
    fn test_plan_duplicates_respects_scope() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "in_scope_hash", false);
        let obj2 = insert_object(&conn, "out_scope_hash", false);

        let in_scope = insert_source(&conn, source_root, "2024/photo.jpg", Some(obj1));
        insert_source(&conn, source_root, "2023/photo.jpg", Some(obj2));
        insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

        let scopes = ScopeMatch::classify_all(&["/source/2024".to_string()]);
        let plan =
            plan_duplicates(&mut conn, &make_duplicates_params(scopes, "/archive")).unwrap();

        assert_eq!(plan.source_ids, vec![in_scope]);
        assert_eq!(plan.scope_count, 1);
    }

    // =========================================================================
    // execute_duplicates() tests
    // =========================================================================

    #[test]
    fn test_execute_duplicates_marks_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "b.jpg", None);

        let plan = ExcludeDuplicatesPlan {
            source_ids: vec![id1, id2],
            paths: vec!["/source/a.jpg".to_string(), "/source/b.jpg".to_string()],
            group_count: 1,
            prefer_prefix: "/archive".to_string(),
            scope_count: 2,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        };

        execute_duplicates(&conn, &plan).unwrap();

        assert!(is_source_excluded(&conn, id1));
        assert!(is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_duplicates_returns_count() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);

        let plan = ExcludeDuplicatesPlan {
            source_ids: vec![id1],
            paths: vec!["/source/a.jpg".to_string()],
            group_count: 1,
            prefer_prefix: "/archive".to_string(),
            scope_count: 1,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        };

        let count = execute_duplicates(&conn, &plan).unwrap();
        assert_eq!(count, 1);
    }

    // =========================================================================
    // plan_set_objects() tests
    // =========================================================================

    fn insert_source_with_size(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        size: i64,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, ?, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id, size],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn make_set_objects_params(scopes: Vec<ScopeMatch>) -> ExcludeSetObjectsParams {
        ExcludeSetObjectsParams {
            scopes,
            filters: vec![],
        }
    }

    fn is_object_excluded(conn: &Connection, object_id: i64) -> bool {
        conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            [object_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v == 1)
        .unwrap_or(false)
    }

    #[test]
    fn test_plan_set_objects_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.total_source_count, 0);
        assert_eq!(plan.total_archive_count, 0);
    }

    #[test]
    fn test_plan_set_objects_includes_non_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash_value_x", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.objects[0].object_id, obj);
    }

    #[test]
    fn test_plan_set_objects_skips_already_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "already_excl_hash", true);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_already_excluded, 1);
    }

    #[test]
    fn test_plan_set_objects_skips_unhashed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "unhashed.jpg", None);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_no_hash, 1);
    }

    #[test]
    fn test_plan_set_objects_skips_empty() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "empty_hash_value_x", false);
        insert_source_with_size(&conn, root, "empty.txt", Some(obj), 0);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_empty, 1);
    }

    #[test]
    fn test_plan_set_objects_computes_source_counts() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "counts_hash_value", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.total_source_count, 2);
        assert_eq!(plan.total_archive_count, 1);
    }

    #[test]
    fn test_plan_set_objects_hash_prefix() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abcdef1234567890extra", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects[0].hash_prefix, "abcdef1234567890");
    }

    #[test]
    fn test_plan_set_objects_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "in_scope_obj_hash", false);
        let obj2 = insert_object(&conn, "out_scope_obj_hsh", false);
        insert_source(&conn, root, "2024/photo.jpg", Some(obj1));
        insert_source(&conn, root, "2023/photo.jpg", Some(obj2));

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_set_objects(&mut conn, &make_set_objects_params(scopes)).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.objects[0].object_id, obj1);
    }

    #[test]
    fn test_plan_set_objects_deduplicates_objects() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "shared_obj_hash_xx", false);
        // Two sources sharing the same object
        insert_source(&conn, root, "copy1.jpg", Some(obj));
        insert_source(&conn, root, "copy2.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1, "Same object should appear once");
    }

    #[test]
    fn test_plan_set_objects_source_sort_order() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "sort_order_hash_xx", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        let sources = &plan.objects[0].sources;
        assert_eq!(sources.len(), 2);
        // Source roots come first (role DESC: 'source' > 'archive')
        assert!(!sources[0].is_archive, "Source root should come first");
        assert!(sources[1].is_archive, "Archive root should come second");
    }

    // =========================================================================
    // execute_set_objects() tests
    // =========================================================================

    #[test]
    fn test_execute_set_objects_marks_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "exec_obj_hash1_xx", false);
        let obj2 = insert_object(&conn, "exec_obj_hash2_xx", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, root, "b.jpg", Some(obj2));

        let plan = ExcludeSetObjectsPlan {
            objects: vec![
                ObjectPlanEntry {
                    object_id: obj1,
                    hash_prefix: "exec_obj_hash1_x".to_string(),
                    sources: vec![],
                },
                ObjectPlanEntry {
                    object_id: obj2,
                    hash_prefix: "exec_obj_hash2_x".to_string(),
                    sources: vec![],
                },
            ],
            total_source_count: 2,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        };

        execute_set_objects(&conn, &plan).unwrap();

        assert!(is_object_excluded(&conn, obj1));
        assert!(is_object_excluded(&conn, obj2));
    }

    #[test]
    fn test_execute_set_objects_returns_count() {
        let conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "count_obj_hash_xxx", false);

        let plan = ExcludeSetObjectsPlan {
            objects: vec![ObjectPlanEntry {
                object_id: obj,
                hash_prefix: "count_obj_hash_x".to_string(),
                sources: vec![],
            }],
            total_source_count: 1,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        };

        let count = execute_set_objects(&conn, &plan).unwrap();
        assert_eq!(count, 1);
    }
}
