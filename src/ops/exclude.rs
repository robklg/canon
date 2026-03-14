//! Exclude operations — plan/execute for source exclusion management.
//!
//! Provides plan/execute functions for `exclude set` and `exclude clear`.
//! Plan functions compute what would happen (no side effects), returning
//! typed plan structs with all data needed for display and confirmation.
//! Execute functions perform the writes.

use std::collections::HashSet;

use anyhow::Result;

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
}
