//! Source selection — Canon's standard query contract.
//!
//! Provides `select_sources()`, the single implementation that replaces per-command
//! `get_matching_sources()` functions. All commands needing filtered sources build
//! a `SelectionParams` and receive a `Selection` back.

use std::collections::HashSet;

use anyhow::Result;

use crate::domain::include::IncludeSet;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::expr::filter::{self, Filter};
use crate::repo::{self, Connection};

/// How to handle root role filtering during source selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolePolicy {
    /// Include archive sources only when `IncludeSet::includes_archived()` is true.
    /// Used by: ls, facts, coverage, worklist, cluster.
    SourceUnlessIncluded,
    /// Source roots only, regardless of IncludeSet.
    /// Used by: exclude, survey (selection side).
    SourceOnly,
    /// All roles — no role filtering.
    /// Used by: compare.
    AnyRole,
}

/// Parameters for source selection.
pub struct SelectionParams {
    /// Path scopes to restrict results. Empty means no scope filtering.
    pub scopes: Vec<ScopeMatch>,
    /// Visibility: whether to include excluded and/or archived sources.
    pub include: IncludeSet,
    /// `--where` filter expressions. Empty means no additional filtering.
    pub filters: Vec<Filter>,
    /// Role filtering strategy.
    pub role_policy: RolePolicy,
}

/// Result of source selection — filtered sources plus visibility metadata.
///
/// All counts are always tracked so that no command can "forget" to report
/// excluded-source hints to the user.
pub struct Selection {
    /// Sources that passed all filters.
    pub sources: Vec<Source>,
    /// Sources that matched scope/role but were hidden by exclusion filtering.
    pub excluded_count: usize,
    /// Excluded sources that were kept (when `include.excluded` is set).
    pub included_excluded_count: usize,
    /// Archive-role sources that were kept (when `include.archived` is set).
    pub included_archived_count: usize,
}

impl Selection {
    /// Convenience: extract source IDs from the selection.
    pub fn source_ids(&self) -> Vec<i64> {
        self.sources.iter().map(|s| s.id).collect()
    }
}

/// Select sources matching the given parameters.
///
/// Pipeline: fetch all roots and sources, apply domain predicates (active, role,
/// scope, exclusion) while tracking visibility counts, then apply `--where` filters.
pub fn select_sources(conn: &mut Connection, params: &SelectionParams) -> Result<Selection> {
    let roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    let mut excluded_count = 0usize;
    let mut included_excluded_count = 0usize;
    let mut included_archived_count = 0usize;

    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| match params.role_policy {
            RolePolicy::SourceUnlessIncluded => {
                params.include.includes_archived() || s.is_from_role("source")
            }
            RolePolicy::SourceOnly => s.is_from_role("source"),
            RolePolicy::AnyRole => true,
        })
        .filter(|s| s.matches_scope(&params.scopes))
        .filter(|s| {
            if s.is_excluded() && !params.include.includes_excluded() {
                excluded_count += 1;
                return false;
            }
            if s.is_excluded() {
                included_excluded_count += 1;
            }
            if s.is_from_role("archive") {
                included_archived_count += 1;
            }
            true
        })
        .collect();

    if params.filters.is_empty() {
        return Ok(Selection {
            sources: filtered,
            excluded_count,
            included_excluded_count,
            included_archived_count,
        });
    }

    // Apply --where filters: get passing IDs, then keep matching sources
    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let passing_ids = filter::apply_filters(conn, &source_ids, &params.filters)?;
    let passing_set: HashSet<i64> = passing_ids.into_iter().collect();

    let result: Vec<Source> = filtered
        .into_iter()
        .filter(|s| passing_set.contains(&s.id))
        .collect();

    Ok(Selection {
        sources: result,
        excluded_count,
        included_excluded_count,
        included_archived_count,
    })
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

    /// Insert a source with a specific size (for --where filter tests).
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

    fn make_params(role_policy: RolePolicy) -> SelectionParams {
        SelectionParams {
            scopes: vec![],
            include: IncludeSet::default(),
            filters: vec![],
            role_policy,
        }
    }

    // =========================================================================
    // Suspension filtering
    // =========================================================================

    #[test]
    fn select_sources_returns_active_sources_only() {
        let mut conn = setup_test_db();
        let active_root = insert_root(&conn, "/active", "source", false);
        let suspended_root = insert_root(&conn, "/suspended", "source", true);
        let id1 = insert_source(&conn, active_root, "a.jpg", None);
        let _id2 = insert_source(&conn, suspended_root, "b.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        assert_eq!(sel.sources.len(), 1);
        assert_eq!(sel.sources[0].id, id1);
    }

    // =========================================================================
    // Role policy
    // =========================================================================

    #[test]
    fn select_sources_role_source_unless_included_default() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src", "source", false);
        let arc_root = insert_root(&conn, "/arc", "archive", false);
        insert_source(&conn, src_root, "a.jpg", None);
        insert_source(&conn, arc_root, "b.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        assert_eq!(sel.sources.len(), 1);
        assert_eq!(sel.sources[0].root_id, src_root);
        assert_eq!(sel.included_archived_count, 0);
    }

    #[test]
    fn select_sources_role_source_unless_included_with_archived() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src", "source", false);
        let arc_root = insert_root(&conn, "/arc", "archive", false);
        insert_source(&conn, src_root, "a.jpg", None);
        insert_source(&conn, arc_root, "b.jpg", None);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.include = IncludeSet {
            excluded: false,
            archived: true,
        };
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 2);
        assert_eq!(sel.included_archived_count, 1);
    }

    #[test]
    fn select_sources_role_source_only_ignores_include_archived() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src", "source", false);
        let arc_root = insert_root(&conn, "/arc", "archive", false);
        insert_source(&conn, src_root, "a.jpg", None);
        insert_source(&conn, arc_root, "b.jpg", None);

        let mut params = make_params(RolePolicy::SourceOnly);
        params.include = IncludeSet {
            excluded: false,
            archived: true,
        };
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 1);
        assert_eq!(sel.sources[0].root_id, src_root);
    }

    #[test]
    fn select_sources_role_any() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src", "source", false);
        let arc_root = insert_root(&conn, "/arc", "archive", false);
        insert_source(&conn, src_root, "a.jpg", None);
        insert_source(&conn, arc_root, "b.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::AnyRole)).unwrap();
        assert_eq!(sel.sources.len(), 2);
    }

    // =========================================================================
    // Scope filtering
    // =========================================================================

    #[test]
    fn select_sources_scope_filtering() {
        let mut conn = setup_test_db();
        let photos = insert_root(&conn, "/photos", "source", false);
        let videos = insert_root(&conn, "/videos", "source", false);
        insert_source(&conn, photos, "a.jpg", None);
        insert_source(&conn, photos, "b.jpg", None);
        insert_source(&conn, videos, "c.mp4", None);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.scopes = vec![ScopeMatch::UnderDirectory("/photos".to_string())];
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 2);
        assert!(sel.sources.iter().all(|s| s.root_id == photos));
    }

    #[test]
    fn select_sources_empty_scopes_returns_all() {
        let mut conn = setup_test_db();
        let r1 = insert_root(&conn, "/a", "source", false);
        let r2 = insert_root(&conn, "/b", "source", false);
        insert_source(&conn, r1, "x.jpg", None);
        insert_source(&conn, r2, "y.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        assert_eq!(sel.sources.len(), 2);
    }

    // =========================================================================
    // Exclusion filtering
    // =========================================================================

    #[test]
    fn select_sources_excludes_excluded_by_default() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let excl_obj = insert_object(&conn, "hash_excl", true);

        // Source-level excluded
        insert_source_excluded(&conn, root, "src_excl.jpg", None);
        // Object-level excluded
        insert_source(&conn, root, "obj_excl.jpg", Some(excl_obj));
        // Normal
        let normal_id = insert_source(&conn, root, "normal.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        assert_eq!(sel.sources.len(), 1);
        assert_eq!(sel.sources[0].id, normal_id);
        assert_eq!(sel.excluded_count, 2);
        assert_eq!(sel.included_excluded_count, 0);
    }

    #[test]
    fn select_sources_includes_excluded_when_requested() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let excl_obj = insert_object(&conn, "hash_excl", true);

        insert_source_excluded(&conn, root, "src_excl.jpg", None);
        insert_source(&conn, root, "obj_excl.jpg", Some(excl_obj));
        insert_source(&conn, root, "normal.jpg", None);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.include = IncludeSet {
            excluded: true,
            archived: false,
        };
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 3);
        assert_eq!(sel.excluded_count, 0);
        assert_eq!(sel.included_excluded_count, 2);
    }

    // =========================================================================
    // Count tracking
    // =========================================================================

    #[test]
    fn select_sources_counts_included_archived() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src", "source", false);
        let arc_root = insert_root(&conn, "/arc", "archive", false);
        insert_source(&conn, src_root, "a.jpg", None);
        insert_source(&conn, arc_root, "b.jpg", None);
        insert_source(&conn, arc_root, "c.jpg", None);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.include = IncludeSet {
            excluded: false,
            archived: true,
        };
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 3);
        assert_eq!(sel.included_archived_count, 2);
    }

    // =========================================================================
    // --where filter integration
    // =========================================================================

    #[test]
    fn select_sources_applies_where_filters() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        insert_source_with_size(&conn, root, "small.jpg", None, 100);
        let big_id = insert_source_with_size(&conn, root, "big.jpg", None, 10_000);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.filters = vec![Filter::parse("source.size > 5000").unwrap()];
        let sel = select_sources(&mut conn, &params).unwrap();
        assert_eq!(sel.sources.len(), 1);
        assert_eq!(sel.sources[0].id, big_id);
        // Excluded counts are unaffected by --where filtering
        assert_eq!(sel.excluded_count, 0);
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn select_sources_empty_database() {
        let mut conn = setup_test_db();
        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        assert!(sel.sources.is_empty());
        assert_eq!(sel.excluded_count, 0);
        assert_eq!(sel.included_excluded_count, 0);
        assert_eq!(sel.included_archived_count, 0);
    }

    #[test]
    fn select_sources_no_matching_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "a.jpg", None);

        let mut params = make_params(RolePolicy::SourceUnlessIncluded);
        params.scopes = vec![ScopeMatch::UnderDirectory("/videos".to_string())];
        let sel = select_sources(&mut conn, &params).unwrap();
        assert!(sel.sources.is_empty());
    }

    #[test]
    fn select_sources_source_ids_convenience() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "b.jpg", None);

        let sel = select_sources(&mut conn, &make_params(RolePolicy::SourceUnlessIncluded)).unwrap();
        let ids = sel.source_ids();
        assert_eq!(ids.len(), 2);
        // IDs match sources in the same order
        assert_eq!(ids[0], sel.sources[0].id);
        assert_eq!(ids[1], sel.sources[1].id);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }
}
