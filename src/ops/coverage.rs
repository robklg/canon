//! Coverage statistics computation.
//!
//! Computes archive coverage statistics from a set of sources.
//! Used by the `coverage` command.

use anyhow::Result;

use crate::domain::root::Root;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection};

use crate::domain::scope::ScopeMatch;

/// Statistics for a set of sources.
pub struct CoverageStats {
    pub root_id: Option<i64>,
    pub root_path: Option<String>,
    pub root_role: Option<String>,
    pub total_sources: i64,
    pub excluded_sources: i64,
    pub hashed_sources: i64,
    pub archived_sources: i64,
}

impl CoverageStats {
    pub fn new() -> Self {
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

    pub fn included_sources(&self) -> i64 {
        self.total_sources - self.excluded_sources
    }

    pub fn hashed_pct(&self) -> f64 {
        let included = self.included_sources();
        if included == 0 {
            0.0
        } else {
            (self.hashed_sources as f64 / included as f64) * 100.0
        }
    }

    pub fn archived_pct(&self) -> f64 {
        if self.hashed_sources == 0 {
            0.0
        } else {
            (self.archived_sources as f64 / self.hashed_sources as f64) * 100.0
        }
    }

    pub fn unarchived(&self) -> i64 {
        self.hashed_sources - self.archived_sources
    }
}

/// Compute coverage statistics from source references.
/// Uses `is_excluded()` which checks BOTH source-level and object-level exclusion.
pub fn compute_stats(
    conn: &mut Connection,
    sources: &[&Source],
    archive_root_id: Option<i64>,
) -> Result<CoverageStats> {
    let mut stats = CoverageStats::new();

    stats.total_sources = sources.len() as i64;

    // Excluded sources — uses is_excluded() which checks BOTH source and object level
    stats.excluded_sources = sources.iter().filter(|s| s.is_excluded()).count() as i64;

    // Hashed sources (have object_id AND not excluded)
    let hashed_sources: Vec<&&Source> = sources
        .iter()
        .filter(|s| s.object_id.is_some() && !s.is_excluded())
        .collect();
    stats.hashed_sources = hashed_sources.len() as i64;

    // Archived sources — use batch archive detection
    if stats.hashed_sources > 0 {
        let object_ids: Vec<i64> = hashed_sources.iter().filter_map(|s| s.object_id).collect();
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

/// Compute coverage for a scoped selection.
pub fn compute_scoped(
    conn: &mut Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include: &IncludeSet,
) -> Result<CoverageStats> {
    let params = SelectionParams {
        scopes: scopes.to_vec(),
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = selection::select_sources(conn, &params)?;
    let refs: Vec<&Source> = sel.sources.iter().collect();
    compute_stats(conn, &refs, archive_root_id)
}

/// Compute per-root breakdown plus overall totals.
pub fn compute_per_root(
    conn: &mut Connection,
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include: &IncludeSet,
) -> Result<(Vec<CoverageStats>, CoverageStats)> {
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: Vec<&Root> = all_roots
        .iter()
        .filter(|r| r.is_active())
        .filter(|r| include.includes_archived() || r.is_source())
        .collect();

    let params = SelectionParams {
        scopes: vec![],
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let all_sel = selection::select_sources(conn, &params)?;
    let all_sources = all_sel.sources;

    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for root in &roots {
        let root_sources: Vec<&Source> = all_sources
            .iter()
            .filter(|s| s.root_id == root.id)
            .collect();

        let mut stats = compute_stats(conn, &root_sources, archive_root_id)?;
        stats.root_id = Some(root.id);
        stats.root_path = Some(root.path.clone());
        stats.root_role = Some(root.role.clone());

        overall.total_sources += stats.total_sources;
        overall.excluded_sources += stats.excluded_sources;
        overall.hashed_sources += stats.hashed_sources;
        overall.archived_sources += stats.archived_sources;

        per_root_stats.push(stats);
    }

    Ok((per_root_stats, overall))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, setup_test_db,
    };

    /// Test that archived counts sources, not unique objects.
    /// Guards against the Object Infrastructure bug pattern where
    /// archived_set.len() was used instead of counting sources.
    #[test]
    fn coverage_archived_counts_sources_not_objects() {
        let mut conn = setup_test_db();

        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // One object archived, shared by 3 sources
        let archived_obj = insert_object(&conn, "abc123archived", false);
        insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj));

        // One unarchived object
        let unarchived_obj = insert_object(&conn, "def456unarchived", false);
        insert_source(&conn, source_root, "photo4.jpg", Some(unarchived_obj));

        // Put archived object in archive
        insert_source(&conn, archive_root, "backup.jpg", Some(archived_obj));

        let sources = repo::source::batch_fetch_by_roots(&conn, &[source_root]).unwrap();
        let refs: Vec<&Source> = sources.iter().collect();
        let stats = compute_stats(&mut conn, &refs, None).unwrap();

        assert_eq!(stats.archived_sources, 3, "Should count 3 sources, not 1 object");
        assert_eq!(stats.total_sources, 4);
        assert_eq!(stats.hashed_sources, 4);
    }

    #[test]
    fn coverage_stats_empty() {
        let mut conn = setup_test_db();
        let stats = compute_stats(&mut conn, &[], None).unwrap();
        assert_eq!(stats.total_sources, 0);
        assert_eq!(stats.hashed_sources, 0);
        assert_eq!(stats.archived_sources, 0);
    }

    #[test]
    fn coverage_stats_excludes_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);

        insert_source(&conn, root, "included.jpg", Some(obj));

        use crate::ops::test_helpers::insert_source_excluded;
        let excl_obj = insert_object(&conn, "hash2", false);
        insert_source_excluded(&conn, root, "excluded.jpg", Some(excl_obj));

        let sources = repo::source::batch_fetch_by_roots(&conn, &[root]).unwrap();
        let refs: Vec<&Source> = sources.iter().collect();
        let stats = compute_stats(&mut conn, &refs, None).unwrap();

        assert_eq!(stats.total_sources, 2);
        assert_eq!(stats.excluded_sources, 1);
        assert_eq!(stats.hashed_sources, 1); // excluded sources don't count as hashed
    }
}
