//! Compare operations — symmetric content comparison between two locations.
//!
//! `run_compare` owns the whole fetch: root list, both path resolutions,
//! source-existence validation, and the per-side content map (including the
//! contentless-law accounting) — the interface layer is left with output
//! formatting only. Closes compare's status as the one interface-side
//! contentless-law site (CLAUDE.md, contentless-law conventions).

use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::domain::scope::ScopeMatch;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::scope::{resolve_path, validate_sources_exist};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo;

/// Result of comparing two locations by content.
#[derive(Debug)]
pub struct CompareRun {
    pub prefix_a: String,
    pub prefix_b: String,
    pub in_both_count: usize,
    /// Sorted, absolute paths present only at A.
    pub only_in_a_paths: Vec<String>,
    /// Sorted, absolute paths present only at B.
    pub only_in_b_paths: Vec<String>,
    pub total_unhashed: usize,
    pub total_contentless: usize,
    /// Whether an `excluded?` predicate was used on either side without
    /// `--include excluded` (drives the visibility hint).
    pub used_excluded: bool,
    pub total_excluded: usize,
    pub is_identical: bool,
}

/// Compare two locations by content, returning everything the interface
/// needs to print. Step order below is observable: which error surfaces
/// first when two inputs are simultaneously invalid depends on it.
pub fn run_compare(
    conn: &mut Connection,
    path_a: &Path,
    path_b: &Path,
    filter_strs: &[String],
    include: &IncludeSet,
) -> Result<CompareRun> {
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let all_roots = repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let prefix_a = resolve_path(path_a, &all_roots, &cwd)?;
    let prefix_b = resolve_path(path_b, &all_roots, &cwd)?;

    validate_sources_exist(conn, &[prefix_a.clone(), prefix_b.clone()], &all_roots)?;

    // The two selection calls share one connection and must stay
    // sequential — select_sources takes &mut Connection, so side A must
    // fully resolve before side B starts. Not parallelizable without a
    // second connection, which is a different design entirely.
    let (sources_a, unhashed_a, contentless_a, used_status_a, excluded_count_a) =
        select_and_build_map(conn, &prefix_a, &filters, include)?;
    let (sources_b, unhashed_b, contentless_b, used_status_b, excluded_count_b) =
        select_and_build_map(conn, &prefix_b, &filters, include)?;

    let objects_a: HashSet<i64> = sources_a.keys().copied().collect();
    let objects_b: HashSet<i64> = sources_b.keys().copied().collect();

    let in_both: HashSet<i64> = objects_a.intersection(&objects_b).copied().collect();
    let only_in_a: HashSet<i64> = objects_a.difference(&objects_b).copied().collect();
    let only_in_b: HashSet<i64> = objects_b.difference(&objects_a).copied().collect();

    let mut only_in_a_paths: Vec<String> = only_in_a
        .iter()
        .filter_map(|oid| sources_a.get(oid))
        .cloned()
        .collect();
    only_in_a_paths.sort();
    let mut only_in_b_paths: Vec<String> = only_in_b
        .iter()
        .filter_map(|oid| sources_b.get(oid))
        .cloned()
        .collect();
    only_in_b_paths.sort();

    let is_identical = only_in_a.is_empty() && only_in_b.is_empty();
    let used_excluded = used_status_a.excluded || used_status_b.excluded;

    Ok(CompareRun {
        prefix_a,
        prefix_b,
        in_both_count: in_both.len(),
        only_in_a_paths,
        only_in_b_paths,
        total_unhashed: unhashed_a + unhashed_b,
        total_contentless: contentless_a + contentless_b,
        used_excluded,
        total_excluded: excluded_count_a + excluded_count_b,
        is_identical,
    })
}

/// (object_id -> path map, unhashed count, contentless count, used status, excluded count).
type ScopeContentMap = (
    HashMap<i64, String>,
    usize,
    usize,
    crate::expr::filter::UsedStatus,
    usize,
);

/// Select sources in scope and build an object_id → path map for content comparison.
///
/// Returns (object_id -> path map, unhashed count, contentless count).
/// Compare uses AnyRole — all sources are included regardless of whether
/// root is "source" or "archive".
fn select_and_build_map(
    conn: &mut Connection,
    scope_prefix: &str,
    filters: &[Filter],
    include: &IncludeSet,
) -> Result<ScopeContentMap> {
    let scopes = ScopeMatch::classify_all(&[scope_prefix.to_string()]);
    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::AnyRole,
    };
    let sel = selection::select_sources(conn, &params)?;

    // Build object_id → path map, counting unhashed and contentless sources.
    // The contentless law: every empty file shares the one universal empty
    // object, so "match by content" between empty files is a claim about
    // nothing — they leave the comparison and are counted, never silent.
    let mut result: HashMap<i64, String> = HashMap::new();
    let mut unhashed = 0;
    let mut contentless = 0;

    for source in &sel.sources {
        if source.is_contentless() {
            contentless += 1;
            continue;
        }
        match source.object_id {
            Some(oid) => {
                result.entry(oid).or_insert_with(|| source.path());
            }
            None => {
                unhashed += 1;
            }
        }
    }

    Ok((
        result,
        unhashed,
        contentless,
        sel.used_status,
        sel.excluded_count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::*;

    fn setup() -> Connection {
        setup_test_db()
    }

    #[test]
    fn identical_scopes_report_identical() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source_with_size(&conn, root_a, "file.bin", Some(obj), 100);
        insert_source_with_size(&conn, root_b, "file.bin", Some(obj), 100);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert!(run.is_identical);
        assert_eq!(run.in_both_count, 1);
        assert!(run.only_in_a_paths.is_empty());
        assert!(run.only_in_b_paths.is_empty());
    }

    #[test]
    fn content_only_in_a_is_reported() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let _root_b = insert_root(&conn, "/b", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source_with_size(&conn, root_a, "unique.bin", Some(obj), 100);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert!(!run.is_identical);
        assert_eq!(run.only_in_a_paths, vec!["/a/unique.bin".to_string()]);
        assert!(run.only_in_b_paths.is_empty());
    }

    #[test]
    fn content_only_in_b_is_reported() {
        let mut conn = setup();
        let _root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source_with_size(&conn, root_b, "unique.bin", Some(obj), 100);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert!(!run.is_identical);
        assert!(run.only_in_a_paths.is_empty());
        assert_eq!(run.only_in_b_paths, vec!["/b/unique.bin".to_string()]);
    }

    #[test]
    fn contentless_sources_are_counted_and_excluded_from_comparison() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "archive", false);
        let empty_obj = insert_object(&conn, "emptyhash", false);
        insert_source_with_size(&conn, root_a, "empty.log", Some(empty_obj), 0);
        insert_source_with_size(&conn, root_b, "empty.log", Some(empty_obj), 0);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert_eq!(run.total_contentless, 2);
        assert_eq!(run.in_both_count, 0);
        assert!(run.only_in_a_paths.is_empty());
        assert!(run.only_in_b_paths.is_empty());
    }

    #[test]
    fn unhashed_sources_are_counted_and_excluded_from_comparison() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let _root_b = insert_root(&conn, "/b", "archive", false);
        insert_source_with_size(&conn, root_a, "unhashed.bin", None, 100);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert_eq!(run.total_unhashed, 1);
        assert_eq!(run.in_both_count, 0);
        assert!(run.only_in_a_paths.is_empty());
    }

    #[test]
    fn duplicate_object_within_one_side_counts_once() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source_with_size(&conn, root_a, "copy1.bin", Some(obj), 100);
        insert_source_with_size(&conn, root_a, "copy2.bin", Some(obj), 100);
        insert_source_with_size(&conn, root_b, "file.bin", Some(obj), 100);

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap();

        assert_eq!(run.in_both_count, 1, "object identity, not source count");
        assert!(run.only_in_a_paths.is_empty());
        assert!(run.only_in_b_paths.is_empty());
    }

    #[test]
    fn excluded_visibility_hint_reflects_hidden_sources() {
        let mut conn = setup();
        let root_a = insert_root(&conn, "/a", "source", false);
        let _root_b = insert_root(&conn, "/b", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source_excluded(&conn, root_a, "file.bin", Some(obj));

        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &["excluded?".to_string()],
            &IncludeSet::default(),
        )
        .unwrap();

        assert!(run.used_excluded);
        assert_eq!(run.total_excluded, 1);
        assert!(
            run.only_in_a_paths.is_empty(),
            "excluded source hidden by default"
        );

        let include_excluded = IncludeSet {
            excluded: true,
            archived: false,
        };
        let run = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/b"),
            &["excluded?".to_string()],
            &include_excluded,
        )
        .unwrap();
        assert_eq!(run.in_both_count, 0);
        assert_eq!(run.only_in_a_paths, vec!["/a/file.bin".to_string()]);
    }

    #[test]
    fn filter_parse_error_surfaces_before_path_resolution_error() {
        let mut conn = setup();
        insert_root(&conn, "/a", "source", false);

        let err = run_compare(
            &mut conn,
            Path::new("/not/a/root"),
            Path::new("/also/not/a/root"),
            &["archived = true".to_string()],
            &IncludeSet::default(),
        )
        .unwrap_err();

        // The filter error, not a path-resolution error, must surface —
        // filter parsing runs before either path is touched. Status
        // predicates are boolean-only; `=` is a real parse error.
        assert!(
            !err.to_string().to_lowercase().contains("resolve path"),
            "expected a filter parse error, got: {err}"
        );
    }

    #[test]
    fn non_root_path_propagates_resolution_error() {
        let mut conn = setup();
        insert_root(&conn, "/a", "source", false);

        let err = run_compare(
            &mut conn,
            Path::new("/a"),
            Path::new("/nowhere"),
            &[],
            &IncludeSet::default(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("/nowhere"));
    }
}
