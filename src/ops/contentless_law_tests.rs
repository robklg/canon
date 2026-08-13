//! The contentless-law canary — one module, every surface (ADR
//! 2026-08-04-contentless-law).
//!
//! One empty file stands in the archive and one in a source root, beside a
//! genuine duplicate pair. No surface may read the empty content as
//! archived, covering, overlapping, conflicting, or skippable: the law says
//! identity claims about no-content are vacuous. If any single surface
//! regresses — a query losing its `size > 0` arm, the index losing its
//! build filter, the classifier reordering — exactly one assertion here
//! names it.

use std::collections::HashSet;

use crate::core::domain::resolution::{classify_present, StandingBucket};
use crate::ops::cluster::{plan_generate, ClusterGenerateParams};
use crate::ops::test_helpers::*;
use crate::repo;
use crate::survey::ObjectIndex;

struct Canary {
    conn: crate::repo::Connection,
    source_root: i64,
    empty_obj: i64,
    data_obj: i64,
}

/// Archive: keep/empty.log (0 B) + keep/data.bin (100 B).
/// Source root: folder/empty.log (0 B, same object) + folder/data.bin
/// (100 B, same object).
fn canary() -> Canary {
    let conn = setup_test_db();
    let source_root = insert_root(&conn, "/r", "source", false);
    let archive = insert_root(&conn, "/archive", "archive", false);
    let empty_obj = insert_object(&conn, "emptyhash", false);
    let data_obj = insert_object(&conn, "datahash", false);
    insert_source_with_size(&conn, archive, "keep/empty.log", Some(empty_obj), 0);
    insert_source_with_size(&conn, archive, "keep/data.bin", Some(data_obj), 100);
    insert_source_with_size(&conn, source_root, "folder/empty.log", Some(empty_obj), 0);
    insert_source_with_size(&conn, source_root, "folder/data.bin", Some(data_obj), 100);
    Canary {
        conn,
        source_root,
        empty_obj,
        data_obj,
    }
}

#[test]
fn archived_ness_never_reads_empty_content() {
    // The `archived?` predicate, coverage stats, the ceremonies' archive
    // context, and the readiness archived-set all derive here.
    let c = canary();
    let archived =
        repo::object::batch_check_archived(&c.conn, &[c.empty_obj, c.data_obj], None).unwrap();
    assert!(archived.contains(&c.data_obj));
    assert!(
        !archived.contains(&c.empty_obj),
        "an empty archive copy is no coverage evidence"
    );
}

#[test]
fn coverage_locations_never_name_the_empty_object() {
    // Cluster's skip set, the story's covered-where, and the compile's
    // locations all derive here.
    let c = canary();
    let paths =
        repo::object::batch_find_archive_paths(&c.conn, &[c.empty_obj, c.data_obj]).unwrap();
    assert!(paths.contains_key(&c.data_obj));
    assert!(
        !paths.contains_key(&c.empty_obj),
        "the universal empty object claims no locations"
    );
}

#[test]
fn archive_conflicts_never_fire_on_empty_content() {
    // Apply's preflight archive-conflict check derives here: an empty file
    // being applied must not "conflict" with every empty file already in
    // the archive.
    let c = canary();
    let info =
        repo::object::batch_find_archive_info_by_hash(&c.conn, &["emptyhash", "datahash"]).unwrap();
    assert!(info.contains_key("datahash"));
    assert!(!info.contains_key("emptyhash"));
}

#[test]
fn the_index_refuses_contentless_sources() {
    // Survey's overlap, archive-status, and only-here reasoning all start
    // at the index.
    let c = canary();
    let all = repo::source::batch_fetch_by_roots(&c.conn, &[c.source_root]).unwrap();
    let index = ObjectIndex::build(&all);
    assert!(index.locations_of(c.empty_obj).is_empty());
    assert_eq!(index.locations_of(c.data_obj).len(), 1);
}

#[test]
fn the_classifier_reads_the_empty_source_as_contentless() {
    // SQL → classifier, tied end to end: with the law in the queries the
    // empty object never enters the archived set, and the Contentless arm
    // (which precedes the identity tests) is what keeps it from falling
    // through to Unresolved.
    let c = canary();
    let sources = repo::source::batch_fetch_by_roots(&c.conn, &[c.source_root]).unwrap();
    let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
    let archived = repo::object::batch_check_archived(&c.conn, &object_ids, None).unwrap();
    let empty = sources
        .iter()
        .find(|s| s.rel_path == "folder/empty.log")
        .unwrap();
    let data = sources
        .iter()
        .find(|s| s.rel_path == "folder/data.bin")
        .unwrap();
    assert_eq!(
        classify_present(empty, &archived, &HashSet::new()),
        StandingBucket::Contentless
    );
    assert_eq!(
        classify_present(data, &archived, &HashSet::new()),
        StandingBucket::Covered
    );
}

#[test]
fn cluster_generate_carries_empty_files_instead_of_skipping_them() {
    // The ratified default flip: contentless files are never "already
    // archived", so an archive-everything pass carries them with their
    // folders — the verbatim-folder copy stays faithful.
    let mut c = canary();
    let plan = plan_generate(
        &mut c.conn,
        &ClusterGenerateParams {
            scopes: vec![],
            filters: vec![],
            allow_archived: false,
            allow_duplicates: false,
        },
    )
    .unwrap();
    let planned: Vec<&str> = plan.lock_entries.iter().map(|e| e.path.as_str()).collect();
    assert!(
        planned.iter().any(|p| p.contains("empty.log")),
        "the empty file is planned, not skipped"
    );
    let skipped: Vec<&str> = plan.archived.iter().map(|(s, _)| s.as_str()).collect();
    assert!(
        skipped.iter().any(|s| s.contains("data.bin")),
        "the genuinely covered file still skips"
    );
    assert!(
        !skipped.iter().any(|s| s.contains("empty.log")),
        "no empty file in the already-archived skip list"
    );
}

#[test]
fn set_object_planning_sets_the_empty_file_aside() {
    // Identity-keyed exclusion over a selection: the empty file's object is
    // the one every empty file shares, so excluding it would dismiss every
    // empty file in the universe. Set aside, counted, never silent.
    let mut c = canary();
    let plan = crate::ops::exclude::plan_set_objects(
        &mut c.conn,
        &crate::ops::exclude::ExcludeSetObjectsParams {
            scopes: vec![],
            filters: vec![],
        },
    )
    .unwrap();
    assert_eq!(plan.skipped_empty, 1, "the empty file is counted aside");
    assert!(
        plan.objects.iter().all(|o| o.object_id != c.empty_obj),
        "the universal empty object is never planned for exclusion"
    );
    assert!(
        plan.objects.iter().any(|o| o.object_id == c.data_obj),
        "the genuine duplicate still plans"
    );
}

#[test]
fn survey_counts_the_empty_files_it_sets_aside() {
    // "Stated, never silent": the index refuses contentless sources, so
    // empties vanish from overlap, coverage, and only-here — the summary
    // must count what it set aside, like sweep, compare, and coverage do.
    let mut c = canary();
    let root_ids = vec![c.source_root];
    let all_sources = crate::repo::source::batch_fetch_by_roots(&c.conn, &root_ids).unwrap();
    let outcome = crate::survey::compute_survey(
        &mut c.conn,
        &["/r".to_string()],
        &[],
        &crate::survey::SurveyParams {
            include: Default::default(),
            compute_affinity: false,
            compute_overlap_pairs: false,
            compute_residual: false,
            compute_archived_pairs: false,
        },
        &all_sources,
        &[],
        None,
    )
    .unwrap();
    match outcome {
        crate::survey::SurveyOutcome::Result(result) => {
            assert_eq!(result.contentless_count, 1, "the empty file is counted");
        }
        _ => panic!("expected a survey result"),
    }
}

#[test]
fn compare_counts_the_empty_files_it_sets_aside() {
    // Mirrors survey_counts_the_empty_files_it_sets_aside: compare builds
    // its own object maps rather than routing through the SQL or the
    // index, so it must do its own contentless accounting — this pins that
    // it does, matching the "stated, never silent" rule every other law
    // site follows.
    let mut c = canary();
    let run = crate::ops::compare::run_compare(
        &mut c.conn,
        std::path::Path::new("/r"),
        std::path::Path::new("/archive"),
        &[],
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        run.total_contentless, 2,
        "one empty file set aside on each side"
    );
    assert_eq!(run.in_both_count, 1, "the data object matches by content");
    assert!(run.only_in_a_paths.is_empty());
    assert!(run.only_in_b_paths.is_empty());
}

#[test]
fn set_object_by_path_refuses_the_empty_file() {
    // A path names one file, but its object is the one every empty file
    // shares — refused toward the explicit `--hash` intent.
    let c = canary();
    let err = crate::ops::exclude::check_set_object_by_file(
        &c.conn,
        c.source_root,
        "folder/empty.log",
        "/r/folder/empty.log",
    )
    .unwrap_err();
    assert!(err.to_string().contains("empty file"), "{err:#}");
}
