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

use std::collections::HashMap;

use crate::archive::{plan_generate, ClusterGenerateParams};
use crate::core::domain::resolution::{classify_present, FromHereEvidence, StandingBucket};
use crate::core::domain::time::zone::UserZone;
use crate::core::repo;
use crate::core::testing::*;
use crate::survey::ObjectIndex;

struct Canary {
    conn: crate::core::repo::Connection,
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
    // The archived-ness WHERE is spelled once, in `batch_archive_copies`;
    // this asks it through `batch_check_archived`, its key set, which is the
    // shape the `archived?` predicate and the ceremonies' archive context
    // use. Coverage and the readiness account read the copies directly, and
    // inherit the same predicate from the same spelling.
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
fn the_coverage_rollup_carries_the_empty_files_it_set_aside() {
    // The per-root lines get their set-aside from the query above; the
    // totals line is arithmetic on top of them and has no query to inherit
    // from. It must carry the set-aside too, or the summary of a fully
    // covered universe reads below 100% while every line above it reads
    // 100%.
    let mut c = canary();
    let (_, overall, _, _) = crate::coverage::compute_per_root(
        &mut c.conn,
        &crate::expr::BoundFilters::none(),
        None,
        &Default::default(),
        &crate::core::domain::time::zone::UserZone::utc(),
    )
    .unwrap();
    assert_eq!(overall.hashed_sources, 2);
    assert_eq!(overall.contentless_sources, 1, "the empty file is counted");
    assert_eq!(overall.coverable_sources(), 1);
    assert_eq!(overall.archived_sources, 1);
    assert_eq!(overall.unarchived(), 0);
    assert_eq!(overall.archived_pct(), 100.0);
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
        classify_present(
            empty,
            &archived,
            FromHereEvidence::by_directory(&HashMap::new())
        ),
        StandingBucket::Contentless
    );
    assert_eq!(
        classify_present(
            data,
            &archived,
            FromHereEvidence::by_directory(&HashMap::new())
        ),
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
            filters: crate::expr::BoundFilters::none(),
            allow_archived: false,
            allow_duplicates: false,
            clock: UserZone::utc(),
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
fn cluster_generate_never_reads_empty_files_as_duplicates() {
    // A duplicate is an identity claim, and every empty file shares the one empty content:
    // two empty files in a directory travel with it, never refuse the run as duplicates.
    let conn = setup_test_db();
    let root = insert_root(&conn, "/r", "source", false);
    let empty = insert_object(&conn, "emptyhash", false);
    let data = insert_object(&conn, "datahash", false);
    insert_source_with_size(&conn, root, "dir/a.log", Some(empty), 0);
    insert_source_with_size(&conn, root, "dir/b.log", Some(empty), 0);
    insert_source_with_size(&conn, root, "dir/c.bin", Some(data), 100);
    let params = ClusterGenerateParams {
        scopes: vec![],
        filters: crate::expr::BoundFilters::none(),
        allow_archived: false,
        allow_duplicates: false,
        clock: UserZone::utc(),
    };
    let mut conn = conn;
    let plan = plan_generate(&mut conn, &params).expect("empty files are no duplicates");
    let planned: Vec<&str> = plan.lock_entries.iter().map(|e| e.path.as_str()).collect();
    assert!(
        planned.iter().any(|p| p.ends_with("a.log"))
            && planned.iter().any(|p| p.ends_with("b.log")),
        "both empty files are in the lock: {planned:?}"
    );

    // The refusal stays for real duplicates, in the same state.
    insert_source_with_size(&conn, root, "dir/d.bin", Some(data), 100);
    assert!(
        plan_generate(&mut conn, &params).is_err(),
        "two copies of the same content still refuse"
    );
}

#[test]
fn exclude_duplicates_sets_the_empty_files_aside() {
    // An empty file is never excluded as a duplicate of another empty file: there is no
    // content to match a copy by. Counted as empty before the hash test, so the unhashed empty
    // file is empty too, never "no hash"; the empty copies under --prefer were never candidates.
    let conn = setup_test_db();
    let scope = insert_root(&conn, "/r", "source", false);
    let keep = insert_root(&conn, "/keep", "source", false);
    let empty = insert_object(&conn, "emptyhash", false);
    let data = insert_object(&conn, "datahash", false);
    insert_source_with_size(&conn, scope, "a.log", Some(empty), 0);
    insert_source_with_size(&conn, scope, "u.log", None, 0);
    let dup = insert_source_with_size(&conn, scope, "c.bin", Some(data), 100);
    insert_source_with_size(&conn, keep, "a.log", Some(empty), 0);
    insert_source_with_size(&conn, keep, "c.bin", Some(data), 100);
    let mut conn = conn;
    let plan = crate::exclude::plan_duplicates(
        &mut conn,
        &crate::exclude::ExcludeDuplicatesParams {
            scopes: vec![],
            filters: crate::expr::BoundFilters::none(),
            prefer_prefix: "/keep".to_string(),
            clock: UserZone::utc(),
        },
    )
    .unwrap();
    assert_eq!(
        plan.source_ids(),
        vec![dup],
        "only the non-empty duplicate is excluded"
    );
    assert_eq!(
        plan.skipped_empty, 2,
        "both empty files outside --prefer are counted"
    );
    assert_eq!(
        plan.skipped_no_hash, 0,
        "the unhashed empty file is counted as empty"
    );
}

#[test]
fn set_object_planning_sets_the_empty_file_aside() {
    // Identity-keyed exclusion over a selection: the empty file's object is
    // the one every empty file shares, so excluding it would dismiss every
    // empty file in the universe. Set aside, counted, never silent.
    let mut c = canary();
    let plan = crate::exclude::plan_set_objects(
        &mut c.conn,
        &crate::exclude::ExcludeSetObjectsParams {
            scopes: vec![],
            filters: crate::expr::BoundFilters::none(),
            clock: crate::core::domain::time::zone::UserZone::utc(),
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
fn the_holder_count_sets_empty_files_apart() {
    // The holders of content are what `exclude set-object`'s totals and `facts delete --on
    // object`'s reach count: an empty file holds the one content every empty file shares, so it
    // is counted apart, never as a holder in a source root or an archive.
    let c = canary();
    let held =
        repo::source::fetch_sources_by_object_ids(&c.conn, &[c.empty_obj, c.data_obj]).unwrap();
    let holders = crate::core::domain::object::Holders::of(held.values().flatten());
    assert_eq!(holders.empty_files, 2, "both empty files are counted apart");
    assert_eq!(
        (
            holders.in_source_roots,
            holders.in_archives,
            holders.total()
        ),
        (1, 1, 2),
        "only the genuine copies hold content"
    );
}

#[test]
fn survey_counts_the_empty_files_it_sets_aside() {
    // "Stated, never silent": the index refuses contentless sources, so
    // empties vanish from overlap, coverage, and only-here — the summary
    // must count what it set aside, like sweep, compare, and coverage do.
    let mut c = canary();
    let root_ids = vec![c.source_root];
    let all_sources = crate::core::repo::source::batch_fetch_by_roots(&c.conn, &root_ids).unwrap();
    let outcome = crate::survey::compute_survey(
        &mut c.conn,
        &["/r".to_string()],
        &crate::expr::BoundFilters::none(),
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
        &crate::core::domain::time::zone::UserZone::utc(),
    )
    .unwrap();
    match outcome {
        crate::survey::SurveyOutcome::Result(result) => {
            assert_eq!(result.contentless_count, 1, "the empty file is counted");
            // Uniqueness is an identity claim: the empty file's shared
            // object is absent from the index and must never fall through
            // as vacuously unique. Only the data object counts here.
            assert_eq!(result.unique_count, 1, "the empty file never counts unique");
            assert!(
                result.unique_paths.iter().all(|p| !p.contains("empty.log")),
                "no empty file in the unique listing"
            );
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
    let run = crate::compare::run_compare(
        &mut c.conn,
        std::path::Path::new("/r"),
        std::path::Path::new("/archive"),
        &crate::expr::ExpressionText::plain(&[]),
        &Default::default(),
        &crate::core::domain::time::zone::UserZone::utc(),
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
fn contentless_is_never_relocated() {
    // Scan's identity law. Every empty file's content agrees with every other
    // empty file's, so "the content matches" tells one from another not at
    // all — and a claim that would move a row to a different path is a claim
    // about where content was. Refused on vacuous evidence; the same evidence
    // shape carries the claim for a file that actually holds content.
    let c = canary();

    let empty = repo::source::fetch_by_path(&c.conn, c.source_root, "folder/empty.log")
        .unwrap()
        .unwrap();
    let relocated = |source: &crate::core::domain::source::Source, rel_path: &str| {
        crate::scan::FileObservation {
            root_id: source.root_id,
            rel_path: rel_path.to_string(),
            device: 1,
            inode: 1,
            size: source.size,
            mtime: source.mtime,
            partial_hash: None,
        }
    };

    assert!(
        !crate::scan::same_physical_file(
            &empty,
            &relocated(&empty, "folder/moved/empty.log"),
            Some(&empty.partial_hash),
            crate::scan::IdentityClaim::Relocation,
        ),
        "an empty source never earns a relocation"
    );

    let data = repo::source::fetch_by_path(&c.conn, c.source_root, "folder/data.bin")
        .unwrap()
        .unwrap();
    assert!(
        crate::scan::same_physical_file(
            &data,
            &relocated(&data, "folder/moved/data.bin"),
            Some(&data.partial_hash),
            crate::scan::IdentityClaim::Relocation,
        ),
        "the refusal is the emptiness, not the evidence shape"
    );
}

#[test]
fn set_object_by_path_refuses_the_empty_file() {
    // A path names one file, but its object is the one every empty file
    // shares — refused toward the explicit `--hash` intent.
    let c = canary();
    let err = crate::exclude::check_set_object_by_file(
        &c.conn,
        c.source_root,
        "folder/empty.log",
        "/r/folder/empty.log",
    )
    .unwrap_err();
    assert!(err.to_string().contains("empty file"), "{err:#}");
}

/// **The conversion counts empty files by what each shows**: the fact on the empty content is
/// shown by the three empty files holding that content — the archive copy, the source, and a
/// source whose own fact is hidden behind the content's (the source-value law) — and not by an
/// unhashed empty file, which shows its own fact and converts with the rest.
#[test]
fn the_conversion_counts_empty_files_by_what_each_shows() {
    let mut c = canary();
    insert_object_fact(&c.conn, c.empty_obj, "content.K", "nothing");
    let unhashed = insert_source_with_size(&c.conn, c.source_root, "folder/other.log", None, 0);
    insert_fact(&c.conn, unhashed, "content.K", "its own");
    let own = insert_source_with_size(
        &c.conn,
        c.source_root,
        "folder/third.log",
        Some(c.empty_obj),
        0,
    );
    insert_fact(&c.conn, own, "content.K", "its own too");
    let home = tempfile::tempdir().unwrap();
    let planned = crate::facts::plan_convert(
        &mut c.conn,
        "K",
        crate::facts::TypeWord::Type(crate::core::domain::fact::FactType::Time),
        home.path(),
    )
    .unwrap();
    let (empty, _) = planned
        .plan
        .empty_files
        .expect("the empty content's fact has its line");
    assert_eq!(empty.shown_by.total(), 3, "{empty:?}");
    assert_eq!(empty.shown_by.counted, 2, "{empty:?}");
    assert_eq!(empty.shown_by.archive_copy, 1, "{empty:?}");
    assert_eq!(
        planned.plan.bridge.sources.total(),
        1,
        "the unhashed empty file shows its own fact, counted with the rest"
    );
    assert_eq!(
        planned.plan.bridge.hidden, 1,
        "the third file's own fact is hidden"
    );
}
