use rusqlite::Connection;

use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::test_helpers::{
    insert_object, insert_root, insert_source, insert_source_excluded, insert_source_with_size,
    setup_test_db,
};
use crate::survey::domain::analysis::LocationKind;
use crate::survey::ops::compute::{compute_survey, SurveyOutcome, SurveyParams};

use super::fixtures::test_params;

/// Helper to run compute_survey with test data.
fn run_compute(
    conn: &mut Connection,
    scope_paths: &[&str],
    params: &SurveyParams,
    filters: &[Filter],
    other_paths: &[&str],
    archive_root_id: Option<i64>,
) -> SurveyOutcome {
    let root_ids: Vec<i64> = crate::repo::root::fetch_all(conn)
        .unwrap()
        .iter()
        .map(|r| r.id)
        .collect();
    let all_sources = crate::repo::source::batch_fetch_by_roots(conn, &root_ids).unwrap();

    let prefixes: Vec<String> = scope_paths.iter().map(|s| s.to_string()).collect();
    let other: Vec<String> = other_paths.iter().map(|s| s.to_string()).collect();
    compute_survey(
        conn,
        &prefixes,
        filters,
        params,
        &all_sources,
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
        SurveyOutcome::Empty => {}
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
        SurveyOutcome::AllUnhashed { total_count } => {
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
            assert_eq!(loc.kind, Some(LocationKind::Lead));
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
                Some(LocationKind::Superset)
            );
            assert_eq!(result.location_results[0].shared_count, 9);
            assert_eq!(result.location_results[0].complementary_count, Some(5));
            assert!(result.location_results[1].path.contains("partner"));
            assert_eq!(result.location_results[1].kind, Some(LocationKind::Lead));
            assert_eq!(result.location_results[1].complementary_count, Some(20));
            assert!(result.location_results[2].path.contains("old-copy"));
            assert_eq!(result.location_results[2].kind, Some(LocationKind::Subset));
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
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive/photos"],
        &params,
        &filters,
        &[],
        None,
    );

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
            assert_eq!(loc.kind, Some(LocationKind::Subset));
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
    let other = vec![("/mnt/backup/trip")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &other,
        None,
    );

    match outcome {
        SurveyOutcome::Result(result) => {
            assert!(result.is_other_mode);
            assert_eq!(result.location_results.len(), 1);
            let loc = &result.location_results[0];
            assert_eq!(loc.path, "/mnt/backup/trip");
            assert_eq!(loc.shared_count, 2);
            assert_eq!(loc.complementary_count, Some(2));
            assert_eq!(loc.only_here_count, Some(2));
            assert_eq!(loc.kind, Some(LocationKind::Lead));
        }
        _ => panic!("Expected SurveyOutcome::Result"),
    }
}

#[test]
fn test_other_never_reads_empty_files_as_shared_or_unique() {
    // The contentless law at the --other direct reads: every empty file
    // shares the one empty-content object, so an empty file on each side
    // must create no shared content, no uniqueness, and no location total.
    let mut conn = setup_test_db();

    let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
    let root_b = insert_root(&conn, "/mnt/backup", "source", false);

    let obj1 = insert_object(&conn, "hash_001", false);
    let empty_obj = insert_object(&conn, "hash_empty", false);

    insert_source_with_size(&conn, root_a, "photos/IMG_001.jpg", Some(obj1), 100);
    insert_source_with_size(&conn, root_a, "photos/empty.jpg", Some(empty_obj), 0);

    insert_source_with_size(&conn, root_b, "trip/IMG_001.jpg", Some(obj1), 100);
    insert_source_with_size(&conn, root_b, "trip/empty.jpg", Some(empty_obj), 0);

    let params = test_params();
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &[],
        &["/mnt/backup/trip"],
        None,
    );

    match outcome {
        SurveyOutcome::Result(result) => {
            assert_eq!(result.contentless_count, 1);
            let loc = &result.location_results[0];
            assert_eq!(loc.shared_count, 1, "the empty pair is not shared content");
            assert_eq!(
                loc.total_count, 1,
                "the location total counts no empty file"
            );
            assert_eq!(result.unique_count, 0, "the empty file never counts unique");
        }
        _ => panic!("Expected SurveyOutcome::Result"),
    }
}

#[test]
fn test_affinity_never_reads_empty_files_as_complementary() {
    // The contentless law at the affinity direct read: an empty file at the
    // compared location must not count as complementary content, nor —
    // being absent from the index — as vacuously "only here".
    let mut conn = setup_test_db();

    let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
    let root_b = insert_root(&conn, "/mnt/backup", "source", false);

    let obj1 = insert_object(&conn, "hash_001", false);
    let obj2 = insert_object(&conn, "hash_002", false);
    let empty_obj = insert_object(&conn, "hash_empty", false);

    insert_source_with_size(&conn, root_a, "photos/IMG_001.jpg", Some(obj1), 100);

    insert_source_with_size(&conn, root_b, "trip/IMG_001.jpg", Some(obj1), 100);
    insert_source_with_size(&conn, root_b, "trip/IMG_002.jpg", Some(obj2), 100);
    insert_source_with_size(&conn, root_b, "trip/empty.jpg", Some(empty_obj), 0);

    let params = SurveyParams {
        compute_affinity: true,
        ..test_params()
    };
    let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &["/mnt/backup/trip"],
        None,
    );

    match outcome {
        SurveyOutcome::Result(result) => {
            let loc = &result.location_results[0];
            assert_eq!(loc.shared_count, 1);
            assert_eq!(
                loc.complementary_count,
                Some(1),
                "the empty file is not complementary content"
            );
            assert_eq!(
                loc.only_here_count,
                Some(1),
                "the empty file is never \"only here\""
            );
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
    let other = vec![("/mnt/backup")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &other,
        None,
    );

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

    let other = vec![("/mnt/root-c"), ("/mnt/root-b")];
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
    let other = vec![("/archive")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &other,
        None,
    );

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
    let other = vec![("/mnt/backup/trip")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &other,
        None,
    );

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
    let other = vec![("/mnt/drive/documents")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive/photos"],
        &params,
        &filters,
        &other,
        None,
    );

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
    let other = vec![("/mnt/backup")];
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive-a"],
        &params,
        &filters,
        &other,
        None,
    );

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

    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive"],
        &params,
        &[],
        &[],
        Some(archive_a),
    );
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
    let outcome = run_compute(
        &mut conn,
        &["/mnt/drive"],
        &params,
        &[],
        &[],
        Some(archive_a),
    );

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
    let outcome_filtered = run_compute(
        &mut conn,
        &["/mnt/drive"],
        &params,
        &[],
        &[],
        Some(archive_a),
    );

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
            assert_eq!(loc.kind, Some(LocationKind::Subset));
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
            assert_eq!(loc.kind, Some(LocationKind::Mirror));
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
    let other = vec![("/mnt/backup/trip")];
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
    let other = vec![("/mnt/backup")];
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
    let other = vec![("/mnt/backup/trip")];
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
    let other = vec![("/mnt/backup")];
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
    let other = vec![("/mnt/backup")];
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
    let other = vec![("/mnt/backup")];
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
    let other = vec![("/mnt/backup")];
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
    let other = vec![("/mnt/backup-1"), ("/mnt/backup-2")];
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
