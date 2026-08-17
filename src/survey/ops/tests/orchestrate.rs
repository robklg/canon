use std::path::PathBuf;

use crate::core::testing::{insert_note, insert_object, insert_root, insert_source, setup_test_db};
use crate::survey::ops::compute::SurveyOutcome;
use crate::survey::ops::orchestrate::{run_survey, SurveyOrchestration};

use super::fixtures::test_params;

// =========================================================================
// run_survey — orchestration
// =========================================================================

fn test_orchestration() -> SurveyOrchestration {
    SurveyOrchestration {
        other_paths: Vec::new(),
        archive: None,
        want_location_note_counts: false,
    }
}

#[test]
fn run_survey_returns_note_context_for_single_prefix_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "photos/a.jpg", Some(obj));
    insert_note(&conn, root, "photos", "worth revisiting", 100);

    let params = test_params();
    let orchestration = test_orchestration();
    let run = run_survey(
        &mut conn,
        &["/mnt/drive/photos".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    let (ctx, scope_rel) = run
        .note_context
        .expect("note context expected for a single-prefix scope");
    assert_eq!(scope_rel, "photos");
    assert_eq!(ctx.subtree_notes.len(), 1);
    assert_eq!(ctx.subtree_notes[0].text, "worth revisiting");
}

#[test]
fn run_survey_omits_note_context_for_multi_prefix_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "a.jpg", Some(obj));
    insert_source(&conn, root, "b.jpg", Some(obj));
    insert_note(&conn, root, "", "a root-level note", 100);

    let params = test_params();
    let orchestration = test_orchestration();
    let run = run_survey(
        &mut conn,
        &[
            "/mnt/drive/a.jpg".to_string(),
            "/mnt/drive/b.jpg".to_string(),
        ],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    assert!(
        run.note_context.is_none(),
        "note context is only built for a single-prefix scope"
    );
}

#[test]
fn run_survey_note_context_survives_empty_outcome() {
    // Note context is independent of the selection's size — it must
    // still surface for an empty selection, not just a populated one.
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "photos/a.jpg", Some(obj));
    insert_note(&conn, root, "other", "a note on an empty scope", 100);

    let params = test_params();
    let orchestration = test_orchestration();
    let run = run_survey(
        &mut conn,
        &["/mnt/drive/other".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    assert!(matches!(run.outcome, SurveyOutcome::Empty));
    let (ctx, _) = run
        .note_context
        .expect("note context must survive an empty-selection early exit");
    assert_eq!(ctx.subtree_notes.len(), 1);
}

#[test]
fn run_survey_note_context_survives_all_unhashed_outcome() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    insert_source(&conn, root, "a.jpg", None);
    insert_note(&conn, root, "", "a note on an unhashed scope", 100);

    let params = test_params();
    let orchestration = test_orchestration();
    let run = run_survey(
        &mut conn,
        &["/mnt/drive".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    assert!(matches!(
        run.outcome,
        SurveyOutcome::AllUnhashed { total_count: 1 }
    ));
    let (ctx, _) = run
        .note_context
        .expect("note context must survive an all-unhashed early exit");
    assert_eq!(ctx.subtree_notes.len(), 1);
}

#[test]
fn run_survey_rejects_other_identical_to_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "a.jpg", Some(obj));

    let params = test_params();
    let orchestration = SurveyOrchestration {
        other_paths: vec![PathBuf::from("/mnt/drive")],
        ..test_orchestration()
    };
    let err = match run_survey(
        &mut conn,
        &["/mnt/drive".to_string()],
        &[],
        &orchestration,
        &params,
    ) {
        Err(e) => e,
        Ok(_) => panic!("expected --other == scope to be rejected"),
    };

    assert_eq!(
        err.to_string(),
        "Error: --other location is identical to the surveyed scope. \
             Comparing a location to itself is not meaningful."
    );
}

#[test]
fn run_survey_sets_archive_label_from_archive_spec() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let archive = insert_root(&conn, "/archive/photos", "archive", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "a.jpg", Some(obj));
    insert_source(&conn, archive, "2024/a.jpg", Some(obj));

    let params = test_params();
    let orchestration = SurveyOrchestration {
        archive: Some(format!("id:{archive}")),
        ..test_orchestration()
    };
    let run = run_survey(
        &mut conn,
        &["/mnt/drive".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    match run.outcome {
        SurveyOutcome::Result(result) => {
            assert_eq!(result.archive_label.as_deref(), Some("in /archive/photos"));
        }
        _ => panic!("Expected SurveyOutcome::Result"),
    }
}

#[test]
fn run_survey_archive_spec_rejects_non_archive_root() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/mnt/drive", "source", false);
    let other_source = insert_root(&conn, "/mnt/backup", "source", false);
    let obj = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root, "a.jpg", Some(obj));

    let params = test_params();
    let orchestration = SurveyOrchestration {
        archive: Some(format!("id:{other_source}")),
        ..test_orchestration()
    };
    let err = match run_survey(
        &mut conn,
        &["/mnt/drive".to_string()],
        &[],
        &orchestration,
        &params,
    ) {
        Err(e) => e,
        Ok(_) => panic!("expected a non-archive root to be rejected"),
    };

    assert!(err.to_string().contains("expected 'archive'"), "{err:#}");
}

#[test]
fn run_survey_computes_location_note_counts_when_requested() {
    let mut conn = setup_test_db();
    let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
    let root_b = insert_root(&conn, "/mnt/backup", "source", false);
    let obj1 = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
    insert_source(&conn, root_b, "vacation/IMG_001.jpg", Some(obj1));
    insert_note(
        &conn,
        root_b,
        "vacation",
        "a note at the related location",
        100,
    );

    let params = test_params();
    let orchestration = SurveyOrchestration {
        want_location_note_counts: true,
        ..test_orchestration()
    };
    let run = run_survey(
        &mut conn,
        &["/mnt/drive-a".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    match run.outcome {
        SurveyOutcome::Result(result) => {
            assert_eq!(result.location_results.len(), 1);
            assert_eq!(result.location_results[0].path, "/mnt/backup/vacation");
            assert_eq!(
                run.location_note_counts.get("/mnt/backup/vacation"),
                Some(&1)
            );
        }
        _ => panic!("Expected SurveyOutcome::Result"),
    }
}

#[test]
fn run_survey_skips_location_note_counts_when_not_requested() {
    let mut conn = setup_test_db();
    let root_a = insert_root(&conn, "/mnt/drive-a", "source", false);
    let root_b = insert_root(&conn, "/mnt/backup", "source", false);
    let obj1 = insert_object(&conn, "hash_001", false);
    insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
    insert_source(&conn, root_b, "vacation/IMG_001.jpg", Some(obj1));
    insert_note(
        &conn,
        root_b,
        "vacation",
        "a note at the related location",
        100,
    );

    let params = test_params();
    let orchestration = test_orchestration(); // want_location_note_counts: false
    let run = run_survey(
        &mut conn,
        &["/mnt/drive-a".to_string()],
        &[],
        &orchestration,
        &params,
    )
    .unwrap();

    match run.outcome {
        SurveyOutcome::Result(_) => {
            assert!(run.location_note_counts.is_empty());
        }
        _ => panic!("Expected SurveyOutcome::Result"),
    }
}
