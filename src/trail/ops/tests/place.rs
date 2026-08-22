//! The evidence gate: what makes a place one the trail will speak about.

use crate::core::domain::root::Root;
use crate::core::repo;
use crate::core::repo::Connection;
use crate::core::testing::{insert_root, insert_source, setup_test_db};
use crate::trail::ops::place::{history_evidence_at, place_knowledge, PlaceKnowledge};

use super::fixtures::{extraction_row, insert_decision_at, insert_note_at, scope};

fn roots(conn: &Connection) -> Vec<Root> {
    repo::root::fetch_all(conn).unwrap()
}

/// A source that once stood here is evidence, present or not — a deleted
/// file's place still has a story.
#[test]
fn an_absent_source_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root_id, "gone/a.jpg", None);
    repo::source::mark_missing(&conn, &[id], 1000, None).unwrap();

    assert!(history_evidence_at(&conn, "/photos/gone", &roots(&conn)).unwrap());
}

#[test]
fn a_note_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_note_at(&conn, root_id, "thought-about", 1000);

    assert!(history_evidence_at(&conn, "/photos/thought-about", &roots(&conn)).unwrap());
}

/// An emptied place — a move-mode apply took everything — is exactly what
/// the trail's leniency was built for, and the extraction origin is what
/// keeps it answering.
#[test]
fn an_extraction_origin_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 1000);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root_id,
            "/photos",
            "emptied",
            3,
            Some(30),
            "/archive/media",
        )],
    )
    .unwrap();

    assert!(history_evidence_at(&conn, "/photos/emptied", &roots(&conn)).unwrap());
}

/// The destination endpoint is matched on the absolute write-time snapshot
/// path rather than on `destination_root_id`, so it answers even when that
/// id no longer resolves. It cannot answer for a path under no known root
/// at all — `history_evidence_at` returns false before the query, and
/// `validate_paths_in_roots` refuses such a path earlier still.
#[test]
fn an_extraction_destination_is_matched_on_its_snapshot_path() {
    let conn = setup_test_db();
    let source_id = insert_root(&conn, "/photos", "source", false);
    insert_root(&conn, "/archive", "archive", false);
    let decision_id = insert_decision_at(&conn, "apply", 1000);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            source_id,
            "/photos",
            "from",
            3,
            Some(30),
            "/archive/media/2014",
        )],
    )
    .unwrap();

    // The row's `destination_root_id` is a stale id no root carries; the
    // snapshot path is what answers.
    assert!(history_evidence_at(&conn, "/archive/media/2014", &roots(&conn)).unwrap());
    // And the containing directory, which the row's location sits under.
    assert!(history_evidence_at(&conn, "/archive/media", &roots(&conn)).unwrap());
}

#[test]
fn a_descendant_decision_scope_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let decision_id = insert_decision_at(&conn, "exclude", 1000);
    scope(&conn, decision_id, root_id, "2011/summer");

    // Asked at the scope itself, and at a directory containing it.
    assert!(history_evidence_at(&conn, "/photos/2011/summer", &roots(&conn)).unwrap());
    assert!(history_evidence_at(&conn, "/photos/2011", &roots(&conn)).unwrap());
}

/// The two-claims placement law, in the direction that made `canon trail 191`
/// render a plausible view: a decision scoped at an ancestor is a claim about
/// the ancestor, never about every path anyone might type beneath it.
#[test]
fn an_ancestor_scope_is_never_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/archive", "archive", false);
    let decision_id = insert_decision_at(&conn, "apply", 1000);
    scope(&conn, decision_id, root_id, "media");

    assert!(!history_evidence_at(&conn, "/archive/media/x/191", &roots(&conn)).unwrap());
}

/// The containment spelling is a string comparison, never a LIKE pattern:
/// `_` and `%` in a real folder name must match themselves.
///
/// One case per query the gate can reach, because each is its own SQL and a
/// hand-spelled `LIKE` in any one would be invisible to the others' coverage.
/// The positives alone prove nothing — a wildcard spelling matches a literal
/// name too. What carries the guard is the second fixture, where the only
/// evidence sits at a *different* place that a wildcard reading would sweep
/// in: `alpha_beta` must not borrow `alphaXbeta`'s history.
#[test]
fn evidence_matching_treats_wildcard_bytes_as_literal() {
    // Real names, with their wildcard bytes, answer for themselves.
    {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let decision_id = insert_decision_at(&conn, "exclude", 1000);
        scope(&conn, decision_id, root_id, "a_b");
        insert_note_at(&conn, root_id, "100%", 1000);
        let apply_id = insert_decision_at(&conn, "apply", 1000);
        repo::decision::replace_extractions(
            &conn,
            &[extraction_row(
                apply_id,
                root_id,
                "/photos",
                "or_igin",
                1,
                Some(10),
                "/photos/dest%dir",
            )],
        )
        .unwrap();

        assert!(history_evidence_at(&conn, "/photos/a_b", &roots(&conn)).unwrap());
        assert!(history_evidence_at(&conn, "/photos/100%", &roots(&conn)).unwrap());
        assert!(history_evidence_at(&conn, "/photos/or_igin", &roots(&conn)).unwrap());
        assert!(history_evidence_at(&conn, "/photos/dest%dir", &roots(&conn)).unwrap());
    }

    // The guard: every piece of evidence sits somewhere a wildcard reading
    // of the asked path would reach, and nowhere the literal path does.
    {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let decision_id = insert_decision_at(&conn, "exclude", 1000);
        scope(&conn, decision_id, root_id, "aXb");
        insert_note_at(&conn, root_id, "1005", 1000);
        let apply_id = insert_decision_at(&conn, "apply", 1000);
        repo::decision::replace_extractions(
            &conn,
            &[extraction_row(
                apply_id,
                root_id,
                "/photos",
                "orXigin",
                1,
                Some(10),
                "/photos/destZZdir",
            )],
        )
        .unwrap();

        // `_` and `%` are path bytes, not patterns — none of these places
        // is the one the evidence is about.
        assert!(!history_evidence_at(&conn, "/photos/a_b", &roots(&conn)).unwrap());
        assert!(!history_evidence_at(&conn, "/photos/100%", &roots(&conn)).unwrap());
        assert!(!history_evidence_at(&conn, "/photos/or_igin", &roots(&conn)).unwrap());
        assert!(!history_evidence_at(&conn, "/photos/dest%dir", &roots(&conn)).unwrap());
    }
}

#[test]
fn a_place_under_no_known_root_has_no_evidence() {
    let conn = setup_test_db();
    insert_root(&conn, "/photos", "source", false);
    assert!(!history_evidence_at(&conn, "/elsewhere", &roots(&conn)).unwrap());
}

/// The chain's own answer: a place nothing records is stated, not rendered.
#[test]
fn an_unknown_path_is_stated_not_rendered() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root_id, "2011/a.jpg", None);

    assert!(matches!(
        place_knowledge(&conn, "/photos/2011", &roots(&conn)).unwrap(),
        PlaceKnowledge::Evidence
    ));
    assert!(matches!(
        place_knowledge(&conn, "/photos/191", &roots(&conn)).unwrap(),
        PlaceKnowledge::Unknown
    ));
}

/// The emptied-place guard: what the leniency was built for keeps working.
#[test]
fn a_sourceless_path_with_evidence_still_renders_its_history() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 1000);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root_id,
            "/photos",
            "emptied",
            3,
            Some(30),
            "/archive/media",
        )],
    )
    .unwrap();

    // No sources stand here at all…
    assert!(
        !repo::source::sources_exist_at_scope(&conn, root_id, "emptied").unwrap(),
        "the fixture must leave the place empty"
    );
    // …and the trail still knows it.
    assert!(matches!(
        place_knowledge(&conn, "/photos/emptied", &roots(&conn)).unwrap(),
        PlaceKnowledge::Evidence
    ));
}
