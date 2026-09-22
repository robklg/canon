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

    assert!(history_evidence_at(&conn, "/photos/gone", &roots(&conn))
        .unwrap()
        .is_some());
}

#[test]
fn a_note_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_note_at(&conn, root_id, "thought-about", 1000);

    assert!(
        history_evidence_at(&conn, "/photos/thought-about", &roots(&conn))
            .unwrap()
            .is_some()
    );
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

    assert!(history_evidence_at(&conn, "/photos/emptied", &roots(&conn))
        .unwrap()
        .is_some());
}

/// The destination endpoint is matched on the absolute write-time snapshot
/// path rather than on `destination_root_id`, so it answers even when that
/// id no longer resolves. It cannot answer for a path under no known root
/// at all — `history_evidence_at` answers `None` before the query, and
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
    assert!(
        history_evidence_at(&conn, "/archive/media/2014", &roots(&conn))
            .unwrap()
            .is_some()
    );
    // And the containing directory, which the row's location sits under.
    assert!(history_evidence_at(&conn, "/archive/media", &roots(&conn))
        .unwrap()
        .is_some());
}

#[test]
fn a_descendant_decision_scope_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let decision_id = insert_decision_at(&conn, "exclude", 1000);
    scope(&conn, decision_id, root_id, "2011/summer");

    // Asked at the scope itself, and at a directory containing it.
    assert!(
        history_evidence_at(&conn, "/photos/2011/summer", &roots(&conn))
            .unwrap()
            .is_some()
    );
    assert!(history_evidence_at(&conn, "/photos/2011", &roots(&conn))
        .unwrap()
        .is_some());
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

    assert!(
        history_evidence_at(&conn, "/archive/media/x/191", &roots(&conn))
            .unwrap()
            .is_none()
    );
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

        assert!(history_evidence_at(&conn, "/photos/a_b", &roots(&conn))
            .unwrap()
            .is_some());
        assert!(history_evidence_at(&conn, "/photos/100%", &roots(&conn))
            .unwrap()
            .is_some());
        assert!(history_evidence_at(&conn, "/photos/or_igin", &roots(&conn))
            .unwrap()
            .is_some());
        assert!(
            history_evidence_at(&conn, "/photos/dest%dir", &roots(&conn))
                .unwrap()
                .is_some()
        );
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
        assert!(history_evidence_at(&conn, "/photos/a_b", &roots(&conn))
            .unwrap()
            .is_none());
        assert!(history_evidence_at(&conn, "/photos/100%", &roots(&conn))
            .unwrap()
            .is_none());
        assert!(history_evidence_at(&conn, "/photos/or_igin", &roots(&conn))
            .unwrap()
            .is_none());
        assert!(
            history_evidence_at(&conn, "/photos/dest%dir", &roots(&conn))
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn a_place_under_no_known_root_has_no_evidence() {
    let conn = setup_test_db();
    insert_root(&conn, "/photos", "source", false);
    assert!(history_evidence_at(&conn, "/elsewhere", &roots(&conn))
        .unwrap()
        .is_none());
}

/// The chain's own answer: a place nothing records is stated, not rendered.
#[test]
fn an_unknown_path_is_stated_not_rendered() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root_id, "2011/a.jpg", None);

    assert!(matches!(
        place_knowledge(&conn, "/photos/2011", &roots(&conn)).unwrap(),
        PlaceKnowledge::Evidence(_)
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
        PlaceKnowledge::Evidence(_)
    ));
}

// =========================================================================
// The form-tolerance law at the evidence gate
// =========================================================================

const NFC_DIR: &str = "caf\u{e9}";
const NFD_DIR: &str = "cafe\u{301}";

/// The gate's remainder half cannot be the *sources* one. This place has no
/// sources at all — which is the case the gate exists for — so a heal that
/// asked about sources would be a no-op here and the note recorded in the
/// other byte-form would go unseen: a false "nothing records this place".
#[test]
fn a_note_in_the_other_form_is_still_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_note_at(&conn, root_id, NFD_DIR, 1000);

    assert!(
        !repo::source::sources_exist_at_scope(&conn, root_id, NFD_DIR).unwrap(),
        "the fixture must leave the place sourceless, or the sources arm answers"
    );
    assert_eq!(
        history_evidence_at(&conn, &format!("/photos/{NFC_DIR}"), &roots(&conn)).unwrap(),
        Some(format!("/photos/{NFD_DIR}"))
    );
}

/// The same for an extraction endpoint — the class an emptied place keeps.
#[test]
fn an_extraction_origin_in_the_other_form_is_still_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 1000);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root_id,
            "/photos",
            NFD_DIR,
            3,
            Some(30),
            "/archive/media",
        )],
    )
    .unwrap();

    assert_eq!(
        history_evidence_at(&conn, &format!("/photos/{NFC_DIR}"), &roots(&conn)).unwrap(),
        Some(format!("/photos/{NFD_DIR}"))
    );
}

/// And the root half, which this arm never had: a root stored in one form,
/// asked about in the other.
#[test]
fn the_gate_finds_a_root_stored_in_the_other_form() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, &format!("/mnt/{NFD_DIR}"), "source", false);
    insert_source(&conn, root_id, "2016/a.jpg", None);

    assert_eq!(
        history_evidence_at(&conn, &format!("/mnt/{NFC_DIR}/2016"), &roots(&conn)).unwrap(),
        Some(format!("/mnt/{NFD_DIR}/2016"))
    );
}

/// **The gate answers with the form it found, and the view is read at that
/// form.** A gate that healed internally and returned a bare yes would open
/// on a place and then render none of it — a third silent class beside the
/// two this pass closes.
#[test]
fn the_gate_hands_back_the_form_it_found_the_evidence_under() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);

    let PlaceKnowledge::Evidence(found) =
        place_knowledge(&conn, &format!("/photos/{NFC_DIR}"), &roots(&conn)).unwrap()
    else {
        panic!("the place is recorded");
    };
    assert_eq!(found, format!("/photos/{NFD_DIR}"));
}

/// The floor, unmoved: a place no byte-form records is still unknown.
#[test]
fn a_place_no_form_records_stays_unknown() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);

    assert!(matches!(
        place_knowledge(
            &conn,
            &format!("/photos/{NFC_DIR}-elsewhere"),
            &roots(&conn)
        )
        .unwrap(),
        PlaceKnowledge::Unknown
    ));
}

/// **A root top answers from its evidence, like every other place.** The
/// remainder half's *sources* reading keeps an empty remainder for its own
/// reason — a root is valid whether or not anything was scanned into it — and
/// that reason is false for this gate, whose question is whether anything
/// records the place. A loop answering for it would report every registered
/// root as having a history with no class of evidence consulted, and the
/// counterpart door, which takes no root-top exemption, would open on it.
#[test]
fn an_empty_root_top_records_nothing_and_says_so() {
    let conn = setup_test_db();
    insert_root(&conn, "/photos", "source", false);

    assert_eq!(
        history_evidence_at(&conn, "/photos", &roots(&conn)).unwrap(),
        None
    );
    assert!(matches!(
        place_knowledge(&conn, "/photos", &roots(&conn)).unwrap(),
        PlaceKnowledge::Unknown
    ));
}

/// And it answers *yes* from any one of the four classes at the root top.
///
/// The over-correction this guards is the opposite of the one beside it: a
/// root top whose evidence is real must still be found. It does **not** show
/// that the gate is asked rather than skipped there — a loop that answered
/// yes for every empty remainder would pass this too, which is what
/// `an_empty_root_top_records_nothing_and_says_so` is for.
#[test]
fn a_root_top_with_a_note_is_history_evidence() {
    let conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    insert_note_at(&conn, root_id, "", 1000);

    assert_eq!(
        history_evidence_at(&conn, "/photos", &roots(&conn)).unwrap(),
        Some("/photos".to_string())
    );
}
