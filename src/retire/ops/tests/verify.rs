use crate::core::ops::root_story::fetch_root_story;
use crate::domain::config::RecordingMode;
use crate::repo;
use crate::repo::db::open_in_memory_for_test;
use crate::repo::insert_test_root;
use crate::retire::ops::frame::TellingArtifact;
use crate::retire::ops::{
    compile_book, readiness_lens, verify_book, CompileParams, ReleaseOutcome,
};
use crate::story::StoryParams;

use super::fixtures::{
    begin_with, compile_to, every_fate_fixture, insert_object, insert_source, test_telling,
};

#[test]
fn round_trip_law_verify_matches_the_compiled_db_state() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    let verified = verify_book(&dest).unwrap();
    assert_eq!(verified.entries, book.entry_count);
    assert_eq!(verified.entries, 8);

    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
    let counts = &meta["counts"];
    assert_eq!(counts["archived_from_here"].as_integer(), Some(2));
    assert_eq!(counts["covered"].as_integer(), Some(1));
    assert_eq!(counts["excluded"].as_integer(), Some(1));
    assert_eq!(counts["deleted"].as_integer(), Some(1));
    assert_eq!(counts["present"].as_integer(), Some(2));
    assert_eq!(counts["missing_unexplained"].as_integer(), Some(1));

    // The account and the book tell one story: covered bucket = the two
    // covered rows (one enriched into archived-from-here), unresolved =
    // the present entries, absent buckets match one-to-one.
    let story = fetch_root_story(&conn, root_id).unwrap();
    let review = readiness_lens(&story);
    assert_eq!(review.account.covered, 2);
    assert_eq!(review.account.excluded, 1);
    assert_eq!(review.account.unresolved, 2);
    assert_eq!(review.account.deleted, 1);
    assert_eq!(review.account.unexplained_missing, 1);
}

#[test]
fn inventory_lines_carry_the_fate_determining_decision() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);

    let id_of = |command: &str| -> i64 {
        conn.query_row(
            "SELECT id FROM decisions WHERE command = ?1",
            [command],
            |r| r.get(0),
        )
        .unwrap()
    };
    let lines = super::fixtures::inventory_lines(&dest);
    let by_path = |p: &str| {
        lines
            .iter()
            .find(|l| l["path"] == p)
            .unwrap_or_else(|| panic!("no line for {p}"))
            .clone()
    };

    // Archived-from-here points at the apply — row-backed and recovered
    // alike — not at the row's indexing stamp.
    let apply = id_of("apply");
    assert_eq!(by_path("copied.jpg")["decision"].as_i64(), Some(apply));
    assert_eq!(by_path("gone/moved.jpg")["decision"].as_i64(), Some(apply));
    // Excluded points at the stamping exclusion.
    assert_eq!(
        by_path("junk.jpg")["decision"].as_i64(),
        Some(id_of("exclude_set"))
    );
    // No recorded decision → the key is absent, never guessed.
    assert!(by_path("loose.jpg")["decision"].is_null());
}

#[test]
fn verify_book_catches_a_tampered_inventory() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);

    let inventory = std::fs::read_to_string(dest.join("inventory.jsonl")).unwrap();
    let truncated: Vec<&str> = inventory.lines().skip(1).collect();
    std::fs::write(dest.join("inventory.jsonl"), truncated.join("\n")).unwrap();

    let err = verify_book(&dest).unwrap_err();
    assert!(err.to_string().contains("disagree"));
}

#[test]
fn verify_book_requires_the_readme() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);

    std::fs::remove_file(dest.join("README.md")).unwrap();
    let err = verify_book(&dest).unwrap_err();
    assert!(err.to_string().contains("README.md is missing"));
}

#[test]
fn verify_book_catches_a_missing_gathered_ledger() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);
    assert!(book.ledger_files.is_none() || book.ledger_files == Some(0));

    // Claim a gathered ledger the directory doesn't hold.
    let meta = std::fs::read_to_string(dest.join("meta.toml")).unwrap();
    let tampered = meta.replace("files = 0", "files = 3");
    std::fs::write(dest.join("meta.toml"), tampered).unwrap();

    let err = verify_book(&dest).unwrap_err();
    assert!(err.to_string().contains("ledger disagrees"));
}

#[test]
fn the_compile_binds_and_claims_the_story() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);

    // The telling on disk, verbatim.
    assert_eq!(
        std::fs::read_to_string(dest.join("story.md")).unwrap(),
        "# a test telling\n"
    );
    // Claimed in meta, with the honesty marker and the reading settings.
    let meta = std::fs::read_to_string(dest.join("meta.toml")).unwrap();
    assert!(meta.contains("[story]"), "{meta}");
    assert!(meta.contains("file = \"story.md\""));
    assert!(meta.contains("hand_edited = false"));
    assert!(meta.contains("signature_tolerance"));
    // The README opens as a story's front door and bridges the words.
    let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
    assert!(readme.contains("Start with story.md"));
    assert!(readme.contains("- story.md — the story as told"));
    assert!(readme.contains("- chosen for the archive = archived"));
    assert!(readme.contains("- returned to consideration = restored"));
    assert!(readme.contains("- let go = excluded"));

    verify_book(&dest).unwrap();
}

#[test]
fn a_claimed_but_missing_or_empty_story_fails_verification() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);
    verify_book(&dest).unwrap();

    std::fs::remove_file(dest.join("story.md")).unwrap();
    let err = verify_book(&dest).unwrap_err();
    assert!(err.to_string().contains("story"), "{err}");

    std::fs::write(dest.join("story.md"), "").unwrap();
    let err = verify_book(&dest).unwrap_err();
    assert!(err.to_string().contains("story"), "{err}");
}

#[test]
fn a_book_without_a_story_claim_still_verifies() {
    // Pre-telling books carry no [story] table — they verify unchanged
    // (verification requires claimed artifacts, never retro-claims).
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let book_dir = tempfile::tempdir().unwrap();
    let dest = book_dir.path().join("book");
    let story = fetch_root_story(&conn, root_id).unwrap();
    compile_book(
        &conn,
        &story,
        &CompileParams {
            reason: None,
            now: 1_753_000_000,
            dest_dir: dest.clone(),
            ceremony_decision_id: None,
            telling: None,
        },
    )
    .unwrap();
    assert!(!dest.join("story.md").exists());
    let meta = std::fs::read_to_string(dest.join("meta.toml")).unwrap();
    assert!(!meta.contains("[story]"));
    verify_book(&dest).unwrap();
}

#[test]
fn a_hand_refined_telling_is_marked_in_the_meta() {
    // The honesty marker travels: a refined page is never passed off as
    // pure derivation, and the text binds verbatim either way.
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony
        .bind(
            &conn,
            TellingArtifact {
                text: "# my own words\n".to_string(),
                hand_edited: true,
                params: StoryParams::default(),
            },
        )
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(bound.dir.join("story.md")).unwrap(),
        "# my own words\n"
    );
    let meta = std::fs::read_to_string(bound.dir.join("meta.toml")).unwrap();
    assert!(meta.contains("hand_edited = true"), "{meta}");
    verify_book(&bound.dir).unwrap();
}

#[test]
fn the_telling_reads_the_ceremony_snapshot() {
    // The one-fetch law: the composed telling reflects the world the
    // ceremony fetched at review time — a concurrent write after `begin`
    // must not leak into the story the user confirms and binds.
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let ceremony = begin_with(&conn, root_id, RecordingMode::Full);

    insert_source(&conn, root_id, "late/arrival.jpg", None, true, false, None);

    let draft = ceremony.compose_telling(&conn).unwrap();
    assert!(
        !draft.contains("arrival.jpg") && !draft.contains("late/"),
        "a post-begin source leaked into the telling:\n{draft}"
    );
    assert!(draft.contains("## The places"));
    assert!(draft.contains("## Where everything went"));
}

#[test]
fn scale_ceremony_round_trips_past_the_chunking_boundary() {
    // The whole ceremony — bind (compile + verify + place) and release —
    // over a root past the SQL chunking boundary.
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let arch_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    let archive_id = insert_test_root(&conn, arch_dir.path().to_str().unwrap(), "archive", false);

    conn.execute_batch("BEGIN").unwrap();
    for i in 0..2000 {
        let object = insert_object(&conn, &format!("hash{i:05}"));
        insert_source(
            &conn,
            root_id,
            &format!("d{}/f{i:05}.jpg", i % 7),
            Some(object),
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive_id,
            &format!("a/f{i:05}.jpg"),
            Some(object),
            true,
            false,
            None,
        );
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();
    assert_eq!(bound.entry_count, 2000);
    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(bound.dir.join("meta.toml")).unwrap()).unwrap();
    assert_eq!(meta["counts"]["covered"].as_integer(), Some(2000));

    match ceremony.release(&conn).unwrap() {
        ReleaseOutcome::Released {
            deleted_sources, ..
        } => assert_eq!(deleted_sources, 2000),
        ReleaseOutcome::WorldMoved { detail, .. } => panic!("world moved: {detail}"),
    }
    assert!(!repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
}

#[test]
fn existing_compile_target_is_an_explicit_collision() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);

    let dest = book_dir.path().join("book");
    std::fs::create_dir(&dest).unwrap();
    let story = fetch_root_story(&conn, root_id).unwrap();
    let err = compile_book(
        &conn,
        &story,
        &CompileParams {
            reason: None,
            now: 0,
            dest_dir: dest,
            ceremony_decision_id: None,
            telling: None,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("already exists"));
}
