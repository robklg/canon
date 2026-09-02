use rusqlite::Connection;

use crate::core::domain::config::RecordingMode;
use crate::core::domain::decision::Decision;
use crate::core::ops::root_story::fetch_root_story;
use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::retire::domain::book_dir_name;
use crate::retire::ops::ceremony::BindPlan;
use crate::retire::ops::verify::verify_book;
use crate::retire::ops::{iso_date, plan_bind, ReleaseOutcome, RetireCeremony, SHELF_DIR};

use super::fixtures::{
    begin_with, compile_to, every_fate_fixture, insert_decision, insert_source, ledger_config,
    scope, set_decision_extras, test_telling, CEREMONY_NOW,
};

fn plan_for(conn: &Connection, root_id: i64) -> BindPlan {
    let story = fetch_root_story(conn, root_id).unwrap();
    plan_bind(&story, &ledger_config(), CEREMONY_NOW).unwrap()
}

fn count_retire_decisions(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM decisions WHERE command = 'roots_retire'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

/// A directory that identifies itself as some root's book — just enough
/// meta.toml for the collision probe.
fn fake_book(shelf: &std::path::Path, name: &str, root_path: &str) {
    let dir = shelf.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("meta.toml"),
        format!("version = 1\n\n[identity]\npath = \"{root_path}\"\n"),
    )
    .unwrap();
}

#[test]
fn plan_bind_fresh_shelf_takes_the_plain_name() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let story = fetch_root_story(&conn, root_id).unwrap();

    let plan = plan_for(&conn, root_id);

    let shelf = arch.path().join(SHELF_DIR);
    assert_eq!(plan.shelf, shelf);
    assert!(!plan.shelf_exists);
    assert!(!plan.replaces_existing);
    let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
    assert_eq!(plan.final_dir, shelf.join(&name));
    assert_eq!(plan.temp_dir, shelf.join(format!(".compiling-{name}")));
}

#[test]
fn plan_bind_same_root_book_is_a_replacement() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let story = fetch_root_story(&conn, root_id).unwrap();
    let shelf = arch.path().join(SHELF_DIR);
    let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
    fake_book(&shelf, &name, &story.root.path);

    let plan = plan_for(&conn, root_id);

    assert!(plan.replaces_existing);
    assert_eq!(plan.final_dir, shelf.join(&name));
}

#[test]
fn plan_bind_different_root_book_probes_to_a_sibling_name() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let story = fetch_root_story(&conn, root_id).unwrap();
    let shelf = arch.path().join(SHELF_DIR);
    let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
    fake_book(&shelf, &name, "/somebody/else");

    let plan = plan_for(&conn, root_id);
    assert!(!plan.replaces_existing);
    assert_eq!(plan.final_dir, shelf.join(format!("{name}-2")));

    // A second stranger pushes to -3.
    fake_book(&shelf, &format!("{name}-2"), "/a/third/party");
    let plan = plan_for(&conn, root_id);
    assert_eq!(plan.final_dir, shelf.join(format!("{name}-3")));
}

#[test]
fn plan_bind_refuses_an_unidentifiable_directory() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let story = fetch_root_story(&conn, root_id).unwrap();
    let shelf = arch.path().join(SHELF_DIR);
    let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
    // A directory at the book's name with no meta.toml at all.
    std::fs::create_dir_all(shelf.join(&name)).unwrap();

    let err = plan_bind(
        &fetch_root_story(&conn, root_id).unwrap(),
        &ledger_config(),
        CEREMONY_NOW,
    )
    .unwrap_err();
    assert!(err.to_string().contains("refusing to replace"), "{err:#}");
}

#[test]
fn bind_places_a_verified_book_and_records_the_pointer() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    assert!(bound.dir.is_dir());
    assert_eq!(bound.entry_count, 8);
    assert!(!bound.replaced_previous);
    assert!(bound.warnings.is_empty(), "{:?}", bound.warnings);
    verify_book(&bound.dir).unwrap();
    assert!(arch.path().join(SHELF_DIR).join("README.md").is_file());
    // No compile residue on the shelf.
    assert!(!ceremony.plan.temp_dir.exists());

    // The decision is still open (release hasn't run) but the pointer is
    // already recorded — abort-after-bind stays findable.
    //
    // `started` is correct here and must not be "fixed" by a later sweep of
    // this class: the ceremony has not reached its last act. Mid-run is
    // precisely the state the word is reserved for.
    let decision_id = ceremony.recorder.decision_id().unwrap();
    let decision = repo::decision::fetch_by_id(&conn, decision_id)
        .unwrap()
        .unwrap();
    assert_eq!(decision.status, "started");
    let name = bound.dir.file_name().unwrap().to_string_lossy();
    assert_eq!(
        decision.receipt_rel_path.as_deref(),
        Some(format!("{SHELF_DIR}/{name}").as_str())
    );
    let (arch_id, _) = crate::core::ops::receipt::resolve_ledger_root(
        &fetch_root_story(&conn, root_id).unwrap().roots,
        &ledger_config(),
    )
    .unwrap();
    assert_eq!(decision.receipt_root_id, Some(arch_id));

    // The book names the decision that bound it — the index reference is
    // readable from the shelf alone.
    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(bound.dir.join("meta.toml")).unwrap()).unwrap();
    assert_eq!(
        meta["identity"]["decision_id"].as_integer(),
        Some(decision_id)
    );
}

#[test]
fn timeline_indents_multi_line_summaries_and_reasons() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    insert_source(&conn, root_id, "a.jpg", None, true, false, None);
    let scan = insert_decision(&conn, "scan", 100);
    scope(&conn, scan, root_id);
    set_decision_extras(
        &conn,
        scan,
        Some("Scanned 4 files: 4 new\nHashed 7 files"),
        Some("first pass\nsecond thoughts, still open"),
    );

    let dest = book_dir.path().join("book");
    compile_to(&conn, root_id, &dest);

    let timeline = std::fs::read_to_string(dest.join("timeline.md")).unwrap();
    assert!(
        timeline.contains(": Scanned 4 files: 4 new\n  Hashed 7 files\n"),
        "{timeline}"
    );
    assert!(
        timeline.contains("  reason: first pass\n  second thoughts, still open\n"),
        "{timeline}"
    );
}

#[test]
fn bind_keeps_its_own_in_flight_decision_out_of_the_timeline() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    let timeline = std::fs::read_to_string(bound.dir.join("timeline.md")).unwrap();
    assert!(!timeline.contains("roots_retire"), "{timeline}");
}

#[test]
fn a_prior_attempts_decision_renders_in_the_next_books_timeline() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut first = begin_with(&conn, root_id, RecordingMode::Full);
    first.bind(&conn, test_telling()).unwrap();
    first.abandon(&conn);

    let mut second = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = second.bind(&conn, test_telling()).unwrap();

    // The abandoned attempt is history and narrates itself; only the
    // current in-flight decision stays out.
    let timeline = std::fs::read_to_string(bound.dir.join("timeline.md")).unwrap();
    assert!(timeline.contains("release declined"), "{timeline}");
    let own_id = second.recorder.decision_id().unwrap();
    assert!(!timeline.contains(&format!("#{own_id} ")), "{timeline}");
}

#[test]
fn bind_clears_a_leftover_temp_compile() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    std::fs::create_dir_all(&ceremony.plan.temp_dir).unwrap();
    std::fs::write(ceremony.plan.temp_dir.join("junk.txt"), "leftover").unwrap();

    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    verify_book(&bound.dir).unwrap();
    assert!(!ceremony.plan.temp_dir.exists());
}

#[test]
fn shelf_readme_is_written_once_and_kept() {
    let (conn, _src, arch, root_id) = every_fate_fixture();
    let shelf = arch.path().join(SHELF_DIR);
    std::fs::create_dir_all(&shelf).unwrap();
    std::fs::write(shelf.join("README.md"), "my own shelf notes\n").unwrap();

    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    ceremony.bind(&conn, test_telling()).unwrap();

    let readme = std::fs::read_to_string(shelf.join("README.md")).unwrap();
    assert_eq!(readme, "my own shelf notes\n");
}

#[test]
fn bind_replaces_a_standing_same_root_book_without_residue() {
    let (conn, _src, arch, root_id) = every_fate_fixture();

    // First ceremony binds, then is abandoned before release.
    let mut first = begin_with(&conn, root_id, RecordingMode::Full);
    let first_book = first.bind(&conn, test_telling()).unwrap();
    let sentinel = first_book.dir.join("meta.toml");
    let first_meta = std::fs::read_to_string(&sentinel).unwrap();

    // The re-run converges: same name, fresh compile, old book gone.
    let mut second = begin_with(&conn, root_id, RecordingMode::Full);
    assert!(second.plan.replaces_existing);
    let second_book = second.bind(&conn, test_telling()).unwrap();

    assert!(second_book.replaced_previous);
    assert_eq!(second_book.dir, first_book.dir);
    verify_book(&second_book.dir).unwrap();
    // Freshly written: the standing book names the *second* binding's
    // decision — the old id is exactly the residue that must be gone —
    // and everything else is identical (the compile stamps the same now).
    let second_meta = std::fs::read_to_string(&sentinel).unwrap();
    let first_id = first.recorder.decision_id().unwrap();
    let second_id = second.recorder.decision_id().unwrap();
    assert!(second_meta.contains(&format!("decision_id = {second_id}")));
    assert!(!second_meta.contains(&format!("decision_id = {first_id}")));
    assert_eq!(
        second_meta.replace(
            &format!("decision_id = {second_id}"),
            &format!("decision_id = {first_id}")
        ),
        first_meta
    );

    // No swap residue on the shelf: only the book and the README.
    let shelf = arch.path().join(SHELF_DIR);
    let mut names: Vec<String> = std::fs::read_dir(&shelf)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let book_name = second_book.dir.file_name().unwrap().to_string_lossy();
    assert_eq!(names, vec!["README.md".to_string(), book_name.to_string()]);
}

#[test]
fn bind_failure_at_placement_leaves_the_verified_temp_standing() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

    // The plan saw a free final name; an interloper appears before the
    // rename (a concurrent process racing the shelf). Placement must
    // fail, and fail without touching anything.
    std::fs::create_dir_all(&ceremony.plan.final_dir).unwrap();
    std::fs::write(ceremony.plan.final_dir.join("interloper.txt"), "mine").unwrap();

    let err = ceremony.bind(&conn, test_telling()).unwrap_err();
    assert!(err.to_string().contains("Could not place"), "{err:#}");

    // The verified compile still stands at its temp name for inspection…
    verify_book(&ceremony.plan.temp_dir).unwrap();
    // …and the interloper is untouched.
    let interloper = ceremony.plan.final_dir.join("interloper.txt");
    assert_eq!(std::fs::read_to_string(interloper).unwrap(), "mine");
}

#[test]
fn bind_under_recording_off_places_the_book_and_indexes_nothing() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Off);

    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    verify_book(&bound.dir).unwrap();
    assert!(bound.warnings.is_empty(), "{:?}", bound.warnings);
    assert_eq!(count_retire_decisions(&conn), 0);
}

// The ceremony: release, abandon, world-moved

fn retire_decision_row(conn: &Connection, ceremony: &RetireCeremony) -> Decision {
    repo::decision::fetch_by_id(conn, ceremony.recorder.decision_id().unwrap())
        .unwrap()
        .unwrap()
}

#[test]
fn full_ceremony_releases_the_root_and_completes_the_decision() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();
    let rows_before = crate::retire::repo::count_all_by_root(&conn, root_id).unwrap();

    // Between review and release, the only new world state is the
    // ceremony's own decision and its scope row — releasing cleanly here
    // is the self-exclusion regression test.
    let outcome = ceremony.release(&conn).unwrap();

    let ReleaseOutcome::Released {
        deleted_sources,
        summary,
        warnings,
        ..
    } = outcome
    else {
        panic!("expected Released");
    };
    assert_eq!(deleted_sources, rows_before);
    assert!(warnings.is_empty(), "{warnings:?}");
    let root_path = &ceremony.story.root.path;
    assert!(summary.contains(&format!("Retired {root_path}")));
    assert!(summary.contains("the story is bound at"));
    assert!(summary.contains(bound.dir.to_str().unwrap()));

    // The root and its rows are gone; the book stands.
    assert!(!repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
    assert_eq!(
        crate::retire::repo::count_all_by_root(&conn, root_id).unwrap(),
        0
    );
    verify_book(&bound.dir).unwrap();

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "completed");
    assert_eq!(decision.count_attempted, Some(rows_before));
    assert_eq!(decision.count_completed, Some(rows_before));
    assert_eq!(decision.summary.as_deref(), Some(summary.as_str()));
}

/// **Retirement's projection of the status conjugation**, examined: the ceremony
/// registers its row only after the first confirmation, and every returning exit
/// settles that row at its last act — `interrupt` for a bind that produced no
/// book, and after one stands, this one, `abandon_on_prompt_failure` and
/// `release`'s three arms. The conjugation is owned by
/// `core/ops/decision.rs`; the ceremony supplies the word, because which outcome
/// a declined release deserves is the caller's knowledge. The `partial`
/// assertion below is that projection's pin.
///
/// **The coverage claim is structural, not a list**: `release`'s four internal
/// `?` — opening the immediate transaction, the world-moved probe, the removal,
/// the commit — all leave through the wrapper's single `Err` arm, so a `?` added
/// inside the body settles without anyone remembering to name it. The two
/// directly drivable ones are pinned by
/// `a_release_that_cannot_begin_is_never_left_started` and
/// `a_release_whose_removal_fails_settles_after_the_rollback`.
#[test]
fn abandon_after_bind_leaves_root_and_book_standing() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    let abandoned = ceremony.abandon(&conn);

    assert!(abandoned.summary.contains("release declined"));
    assert!(abandoned.summary.contains(bound.dir.to_str().unwrap()));
    // Root intact, book standing — the safety invariant's abort arm.
    assert!(repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
    verify_book(&bound.dir).unwrap();

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "partial");

    // The rm guard now reads the pointer: the story is already bound.
    let plan = crate::roots::plan_remove(&conn, root_id).unwrap();
    let pointer = plan.retirement.expect("retirement pointer");
    assert!(pointer.artifact_display.contains("retired/"));
}

#[test]
fn release_stops_when_a_source_row_appeared_since_review() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    // A concurrent scan indexed a new file between review and release.
    insert_source(&conn, root_id, "new/arrival.jpg", None, true, false, None);

    let outcome = ceremony.release(&conn).unwrap();
    let ReleaseOutcome::WorldMoved { detail, .. } = outcome else {
        panic!("expected WorldMoved");
    };
    assert!(detail.contains("source rows"), "{detail}");

    // Root intact (the transaction wrote nothing), book standing,
    // decision partial — never both partial.
    assert!(repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
    verify_book(&bound.dir).unwrap();
    assert_eq!(retire_decision_row(&conn, &ceremony).status, "partial");
}

#[test]
fn release_stops_when_a_foreign_decision_touched_the_root() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    // Another process's decision landed a scope row on this root.
    let foreign = insert_decision(&conn, "exclude_set", 9_999);
    scope(&conn, foreign, root_id);

    let outcome = ceremony.release(&conn).unwrap();
    let ReleaseOutcome::WorldMoved { detail, .. } = outcome else {
        panic!("expected WorldMoved");
    };
    assert!(detail.contains("another decision"), "{detail}");
    assert!(repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
    verify_book(&bound.dir).unwrap();
}

#[test]
fn off_then_full_rerun_converges_from_disk() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();

    // First ceremony under Off: the book binds, nothing is indexed.
    let mut first = begin_with(&conn, root_id, RecordingMode::Off);
    let first_book = first.bind(&conn, test_telling()).unwrap();
    first.abandon(&conn);
    assert_eq!(count_retire_decisions(&conn), 0);

    // The re-run under Full finds the standing book on disk — collision
    // detection is meta.toml-keyed, so no decision row is needed.
    let mut second = begin_with(&conn, root_id, RecordingMode::Full);
    assert!(second.plan.replaces_existing);
    let second_book = second.bind(&conn, test_telling()).unwrap();
    assert_eq!(second_book.dir, first_book.dir);

    let outcome = second.release(&conn).unwrap();
    assert!(matches!(outcome, ReleaseOutcome::Released { .. }));
    let decision = retire_decision_row(&conn, &second);
    assert_eq!(decision.status, "completed");
    assert!(decision.receipt_rel_path.unwrap().starts_with("retired/"));
}

#[test]
fn interrupt_records_a_findable_interrupted_decision() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    // Force a placement failure: an interloper at the final name.
    std::fs::create_dir_all(&ceremony.plan.final_dir).unwrap();
    std::fs::write(ceremony.plan.final_dir.join("interloper.txt"), "mine").unwrap();
    let err = ceremony.bind(&conn, test_telling()).unwrap_err();

    ceremony.interrupt(&conn, &format!("{err:#}"));

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "interrupted");
    assert!(decision
        .summary
        .unwrap()
        .contains("Retirement interrupted during bind"));
    // The root is untouched.
    assert!(repo::root::fetch_all(&conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
}

// The release window: every exit after the bind settles the row

/// Drive a release that is expected to fail, and hand back the error.
/// `ReleaseOutcome` carries no `Debug`, so `unwrap_err` is unavailable — and
/// an unexpected *success* here deserves its own message rather than a
/// formatting bound on a production type.
fn release_must_fail(ceremony: &mut RetireCeremony, conn: &Connection) -> anyhow::Error {
    match ceremony.release(conn) {
        Ok(_) => panic!("the release was expected to fail"),
        Err(e) => e,
    }
}

/// The book stands and the root is still indexed — the standing every
/// non-releasing exit leaves behind, asserted the same way for each of them.
fn root_and_book_still_stand(conn: &Connection, root_id: i64, book: &std::path::Path) {
    assert!(repo::root::fetch_all(conn)
        .unwrap()
        .iter()
        .any(|r| r.id == root_id));
    verify_book(book).unwrap();
}

/// Site 1 — `BEGIN IMMEDIATE` fails, before anything at all is written.
///
/// **The substitution is stated, not hidden.** In production the cause is a
/// second canon process holding the write lock; this test substitutes a
/// transaction already open on the same connection, which fires the same `?`
/// deterministically and in-process — no second connection, no 30-second busy
/// timeout. Same site, same exit, different cause: this is *not* a contention
/// test, and must not be read as one.
#[test]
fn a_release_that_cannot_begin_is_never_left_started() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    conn.execute_batch("BEGIN").unwrap();
    let err = release_must_fail(&mut ceremony, &conn);
    // Close the ambient transaction so the settling UPDATE it swallowed is
    // durable. (The assertions below read on this same connection, which would
    // see the write either way — committing is hygiene, not a precondition.)
    conn.execute_batch("COMMIT").unwrap();

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "partial");
    let summary = decision.summary.expect("a settled row carries its summary");
    assert!(summary.contains("release failed"), "{summary}");
    assert!(summary.contains(&format!("{err:#}")), "{summary}");
    root_and_book_still_stand(&conn, root_id, &bound.dir);
}

/// Site 3 — the removal fails *inside* the transaction, so the settlement has
/// to outlive the rollback. `remove_root_data` deletes notes first and the
/// world-moved probe never reads that table, so dropping it lands the failure
/// past the probe and inside the transaction: that placement is what makes
/// this the rollback case rather than site 1 again.
#[test]
fn a_release_whose_removal_fails_settles_after_the_rollback() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    let bound = ceremony.bind(&conn, test_telling()).unwrap();

    conn.execute_batch("DROP TABLE notes").unwrap();
    let err = release_must_fail(&mut ceremony, &conn);

    // Written on the connection after the transaction was dropped — a
    // settlement written inside it would have rolled back with the removal.
    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "partial");
    let summary = decision.summary.expect("a settled row carries its summary");
    assert!(summary.contains("release failed"), "{summary}");
    assert!(summary.contains(&format!("{err:#}")), "{summary}");
    // The removal itself rolled back: the root is whole, not half-deleted.
    root_and_book_still_stand(&conn, root_id, &bound.dir);
}

/// The composer is the sentence's one home: every exit that leaves the book
/// bound and the root indexed speaks one standing, and differs from its
/// siblings only in the parenthetical that names what happened.
#[test]
fn every_unreleased_exit_speaks_one_standing() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();

    let summary_of = |conn: &Connection, ceremony: &RetireCeremony| -> String {
        retire_decision_row(conn, ceremony)
            .summary
            .expect("a settled row carries its summary")
    };

    // The declined confirmation.
    let mut declined = begin_with(&conn, root_id, RecordingMode::Full);
    declined.bind(&conn, test_telling()).unwrap();
    let declined_summary = declined.abandon(&conn).summary;

    // The confirmation prompt that could not be read.
    let mut prompted = begin_with(&conn, root_id, RecordingMode::Full);
    prompted.bind(&conn, test_telling()).unwrap();
    prompted.abandon_on_prompt_failure(&conn, "the terminal is not readable");
    let prompted_summary = summary_of(&conn, &prompted);

    // A release that failed outright (site 1, same substitution as above).
    let mut failed = begin_with(&conn, root_id, RecordingMode::Full);
    failed.bind(&conn, test_telling()).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    release_must_fail(&mut failed, &conn);
    conn.execute_batch("COMMIT").unwrap();
    let failed_summary = summary_of(&conn, &failed);

    // The world moved — last, because it perturbs the world the other
    // ceremonies' reviews snapshotted.
    let mut moved = begin_with(&conn, root_id, RecordingMode::Full);
    moved.bind(&conn, test_telling()).unwrap();
    insert_source(&conn, root_id, "new/arrival.jpg", None, true, false, None);
    let outcome = moved.release(&conn).unwrap();
    assert!(matches!(outcome, ReleaseOutcome::WorldMoved { .. }));
    let moved_summary = summary_of(&conn, &moved);

    let prefix = format!(
        "The book is bound at {}; the root remains in the index (",
        declined.plan.final_dir.display()
    );
    let arms = [
        ("declined", &declined_summary, "release declined"),
        (
            "prompt failed",
            &prompted_summary,
            "the release prompt failed: the terminal is not readable",
        ),
        ("release failed", &failed_summary, "release failed: "),
        (
            "world moved",
            &moved_summary,
            "the world moved before release: ",
        ),
    ];

    let mut parentheticals = Vec::new();
    for (name, summary, expected) in arms {
        let rest = summary
            .strip_prefix(&prefix)
            .unwrap_or_else(|| panic!("{name}: not the shared sentence: {summary}"));
        let reason = rest
            .strip_suffix(')')
            .unwrap_or_else(|| panic!("{name}: unterminated parenthetical: {summary}"));
        assert!(
            reason.starts_with(expected),
            "{name}: the register must name what happened: {reason}"
        );
        parentheticals.push(reason.to_string());
    }

    // Four exits, four distinct reasons — one sentence.
    let mut distinct = parentheticals.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), parentheticals.len(), "{parentheticals:?}");
}

/// A failed release released nothing, and the row must not imply otherwise:
/// `completed` stays SQL NULL rather than counting the sources the ceremony
/// was about to remove.
#[test]
fn a_failed_release_claims_no_sources_released() {
    let (conn, _src, _arch, root_id) = every_fate_fixture();
    let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
    ceremony.bind(&conn, test_telling()).unwrap();
    let rows_before = crate::retire::repo::count_all_by_root(&conn, root_id).unwrap();

    conn.execute_batch("DROP TABLE notes").unwrap();
    release_must_fail(&mut ceremony, &conn);

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.count_attempted, Some(rows_before));
    assert!(
        decision.count_completed.is_none(),
        "a failed release released nothing: {:?}",
        decision.count_completed
    );
    assert!(decision.count_failed.is_none());
    assert!(decision.count_skipped.is_none());
}
