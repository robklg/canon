use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::core::repo::Connection;
use crate::trail::ops::show::{compute_show, PointerRelocation};

use super::fixtures::{
    extraction_row, insert_decision_at, insert_decision_full, insert_zero_transfer_decision,
};

#[test]
fn show_lists_receipt_pointers_per_root() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "scan", 100);
    repo::decision::insert_scopes(&conn, d, &[(root, "/a".to_string(), "".to_string())]).unwrap();
    repo::decision::set_scope_receipt(&conn, d, root, "/a", ".canon-ledger/000001-scan.toml")
        .unwrap();
    // A second, since-removed root also wrote a receipt.
    repo::decision::set_scope_receipt(&conn, d, 999, "/gone", ".canon-ledger/000001-scan.toml")
        .unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.receipts.len(), 2);
    assert_eq!(show.receipts[0].root_display, "/a");
    // The removed root's pointer renders its snapshotted path — the
    // observable bug this snapshot exists to fix.
    assert_eq!(show.receipts[1].root_display, "/gone");
    assert!(show.receipt_absence.is_none());
}

#[test]
fn show_receipt_pointer_without_snapshot_renders_marked_fallback() {
    // A pre-snapshot row the migration hook couldn't recover: NULL
    // root_path, root long removed. The pointer line still renders,
    // marked — never silently absent.
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    conn.execute(
        "INSERT INTO decision_scopes
         (decision_id, root_id, root_path, rel_prefix, receipt_rel_path)
         VALUES (?1, 999, NULL, '', '.canon-ledger/000001-scan.toml')",
        [d],
    )
    .unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.receipts.len(), 1);
    assert_eq!(show.receipts[0].root_display, "root #999 (removed)");
    assert!(show.receipt_absence.is_none());
}

/// A bound retirement of `root_id`: a `roots_retire` decision whose
/// receipt columns reference the book, with a scope-row snapshot —
/// the shape the ceremony's `begin`/`bind` leave behind.
fn insert_retire_decision(
    conn: &Connection,
    retired_root_id: i64,
    retired_root_path: &str,
    created_at: i64,
    receipt_root_id: i64,
    receipt_rel_path: &str,
    status: &str,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions
         (command, command_line, status, canon_version, created_at,
          receipt_root_id, receipt_rel_path)
         VALUES ('roots_retire', 'canon roots retire', ?1, 'test', ?2, ?3, ?4)",
        rusqlite::params![status, created_at, receipt_root_id, receipt_rel_path],
    )
    .unwrap();
    let decision_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
         VALUES (?1, ?2, ?3, '')",
        rusqlite::params![decision_id, retired_root_id, retired_root_path],
    )
    .unwrap();
    decision_id
}

/// A scan decision with a source-local deletion receipt on a root that
/// is no longer in the index (snapshot row only).
fn insert_deletion_on_removed_root(
    conn: &Connection,
    removed_root_id: i64,
    removed_root_path: &str,
    receipt_rel: &str,
) -> i64 {
    let d = insert_decision_at(conn, "scan", 100);
    conn.execute(
        "INSERT INTO decision_scopes
         (decision_id, root_id, root_path, rel_prefix, receipt_rel_path)
         VALUES (?1, ?2, ?3, '', ?4)",
        rusqlite::params![d, removed_root_id, removed_root_path, receipt_rel],
    )
    .unwrap();
    d
}

#[test]
fn show_relocates_a_retired_roots_receipt_into_the_gathered_ledger() {
    let conn = open_in_memory_for_test();
    let shelf = tempfile::tempdir().unwrap();
    let archive_path = shelf.path().to_str().unwrap().to_string();
    let archive = insert_test_root(&conn, &archive_path, "archive", false);
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    insert_retire_decision(
        &conn,
        999,
        "/gone",
        200,
        archive,
        ".canon-ledger/retired/gone",
        "completed",
    );
    let ledger_dir = shelf.path().join(".canon-ledger/retired/gone/ledger");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    std::fs::write(ledger_dir.join("000042-scan.toml"), "x").unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.receipts.len(), 1);
    match &show.receipts[0].relocation {
        Some(PointerRelocation::Gathered { book_ledger_path }) => assert_eq!(
            book_ledger_path,
            &format!("{archive_path}/.canon-ledger/retired/gone/ledger/000042-scan.toml")
        ),
        other => panic!("expected Gathered, got {:?}", relocation_name(other)),
    }
}

#[test]
fn show_relocation_preserves_a_nested_receipt_subpath() {
    let conn = open_in_memory_for_test();
    let shelf = tempfile::tempdir().unwrap();
    let archive_path = shelf.path().to_str().unwrap().to_string();
    let archive = insert_test_root(&conn, &archive_path, "archive", false);
    let d =
        insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/sub/000042-scan.toml");
    insert_retire_decision(
        &conn,
        999,
        "/gone",
        200,
        archive,
        "retired/gone",
        "completed",
    );
    let ledger_dir = shelf.path().join("retired/gone/ledger/sub");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    std::fs::write(ledger_dir.join("000042-scan.toml"), "x").unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    match &show.receipts[0].relocation {
        Some(PointerRelocation::Gathered { book_ledger_path }) => assert_eq!(
            book_ledger_path,
            &format!("{archive_path}/retired/gone/ledger/sub/000042-scan.toml")
        ),
        other => panic!("expected Gathered, got {:?}", relocation_name(other)),
    }
}

#[test]
fn show_relocation_delegates_when_the_book_holds_no_gathered_copy() {
    let conn = open_in_memory_for_test();
    let shelf = tempfile::tempdir().unwrap();
    let archive_path = shelf.path().to_str().unwrap().to_string();
    let archive = insert_test_root(&conn, &archive_path, "archive", false);
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    insert_retire_decision(
        &conn,
        999,
        "/gone",
        200,
        archive,
        "retired/gone",
        "completed",
    );
    // The book stands — retired on faith, no ledger/ inside.
    std::fs::create_dir_all(shelf.path().join("retired/gone")).unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    match &show.receipts[0].relocation {
        Some(PointerRelocation::NotGathered { book_path }) => {
            assert_eq!(book_path, &format!("{archive_path}/retired/gone"));
        }
        other => panic!("expected NotGathered, got {:?}", relocation_name(other)),
    }
}

#[test]
fn show_relocation_hedges_when_the_book_is_unreachable() {
    let conn = open_in_memory_for_test();
    // The archive root is in the index but its path doesn't exist on
    // this machine — the shelf can't be observed right now.
    let archive = insert_test_root(&conn, "/no/such/archive", "archive", false);
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    insert_retire_decision(
        &conn,
        999,
        "/gone",
        200,
        archive,
        "retired/gone",
        "completed",
    );

    let show = compute_show(&conn, d).unwrap().unwrap();
    match &show.receipts[0].relocation {
        Some(PointerRelocation::Unreachable { book_path }) => {
            assert_eq!(book_path, "/no/such/archive/retired/gone");
        }
        other => panic!("expected Unreachable, got {:?}", relocation_name(other)),
    }

    // The shelf's own root gone from the index: no path to observe —
    // the marked fallback, same hedge.
    let conn = open_in_memory_for_test();
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    insert_retire_decision(&conn, 999, "/gone", 200, 777, "retired/gone", "completed");
    let show = compute_show(&conn, d).unwrap().unwrap();
    match &show.receipts[0].relocation {
        Some(PointerRelocation::Unreachable { book_path }) => {
            assert_eq!(book_path, "root #777 (removed)/retired/gone");
        }
        other => panic!("expected Unreachable, got {:?}", relocation_name(other)),
    }
}

#[test]
fn show_relocation_ignores_a_plain_removed_root() {
    // Removed but never retired (plain rm, or an Off-mode ceremony):
    // no retire decision to project — today's pointer stands unchanged.
    let conn = open_in_memory_for_test();
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.receipts.len(), 1);
    assert!(show.receipts[0].relocation.is_none());
}

#[test]
fn show_relocates_every_pointer_on_a_retired_root() {
    let conn = open_in_memory_for_test();
    let shelf = tempfile::tempdir().unwrap();
    let archive_path = shelf.path().to_str().unwrap().to_string();
    let archive = insert_test_root(&conn, &archive_path, "archive", false);
    // One decision, two receipt rows on the same removed root (two
    // prefixes) — both pointers must relocate.
    let d = insert_decision_at(&conn, "scan", 100);
    for (prefix, rel) in [
        ("a", ".canon-ledger/000042-scan.toml"),
        ("b", ".canon-ledger/000043-scan.toml"),
    ] {
        conn.execute(
            "INSERT INTO decision_scopes
             (decision_id, root_id, root_path, rel_prefix, receipt_rel_path)
             VALUES (?1, 999, '/gone', ?2, ?3)",
            rusqlite::params![d, prefix, rel],
        )
        .unwrap();
    }
    insert_retire_decision(
        &conn,
        999,
        "/gone",
        200,
        archive,
        "retired/gone",
        "completed",
    );
    let ledger_dir = shelf.path().join("retired/gone/ledger");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    std::fs::write(ledger_dir.join("000042-scan.toml"), "x").unwrap();
    std::fs::write(ledger_dir.join("000043-scan.toml"), "x").unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.receipts.len(), 2);
    for receipt in &show.receipts {
        assert!(matches!(
            receipt.relocation,
            Some(PointerRelocation::Gathered { .. })
        ));
    }
}

#[test]
fn show_relocates_after_abandoned_bind_then_rm() {
    // An abandoned-after-bind ceremony (partial, bound-not-released)
    // followed by a plain rm: the root left through the rm door, but a
    // bound book with a gathered ledger stands — the stat finds the
    // copy and renders the redirect. The disk-truth case.
    let conn = open_in_memory_for_test();
    let shelf = tempfile::tempdir().unwrap();
    let archive_path = shelf.path().to_str().unwrap().to_string();
    let archive = insert_test_root(&conn, &archive_path, "archive", false);
    let d = insert_deletion_on_removed_root(&conn, 999, "/gone", ".canon-ledger/000042-scan.toml");
    insert_retire_decision(&conn, 999, "/gone", 200, archive, "retired/gone", "partial");
    let ledger_dir = shelf.path().join("retired/gone/ledger");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    std::fs::write(ledger_dir.join("000042-scan.toml"), "x").unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert!(matches!(
        show.receipts[0].relocation,
        Some(PointerRelocation::Gathered { .. })
    ));
}

/// Test-failure labels for the relocation variants (the production enum
/// deliberately carries no Debug — it is a rendering contract).
fn relocation_name(r: &Option<PointerRelocation>) -> &'static str {
    match r {
        None => "None",
        Some(PointerRelocation::Gathered { .. }) => "Gathered",
        Some(PointerRelocation::NotGathered { .. }) => "NotGathered",
        Some(PointerRelocation::Unreachable { .. }) => "Unreachable",
    }
}

#[test]
fn show_explains_receipt_absence() {
    let conn = open_in_memory_for_test();
    let quiet = insert_decision_full(
        &conn,
        "exclude_set",
        100,
        "canon exclude set --no-receipt x",
    );
    let plain = insert_decision_at(&conn, "exclude_set", 200);

    let show = compute_show(&conn, quiet).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (--no-receipt)")
    );
    let show = compute_show(&conn, plain).unwrap().unwrap();
    assert_eq!(show.receipt_absence.as_deref(), Some("no receipt recorded"));
}

/// A run that transferred nothing writes no receipt, and since the recorder
/// retracts the claim the row has no pointer either. The absence is not mute:
/// the row's own counts say why, and `show` reads them rather than leaving the
/// reader with the generic arm.
#[test]
fn trail_show_says_nothing_transferred_for_a_receiptless_zero_transfer_decision() {
    let conn = open_in_memory_for_test();
    let failed = insert_zero_transfer_decision(&conn, "apply", 300, 1240);

    let show = compute_show(&conn, failed).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (nothing transferred)")
    );
}

/// Zero completed out of some attempted is a shape many commands can land in,
/// and most of them never write a receipt under any circumstance. An import
/// whose records all went stale reads exactly like a failed apply in the
/// counts — but nothing was ever going to be transferred, so saying so would
/// name work the command does not do.
#[test]
fn a_zero_completed_import_still_takes_the_generic_arm() {
    let conn = open_in_memory_for_test();
    let stale = insert_zero_transfer_decision(&conn, "import_facts", 400, 12);

    let show = compute_show(&conn, stale).unwrap().unwrap();
    assert_eq!(show.receipt_absence.as_deref(), Some("no receipt recorded"));
}

#[test]
fn show_unknown_id_is_none() {
    let conn = open_in_memory_for_test();
    assert!(compute_show(&conn, 12345).unwrap().is_none());
}

#[test]
fn show_lists_extractions_including_removed_root_snapshot() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[
            extraction_row(
                d,
                root,
                "/a",
                "photos/2016/italy",
                47,
                Some(3_900_000),
                "/archive/x",
            ),
            // A second root already removed from the DB — the row's
            // root_path snapshot must still render.
            extraction_row(
                d,
                999,
                "/Volumes/gone",
                "dcim",
                12,
                Some(401_000),
                "/archive/y",
            ),
        ],
    )
    .unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.extractions.len(), 2);
    let a = show
        .extractions
        .iter()
        .find(|e| e.location == "/a/photos/2016/italy")
        .unwrap();
    assert_eq!(a.files, 47);
    assert!(!a.root_removed);
    // Single-directory draws carry no directory listing — the location
    // already says it.
    assert!(a.directories.is_empty());
    let gone = show
        .extractions
        .iter()
        .find(|e| e.location == "/Volumes/gone/dcim")
        .unwrap();
    assert!(gone.root_removed);
}

#[test]
fn show_folds_placement_rows_into_per_root_lines_with_directories() {
    // Directory-precision rows: one root drawing from two directories is
    // one `drew from:` line at the collapsed location, with each
    // directory's own share carried for the capped listing.
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[
            extraction_row(d, root, "/a", "m/01", 105, Some(1_050), "/archive/x"),
            extraction_row(d, root, "/a", "m/02", 100, Some(1_000), "/archive/x"),
            extraction_row(d, root, "/a", "m/02", 40, Some(400), "/archive/y"),
        ],
    )
    .unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.extractions.len(), 1);
    let line = &show.extractions[0];
    assert_eq!(line.location, "/a/m");
    assert_eq!(line.files, 245);
    assert_eq!(line.bytes, Some(2_450));
    // Two distinct directories: m/02's two placement rows fold into one
    // directory share.
    assert_eq!(line.directories.len(), 2);
    assert_eq!(line.directories[0].dir, "m/01");
    assert_eq!(line.directories[0].files, 105);
    assert_eq!(line.directories[1].dir, "m/02");
    assert_eq!(line.directories[1].files, 140);
    assert_eq!(line.directories[1].bytes, Some(1_400));
}

#[test]
fn show_does_not_mark_a_re_added_root_as_removed() {
    // The row's snapshot id is stale because the root was removed and
    // re-added, but the location is registered and visitable — matching
    // on the path is what keeps `drew from:` honest.
    let conn = open_in_memory_for_test();
    let re_added = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    let mut row = extraction_row(d, re_added, "/a", "photos", 3, Some(30), "/archive/x");
    row.root_id = 999; // the id the root carried before it was re-added
    repo::decision::replace_extractions(&conn, &[row]).unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.extractions.len(), 1);
    assert!(
        !show.extractions[0].root_removed,
        "a live location must not read as removed"
    );
}

#[test]
fn show_points_a_retired_origin_at_its_book() {
    // A `drew from:` origin whose root was retired reads as bound
    // history — the book's location — never a dead-end "(root removed)".
    // A plain-`rm`'d root (no bound retirement) keeps the plain marker.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_decision_at(&conn, "apply", 100);
    // Origin root id 999: the retired root's old id — no live root has
    // its path.
    let row = extraction_row(d, 999, "/vol/gone", "photos", 3, Some(30), "/archive/x");
    repo::decision::replace_extractions(&conn, &[row]).unwrap();

    // The bound retirement of /vol/gone, artifact reference recorded.
    conn.execute(
        "INSERT INTO decisions
             (command, command_line, status, canon_version, created_at,
              receipt_root_id, receipt_rel_path)
             VALUES ('roots_retire', 'canon roots retire', 'completed', 'test', 200,
                     ?1, 'retired/gone-2026-08-05')",
        rusqlite::params![archive],
    )
    .unwrap();
    let retire_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, 999, '/vol/gone', '')",
        rusqlite::params![retire_id],
    )
    .unwrap();

    let show = compute_show(&conn, d).unwrap().unwrap();
    assert_eq!(show.extractions.len(), 1);
    assert!(show.extractions[0].root_removed);
    assert_eq!(
        show.extractions[0].retired_book.as_deref(),
        Some("/archive/retired/gone-2026-08-05")
    );
}

#[test]
fn show_no_extractions_is_empty_not_absent() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    let show = compute_show(&conn, d).unwrap().unwrap();
    assert!(show.extractions.is_empty());
}
