use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::core::repo::Connection;
use crate::trail::ops::show::{compute_show, PointerRelocation, ScopeRelation};

use super::fixtures::{
    extraction_row, insert_decision_at, insert_decision_full, insert_refused_decision,
    insert_zero_transfer_decision,
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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
    let show = compute_show(&conn, d, None).unwrap().unwrap();
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
    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, quiet, None).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (--no-receipt)")
    );
    let show = compute_show(&conn, plain, None).unwrap().unwrap();
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

    let show = compute_show(&conn, failed, None).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (nothing transferred)")
    );
}

/// A refused run's row can explain its own missing receipt: the status says a
/// pre-flight check said no, so the reader is not left with the generic arm —
/// which reads as "something suppressed the receipt and the row can't say
/// what". The user's question here is "did that run do anything?", and the
/// answer is on the row.
#[test]
fn a_refused_run_explains_its_missing_receipt() {
    let conn = open_in_memory_for_test();
    let refused = insert_refused_decision(&conn, "apply", 500, "canon apply test.toml");

    let show = compute_show(&conn, refused, None).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (the run refused)")
    );
}

/// The arm ordering is a claim, not an accident. A run invoked with
/// `--no-receipt` was never going to write a receipt whatever it then did, so
/// the opt-out is the better explanation — and it stays the better one when
/// the run also refused.
#[test]
fn an_explicit_opt_out_still_wins_over_a_refusal() {
    let conn = open_in_memory_for_test();
    let refused =
        insert_refused_decision(&conn, "apply", 600, "canon apply test.toml --no-receipt");

    let show = compute_show(&conn, refused, None).unwrap().unwrap();
    assert_eq!(
        show.receipt_absence.as_deref(),
        Some("no receipt (--no-receipt)")
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

    let show = compute_show(&conn, stale, None).unwrap().unwrap();
    assert_eq!(show.receipt_absence.as_deref(), Some("no receipt recorded"));
}

#[test]
fn show_unknown_id_is_none() {
    let conn = open_in_memory_for_test();
    assert!(compute_show(&conn, 12345, None).unwrap().is_none());
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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

    let show = compute_show(&conn, d, None).unwrap().unwrap();
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
    let show = compute_show(&conn, d, None).unwrap().unwrap();
    assert!(show.extractions.is_empty());
}

// ----------------------------------------------------------------------
// The scope list — classified against where the reader stands
// ----------------------------------------------------------------------

/// Set a decision's `scope` display column: the durable JSON list `show`
/// renders.
fn scope_display(conn: &Connection, decision_id: i64, paths: &[&str]) {
    conn.execute(
        "UPDATE decisions SET scope = ?2 WHERE id = ?1",
        rusqlite::params![decision_id, serde_json::to_string(paths).unwrap()],
    )
    .unwrap();
}

fn relations(show: &crate::trail::ops::show::ShowResult) -> Vec<(&str, ScopeRelation)> {
    show.scopes
        .iter()
        .map(|s| (s.display_path.as_str(), s.relation))
        .collect()
}

/// The answer to "why did this surface": the scope that contains where I am
/// stands first and is marked.
#[test]
fn a_scope_covering_the_cwd_is_marked_here_and_hoisted() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/a/admin", "/a/foto", "/a/misc"]);

    let show = compute_show(&conn, d, Some("/a/foto/2016/italy"))
        .unwrap()
        .unwrap();
    assert_eq!(
        relations(&show),
        vec![
            ("/a/foto", ScopeRelation::Here),
            ("/a/admin", ScopeRelation::Unrelated),
            ("/a/misc", ScopeRelation::Unrelated),
        ]
    );
}

#[test]
fn a_scope_equal_to_the_cwd_is_marked_here() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/a/foto"]);

    let show = compute_show(&conn, d, Some("/a/foto")).unwrap().unwrap();
    assert_eq!(relations(&show), vec![("/a/foto", ScopeRelation::Here)]);
}

#[test]
fn a_scope_inside_the_cwd_is_marked_within_here() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/a/foto/2016", "/b"]);

    let show = compute_show(&conn, d, Some("/a/foto")).unwrap().unwrap();
    assert_eq!(
        relations(&show),
        vec![
            ("/a/foto/2016", ScopeRelation::WithinHere),
            ("/b", ScopeRelation::Unrelated),
        ]
    );
}

/// Both markers at once, in their stated order: `Here` before `WithinHere`
/// before the rest.
#[test]
fn here_is_hoisted_above_within_here() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/z", "/a/foto/2016", "/a"]);

    let show = compute_show(&conn, d, Some("/a/foto")).unwrap().unwrap();
    assert_eq!(
        relations(&show),
        vec![
            ("/a", ScopeRelation::Here),
            ("/a/foto/2016", ScopeRelation::WithinHere),
            ("/z", ScopeRelation::Unrelated),
        ]
    );
}

#[test]
fn a_cwd_outside_every_scope_leaves_recorded_order_unmarked() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/a/admin", "/a/foto"]);

    let show = compute_show(&conn, d, Some("/elsewhere")).unwrap().unwrap();
    assert_eq!(
        relations(&show),
        vec![
            ("/a/admin", ScopeRelation::Unrelated),
            ("/a/foto", ScopeRelation::Unrelated),
        ]
    );
}

/// An unresolvable working directory annotates nothing and reorders nothing
/// — exactly what the surface did before markers existed.
#[test]
fn an_unresolvable_cwd_leaves_recorded_order_unmarked() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/a/admin", "/a/foto"]);

    let show = compute_show(&conn, d, None).unwrap().unwrap();
    assert_eq!(
        relations(&show),
        vec![
            ("/a/admin", ScopeRelation::Unrelated),
            ("/a/foto", ScopeRelation::Unrelated),
        ]
    );
}

/// The reason hoisting exists. With a cap and no hoist the one place the
/// reader cares about falls into the truncated remainder — the timeline's own
/// defect, reproduced one surface over.
#[test]
fn the_marked_scope_survives_the_cap() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    let mut paths: Vec<String> = (0..30).map(|i| format!("/a/dir{i:02}")).collect();
    paths.push("/a/foto".to_string());
    let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
    scope_display(&conn, d, &refs);

    let show = compute_show(&conn, d, Some("/a/foto")).unwrap().unwrap();
    assert_eq!(show.scopes.len(), 31);
    assert_eq!(show.scopes[0].display_path, "/a/foto");
    assert_eq!(show.scopes[0].relation, ScopeRelation::Here);
}

/// Recorded order survives inside each relation group — the sort is stable,
/// so a decision's own ordering is not silently rewritten.
#[test]
fn recorded_order_is_stable_within_each_relation_group() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "scan", 100);
    scope_display(&conn, d, &["/z", "/m", "/a/x", "/b", "/a/y"]);

    let show = compute_show(&conn, d, Some("/a")).unwrap().unwrap();
    let paths: Vec<&str> = show
        .scopes
        .iter()
        .map(|s| s.display_path.as_str())
        .collect();
    // The two WithinHere scopes keep their recorded order, and so do the
    // three unrelated ones.
    assert_eq!(paths, vec!["/a/x", "/a/y", "/z", "/m", "/b"]);
}

#[test]
fn a_global_decision_still_prints_global() {
    let conn = open_in_memory_for_test();
    let d = insert_decision_at(&conn, "import_facts", 100);

    let show = compute_show(&conn, d, Some("/a")).unwrap().unwrap();
    assert!(show.scopes.is_empty());
}

/// The coherence property, and the carrier of this story's recognition: a
/// decision surfaced by a scope in a scoped view is marked when `show` runs
/// from the same place. Both surfaces classify through `scopes_touch`; a
/// bespoke rule on either side would make the marker say "this is why you're
/// seeing it" about a decision surfaced for a different reason.
#[test]
fn show_and_the_timeline_agree_on_what_matched() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "scan", 100);
    repo::decision::insert_scopes(
        &conn,
        d,
        &[
            (root, "/a".to_string(), "admin".to_string()),
            (root, "/a".to_string(), "foto".to_string()),
            (root, "/a".to_string(), "misc".to_string()),
        ],
    )
    .unwrap();
    scope_display(&conn, d, &["/a/admin", "/a/foto", "/a/misc"]);

    let here = "/a/foto/2016";
    let timeline = crate::trail::ops::compute::compute_trail(
        &conn,
        &crate::trail::ops::compute::TrailParams {
            prefixes: vec![here.to_string()],
            timeframe: None,
            include_notes: false,
            limit: None,
        },
    )
    .unwrap();
    let matched = &timeline.scope_matches[&d].matched;

    let show = compute_show(&conn, d, Some(here)).unwrap().unwrap();
    let marked: Vec<&str> = show
        .scopes
        .iter()
        .filter(|s| s.relation != ScopeRelation::Unrelated)
        .map(|s| s.display_path.as_str())
        .collect();

    assert_eq!(marked, vec![matched.as_str()]);
}
