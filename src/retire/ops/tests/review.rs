use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::retire::domain::Readiness;
use crate::retire::ops::review::compute_readiness;
use crate::retire::ops::{find_retirement_covering_path, validate_retire_target};

use super::fixtures::{
    extraction_from, insert_bound_retirement, insert_decision, insert_object, insert_source,
    ledger_config, scope,
};

#[test]
fn find_retirement_covers_a_subpath_of_the_snapshot() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_bound_retirement(
        &conn,
        "/gone",
        100,
        archive,
        Some(".canon-ledger/retired/gone"),
        Some("drive failing"),
    );

    for path in ["/gone", "/gone/photos/2016"] {
        let hit = find_retirement_covering_path(&conn, path)
            .unwrap()
            .unwrap_or_else(|| panic!("no hit for {path}"));
        assert_eq!(hit.root_path, "/gone");
        assert_eq!(hit.retired_at, 100);
        assert_eq!(hit.reason.as_deref(), Some("drive failing"));
        assert_eq!(hit.book_display, "/archive/.canon-ledger/retired/gone");
        assert_eq!(hit.decision_id, d);
    }
}

#[test]
fn find_retirement_ignores_an_ancestor_path() {
    // A view merely containing a retired root is not "this place is
    // retired" — descendant-or-equal only. And `/gon` must not match
    // `/gone` (directory boundaries, not string prefixes).
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    insert_bound_retirement(&conn, "/vol/gone", 100, archive, Some("retired/gone"), None);

    assert!(find_retirement_covering_path(&conn, "/vol")
        .unwrap()
        .is_none());
    assert!(find_retirement_covering_path(&conn, "/vol/gon")
        .unwrap()
        .is_none());
    assert!(find_retirement_covering_path(&conn, "/elsewhere")
        .unwrap()
        .is_none());
}

#[test]
fn find_retirement_prefers_the_newest_of_two_retirements() {
    // A re-added, re-retired path resolves to its newest telling.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    insert_bound_retirement(&conn, "/gone", 100, archive, Some("retired/gone"), None);
    let newer = insert_bound_retirement(&conn, "/gone", 200, archive, Some("retired/gone-2"), None);

    let hit = find_retirement_covering_path(&conn, "/gone")
        .unwrap()
        .unwrap();
    assert_eq!(hit.decision_id, newer);
    assert_eq!(hit.book_display, "/archive/retired/gone-2");
}

#[test]
fn a_live_root_is_never_called_retired() {
    // The liveness gate, both shapes. A bound-not-released ceremony
    // (declined release / crash during inspection) records the artifact
    // reference while the root stays indexed — asking about an emptied
    // subpath must fall through to the caller's history answer, never the
    // retired statement. Same for a released root re-added at its old path.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_bound_retirement(&conn, "/gone", 100, archive, Some("retired/gone"), None);
    conn.execute(
        "UPDATE decisions SET status = 'partial' WHERE id = ?1",
        rusqlite::params![d],
    )
    .unwrap();
    insert_test_root(&conn, "/gone", "source", false);

    assert!(find_retirement_covering_path(&conn, "/gone")
        .unwrap()
        .is_none());
    assert!(find_retirement_covering_path(&conn, "/gone/emptied/sub")
        .unwrap()
        .is_none());
}

#[test]
fn a_suspended_root_counts_as_live_for_the_statement() {
    // A suspended root's index is intact, merely awaiting reconnection —
    // its place is not retired.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    insert_bound_retirement(&conn, "/gone", 100, archive, Some("retired/gone"), None);
    insert_test_root(&conn, "/gone", "source", true);

    assert!(find_retirement_covering_path(&conn, "/gone/sub")
        .unwrap()
        .is_none());
}

#[test]
fn find_retirement_requires_a_bound_decision() {
    // A retire decision that never recorded an artifact reference (no
    // bind happened) cannot claim "the story is bound at" — no match,
    // so a caller's original miss behavior stands.
    let conn = open_in_memory_for_test();
    insert_bound_retirement(&conn, "/gone", 100, 1, None, None);

    assert!(find_retirement_covering_path(&conn, "/gone")
        .unwrap()
        .is_none());
}

#[test]
fn find_retirement_marks_a_removed_archive_root() {
    // The shelf's own root left the index — the book display renders
    // the marked fallback rather than silently dropping the pointer.
    let conn = open_in_memory_for_test();
    insert_bound_retirement(&conn, "/gone", 100, 777, Some("retired/gone"), None);

    let hit = find_retirement_covering_path(&conn, "/gone")
        .unwrap()
        .unwrap();
    assert_eq!(hit.book_display, "root #777 (removed)/retired/gone");
}

// validate_retire_target

#[test]
fn validate_refuses_an_archive_root() {
    let conn = open_in_memory_for_test();
    insert_test_root(&conn, "/archive", "archive", false);
    let roots = repo::root::fetch_all(&conn).unwrap();
    let err = validate_retire_target(&roots, roots[0].id, &ledger_config()).unwrap_err();
    assert!(err.to_string().contains("archive root is not retired"));
}

#[test]
fn validate_requires_an_archive_root_to_exist() {
    let conn = open_in_memory_for_test();
    let root_id = insert_test_root(&conn, "/source", "source", false);
    let roots = repo::root::fetch_all(&conn).unwrap();
    let err = validate_retire_target(&roots, root_id, &ledger_config()).unwrap_err();
    assert!(err.to_string().contains("needs an archive root"));
    assert!(err.to_string().contains("canon roots rm"));
}

/// The parked arm names the cause it actually has, and offers the door that
/// undoes the pause — never `canon roots rm`, which destroys the index the
/// user only parked.
#[test]
fn validate_names_a_parked_archive_fleet_and_offers_only_unsuspend() {
    let conn = open_in_memory_for_test();
    let root_id = insert_test_root(&conn, "/source", "source", false);
    insert_test_root(&conn, "/archive", "archive", true);
    let roots = repo::root::fetch_all(&conn).unwrap();
    let err = validate_retire_target(&roots, root_id, &ledger_config())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("every archive root is suspended (/archive)"),
        "{err}"
    );
    assert!(err.contains("canon roots unsuspend path:/archive"), "{err}");
    assert!(!err.contains("no archive root is registered"), "{err}");
    assert!(!err.contains("canon roots rm"), "{err}");
}

#[test]
fn validate_passes_a_source_root_with_an_archive_registered() {
    let conn = open_in_memory_for_test();
    let root_id = insert_test_root(&conn, "/source", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let roots = repo::root::fetch_all(&conn).unwrap();
    validate_retire_target(&roots, root_id, &ledger_config()).unwrap();
}

// compute_readiness — the account, end to end

#[test]
fn readiness_accounts_every_bucket_from_real_rows() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let archive = insert_test_root(&conn, "/archive", "archive", false);

    // Covered: object also present at the archive.
    let covered_obj = insert_object(&conn, "aa");
    insert_source(
        &conn,
        root,
        "covered.jpg",
        Some(covered_obj),
        true,
        false,
        None,
    );
    insert_source(
        &conn,
        archive,
        "kept.jpg",
        Some(covered_obj),
        true,
        false,
        None,
    );
    // Excluded, unresolved-hashed, unresolved-unhashed.
    let lone_obj = insert_object(&conn, "bb");
    insert_source(&conn, root, "excluded.jpg", None, true, true, None);
    insert_source(
        &conn,
        root,
        "unresolved.jpg",
        Some(lone_obj),
        true,
        false,
        None,
    );
    insert_source(&conn, root, "unhashed.jpg", None, true, false, None);
    // Absent: scan-stamped (deleted) and unstamped (unexplained).
    let scan = insert_decision(&conn, "scan", 100);
    scope(&conn, scan, root);
    insert_source(&conn, root, "deleted.jpg", None, false, false, Some(scan));
    insert_source(&conn, root, "vanished.jpg", None, false, false, None);
    // Archived from here: one apply extraction.
    let apply = insert_decision(&conn, "apply", 200);
    extraction_from(&conn, apply, root, 3);

    let review = compute_readiness(&conn, root).unwrap();
    let a = &review.account;
    assert_eq!(a.covered, 1);
    assert_eq!(a.excluded, 1);
    assert_eq!(a.unresolved, 2);
    assert_eq!(a.unhashed_unresolved, 1);
    assert_eq!(a.deleted, 1);
    assert_eq!(a.unexplained_missing, 1);
    assert_eq!(a.archived_files, 3);
    assert_eq!(a.archived_moved, 3);
    assert_eq!(
        review.readiness,
        Readiness::NotReady {
            unresolved: 2,
            unhashed: 1
        }
    );
    assert_eq!(review.snapshot_source_count, 6);
    assert!(review.snapshot_max_decision_id >= Some(apply));
}

#[test]
fn empty_root_has_zero_account_and_no_blockers() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.account.standing(), 0);
    assert_eq!(review.account.ever_indexed(), Some(0));
    assert_eq!(review.readiness, Readiness::NoBlockersFound);
    assert_eq!(review.snapshot_source_count, 0);
    assert_eq!(review.snapshot_max_decision_id, None);
}

#[test]
fn suspended_root_computes_and_carries_suspension() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", true);
    let review = compute_readiness(&conn, root).unwrap();
    assert!(review.root.is_suspended());
    assert_eq!(review.readiness, Readiness::NoBlockersFound);
}

#[test]
fn unknown_root_errors() {
    let conn = open_in_memory_for_test();
    assert!(compute_readiness(&conn, 999).is_err());
}

// first_indexed

#[test]
fn first_indexed_is_the_earliest_row_evidence_not_a_decision_date() {
    // A root scanned long before decision recording existed: the only
    // scan *decision* is recent, but the rows carry the older truth.
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let recent_scan = insert_decision(&conn, "scan", 9_000);
    scope(&conn, recent_scan, root);
    let old = insert_source(&conn, root, "old.jpg", None, true, false, None);
    let tombstone = insert_source(&conn, root, "gone.jpg", None, false, false, None);
    let newer = insert_source(&conn, root, "new.jpg", None, true, false, None);
    conn.execute("UPDATE sources SET scanned_at = 500 WHERE id = ?", [old])
        .unwrap();
    // A tombstone's evidence counts — the absent rows are part of identity.
    conn.execute(
        "UPDATE sources SET scanned_at = 100 WHERE id = ?",
        [tombstone],
    )
    .unwrap();
    conn.execute("UPDATE sources SET scanned_at = 700 WHERE id = ?", [newer])
        .unwrap();

    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.first_indexed, Some(100));
}

#[test]
fn first_indexed_unknown_without_rows() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.first_indexed, None);
}

// open cluster intentions

#[test]
fn cluster_generate_after_last_apply_counts_as_open() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let apply = insert_decision(&conn, "apply", 100);
    extraction_from(&conn, apply, root, 1);
    let cg = insert_decision(&conn, "cluster_generate", 200);
    scope(&conn, cg, root);

    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.gaps.open_cluster_intentions, 1);
}

#[test]
fn cluster_generate_before_a_later_apply_is_settled() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let cg = insert_decision(&conn, "cluster_generate", 100);
    scope(&conn, cg, root);
    let apply = insert_decision(&conn, "apply", 200);
    extraction_from(&conn, apply, root, 1);

    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.gaps.open_cluster_intentions, 0);
}

#[test]
fn cluster_generate_with_no_apply_ever_counts_as_open() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let cg = insert_decision(&conn, "cluster_generate", 100);
    scope(&conn, cg, root);

    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.gaps.open_cluster_intentions, 1);
}

#[test]
fn an_apply_drawing_from_another_root_settles_nothing() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/r", "source", false);
    let other = insert_test_root(&conn, "/other", "source", false);
    let cg = insert_decision(&conn, "cluster_generate", 100);
    scope(&conn, cg, root);
    let apply = insert_decision(&conn, "apply", 200);
    extraction_from(&conn, apply, other, 1);

    let review = compute_readiness(&conn, root).unwrap();
    assert_eq!(review.gaps.open_cluster_intentions, 1);
}

// reachability

#[test]
fn unreachable_path_reads_as_disconnected() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/definitely/not/a/real/path", "source", false);
    let review = compute_readiness(&conn, root).unwrap();
    assert!(!review.gaps.reachable);
}

#[test]
fn reachable_path_reads_as_connected() {
    let conn = open_in_memory_for_test();
    let dir = tempfile::tempdir().unwrap();
    let root = insert_test_root(&conn, dir.path().to_str().unwrap(), "source", false);
    let review = compute_readiness(&conn, root).unwrap();
    assert!(review.gaps.reachable);
}
