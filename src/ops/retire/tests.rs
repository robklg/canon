use super::*;
use crate::ops::telling::TellingArtifact;
use crate::repo::db::open_in_memory_for_test;
use crate::repo::insert_test_root;

fn test_telling() -> TellingArtifact {
    TellingArtifact {
        text: "# a test telling\n".to_string(),
        hand_edited: false,
        params: crate::domain::story::StoryParams::default(),
    }
}

/// A bound retirement of a (since-removed) root: decision with an
/// artifact reference + a scope-row path snapshot — the shape the
/// ceremony's `begin`/`bind` leave behind.
fn insert_bound_retirement(
    conn: &Connection,
    retired_root_path: &str,
    created_at: i64,
    receipt_root_id: i64,
    receipt_rel_path: Option<&str>,
    reason: Option<&str>,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions
             (command, command_line, status, canon_version, created_at, reason,
              receipt_root_id, receipt_rel_path)
             VALUES ('roots_retire', 'canon roots retire', 'completed', 'test', ?1, ?2,
                     ?3, ?4)",
        rusqlite::params![
            created_at,
            reason,
            receipt_rel_path.map(|_| receipt_root_id),
            receipt_rel_path
        ],
    )
    .unwrap();
    let decision_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, 999, ?2, '')",
        rusqlite::params![decision_id, retired_root_path],
    )
    .unwrap();
    decision_id
}

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

/// A minimal identifiable book on the shelf: a directory with a
/// `meta.toml` carrying identity and counts.
fn place_book(shelf: &Path, dir_name: &str, root_path: &str, entries: i64) {
    let dir = shelf.join(dir_name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
            dir.join("meta.toml"),
            format!(
                "version = 1\n\n[identity]\npath = \"{root_path}\"\ncompiled_at = \"2026-08-02T10:00:00Z\"\n\n[counts]\nentries = {entries}\n"
            ),
        )
        .unwrap();
}

fn listing_with_archive(conn: &Connection, archive_path: &str) -> ShelfListing {
    insert_test_root(conn, archive_path, "archive", false);
    compute_shelf_listing(conn, &LedgerConfig::default()).unwrap()
}

#[test]
fn shelf_listing_enriches_a_book_from_its_decision_row() {
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    let shelf = archive.path().join(SHELF_DIR);
    place_book(&shelf, "gone-2026-08-02", "/gone", 3980);
    insert_bound_retirement(
        &conn,
        "/gone",
        100,
        1,
        Some("retired/gone-2026-08-02"),
        Some("drive failing"),
    );

    let listing = listing_with_archive(&conn, archive.path().to_str().unwrap());
    assert!(listing.shelf_reachable);
    assert_eq!(listing.lines.len(), 1);
    match &listing.lines[0] {
        ShelfLine::Book {
            root_path,
            retired_on,
            entries,
            book_dir,
            reason,
            indexed,
        } => {
            assert_eq!(root_path, "/gone");
            // The decision's date, not the meta's compile date.
            assert_eq!(retired_on.as_deref(), Some(iso_date(100).as_str()));
            assert_eq!(*entries, Some(3980));
            assert_eq!(book_dir, "gone-2026-08-02");
            assert_eq!(reason.as_deref(), Some("drive failing"));
            assert!(indexed);
        }
        _ => panic!("expected an enriched Book line"),
    }
}

#[test]
fn shelf_listing_renders_an_unindexed_book_from_meta_alone() {
    // The Off-mode shape: a book stands, no decision row exists.
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    place_book(
        &archive.path().join(SHELF_DIR),
        "gone-2026-08-02",
        "/gone",
        12,
    );

    let listing = listing_with_archive(&conn, archive.path().to_str().unwrap());
    assert_eq!(listing.lines.len(), 1);
    match &listing.lines[0] {
        ShelfLine::Book {
            retired_on,
            indexed,
            reason,
            ..
        } => {
            assert_eq!(retired_on.as_deref(), Some("2026-08-02"));
            assert!(!indexed);
            assert!(reason.is_none());
        }
        _ => panic!("expected a meta-only Book line"),
    }
}

#[test]
fn shelf_listing_marks_a_recorded_retirement_without_a_standing_book() {
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(archive.path().join(SHELF_DIR)).unwrap();
    let archive_path = archive.path().to_str().unwrap().to_string();
    insert_bound_retirement(&conn, "/gone", 100, 1, Some("retired/vanished"), None);

    let listing = listing_with_archive(&conn, &archive_path);
    assert_eq!(listing.lines.len(), 1);
    match &listing.lines[0] {
        ShelfLine::RecordedOnly {
            root_path,
            book_path,
            ..
        } => {
            assert_eq!(root_path, "/gone");
            assert_eq!(book_path, &format!("{archive_path}/retired/vanished"));
        }
        _ => panic!("expected a RecordedOnly line"),
    }
}

#[test]
fn shelf_listing_counts_an_unidentifiable_directory() {
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    let shelf = archive.path().join(SHELF_DIR);
    place_book(&shelf, "gone-2026-08-02", "/gone", 12);
    std::fs::create_dir_all(shelf.join("random-stuff")).unwrap();

    let listing = listing_with_archive(&conn, archive.path().to_str().unwrap());
    assert_eq!(listing.lines.len(), 2);
    // Unidentified sorts last.
    assert!(matches!(listing.lines[0], ShelfLine::Book { .. }));
    match &listing.lines[1] {
        ShelfLine::Unidentified { dir_name } => assert_eq!(dir_name, "random-stuff"),
        _ => panic!("expected an Unidentified line"),
    }
}

#[test]
fn shelf_listing_names_a_stranded_replaced_aside() {
    // A `.replaced-<name>` aside — a swap interrupted between old-aside and
    // old-removed — is a full standing book copy: not fleet, never silent.
    // A `.compiling-` temp stays invisible (it was never a placed book).
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    let shelf = archive.path().join(SHELF_DIR);
    place_book(&shelf, "gone-2026-08-02", "/gone", 12);
    std::fs::create_dir_all(shelf.join(".replaced-gone-2026-08-02")).unwrap();
    std::fs::create_dir_all(shelf.join(".compiling-other")).unwrap();

    let listing = listing_with_archive(&conn, archive.path().to_str().unwrap());
    assert_eq!(listing.lines.len(), 1, "asides and temps are not fleet");
    assert_eq!(listing.aside_dirs, vec![".replaced-gone-2026-08-02"]);
}

#[test]
fn shelf_listing_falls_back_to_rows_when_the_shelf_is_unreachable() {
    let conn = open_in_memory_for_test();
    insert_bound_retirement(
        &conn,
        "/gone",
        100,
        1,
        Some("retired/gone-2026-08-02"),
        None,
    );

    let listing = listing_with_archive(&conn, "/no/such/archive");
    assert!(!listing.shelf_reachable);
    assert_eq!(listing.shelf.as_deref(), Some("/no/such/archive/retired"));
    assert_eq!(listing.lines.len(), 1);
    assert!(matches!(listing.lines[0], ShelfLine::RecordedOnly { .. }));
}

#[test]
fn shelf_listing_empty_shelf_is_empty_and_reachable() {
    let conn = open_in_memory_for_test();
    let archive = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(archive.path().join(SHELF_DIR)).unwrap();

    let listing = listing_with_archive(&conn, archive.path().to_str().unwrap());
    assert!(listing.shelf_reachable);
    assert!(listing.lines.is_empty());
}

fn insert_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    present: bool,
    excluded: bool,
    decision_id: Option<i64>,
) -> i64 {
    conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, ?, ?, 0, 0, 1000, 0, 'hash', 0, 0, ?, ?, ?)",
            rusqlite::params![
                root_id,
                rel_path,
                object_id,
                present as i64,
                excluded as i64,
                decision_id
            ],
        )
        .unwrap();
    conn.last_insert_rowid()
}

fn insert_object(conn: &Connection, hash: &str) -> i64 {
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
        [hash],
    )
    .unwrap();
    conn.last_insert_rowid()
}

fn insert_decision(conn: &Connection, command: &str, created_at: i64) -> i64 {
    conn.execute(
        "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES (?1, 'test', 'completed', '0', ?2)",
        rusqlite::params![command, created_at],
    )
    .unwrap();
    conn.last_insert_rowid()
}

fn scope(conn: &Connection, decision_id: i64, root_id: i64) {
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, ?2, '/r', '')",
        rusqlite::params![decision_id, root_id],
    )
    .unwrap();
}

fn extraction_from(conn: &Connection, decision_id: i64, root_id: i64, files: i64) {
    crate::repo::decision::replace_extractions(
        conn,
        &[crate::domain::extraction::DecisionExtraction {
            decision_id,
            root_id,
            root_path: "/r".to_string(),
            rel_prefix: String::new(),
            files,
            bytes: Some(files * 100),
            destination_root_id: Some(999),
            destination_path: "/archive/dest".to_string(),
            disposition: Some(crate::domain::extraction::OriginDisposition::Relocated),
        }],
    )
    .unwrap();
}

fn ledger_config() -> LedgerConfig {
    LedgerConfig::default()
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

// compile_book

fn set_decision_extras(
    conn: &Connection,
    decision_id: i64,
    summary: Option<&str>,
    reason: Option<&str>,
) {
    conn.execute(
        "UPDATE decisions SET summary = ?2, reason = ?3 WHERE id = ?1",
        rusqlite::params![decision_id, summary, reason],
    )
    .unwrap();
}

fn set_decision_receipt(conn: &Connection, decision_id: i64, root_id: i64, rel_path: &str) {
    conn.execute(
        "UPDATE decisions SET receipt_root_id = ?2, receipt_rel_path = ?3 WHERE id = ?1",
        rusqlite::params![decision_id, root_id, rel_path],
    )
    .unwrap();
}

fn compile_to(conn: &Connection, root_id: i64, dest: &std::path::Path) -> CompiledBook {
    let story = fetch_root_story(conn, root_id).unwrap();
    compile_book(
        conn,
        &story,
        &CompileParams {
            reason: Some("story complete".to_string()),
            now: 1_753_000_000,
            dest_dir: dest.to_path_buf(),
            ceremony_decision_id: None,
            telling: Some(test_telling()),
        },
    )
    .unwrap()
}

fn inventory_lines(dir: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(dir.join("inventory.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn compile_writes_all_artifacts_with_sorted_iso_inventory() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);

    let scan = insert_decision(&conn, "scan", 100);
    scope(&conn, scan, root_id);
    set_decision_extras(&conn, scan, Some("Indexed 2 sources"), Some("first gather"));
    insert_source(&conn, root_id, "z/last.jpg", None, true, false, None);
    insert_source(&conn, root_id, "a/first.jpg", None, true, false, None);
    repo::note::insert(&conn, root_id, "a", "looks like the 2015 batch").unwrap();
    repo::note::insert(&conn, root_id, "", "ready to retire").unwrap();

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert_eq!(book.entry_count, 2);
    for file in [
        "inventory.jsonl",
        "timeline.md",
        "notes.md",
        "meta.toml",
        "README.md",
    ] {
        assert!(dest.join(file).is_file(), "{file} missing");
    }

    let lines = inventory_lines(&dest);
    let paths: Vec<&str> = lines.iter().map(|l| l["path"].as_str().unwrap()).collect();
    assert_eq!(paths, vec!["a/first.jpg", "z/last.jpg"]);
    assert_eq!(lines[0]["mtime"], "1970-01-01T00:00:00Z");
    assert_eq!(lines[0]["fate"], "present");
    assert_eq!(lines[0]["verification"], "name_only");

    let timeline = std::fs::read_to_string(dest.join("timeline.md")).unwrap();
    assert!(timeline.contains("Indexed 2 sources"));
    assert!(timeline.contains("reason: first gather"));
    let notes = std::fs::read_to_string(dest.join("notes.md")).unwrap();
    // Note identity through the one shared rendering: root-relative,
    // `(root)` for the root itself — never a view-relative `.`.
    assert!(notes.contains("a: looks like the 2015 batch"), "{notes}");
    assert!(notes.contains("(root): ready to retire"), "{notes}");

    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
    assert_eq!(meta["version"].as_integer(), Some(1));
    assert_eq!(meta["counts"]["entries"].as_integer(), Some(2));
    assert_eq!(meta["counts"]["present"].as_integer(), Some(2));
    assert_eq!(meta["identity"]["reason"].as_str(), Some("story complete"));
    // A compile outside a ceremony has no retirement decision to name —
    // the field is absent, never guessed.
    assert!(meta["identity"].get("decision_id").is_none());

    let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
    assert!(readme.contains("The book of"));
    assert!(readme.contains("unresolved: 2 (2 unhashed)"));
}

#[test]
fn records_mode_apply_degrades_to_covered_and_records_the_gap() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    let archive_id = insert_test_root(&conn, "/archive", "archive", false);

    let object = insert_object(&conn, "h1");
    insert_source(&conn, root_id, "kept.jpg", Some(object), true, false, None);
    insert_source(
        &conn,
        archive_id,
        "2015/kept.jpg",
        Some(object),
        true,
        false,
        None,
    );
    let apply = insert_decision(&conn, "apply", 200);
    extraction_from(&conn, apply, root_id, 1); // no receipt columns: Records mode

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert!(book
        .gaps
        .iter()
        .any(|g| g.contains("per-item origin unavailable")));
    let lines = inventory_lines(&dest);
    assert_eq!(lines[0]["fate"], "covered", "degrades, never guesses");
    assert_eq!(lines[0]["locations"][0], "/archive/2015/kept.jpg");
    let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
    assert!(readme.contains("per-item origin unavailable"));
}

#[test]
fn receipt_recovers_the_moved_entry_with_live_locations() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let arch_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().to_str().unwrap().to_string();
    let arch_path = arch_dir.path().to_str().unwrap().to_string();
    let root_id = insert_test_root(&conn, &src_path, "source", false);
    let archive_id = insert_test_root(&conn, &arch_path, "archive", false);

    // The moved content now lives in the archive under this object.
    let object = insert_object(&conn, "movedhash");
    insert_source(
        &conn,
        archive_id,
        "2015/gone.jpg",
        Some(object),
        true,
        false,
        None,
    );

    let apply = insert_decision(&conn, "apply", 300);
    extraction_from(&conn, apply, root_id, 1);
    let receipt_rel = ".canon-ledger/000001-apply.toml";
    std::fs::create_dir_all(arch_dir.path().join(".canon-ledger")).unwrap();
    std::fs::write(
        arch_dir.path().join(receipt_rel),
        format!(
            r#"
[meta]
decision_id = {apply}
origin_disposition = "relocated"

[meta.locus]
path = "{arch_path}"
id = {archive_id}

[[items]]
source_root = "{src_path}"
source_rel_path = "moved/gone.jpg"
destination_rel_path = "2015/gone.jpg"
size = 555
hash = "sha256:movedhash"
mtime = 1700000000

[[items]]
source_root = "/some/other/root"
source_rel_path = "not/ours.jpg"
destination_rel_path = "2016/ours.jpg"
size = 1
"#
        ),
    )
    .unwrap();
    set_decision_receipt(&conn, apply, archive_id, receipt_rel);

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert_eq!(book.entry_count, 1, "foreign item skipped, ours recovered");
    assert!(book.gaps.is_empty());
    let lines = inventory_lines(&dest);
    assert_eq!(lines[0]["path"], "moved/gone.jpg");
    assert_eq!(lines[0]["fate"], "archived");
    assert_eq!(lines[0]["disposition"], "moved");
    assert_eq!(lines[0]["size"], 555);
    assert_eq!(lines[0]["hash"], "sha256:movedhash");
    assert_eq!(lines[0]["verification"], "content_verified");
    assert_eq!(
        lines[0]["destination"],
        format!("{arch_path}/2015/gone.jpg")
    );
    assert_eq!(
        lines[0]["locations"][0],
        format!("{arch_path}/2015/gone.jpg")
    );
}

#[test]
fn gather_copies_the_ledger_verbatim() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    std::fs::create_dir_all(src_dir.path().join(".canon-ledger")).unwrap();
    std::fs::write(
        src_dir.path().join(".canon-ledger/000009-scan.toml"),
        b"receipt body",
    )
    .unwrap();

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert_eq!(book.ledger_files, Some(1));
    assert_eq!(
        std::fs::read(dest.join("ledger/000009-scan.toml")).unwrap(),
        b"receipt body".to_vec()
    );
    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
    assert_eq!(meta["ledger"]["gathered"].as_bool(), Some(true));
    assert_eq!(meta["ledger"]["files"].as_integer(), Some(1));
}

#[test]
fn an_empty_gather_leaves_no_ledger_dir_and_says_so() {
    // The empty-drive-ledger shape: a reachable root whose `.canon-ledger/` is empty (or
    // absent). The book lists nothing that doesn't exist — no `ledger/`
    // directory, a README line stating the plain absence and where the
    // archiving/letting-go receipts actually live — and still verifies.
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    std::fs::create_dir_all(src_dir.path().join(".canon-ledger")).unwrap();

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert_eq!(book.ledger_files, Some(0));
    assert!(!dest.join("ledger").exists());
    let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
    assert!(readme.contains("(no ledger/ — the drive kept no receipts of its own"));
    assert!(readme.contains("letting-go receipts live in the archive's own ledger.)"));
    assert!(!readme.contains("gathered verbatim (0 files)"));
    verify_book(&dest).unwrap();
}

#[test]
fn unreachable_root_records_the_gather_gap_and_still_compiles() {
    let conn = open_in_memory_for_test();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, "/definitely/not/a/real/path", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    insert_source(&conn, root_id, "a.jpg", None, true, false, None);

    let dest = book_dir.path().join("book");
    let book = compile_to(&conn, root_id, &dest);

    assert_eq!(book.ledger_files, None);
    assert!(book.gaps.iter().any(|g| g.contains("unreachable")));
    assert!(!dest.join("ledger").exists());
    let meta: toml::Value =
        toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
    assert_eq!(meta["ledger"]["gathered"].as_bool(), Some(false));
    assert_eq!(meta["posture"]["posture"].as_str(), Some("on_faith"));
}

#[test]
fn empty_and_all_excluded_roots_bind_complete_books() {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let book_dir = tempfile::tempdir().unwrap();
    let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
    insert_test_root(&conn, "/archive", "archive", false);

    let empty_dest = book_dir.path().join("empty");
    let book = compile_to(&conn, root_id, &empty_dest);
    assert_eq!(book.entry_count, 0);
    assert!(empty_dest.join("README.md").is_file());

    let exclude = insert_decision(&conn, "exclude_set", 400);
    set_decision_extras(&conn, exclude, None, Some("not worth keeping"));
    insert_source(&conn, root_id, "junk.jpg", None, true, true, Some(exclude));

    let excluded_dest = book_dir.path().join("excluded");
    let book = compile_to(&conn, root_id, &excluded_dest);
    assert_eq!(book.entry_count, 1);
    let lines = inventory_lines(&excluded_dest);
    assert_eq!(lines[0]["fate"], "excluded");
    assert_eq!(lines[0]["reason"], "not worth keeping");
}

// verify_book — the round-trip law and tamper detection

/// A root exercising every fate at once: covered-enriched (archived from
/// here), covered plain, excluded, unresolved hashed + unhashed, deleted,
/// unexplained, and a receipt-recovered moved entry.
fn every_fate_fixture() -> (Connection, tempfile::TempDir, tempfile::TempDir, i64) {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let arch_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().to_str().unwrap().to_string();
    let arch_path = arch_dir.path().to_str().unwrap().to_string();
    let root_id = insert_test_root(&conn, &src_path, "source", false);
    let archive_id = insert_test_root(&conn, &arch_path, "archive", false);

    let copied = insert_object(&conn, "copiedhash");
    let plain = insert_object(&conn, "plainhash");
    let uncovered = insert_object(&conn, "uncoveredhash");
    let moved = insert_object(&conn, "movedhash");
    insert_source(
        &conn,
        archive_id,
        "a/copied.jpg",
        Some(copied),
        true,
        false,
        None,
    );
    insert_source(
        &conn,
        archive_id,
        "a/plain.jpg",
        Some(plain),
        true,
        false,
        None,
    );
    insert_source(
        &conn,
        archive_id,
        "a/moved.jpg",
        Some(moved),
        true,
        false,
        None,
    );

    insert_source(
        &conn,
        root_id,
        "copied.jpg",
        Some(copied),
        true,
        false,
        None,
    );
    insert_source(&conn, root_id, "plain.jpg", Some(plain), true, false, None);
    let exclude = insert_decision(&conn, "exclude_set", 400);
    set_decision_extras(&conn, exclude, None, Some("duplicate"));
    insert_source(&conn, root_id, "junk.jpg", None, true, true, Some(exclude));
    insert_source(
        &conn,
        root_id,
        "loose.jpg",
        Some(uncovered),
        true,
        false,
        None,
    );
    insert_source(&conn, root_id, "unhashed.jpg", None, true, false, None);
    let scan = insert_decision(&conn, "scan", 500);
    insert_source(
        &conn,
        root_id,
        "deleted.jpg",
        None,
        false,
        false,
        Some(scan),
    );
    insert_source(&conn, root_id, "vanished.jpg", None, false, false, None);

    let apply = insert_decision(&conn, "apply", 600);
    extraction_from(&conn, apply, root_id, 2);
    let receipt_rel = ".canon-ledger/000001-apply.toml";
    std::fs::create_dir_all(arch_dir.path().join(".canon-ledger")).unwrap();
    std::fs::write(
        arch_dir.path().join(receipt_rel),
        format!(
            r#"
[meta]
decision_id = {apply}
origin_disposition = "relocated"

[meta.locus]
path = "{arch_path}"
id = {archive_id}

[[items]]
source_root = "{src_path}"
source_rel_path = "copied.jpg"
destination_rel_path = "a/copied.jpg"
size = 1000
hash = "sha256:copiedhash"
mtime = 0

[[items]]
source_root = "{src_path}"
source_rel_path = "gone/moved.jpg"
destination_rel_path = "a/moved.jpg"
size = 555
hash = "sha256:movedhash"
mtime = 1700000000
"#
        ),
    )
    .unwrap();
    set_decision_receipt(&conn, apply, archive_id, receipt_rel);

    (conn, src_dir, arch_dir, root_id)
}

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
    let lines = inventory_lines(&dest);
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
                params: crate::domain::story::StoryParams::default(),
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

// The ceremony: plan_bind + begin + bind

const CEREMONY_NOW: i64 = 1_753_000_000;

fn config_with(recording: RecordingMode) -> LedgerConfig {
    LedgerConfig {
        recording,
        ..LedgerConfig::default()
    }
}

fn plan_for(conn: &Connection, root_id: i64) -> BindPlan {
    let story = fetch_root_story(conn, root_id).unwrap();
    plan_bind(&story, &ledger_config(), CEREMONY_NOW).unwrap()
}

fn begin_with(conn: &Connection, root_id: i64, recording: RecordingMode) -> RetireCeremony {
    let story = fetch_root_story(conn, root_id).unwrap();
    let review = readiness_lens(&story);
    let config = config_with(recording);
    let plan = plan_bind(&story, &config, CEREMONY_NOW).unwrap();
    begin_ceremony(
        conn,
        story,
        &review,
        plan,
        CeremonyParams {
            reason: Some("story complete".to_string()),
            now: CEREMONY_NOW,
            command_line: "canon roots retire".to_string(),
            config,
        },
    )
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
    let (arch_id, _) = ops::receipt::resolve_ledger_root(
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
    let rows_before = repo::source::count_all_by_root(&conn, root_id).unwrap();

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
    assert_eq!(repo::source::count_all_by_root(&conn, root_id).unwrap(), 0);
    verify_book(&bound.dir).unwrap();

    let decision = retire_decision_row(&conn, &ceremony);
    assert_eq!(decision.status, "completed");
    assert_eq!(decision.count_attempted, Some(rows_before));
    assert_eq!(decision.count_completed, Some(rows_before));
    assert_eq!(decision.summary.as_deref(), Some(summary.as_str()));
}

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
    let plan = ops::roots::plan_remove(&conn, root_id).unwrap();
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
