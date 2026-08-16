use std::path::Path;

use rusqlite::Connection;

use crate::core::domain::config::LedgerConfig;
use crate::repo::db::open_in_memory_for_test;
use crate::repo::insert_test_root;
use crate::retire::ops::shelf::ShelfListing;
use crate::retire::ops::{compute_shelf_listing, iso_date, ShelfLine, SHELF_DIR};

use super::fixtures::insert_bound_retirement;

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
