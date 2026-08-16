use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::notes::insert;
use crate::retire::ops::verify::verify_book;

use super::fixtures::{
    compile_to, extraction_from, insert_decision, insert_object, insert_source, inventory_lines,
    scope, set_decision_extras, set_decision_receipt,
};

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
    insert(&conn, root_id, "a", "looks like the 2015 batch").unwrap();
    insert(&conn, root_id, "", "ready to retire").unwrap();

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
