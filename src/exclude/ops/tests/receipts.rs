use crate::core::domain::config::LedgerConfig;
use crate::core::domain::decision::DecisionCommand;
use crate::core::repo::Connection;
use crate::exclude::repo as exclude_repo;
use crate::ops::decision::DecisionParams;
use crate::ops::receipt::{ExcludeReceipt, ReceiptPlacement};
use crate::ops::test_helpers::{
    insert_object, insert_root, insert_source, insert_source_excluded, is_object_excluded,
    is_source_excluded, setup_test_db,
};
use tempfile::{tempdir, TempDir};

use crate::exclude::ops::execute::{
    execute_clear, execute_duplicates, execute_set, execute_set_objects,
};
use crate::exclude::ops::plan::{plan_clear, plan_duplicates, plan_set, plan_set_objects};
use crate::exclude::ops::receipt::counts_all;
use crate::exclude::ops::runner::run_exclusion;
use crate::exclude::ops::single::{
    check_clear_object, check_set_source_by_id, execute_clear_object, execute_set_object,
    execute_set_source, ObjectClearCheck, SourceExclusionCheck,
};
use crate::exclude::ops::types::ExcludeSetPlan;

use super::fixtures::{
    item, make_clear_params, make_duplicates_params, make_set_objects_params, make_set_params,
};

// =========================================================================
// receipt writing + decision_id linkage at the ledger root
// =========================================================================

/// A recording-enabled decision (DB record + receipt file).
fn full_decision(command: DecisionCommand) -> DecisionParams {
    DecisionParams {
        command,
        scope: Vec::new(),
        command_line: "canon test".to_string(),
        reason: None,
        record_enabled: true,
        receipt_enabled: true,
        ledger_config: LedgerConfig::default(),
    }
}

/// Create an active archive root rooted at a temp dir, returning its id, the
/// dir handle (kept alive), and a `LedgerRoot` placement pointing at it.
fn ledger_root(conn: &Connection) -> (i64, TempDir, ReceiptPlacement) {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let root_id = insert_root(conn, &path, "archive", false);
    let placement = ReceiptPlacement::LedgerRoot {
        root_id,
        root_path: path,
    };
    (root_id, dir, placement)
}

fn fetch_source_decision_id(conn: &Connection, source_id: i64) -> Option<i64> {
    conn.query_row(
        "SELECT decision_id FROM sources WHERE id = ?",
        [source_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn run_exclusion_rolls_back_on_error() {
    // The write-path-atomicity guarantee: a failure inside `mutate` rolls
    // back BOTH the source flip and the `started` decision row — no half-state.
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root, "a.jpg", None);

    let decision = DecisionParams {
        command: DecisionCommand::ExcludeSet,
        scope: Vec::new(),
        command_line: "canon test".to_string(),
        reason: None,
        record_enabled: true,
        receipt_enabled: false,
        ledger_config: LedgerConfig::default(),
    };

    let result = run_exclusion::<ExcludeReceipt, _>(
        &mut conn,
        Some(&decision),
        None,
        true,
        counts_all(1),
        "test",
        |tx, decision_id| {
            // Flip the source inside the transaction, then fail.
            exclude_repo::source::set_excluded(tx, id, true, decision_id)?;
            anyhow::bail!("forced failure mid-flip");
        },
    );

    assert!(result.is_err(), "the forced error should propagate");
    assert!(
        !is_source_excluded(&conn, id),
        "the source flip must roll back on error"
    );
    let decisions: i64 = conn
        .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(decisions, 0, "the started decision row must roll back");
}

#[test]
fn decision_scopes_populated_for_scoped_exclude() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root, "a.jpg", None);
    let plan = ExcludeSetPlan {
        items: vec![item(id, "/photos", "a.jpg")],
        root_count: 1,
        not_archived_count: 1,
    };
    let decision = DecisionParams {
        command: DecisionCommand::ExcludeSet,
        scope: vec![crate::core::domain::scope::DecisionScope::new(
            1,
            "/photos".to_string(),
            String::new(),
        )],
        command_line: "canon exclude set /photos".to_string(),
        reason: None,
        record_enabled: true,
        receipt_enabled: false,
        ledger_config: LedgerConfig::default(),
    };

    execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

    let (rid, prefix): (i64, String) = conn
        .query_row("SELECT root_id, rel_prefix FROM decision_scopes", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(rid, root);
    assert_eq!(prefix, "", "root-level scope has empty rel_prefix");
}

#[test]
fn decision_scopes_empty_for_global_exclude() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root, "a.jpg", None);
    let plan = ExcludeSetPlan {
        items: vec![item(id, "/photos", "a.jpg")],
        root_count: 1,
        not_archived_count: 1,
    };
    // scope: None → global → no scope-index rows.
    let decision = DecisionParams {
        command: DecisionCommand::ExcludeSet,
        scope: Vec::new(),
        command_line: "canon exclude set --global".to_string(),
        reason: None,
        record_enabled: true,
        receipt_enabled: false,
        ledger_config: LedgerConfig::default(),
    };

    execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM decision_scopes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_execute_set_writes_receipt_and_links_decision() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "rcpt_hash_val", false);
    let id = insert_source(&conn, src_root, "junk/a.tmp", Some(obj));

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSet);
    let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    // Decision linked on the excluded source.
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));

    // Receipt landed flat at the ledger root with matching content.
    let receipt = dir.path().join(".canon-ledger/000001-exclude_set.toml");
    let content = std::fs::read_to_string(&receipt).unwrap();
    assert!(content.contains("[[items]]"));
    assert!(content.contains("rel_path = \"junk/a.tmp\""));
    assert!(content.contains("hash = \"sha256:rcpt_hash_val\""));
    assert!(content.contains("command = \"exclude_set\""));
}

#[test]
fn test_execute_set_no_placement_records_decision_without_receipt() {
    let mut conn = setup_test_db();
    let src_root = insert_root(&conn, "/source", "source", false);
    let id = insert_source(&conn, src_root, "a.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSet);
    // No archive root → placement None → no receipt, but decision still recorded.
    let result = execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

    assert!(result.warnings.is_empty());
    assert!(is_source_excluded(&conn, id));
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "decision recorded even without a receipt");
}

#[test]
fn test_execute_set_receipt_failure_surfaces_warning() {
    let mut conn = setup_test_db();
    let src_root = insert_root(&conn, "/source", "source", false);
    let id = insert_source(&conn, src_root, "a.jpg", None);

    // A file where the ledger directory needs to be → dir creation fails.
    let dir = tempdir().unwrap();
    let blocker = dir.path().join("not-a-dir");
    std::fs::write(&blocker, b"x").unwrap();
    let placement = ReceiptPlacement::LedgerRoot {
        root_id: src_root,
        root_path: blocker.to_str().unwrap().to_string(),
    };

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSet);
    let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    // Exclusion still happens; the receipt failure is surfaced, not silent.
    assert!(is_source_excluded(&conn, id));
    assert!(!result.warnings.is_empty());
    assert!(result
        .warnings
        .iter()
        .any(|w| w.to_lowercase().contains("receipt")));
}

#[test]
fn test_execute_duplicates_writes_grouped_receipt() {
    let mut conn = setup_test_db();
    let (archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "dup_rcpt_hash", false);
    let excluded_id = insert_source(&conn, src_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive, "kept.jpg", Some(obj));

    let prefer = dir.path().to_str().unwrap().to_string();
    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], &prefer)).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeDuplicates);
    let result = execute_duplicates(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    assert!(is_source_excluded(&conn, excluded_id));

    let receipt = dir
        .path()
        .join(".canon-ledger/000001-exclude_duplicates.toml");
    let content = std::fs::read_to_string(&receipt).unwrap();
    assert!(content.contains("[[groups]]"));
    assert!(content.contains("hash = \"sha256:dup_rcpt_hash\""));
    assert!(content.contains("[[groups.kept]]"));
    assert!(content.contains("[[groups.excluded]]"));
    assert!(content.contains("rel_path = \"photo.jpg\""));
    assert!(content.contains("rel_path = \"kept.jpg\""));
}

#[test]
fn test_execute_set_objects_writes_object_receipt_and_links_all_sources() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "obj_rcpt_hash", false);
    let a = insert_source(&conn, src_root, "copy1.bin", Some(obj));
    let b = insert_source(&conn, src_root, "copy2.bin", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSetObject);
    let result = execute_set_objects(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    // decision_id stamped on every source sharing the object.
    assert_eq!(fetch_source_decision_id(&conn, a), Some(1));
    assert_eq!(fetch_source_decision_id(&conn, b), Some(1));

    let receipt = dir
        .path()
        .join(".canon-ledger/000001-exclude_set_object.toml");
    let content = std::fs::read_to_string(&receipt).unwrap();
    assert!(content.contains("[[objects]]"));
    assert!(content.contains("hash = \"sha256:obj_rcpt_hash\""));
    assert!(content.contains("[[objects.sources]]"));
    assert!(content.contains("rel_path = \"copy1.bin\""));
    assert!(content.contains("rel_path = \"copy2.bin\""));
}

// =========================================================================
// decision_id linkage + provenance chain
// =========================================================================

#[test]
fn test_execute_clear_links_decision_and_writes_receipt() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "clear_link_hash", false);
    let id = insert_source_excluded(&conn, src_root, "sub/a.tmp", Some(obj));

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeClear);
    let result = execute_clear(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    // Clear is itself a transition: source un-excluded AND decision_id stamped.
    assert!(!is_source_excluded(&conn, id));
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));

    let content =
        std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_clear.toml"))
            .unwrap();
    assert!(content.contains("command = \"exclude_clear\""));
    assert!(content.contains("[[items]]"));
    assert!(content.contains("rel_path = \"sub/a.tmp\""));
}

#[test]
fn test_execute_duplicates_links_excluded_not_kept() {
    let mut conn = setup_test_db();
    let (archive, _dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "dup_link_hash", false);
    let excluded_id = insert_source(&conn, src_root, "photo.jpg", Some(obj));
    let kept_id = insert_source(&conn, archive, "kept.jpg", Some(obj));

    let prefer = _dir.path().to_str().unwrap().to_string();
    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], &prefer)).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeDuplicates);
    execute_duplicates(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    // Excluded copy carries the decision_id; the kept archive copy does NOT.
    assert_eq!(fetch_source_decision_id(&conn, excluded_id), Some(1));
    assert_eq!(
        fetch_source_decision_id(&conn, kept_id),
        None,
        "kept copy is not a transition — must not be stamped"
    );
}

#[test]
fn test_execute_set_objects_stamps_all_roles_incl_archive() {
    // D1: object exclusion is universal — every source sharing the object,
    // including archive-role copies, gets the decision_id and appears in the receipt.
    let mut conn = setup_test_db();
    let (archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "universal_hash", false);
    let source_copy = insert_source(&conn, src_root, "junk.bin", Some(obj));
    let archive_copy = insert_source(&conn, archive, "kept/junk.bin", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSetObject);
    let result = execute_set_objects(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    assert!(is_object_excluded(&conn, obj));
    // Both roles stamped.
    assert_eq!(fetch_source_decision_id(&conn, source_copy), Some(1));
    assert_eq!(
        fetch_source_decision_id(&conn, archive_copy),
        Some(1),
        "archive-role source must be stamped — object exclusion is universal"
    );
    // Both roles listed in the receipt.
    let content = std::fs::read_to_string(
        dir.path()
            .join(".canon-ledger/000001-exclude_set_object.toml"),
    )
    .unwrap();
    assert!(content.contains("rel_path = \"junk.bin\""));
    assert!(content.contains("rel_path = \"kept/junk.bin\""));
}

#[test]
fn test_execute_set_captures_previous_decision_id_chain() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let id = insert_source(&conn, src_root, "a.jpg", None);
    // A prior decision (e.g. scan discovery or an earlier op) on the source.
    conn.execute(
        "UPDATE sources SET decision_id = 99 WHERE id = ?",
        rusqlite::params![id],
    )
    .unwrap();

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
    let decision = full_decision(DecisionCommand::ExcludeSet);
    execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    // Live pointer advances to the new decision; the receipt preserves the predecessor.
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
    let content =
        std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_set.toml")).unwrap();
    assert!(
        content.contains("previous_decision_id = 99"),
        "receipt should preserve the predecessor decision\n{content}"
    );
}

#[test]
fn test_execute_set_source_links_and_writes_receipt() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "single_src_hash", false);
    let id = insert_source(&conn, src_root, "one.tmp", Some(obj));

    // Drive the real check -> execute flow.
    let SourceExclusionCheck::Ready { item } = check_set_source_by_id(&conn, id).unwrap() else {
        panic!("expected Ready");
    };
    let decision = full_decision(DecisionCommand::ExcludeSet);
    let result = execute_set_source(&mut conn, &item, Some(&placement), Some(&decision)).unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    assert!(is_source_excluded(&conn, id));
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
    let content =
        std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_set.toml")).unwrap();
    assert!(content.contains("rel_path = \"one.tmp\""));
    assert!(content.contains("hash = \"sha256:single_src_hash\""));
}

#[test]
fn test_execute_clear_object_unstamps_and_writes_receipt() {
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "clear_obj_hash", true); // already excluded
    let id = insert_source(&conn, src_root, "dup.bin", Some(obj));

    let ObjectClearCheck::Ready {
        object_id,
        hash_prefix,
        hash,
    } = check_clear_object(&conn, "clear_obj_hash").unwrap()
    else {
        panic!("expected Ready");
    };
    let decision = full_decision(DecisionCommand::ExcludeClearObject);
    let result = execute_clear_object(
        &mut conn,
        object_id,
        &hash_prefix,
        &hash,
        Some(&placement),
        Some(&decision),
    )
    .unwrap();

    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    assert!(!is_object_excluded(&conn, obj));
    // clear-object is a transition on every source of the object.
    assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
    let content = std::fs::read_to_string(
        dir.path()
            .join(".canon-ledger/000001-exclude_clear_object.toml"),
    )
    .unwrap();
    assert!(content.contains("[[objects]]"));
    assert!(content.contains("hash = \"sha256:clear_obj_hash\""));
    assert!(content.contains("rel_path = \"dup.bin\""));
}

#[test]
fn test_object_exclude_receipt_lists_stamp_set_including_tombstones() {
    // stamp-set = receipt-set (presence-axis constraint): the object-level
    // stamp touches every sharer, present or not, so the receipt must list
    // the tombstones too — otherwise the stamp isn't reconstructable from
    // disk and the chain walk dead-ends on a receipt item that doesn't exist.
    let mut conn = setup_test_db();
    let (_archive, dir, placement) = ledger_root(&conn);
    let src_root = insert_root(&conn, "/source", "source", false);
    let obj = insert_object(&conn, "tomb_obj_hash", false);
    let present_id = insert_source(&conn, src_root, "still-here.bin", Some(obj));
    // A tombstone sharer: deleted from disk earlier, provenance link intact.
    let tomb_id = insert_source(&conn, src_root, "deleted.bin", Some(obj));
    conn.execute(
        "UPDATE sources SET present = 0, decision_id = 77 WHERE id = ?",
        rusqlite::params![tomb_id],
    )
    .unwrap();

    let decision = full_decision(DecisionCommand::ExcludeSetObject);
    execute_set_object(
        &mut conn,
        obj,
        "tomb_obj_hash",
        "sha256:tomb_obj_hash",
        &[],
        Some(&placement),
        Some(&decision),
    )
    .unwrap();

    // The stamp touched both sharers...
    assert_eq!(fetch_source_decision_id(&conn, present_id), Some(1));
    assert_eq!(fetch_source_decision_id(&conn, tomb_id), Some(1));

    // ...and the receipt lists the same set, tombstone marked and carrying
    // its pre-stamp predecessor.
    let content = std::fs::read_to_string(
        dir.path()
            .join(".canon-ledger/000001-exclude_set_object.toml"),
    )
    .unwrap();
    assert!(
        content.contains("rel_path = \"still-here.bin\""),
        "{content}"
    );
    assert!(content.contains("rel_path = \"deleted.bin\""), "{content}");
    assert!(content.contains("present = false"), "{content}");
    assert!(content.contains("previous_decision_id = 77"), "{content}");
    // The present sharer is unmarked — the field is serialized only for
    // the exceptional tombstone case.
    assert_eq!(content.matches("present = ").count(), 1, "{content}");
}

#[test]
fn test_execute_set_empty_plan_records_nothing() {
    // F4: a 0-item plan reaching execute must not leave a decision row, a
    // dangling receipt pointer, or a spurious finalize warning.
    let mut conn = setup_test_db();
    let (_archive, _dir, placement) = ledger_root(&conn);
    let plan = ExcludeSetPlan {
        items: vec![],
        root_count: 0,
        not_archived_count: 0,
    };
    let decision = full_decision(DecisionCommand::ExcludeSet);
    let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

    assert_eq!(result.count, 0);
    assert!(
        result.warnings.is_empty(),
        "warnings: {:?}",
        result.warnings
    );
    let decisions: i64 = conn
        .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(decisions, 0, "empty plan must not record a decision");
}
