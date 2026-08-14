//! Deletion-receipt writing: merging per-root deletion entries and writing
//! each root's source-local receipt at scan completion. The capture of items
//! before the present→absent flip stays in `pipeline.rs`, beside the two
//! sites that perform the flip — this file only merges and writes what the
//! pipeline already captured.

use std::collections::HashMap;

use crate::domain::decision::DecisionStatus;
use crate::ops::decision::{DecisionParams, DecisionRecorder};
use crate::ops::receipt::{DeletionReceipt, DeletionReceiptItem, ReceiptKind, ReceiptPlacement};
use crate::repo::{self, Connection};

/// Merge deletion entries that share a root so each root yields one receipt.
/// Items are concatenated in the order roots were first seen and re-sorted by
/// rel_path for a stable receipt. Sources can't appear twice across a root's
/// entries: each is captured only while present, and each capture flips it, so a
/// later capture for the same root never re-sees it.
fn coalesce_by_root(
    per_root: Vec<(i64, String, Vec<DeletionReceiptItem>)>,
) -> Vec<(i64, String, Vec<DeletionReceiptItem>)> {
    let mut order: Vec<i64> = Vec::new();
    let mut by_root: HashMap<i64, (String, Vec<DeletionReceiptItem>)> = HashMap::new();
    for (root_id, root_path, items) in per_root {
        match by_root.entry(root_id) {
            std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().1.extend(items),
            std::collections::hash_map::Entry::Vacant(e) => {
                order.push(root_id);
                e.insert((root_path, items));
            }
        }
    }
    order
        .into_iter()
        .map(|root_id| {
            let (root_path, mut items) = by_root.remove(&root_id).expect("root_id was recorded");
            items.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
            (root_id, root_path, items)
        })
        .collect()
}

/// Write source-local deletion receipts — one per root that lost sources — under
/// the single scan decision.
///
/// Placement and existence are known only after the walk, so this runs at
/// completion. Gated on `params.receipt_enabled` and a live decision id; a root
/// with no deleted items is skipped, so a scan that deletes nothing writes no
/// receipt. Each receipt lands at its own root's `.canon-ledger/` — the loss
/// travels with that drive. Every written receipt is linked to its root in the
/// scope index (`decision_scopes.receipt_rel_path`) so a by-root query recovers
/// the decision and its receipt — the many-receipts-per-decision case the single
/// `decisions.receipt_*` columns can't hold. Write and index failures are
/// collected as recorder warnings, never halting the scan.
///
/// Entries that share a root are coalesced into one receipt: a single scan can
/// lose files in one root through both the sweep and a `--missing` path, and
/// multiple `--missing` paths can name subtrees of the same root. Each root still
/// gets exactly one receipt listing everything it lost.
pub fn write_deletion_receipts(
    conn: &Connection,
    recorder: &mut DecisionRecorder,
    params: &DecisionParams,
    per_root: Vec<(i64, String, Vec<DeletionReceiptItem>)>,
    summary: &str,
) {
    if !params.receipt_enabled {
        return;
    }
    let Some(decision_id) = recorder.decision_id() else {
        return;
    };
    let command = params.command.as_str();

    for (root_id, root_path, items) in coalesce_by_root(per_root) {
        if items.is_empty() {
            continue;
        }
        let receipt = DeletionReceipt {
            meta: params.receipt_meta(
                decision_id,
                DecisionStatus::Completed,
                summary,
                (root_id, root_path.as_str()),
                ReceiptKind::Deletion,
                None,
            ),
            items,
        };
        let placement = ReceiptPlacement::LedgerRoot {
            root_id,
            root_path: root_path.clone(),
        };
        if let Some(receipt_ref) =
            recorder.write_placed_receipt(&placement, command, &receipt, summary)
        {
            if let Err(e) = repo::decision::set_scope_receipt(
                conn,
                decision_id,
                receipt_ref.root_id,
                &root_path,
                &receipt_ref.rel_path,
            ) {
                recorder.push_warning(format!(
                    "Warning: failed to index deletion receipt for root {}: {e}",
                    receipt_ref.root_id
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::domain::config::{LedgerConfig, RecordingMode};
    use crate::domain::decision::DecisionCommand;
    use crate::ops::decision::DecisionCounts;

    fn scan_params(recording: RecordingMode, no_receipt: bool) -> DecisionParams {
        DecisionParams {
            command: DecisionCommand::Scan,
            scope: Vec::new(),
            command_line: "canon scan".to_string(),
            reason: None,
            record_enabled: recording != RecordingMode::Off,
            receipt_enabled: recording == RecordingMode::Full && !no_receipt,
            ledger_config: LedgerConfig {
                recording,
                ..LedgerConfig::default()
            },
        }
    }

    fn sample_items() -> Vec<DeletionReceiptItem> {
        vec![DeletionReceiptItem {
            rel_path: "gone.txt".to_string(),
            hash: None,
            size: 100,
            mtime: 1000,
            previous_decision_id: Some(3),
        }]
    }

    #[test]
    fn write_deletion_receipts_writes_source_local_file() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![(1, root_path.clone(), sample_items())],
            "Scanned 0 files: 0 new, 0 updated, 0 moved, 0 unchanged, 1 missing",
        );

        let receipt = temp
            .path()
            .join(".canon-ledger")
            .join(format!("{id:06}-scan.toml"));
        assert!(receipt.exists(), "receipt should land on the drive");
        let body = std::fs::read_to_string(&receipt).unwrap();
        assert!(body.contains("command = \"scan\""));
        assert!(body.contains("rel_path = \"gone.txt\""));
        // The what + posture: a scan witnessed a loss, it did not perform one.
        assert!(body.contains("transition = \"deleted\""));
        assert!(body.contains("posture = \"observed\""));
        // The where: this drive's root identity, from placement.
        let locus = &body[body.find("[meta.locus]").expect("locus table present")..];
        assert!(locus.contains(&format!("path = \"{root_path}\"")));
        assert!(locus.contains("id = 1"));
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn write_deletion_receipts_skipped_when_receipts_disabled() {
        // Records mode: DB row yes, receipt file no.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Records, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![(1, root_path, sample_items())],
            "summary",
        );

        assert!(!temp.path().join(".canon-ledger").exists());
    }

    #[test]
    fn write_deletion_receipts_zero_deletions_no_file_but_decision_row_exists() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        // No deletions this scan.
        write_deletion_receipts(&conn, &mut recorder, &params, Vec::new(), "summary");
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(0),
                completed: Some(0),
                failed: None,
                skipped: Some(0),
            },
            "summary",
        );

        assert!(
            !temp.path().join(".canon-ledger").exists(),
            "no receipt for a scan that deleted nothing"
        );
        let d = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(d.status, "completed");
    }

    #[test]
    fn write_deletion_receipts_multi_root_writes_and_indexes_each() {
        // One scan decision, deletions in two roots → one source-local receipt per
        // root, each indexed in decision_scopes for a by-root lookup.
        let conn = repo::open_in_memory_for_test();
        let temp_a = TempDir::new().unwrap();
        let temp_b = TempDir::new().unwrap();
        let root_a = temp_a.path().to_str().unwrap().to_string();
        let root_b = temp_b.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![
                (11, root_a.clone(), sample_items()),
                (22, root_b.clone(), sample_items()),
            ],
            "summary",
        );

        // Each receipt lands on its own drive.
        let name = format!("{id:06}-scan.toml");
        assert!(temp_a.path().join(".canon-ledger").join(&name).exists());
        assert!(temp_b.path().join(".canon-ledger").join(&name).exists());

        // Each per-root receipt carries *its own* locus identity — the whole
        // point for a receipt read after its drive is gone. A shared meta.scope
        // could not disambiguate the two.
        let body_a =
            std::fs::read_to_string(temp_a.path().join(".canon-ledger").join(&name)).unwrap();
        let locus_a = &body_a[body_a.find("[meta.locus]").unwrap()..];
        assert!(locus_a.contains(&format!("path = \"{root_a}\"")));
        assert!(locus_a.contains("id = 11"));
        let body_b =
            std::fs::read_to_string(temp_b.path().join(".canon-ledger").join(&name)).unwrap();
        let locus_b = &body_b[body_b.find("[meta.locus]").unwrap()..];
        assert!(locus_b.contains(&format!("path = \"{root_b}\"")));
        assert!(locus_b.contains("id = 22"));

        // The indexed rel_path is relative to the root (includes .canon-ledger/),
        // matching decisions.receipt_rel_path semantics.
        let rel_path = format!(".canon-ledger/{name}");

        // Both roots are indexed; the retirement query (WHERE root_id = ?) recovers
        // the decision and its receipt for each.
        for root_id in [11_i64, 22] {
            let (did, receipt): (i64, String) = conn
                .query_row(
                    "SELECT decision_id, receipt_rel_path FROM decision_scopes
                     WHERE root_id = ? AND receipt_rel_path IS NOT NULL",
                    [root_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(did, id);
            assert_eq!(receipt, rel_path);
        }
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn write_deletion_receipts_coalesces_same_root() {
        // Two entries for the same root (a sweep plus a --missing subtree, say)
        // merge into one receipt listing all of that root's lost sources.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let params = scan_params(RecordingMode::Full, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();

        let item = |rel: &str| DeletionReceiptItem {
            rel_path: rel.to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            previous_decision_id: None,
        };

        write_deletion_receipts(
            &conn,
            &mut recorder,
            &params,
            vec![
                (7, root_path.clone(), vec![item("work/a.txt")]),
                (7, root_path, vec![item("vacation/b.txt")]),
            ],
            "summary",
        );

        // One receipt file for the root, listing both items sorted by rel_path.
        let receipt = temp
            .path()
            .join(".canon-ledger")
            .join(format!("{id:06}-scan.toml"));
        let body = std::fs::read_to_string(&receipt).unwrap();
        let vac = body.find("vacation/b.txt").expect("vacation item present");
        let work = body.find("work/a.txt").expect("work item present");
        assert!(vac < work, "merged items should be sorted by rel_path");

        // Only one scope row is indexed for the root — a single receipt.
        let indexed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM decision_scopes
                 WHERE root_id = 7 AND receipt_rel_path IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexed, 1);
        assert!(recorder.take_warnings().is_empty());
    }
}
