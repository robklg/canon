//! Apply's receipt body: what a completed transfer says about itself on disk.
//!
//! The document shape is archive's own — the shared machinery serializes any
//! body and never inspects one, so the only part of a receipt that is common
//! across commands is its `[meta]` table.

use serde::Serialize;

use crate::core::ops::receipt::ReceiptMeta;

/// Apply-specific receipt.
#[derive(Serialize)]
pub(super) struct ApplyReceipt {
    pub meta: ReceiptMeta,
    pub items: Vec<ApplyReceiptItem>,
}

/// One item in an apply receipt — a single completed file transfer.
#[derive(Serialize)]
pub(super) struct ApplyReceiptItem {
    pub source_root: String,
    pub source_rel_path: String,
    pub destination_rel_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub size: i64,
    pub mtime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::ops::execute::{
        execute_apply, ApplyExecuteParams, TransferMode, TransferOutcome, TransferProgress,
    };
    use crate::archive::ops::plan::{ApplyPlan, ApplyTransfer, ApplyViolations};
    use crate::core::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
    use crate::core::domain::decision::DecisionCommand;
    use crate::core::domain::extraction::OriginDisposition;
    use crate::core::ops::decision::DecisionParams;
    use crate::core::ops::fs::compute_partial_hash;
    use crate::core::ops::ledger::{reindex_extractions, ReindexParams};
    use crate::core::ops::receipt::{write_receipt, ReceiptLocus, ReceiptPlacement};
    use crate::core::repo::{self, Connection};
    use crate::core::testing::{insert_root, setup_test_db};
    use std::io::Write;
    use std::path::Path;
    use tempfile::tempdir;

    fn make_apply_receipt() -> ApplyReceipt {
        ApplyReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 43,
                command: "apply".to_string(),
                transition: "archived".to_string(),
                posture: "performed".to_string(),
                status: "completed".to_string(),
                timestamp: 1744300800,
                scope: Some(vec!["/Volumes/old-laptop/Photos".to_string()]),
                reason: Some("Italy 2016".to_string()),
                summary: "Applied 2 files".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply manifest.toml".to_string(),
                manifest: Some("/Volumes/Archive/manifest.toml".to_string()),
                origin_disposition: Some("retained".to_string()),
                locus: ReceiptLocus {
                    path: "/Volumes/Archive".to_string(),
                    id: 7,
                },
            },
            items: vec![ApplyReceiptItem {
                source_root: "/Volumes/old-laptop".to_string(),
                source_rel_path: "Photos/italy/IMG_001.jpg".to_string(),
                destination_rel_path: "Media/2016/Italy/IMG_001.jpg".to_string(),
                hash: Some("sha256:abc123".to_string()),
                size: 3456789,
                mtime: 1700000000,
                previous_decision_id: Some(12),
            }],
        }
    }

    #[test]
    fn test_serialize_items_present() {
        let receipt = make_apply_receipt();
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            toml_str.contains("[[items]]"),
            "missing [[items]]\n{toml_str}"
        );
        assert!(toml_str.contains("source_root = \"/Volumes/old-laptop\""));
        assert!(toml_str.contains("destination_rel_path = \"Media/2016/Italy/IMG_001.jpg\""));
        assert!(toml_str.contains("previous_decision_id = 12"));
    }

    #[test]
    fn test_serialize_previous_decision_id_omitted_when_none() {
        let receipt = ApplyReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 1,
                command: "apply".to_string(),
                transition: "archived".to_string(),
                posture: "performed".to_string(),
                status: "completed".to_string(),
                timestamp: 0,
                scope: None,
                reason: None,
                summary: "done".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply m.lock".to_string(),
                manifest: None,
                origin_disposition: None,
                locus: ReceiptLocus {
                    path: "/archive".to_string(),
                    id: 1,
                },
            },
            items: vec![ApplyReceiptItem {
                source_root: "/src".to_string(),
                source_rel_path: "file.jpg".to_string(),
                destination_rel_path: "Media/file.jpg".to_string(),
                hash: None,
                size: 100,
                mtime: 0,
                previous_decision_id: None,
            }],
        };
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            !toml_str.contains("previous_decision_id"),
            "previous_decision_id should be absent\n{toml_str}"
        );
        assert!(
            !toml_str.contains("hash"),
            "hash should be absent\n{toml_str}"
        );
    }

    /// A fully-populated apply receipt survives the real write path and reads
    /// back as TOML. The tests above serialize the body; this one pins the
    /// *file* — the writer prepends a comment header and writes to an
    /// `.incomplete` path, and neither is exercised anywhere else.
    #[test]
    fn apply_receipt_round_trips_through_the_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000043-apply.toml");

        write_receipt(&path, &make_apply_receipt(), "Applied 1 file").unwrap();

        let written = std::fs::read_to_string(dir.path().join("000043-apply.incomplete")).unwrap();
        let parsed: toml::Table =
            toml::from_str(&written).expect("receipt must parse back as TOML");
        assert_eq!(parsed["meta"]["decision_id"].as_integer(), Some(43));
        assert_eq!(parsed["items"].as_array().unwrap().len(), 1);
    }

    struct NoopProgress;
    impl TransferProgress for NoopProgress {
        fn on_start(&self, _total: usize) {}
        fn on_transfer(&self, _: usize, _: usize, _: &str, _: &str, _: &TransferOutcome) {}
        fn on_interrupt(&self) {}
        fn on_finish(&self) {}
    }

    fn make_decision_params(receipt_enabled: bool) -> DecisionParams {
        DecisionParams {
            command: DecisionCommand::Apply,
            scope: Vec::new(),
            command_line: "canon apply m.lock".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled,
            ledger_config: LedgerConfig {
                recording: if receipt_enabled {
                    RecordingMode::Full
                } else {
                    RecordingMode::Records
                },
                layout: ReceiptLayout::Central,
                root: None,
            },
        }
    }

    /// Write one real file, build a single-transfer plan for it, and hand
    /// back plan + expected size (extraction rows need real bytes on disk
    /// for both the forward path and the reindex read-back).
    fn single_transfer_plan(
        src_dir: &Path,
        rel_path: &str,
        archive_rel_path: &str,
        contents: &[u8],
    ) -> ApplyPlan {
        let src_file = src_dir.join(rel_path);
        if let Some(parent) = src_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(contents)
            .unwrap();
        let meta = std::fs::metadata(&src_file).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&src_file, size as u64).unwrap();

        ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.display().to_string(),
                source_rel_path: rel_path.to_string(),
                dest_rel_path: archive_rel_path.to_string(),
                archive_rel_path: archive_rel_path.to_string(),
                object_id: None,
                partial_hash: hash,
                size,
                mtime,
                hash: None,
            }],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
        }
    }

    fn latest_decision_id(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT id FROM decisions ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn wipe_extractions(conn: &Connection) {
        conn.execute("DELETE FROM decision_extractions", [])
            .unwrap();
    }

    fn fetch_rows(
        conn: &Connection,
        decision_id: i64,
    ) -> Vec<crate::core::domain::extraction::DecisionExtraction> {
        repo::decision::fetch_extractions_by_decisions(conn, &[decision_id]).unwrap()
    }

    /// Run a real, receipt-writing apply of one file and return
    /// (conn, decision_id, archive_dir, src_dir).
    fn run_real_apply(
        transfer_mode: TransferMode,
    ) -> (Connection, i64, tempfile::TempDir, tempfile::TempDir) {
        let conn = setup_test_db();
        let src_dir = tempfile::tempdir().unwrap();
        insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let plan = single_transfer_plan(
            src_dir.path(),
            "2016/italy/a.jpg",
            "2016/Italy/a.jpg",
            b"hello world",
        );

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };
        let params = ApplyExecuteParams {
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };
        let decision = make_decision_params(true);
        execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        let decision_id = latest_decision_id(&conn);
        (conn, decision_id, archive_dir, src_dir)
    }

    #[test]
    fn round_trip_law_backfill_matches_forward_recording() {
        let (conn, decision_id, _archive_dir, _src_dir) = run_real_apply(TransferMode::Copy);

        let forward_rows = fetch_rows(&conn, decision_id);
        assert_eq!(forward_rows.len(), 1);

        wipe_extractions(&conn);
        assert!(fetch_rows(&conn, decision_id).is_empty());

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.indexed, vec![decision_id]);
        assert!(result.already_current.is_empty());
        assert_eq!(result.rows_written, 1);

        let backfilled_rows = fetch_rows(&conn, decision_id);
        assert_eq!(backfilled_rows, forward_rows);
    }

    #[test]
    fn round_trip_law_move_mode_relocated() {
        let (conn, decision_id, _archive_dir, _src_dir) = run_real_apply(TransferMode::Move);

        let forward_rows = fetch_rows(&conn, decision_id);
        assert_eq!(
            forward_rows[0].disposition,
            Some(OriginDisposition::Relocated)
        );

        wipe_extractions(&conn);
        reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        let backfilled_rows = fetch_rows(&conn, decision_id);
        assert_eq!(backfilled_rows, forward_rows);
    }

    #[test]
    fn malformed_toml_skipped_and_reported_rest_processed() {
        let (conn, good_decision_id, archive_dir, _src_dir) = run_real_apply(TransferMode::Copy);

        // A second apply decision whose receipt file is garbage TOML.
        let bad_root = archive_dir.path().to_str().unwrap();
        let bad_root_id: i64 = conn
            .query_row("SELECT id FROM roots WHERE path = ?1", [bad_root], |r| {
                r.get(0)
            })
            .unwrap();
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
             VALUES ('apply', 'canon apply bad.lock', 'completed', '0.1.0', 0, ?1, '.canon-ledger/000002-apply.toml')",
            [bad_root_id],
        )
        .unwrap();
        let bad_decision_id = latest_decision_id(&conn);
        std::fs::write(
            archive_dir.path().join(".canon-ledger/000002-apply.toml"),
            "this is not valid toml {{{",
        )
        .unwrap();

        wipe_extractions(&conn);
        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.scanned, 2);
        assert_eq!(result.malformed.len(), 1);
        assert_eq!(result.malformed[0].0, bad_decision_id);
        assert_eq!(result.indexed, vec![good_decision_id]);
    }

    #[test]
    fn idempotent_reindex_twice_identical() {
        let (conn, decision_id, _archive_dir, _src_dir) = run_real_apply(TransferMode::Copy);
        wipe_extractions(&conn);

        reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        let first = fetch_rows(&conn, decision_id);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        let second = fetch_rows(&conn, decision_id);

        assert_eq!(first, second);
        assert_eq!(result.already_current, vec![decision_id]);
        assert!(result.indexed.is_empty());
    }

    #[test]
    fn reindex_after_forward_recording_no_duplicates() {
        let (conn, decision_id, _archive_dir, _src_dir) = run_real_apply(TransferMode::Copy);

        // Rows already exist from the forward path — reindex should converge,
        // not duplicate.
        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.already_current, vec![decision_id]);
        let rows = fetch_rows(&conn, decision_id);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn dry_run_produces_report_without_writing() {
        let (conn, decision_id, _archive_dir, _src_dir) = run_real_apply(TransferMode::Copy);
        wipe_extractions(&conn);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: true }).unwrap();
        assert_eq!(result.indexed, vec![decision_id]);
        assert_eq!(result.rows_written, 0);
        assert!(fetch_rows(&conn, decision_id).is_empty());
    }
}
