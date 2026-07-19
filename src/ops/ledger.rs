//! Extraction ledger maintenance — rebuild the `decision_extractions`
//! projection from apply receipts already on disk.
//!
//! `decision_extractions` is a rebuildable index over durable receipts (the
//! consumption-readiness ADR): `reindex_extractions` is the "insurance"
//! clause exercised. It walks every `apply` decision, reads its receipt
//! leniently (old receipts lack the self-describing fields the writer now
//! emits), and rebuilds rows through the same `build_extraction_rows`
//! aggregation the forward apply path uses — a backfilled row is
//! structurally indistinguishable from a forward-recorded one.
//!
//! This is a maintenance path, not the query path: `ops/receipt.rs` stays
//! writer-only and decision-agnostic. The lenient read types below live
//! here, not there, because they must tolerate absent fields the writer
//! types must never gain.

use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

use crate::domain::decision::DecisionCommand;
use crate::domain::extraction::{build_extraction_rows, ExtractionItem, OriginDisposition};
use crate::repo::{self, Connection};

pub struct ReindexParams {
    pub dry_run: bool,
}

/// Coverage report for one `ledger reindex` run. Every decision examined
/// lands in exactly one of `no_receipt`/`unreachable`/`malformed`/
/// `no_rows_built`, or contributes rows and lands in `indexed`/
/// `already_current` — absence is never silent, and a bucket never claims
/// work it didn't do.
#[derive(Default)]
pub struct ReindexResult {
    /// Apply decisions examined.
    pub scanned: usize,
    /// Decisions whose rows were newly built (no prior extraction rows).
    pub indexed: Vec<i64>,
    /// Total rows upserted (0 in dry-run).
    pub rows_written: usize,
    /// Decisions that already had extraction rows before this run (still
    /// upserted, to converge on the same content).
    pub already_current: Vec<i64>,
    /// No receipt was ever recorded for this decision — reason string.
    pub no_receipt: Vec<(i64, String)>,
    /// A receipt was recorded but isn't readable right now — reason string.
    /// Distinct from `no_receipt`: nothing is concluded from absence here,
    /// only that this run couldn't reach the file.
    pub unreachable: Vec<(i64, String)>,
    /// The receipt read but failed integrity checks — reason string. Skipped.
    pub malformed: Vec<(i64, String)>,
    /// The receipt parsed but yielded no rows at all — every source root it
    /// names is unknown, so there was nothing to index. Its own bucket
    /// rather than `indexed`: a decision that produced no row was not
    /// indexed, and must not count as work done for the exit code.
    pub no_rows_built: Vec<(i64, String)>,
    /// Drawn source-root paths that matched no known root — reported rather
    /// than silently dropped. A decision with *some* matched roots still
    /// indexes (partial is honest and reported); one with none lands in
    /// `no_rows_built` above. (decision_id, root path)
    pub unknown_source_roots: Vec<(i64, String)>,
}

/// Lenient mirror of an apply receipt for the maintenance read path. Every
/// self-describing field is optional: pre-2026-07 receipts lack
/// transition/posture/origin_disposition/[meta.locus]. Item fields below are
/// present since the first apply receipts — required.
#[derive(Deserialize)]
struct ApplyReceiptDoc {
    meta: ReceiptMetaDoc,
    #[serde(default)]
    items: Vec<ApplyItemDoc>,
}

#[derive(Deserialize)]
struct ReceiptMetaDoc {
    decision_id: i64,
    #[serde(default)]
    origin_disposition: Option<String>,
    #[serde(default)]
    locus: Option<ReceiptLocusDoc>,
}

#[derive(Deserialize)]
struct ReceiptLocusDoc {
    path: String,
    id: i64,
}

#[derive(Deserialize)]
struct ApplyItemDoc {
    source_root: String,
    source_rel_path: String,
    destination_rel_path: String,
    size: i64,
    // hash/mtime/previous_decision_id deliberately not read — aggregate-only law.
}

/// Rebuild the extraction index from apply receipts on disk. Global,
/// whole-history — no scope args (this is maintenance, not exploration).
/// Writes only index rows: index maintenance is not a content decision, so
/// no decision row is recorded — this report is its record.
pub fn reindex_extractions(conn: &Connection, params: &ReindexParams) -> Result<ReindexResult> {
    let decisions = repo::decision::fetch_by_command(conn, DecisionCommand::Apply.as_str())?;
    let roots = repo::root::fetch_all(conn)?;

    // Which decisions already carry rows, in one query rather than one per
    // decision — this drives the indexed/already_current split below.
    let decision_ids: Vec<i64> = decisions.iter().map(|d| d.id).collect();
    let had_rows_before: HashSet<i64> =
        repo::decision::fetch_extractions_by_decisions(conn, &decision_ids)?
            .into_iter()
            .map(|row| row.decision_id)
            .collect();

    let mut result = ReindexResult {
        scanned: decisions.len(),
        ..Default::default()
    };

    for decision in &decisions {
        let (Some(receipt_root_id), Some(receipt_rel_path)) =
            (decision.receipt_root_id, decision.receipt_rel_path.as_ref())
        else {
            let reason = if decision.command_line.contains("--no-receipt") {
                "--no-receipt".to_string()
            } else {
                "recording mode had receipts off".to_string()
            };
            result.no_receipt.push((decision.id, reason));
            continue;
        };

        let Some(root) = roots.iter().find(|r| r.id == receipt_root_id) else {
            result.unreachable.push((
                decision.id,
                format!("destination root #{receipt_root_id} no longer known"),
            ));
            continue;
        };

        let root_path = Path::new(&root.path);
        if !root_path.is_dir() {
            result.unreachable.push((
                decision.id,
                format!("root path not present (offline?): {}", root.path),
            ));
            continue;
        }

        let full_path = root_path.join(receipt_rel_path);
        let content = match std::fs::read_to_string(&full_path) {
            Ok(c) => c,
            Err(_) => {
                result.unreachable.push((
                    decision.id,
                    format!("receipt file missing: {}", full_path.display()),
                ));
                continue;
            }
        };

        let doc: ApplyReceiptDoc = match toml::from_str(&content) {
            Ok(d) => d,
            Err(e) => {
                result
                    .malformed
                    .push((decision.id, format!("failed to parse receipt: {e}")));
                continue;
            }
        };

        if doc.meta.decision_id != decision.id {
            result.malformed.push((
                decision.id,
                format!(
                    "receipt decision_id {} does not match decision #{}",
                    doc.meta.decision_id, decision.id
                ),
            ));
            continue;
        }

        if doc.items.is_empty() {
            result
                .malformed
                .push((decision.id, "receipt has no items".to_string()));
            continue;
        }

        let items: Vec<ExtractionItem> = doc
            .items
            .iter()
            .map(|i| ExtractionItem {
                source_root: &i.source_root,
                source_rel_path: &i.source_rel_path,
                destination_rel_path: &i.destination_rel_path,
                size: i.size,
            })
            .collect();

        let (destination_root_id, destination_root_path) = match &doc.meta.locus {
            Some(locus) => (Some(locus.id), locus.path.clone()),
            None => (Some(receipt_root_id), root.path.clone()),
        };

        let disposition = doc
            .meta
            .origin_disposition
            .as_deref()
            .and_then(OriginDisposition::from_str);

        let (rows, unknown_roots) = build_extraction_rows(
            &items,
            &roots,
            (destination_root_id, &destination_root_path),
            disposition,
            decision.id,
        );

        for unknown in unknown_roots {
            result.unknown_source_roots.push((decision.id, unknown));
        }

        // A receipt whose every source root is unknown yields nothing to
        // write. Reporting that as "indexed (0 rows)" would overstate the
        // run and mask a universe where no work was possible, so it gets
        // its own bucket.
        if rows.is_empty() {
            result.no_rows_built.push((
                decision.id,
                "no drawn source root matched a known root".to_string(),
            ));
            continue;
        }

        if !params.dry_run {
            // Atomic per decision: replacing a legacy coarse row with precise
            // rows is delete-then-insert, and a concurrent `canon trail` must
            // never read the gap between the two.
            let tx = conn.unchecked_transaction()?;
            repo::decision::replace_extractions(&tx, &rows)?;
            tx.commit()?;
            result.rows_written += rows.len();
        }

        if had_rows_before.contains(&decision.id) {
            result.already_current.push(decision.id);
        } else {
            result.indexed.push(decision.id);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
    use crate::ops::apply::{
        ApplyExecuteParams, ApplyPlan, ApplyTransfer, ApplyViolations, TransferMode,
        TransferOutcome, TransferProgress,
    };
    use crate::ops::decision::DecisionParams;
    use crate::ops::fs::compute_partial_hash;
    use crate::ops::receipt::ReceiptPlacement;
    use crate::ops::test_helpers::{insert_root, setup_test_db};
    use std::io::Write;
    use std::path::Path;

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
    ) -> Vec<crate::domain::extraction::DecisionExtraction> {
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
        crate::ops::apply::execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision))
            .unwrap();
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
    fn old_receipt_without_self_describing_fields_parses_with_neutral_disposition() {
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        let src_dir = tempfile::tempdir().unwrap();
        insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);

        let decision_id: i64 = conn
            .query_row(
                "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
                 VALUES ('apply', 'canon apply old.lock', 'completed', '0.1.0', 0, ?1, '.canon-ledger/000001-apply.toml')
                 RETURNING id",
                [archive_root],
                |row| row.get(0),
            )
            .unwrap();

        let ledger_dir = archive_dir.path().join(".canon-ledger");
        std::fs::create_dir_all(&ledger_dir).unwrap();
        let receipt_toml = format!(
            r#"[meta]
receipt_version = 1
decision_id = {decision_id}
command = "apply"
status = "completed"
timestamp = 0
summary = "old receipt"
canon_version = "0.1.0"
command_line = "canon apply old.lock"

[[items]]
source_root = "{}"
source_rel_path = "a.jpg"
destination_rel_path = "a.jpg"
size = 5
mtime = 0
"#,
            src_dir.path().display()
        );
        std::fs::write(ledger_dir.join("000001-apply.toml"), receipt_toml).unwrap();

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.indexed, vec![decision_id]);
        assert!(result.malformed.is_empty());

        let rows = fetch_rows(&conn, decision_id);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].disposition, None);
        assert_eq!(rows[0].destination_root_id, Some(archive_root));
    }

    #[test]
    fn no_receipt_columns_categorized_as_no_receipt() {
        let conn = setup_test_db();
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES ('apply', 'canon apply --no-receipt m.lock', 'completed', '0.1.0', 0)",
            [],
        )
        .unwrap();
        let decision_id = latest_decision_id(&conn);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.scanned, 1);
        assert_eq!(result.no_receipt.len(), 1);
        assert_eq!(result.no_receipt[0].0, decision_id);
        assert_eq!(result.no_receipt[0].1, "--no-receipt");
        assert!(result.indexed.is_empty());
    }

    #[test]
    fn no_receipt_reason_when_recording_mode_had_receipts_off() {
        let conn = setup_test_db();
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES ('apply', 'canon apply m.lock', 'completed', '0.1.0', 0)",
            [],
        )
        .unwrap();

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.no_receipt[0].1, "recording mode had receipts off");
    }

    #[test]
    fn unknown_receipt_root_categorized_unreachable() {
        let conn = setup_test_db();
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
             VALUES ('apply', 'canon apply m.lock', 'completed', '0.1.0', 0, 999, '.canon-ledger/000001-apply.toml')",
            [],
        )
        .unwrap();
        let decision_id = latest_decision_id(&conn);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.unreachable.len(), 1);
        assert_eq!(result.unreachable[0].0, decision_id);
        assert!(result.unreachable[0]
            .1
            .contains("destination root #999 no longer known"));
    }

    #[test]
    fn root_path_offline_categorized_unreachable() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/no/such/path/on/disk", "archive", false);
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
             VALUES ('apply', 'canon apply m.lock', 'completed', '0.1.0', 0, ?1, '.canon-ledger/000001-apply.toml')",
            [root_id],
        )
        .unwrap();
        let decision_id = latest_decision_id(&conn);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.unreachable.len(), 1);
        assert_eq!(result.unreachable[0].0, decision_id);
        assert!(result.unreachable[0].1.contains("not present (offline?)"));
    }

    #[test]
    fn root_present_but_receipt_file_missing_categorized_unreachable() {
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let root_id = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
             VALUES ('apply', 'canon apply m.lock', 'completed', '0.1.0', 0, ?1, '.canon-ledger/000001-apply.toml')",
            [root_id],
        )
        .unwrap();
        let decision_id = latest_decision_id(&conn);

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        assert_eq!(result.unreachable.len(), 1);
        assert_eq!(result.unreachable[0].0, decision_id);
        assert!(result.unreachable[0].1.contains("receipt file missing"));
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
    fn receipt_whose_source_roots_are_all_unknown_builds_nothing() {
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        // Note: no source root is registered, so the receipt's source_root
        // matches nothing.
        let decision_id: i64 = conn
            .query_row(
                "INSERT INTO decisions (command, command_line, status, canon_version, created_at, receipt_root_id, receipt_rel_path)
                 VALUES ('apply', 'canon apply old.lock', 'completed', '0.1.0', 0, ?1, '.canon-ledger/000001-apply.toml')
                 RETURNING id",
                [archive_root],
                |row| row.get(0),
            )
            .unwrap();

        let ledger_dir = archive_dir.path().join(".canon-ledger");
        std::fs::create_dir_all(&ledger_dir).unwrap();
        std::fs::write(
            ledger_dir.join("000001-apply.toml"),
            format!(
                r#"[meta]
receipt_version = 1
decision_id = {decision_id}
command = "apply"
status = "completed"
timestamp = 0
summary = "receipt from a root canon no longer knows"
canon_version = "0.1.0"
command_line = "canon apply old.lock"

[[items]]
source_root = "/vol/long-gone"
source_rel_path = "a.jpg"
destination_rel_path = "a.jpg"
size = 5
mtime = 0
"#
            ),
        )
        .unwrap();

        let result = reindex_extractions(&conn, &ReindexParams { dry_run: false }).unwrap();
        // Not "indexed (0 rows)" — nothing was built, and the report says so.
        assert!(result.indexed.is_empty());
        assert!(result.already_current.is_empty());
        assert_eq!(result.no_rows_built.len(), 1);
        assert_eq!(result.no_rows_built[0].0, decision_id);
        assert_eq!(result.unknown_source_roots.len(), 1);
        assert_eq!(result.unknown_source_roots[0].1, "/vol/long-gone");
        assert_eq!(result.rows_written, 0);
        assert!(fetch_rows(&conn, decision_id).is_empty());
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
