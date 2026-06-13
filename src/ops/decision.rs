use std::path::PathBuf;

use crate::domain::config::LedgerConfig;
use crate::domain::decision::{DecisionCommand, DecisionStatus};
use crate::ops::receipt::{
    compute_ledger_root_receipt_rel_path, compute_targeted_receipt_rel_path, finalize_receipt,
    ReceiptPlacement, ReceiptRef,
};
use crate::repo::{self, Connection};

/// Parameters for starting a decision record.
pub struct DecisionParams {
    pub command: DecisionCommand,
    pub scope: Option<Vec<String>>,
    pub command_line: String,
    pub reason: Option<String>,
    /// Whether to write a DB decision record. False for recording=off or dry-run.
    pub record_enabled: bool,
    /// Whether to write a receipt file. False unless recording=full and no --no-receipt.
    pub receipt_enabled: bool,
    /// Ledger config for receipt path computation.
    pub ledger_config: LedgerConfig,
}

/// Outcome counts for a decision record.
pub struct DecisionCounts {
    pub attempted: Option<i64>,
    pub completed: Option<i64>,
    pub failed: Option<i64>,
    pub skipped: Option<i64>,
}

/// Records a decision. Created before execution, completed after.
/// Catches its own errors — recording failure collects warnings, never halts the command.
///
/// Warnings are accumulated in the recorder and can be retrieved via `take_warnings()`.
/// For recorders owned by the interface layer, the interface drains and displays warnings.
/// For recorders created inside ops execute functions, warnings are dropped with the
/// recorder — this is acceptable because database failures that cause recording to fail
/// will also manifest through the main operation's error path.
pub struct DecisionRecorder {
    id: Option<i64>,
    /// Stored ReceiptRef for callers that need root_id/rel_path (e.g. receipt DB linkage).
    receipt_ref: Option<ReceiptRef>,
    /// Absolute path to the final `.toml` file, used for write and finalize.
    receipt_abs_path: Option<PathBuf>,
    warnings: Vec<String>,
}

impl DecisionRecorder {
    /// Expose the decision ID for receipt writing and source decision_id.
    /// Returns None if recording is disabled or the INSERT failed.
    pub fn decision_id(&self) -> Option<i64> {
        self.id
    }

    /// Expose the receipt reference (root_id + rel_path) stored in the DB.
    /// Returns None if receipts are disabled or path computation failed.
    pub fn receipt_ref(&self) -> Option<&ReceiptRef> {
        self.receipt_ref.as_ref()
    }

    /// Expose the absolute path for receipt writing.
    /// Returns None if receipts are disabled or path computation failed.
    pub fn receipt_abs_path(&self) -> Option<&std::path::Path> {
        self.receipt_abs_path.as_deref()
    }

    /// Collect an external warning into the recorder's warning list.
    /// Used by callers (e.g. execute_apply) to report receipt write failures.
    pub fn push_warning(&mut self, msg: String) {
        self.warnings.push(msg);
    }

    /// Insert the initial "started" record.
    ///
    /// If `receipt_enabled` and `receipt_ctx` is `Some`, computes the receipt
    /// path, creates the `.canon-ledger/` directory, and updates the decision
    /// record with the receipt location. Failures here are collected as warnings
    /// and don't prevent the recorder from functioning.
    ///
    /// If `record_enabled` is false (recording=off, dry-run), returns a no-op recorder.
    /// If the INSERT fails, collects a warning and returns a no-op recorder.
    pub fn start(
        conn: &Connection,
        params: &DecisionParams,
        placement: Option<&ReceiptPlacement>,
    ) -> Self {
        if !params.record_enabled {
            return DecisionRecorder {
                id: None,
                receipt_ref: None,
                receipt_abs_path: None,
                warnings: Vec::new(),
            };
        }

        let canon_version = env!("CARGO_PKG_VERSION");

        let id = match repo::decision::insert_started(
            conn,
            params.command.as_str(),
            params.scope.as_deref(),
            &params.command_line,
            params.reason.as_deref(),
            canon_version,
            None, // receipt fields populated below via update_receipt_path
            None,
        ) {
            Ok(id) => id,
            Err(e) => {
                return DecisionRecorder {
                    id: None,
                    receipt_ref: None,
                    receipt_abs_path: None,
                    warnings: vec![format!("Warning: failed to record decision: {e}")],
                };
            }
        };

        // Compute receipt path if receipts are enabled and context is provided.
        let (receipt_ref, receipt_abs_path, warnings) = if params.receipt_enabled {
            if let Some(placement) = placement {
                compute_and_register_receipt(conn, id, params, placement)
            } else {
                (None, None, Vec::new())
            }
        } else {
            (None, None, Vec::new())
        };

        // Warn if receipt was requested but couldn't be set up (ctx missing is not a warning).
        let _ = &warnings; // consumed below

        DecisionRecorder {
            id: Some(id),
            receipt_ref,
            receipt_abs_path,
            warnings,
        }
    }

    /// Update the record with completion data. No-op if disabled or start failed.
    /// Collects a warning if the UPDATE fails.
    ///
    /// If a receipt path is stored, renames the `.incomplete` file to `.toml`
    /// as part of completion. Finalization failure collects a warning.
    pub fn complete(
        &mut self,
        conn: &Connection,
        status: DecisionStatus,
        counts: DecisionCounts,
        summary: &str,
    ) {
        let Some(id) = self.id else {
            return;
        };

        if let Err(e) = repo::decision::update_completed(
            conn,
            id,
            status.as_str(),
            counts.attempted,
            counts.completed,
            counts.failed,
            counts.skipped,
            Some(summary),
        ) {
            self.warnings
                .push(format!("Warning: failed to update decision record: {e}"));
        }

        // Finalize the receipt file: .incomplete → .toml
        if let Some(ref path) = self.receipt_abs_path {
            if let Err(e) = finalize_receipt(path) {
                self.warnings
                    .push(format!("Warning: failed to finalize receipt: {e}"));
            }
        }
    }

    /// Update to interrupted status. Best-effort.
    pub fn interrupted(&mut self, conn: &Connection) {
        let Some(id) = self.id else {
            return;
        };

        if let Err(e) = repo::decision::update_completed(
            conn,
            id,
            DecisionStatus::Interrupted.as_str(),
            None,
            None,
            None,
            None,
            None,
        ) {
            self.warnings
                .push(format!("Warning: failed to update decision record: {e}"));
        }

        // Finalize even on interrupt — partial receipt is better than .incomplete.
        if let Some(ref path) = self.receipt_abs_path {
            if let Err(e) = finalize_receipt(path) {
                self.warnings
                    .push(format!("Warning: failed to finalize receipt: {e}"));
            }
        }
    }

    /// Drain accumulated warnings. Returns an empty vec if no warnings.
    pub fn take_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.warnings)
    }
}

// ---------------------------------------------------------------------------
// Private helper
// ---------------------------------------------------------------------------

/// Compute the receipt path, create the directory, and update the DB record.
///
/// Returns `(receipt_ref, receipt_abs_path, warnings)`.
/// On any failure, returns `(None, None, [warning])` — the command proceeds without receipt.
fn compute_and_register_receipt(
    conn: &Connection,
    decision_id: i64,
    params: &DecisionParams,
    placement: &ReceiptPlacement,
) -> (Option<ReceiptRef>, Option<PathBuf>, Vec<String>) {
    let (root_id, base_abs, rel_path) = match placement {
        ReceiptPlacement::Targeted {
            archive_root_id,
            archive_root_path,
            base_dir_rel,
        } => {
            let rel_path = compute_targeted_receipt_rel_path(
                decision_id,
                params.command.as_str(),
                base_dir_rel,
                &params.ledger_config.layout,
            );
            (*archive_root_id, archive_root_path.clone(), rel_path)
        }
        ReceiptPlacement::LedgerRoot { root_id, root_path } => {
            let rel_path =
                compute_ledger_root_receipt_rel_path(decision_id, params.command.as_str());
            (*root_id, root_path.clone(), rel_path)
        }
    };

    let abs_path = PathBuf::from(&base_abs).join(&rel_path);

    // Ensure the directory exists before the first write.
    if let Some(parent) = abs_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return (
                None,
                None,
                vec![format!(
                    "Warning: could not create receipt directory {}: {e}",
                    parent.display()
                )],
            );
        }
    }

    // Update the DB record with the receipt location.
    if let Err(e) =
        repo::decision::update_receipt_path(conn, decision_id, Some(root_id), Some(&rel_path))
    {
        return (
            None,
            None,
            vec![format!(
                "Warning: failed to store receipt path in decision record: {e}"
            )],
        );
    }

    let receipt_ref = ReceiptRef { root_id, rel_path };

    (Some(receipt_ref), Some(abs_path), Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::config::{LedgerConfig, RecordingMode};
    use crate::repo::db::open_in_memory_for_test;
    use tempfile::tempdir;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    fn count_decisions(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM decisions", [], |row| row.get(0))
            .unwrap()
    }

    fn make_params(command: DecisionCommand, record_enabled: bool) -> DecisionParams {
        DecisionParams {
            command,
            scope: None,
            command_line: "canon test".to_string(),
            reason: None,
            record_enabled,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        }
    }

    #[test]
    fn recorder_start_creates_record() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);

        let recorder = DecisionRecorder::start(&conn, &params, None);

        assert!(recorder.id.is_some());
        assert!(recorder.warnings.is_empty());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn recorder_complete_updates_record() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::ExcludeSet, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(10),
                completed: Some(10),
                failed: Some(0),
                skipped: None,
            },
            "Excluded 10 sources",
        );

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.status, "completed");
        assert_eq!(decision.count_attempted, Some(10));
        assert_eq!(decision.count_completed, Some(10));
        assert_eq!(decision.summary, Some("Excluded 10 sources".to_string()));
    }

    #[test]
    fn recorder_disabled_creates_no_record() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);

        let recorder = DecisionRecorder::start(&conn, &params, None);

        assert!(recorder.id.is_none());
        assert_eq!(count_decisions(&conn), 0);
    }

    #[test]
    fn recorder_interrupted_sets_status() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Apply, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        recorder.interrupted(&conn);

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.status, "interrupted");
    }

    #[test]
    fn recorder_disabled_complete_is_noop() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        // Should not panic
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: None,
                completed: None,
                failed: None,
                skipped: None,
            },
            "test",
        );

        assert_eq!(count_decisions(&conn), 0);
    }

    #[test]
    fn recorder_start_with_reason_and_scope() {
        let conn = setup_test_db();
        let params = DecisionParams {
            command: DecisionCommand::ExcludeSet,
            scope: Some(vec!["/photos".to_string()]),
            command_line: "canon exclude set --reason 'OS files'".to_string(),
            reason: Some("OS files".to_string()),
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, None);

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.command, "exclude_set");
        assert_eq!(decision.scope, Some(vec!["/photos".to_string()]));
        assert_eq!(decision.reason, Some("OS files".to_string()));
        assert!(decision.command_line.contains("--reason"));
    }

    #[test]
    fn recorder_canon_version_populated() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let recorder = DecisionRecorder::start(&conn, &params, None);

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.canon_version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_recorder_record_enabled_creates_row() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn test_recorder_record_disabled_no_row() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_none());
        assert_eq!(count_decisions(&conn), 0);
    }

    #[test]
    fn test_recorder_receipt_disabled_still_records_db() {
        let conn = setup_test_db();
        let params = DecisionParams {
            command: DecisionCommand::Apply,
            scope: None,
            command_line: "canon apply m.lock".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn test_recorder_decision_id_some_when_enabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_some());
    }

    #[test]
    fn test_recorder_decision_id_none_when_disabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_none());
    }

    #[test]
    fn test_recorder_insert_started_receipt_columns_null_without_ctx() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Apply, true);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();
        let d = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(d.receipt_root_id.is_none());
        assert!(d.receipt_rel_path.is_none());
        assert!(recorder.receipt_ref().is_none());
        assert!(recorder.receipt_abs_path().is_none());
    }

    #[test]
    fn test_recorder_complete_updates() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::ExcludeSet, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(5),
                completed: Some(5),
                failed: Some(0),
                skipped: None,
            },
            "Excluded 5 sources",
        );
        let d = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(d.status, "completed");
        assert_eq!(d.count_completed, Some(5));
    }

    #[test]
    fn test_recorder_warnings_collected() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.warnings.is_empty());
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: None,
                completed: None,
                failed: None,
                skipped: None,
            },
            "done",
        );
        let warnings = recorder.take_warnings();
        assert!(warnings.is_empty());
    }

    // =========================================================================
    // Receipt context tests
    // =========================================================================

    fn make_receipt_params() -> DecisionParams {
        DecisionParams {
            command: DecisionCommand::Apply,
            scope: None,
            command_line: "canon apply manifest.toml".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: LedgerConfig::default(),
        }
    }

    #[test]
    fn test_recorder_with_receipt_ctx_sets_receipt_ref() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 7,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: "Media/2016/Italy".to_string(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));

        assert!(recorder.decision_id().is_some());
        assert!(
            recorder.receipt_ref().is_some(),
            "receipt_ref should be set"
        );
        assert!(
            recorder.receipt_abs_path().is_some(),
            "receipt_abs_path should be set"
        );

        let rr = recorder.receipt_ref().unwrap();
        assert_eq!(rr.root_id, 7);
        assert!(
            rr.rel_path.contains("000001-apply.toml"),
            "got: {}",
            rr.rel_path
        );
        assert!(
            rr.rel_path.starts_with(".canon-ledger/"),
            "got: {}",
            rr.rel_path
        );
    }

    #[test]
    fn test_recorder_with_receipt_ctx_db_updated() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 7,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: "Media".to_string(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let id = recorder.decision_id().unwrap();
        let d = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();

        assert_eq!(d.receipt_root_id, Some(7));
        assert!(d.receipt_rel_path.is_some());
        assert!(d.receipt_rel_path.unwrap().contains("apply.toml"));
    }

    #[test]
    fn test_recorder_receipt_disabled_with_ctx_no_receipt_ref() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = DecisionParams {
            command: DecisionCommand::Apply,
            scope: None,
            command_line: "canon apply m.lock".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false, // disabled
            ledger_config: LedgerConfig::default(),
        };
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: "Media".to_string(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));

        assert!(recorder.receipt_ref().is_none());
        assert!(recorder.receipt_abs_path().is_none());
    }

    #[test]
    fn test_recorder_complete_finalizes_receipt() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));

        // Manually create the .incomplete file so finalize_receipt has something to rename
        let receipt_path = recorder.receipt_abs_path().unwrap().to_path_buf();
        let incomplete = receipt_path.with_extension("incomplete");
        std::fs::create_dir_all(incomplete.parent().unwrap()).unwrap();
        std::fs::write(&incomplete, b"receipt content").unwrap();

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(1),
                failed: Some(0),
                skipped: None,
            },
            "Applied 1 file",
        );

        // .toml should exist, .incomplete should be gone
        assert!(receipt_path.exists(), ".toml should exist after complete()");
        assert!(!incomplete.exists(), ".incomplete should be gone");
        assert!(
            recorder.warnings.is_empty(),
            "unexpected warnings: {:?}",
            recorder.warnings
        );
    }

    #[test]
    fn test_recorder_interrupted_finalizes_receipt() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));

        let receipt_path = recorder.receipt_abs_path().unwrap().to_path_buf();
        let incomplete = receipt_path.with_extension("incomplete");
        std::fs::create_dir_all(incomplete.parent().unwrap()).unwrap();
        std::fs::write(&incomplete, b"partial receipt").unwrap();

        recorder.interrupted(&conn);

        assert!(
            receipt_path.exists(),
            ".toml should exist after interrupted()"
        );
        assert!(!incomplete.exists());
    }

    // =========================================================================
    // Recording mode tests
    // =========================================================================

    fn make_params_with_config(
        command: DecisionCommand,
        config: LedgerConfig,
        no_receipt: bool,
    ) -> DecisionParams {
        DecisionParams {
            command,
            scope: None,
            command_line: "canon test".to_string(),
            reason: None,
            record_enabled: config.recording != RecordingMode::Off,
            receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
            ledger_config: config,
        }
    }

    #[test]
    fn test_recording_off_no_db_record() {
        let conn = setup_test_db();
        let config = LedgerConfig {
            recording: RecordingMode::Off,
            ..LedgerConfig::default()
        };
        let params = make_params_with_config(DecisionCommand::Scan, config, false);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_none());
        assert_eq!(count_decisions(&conn), 0);
    }

    #[test]
    fn test_recording_records_db_only() {
        let conn = setup_test_db();
        let config = LedgerConfig {
            recording: RecordingMode::Records,
            ..LedgerConfig::default()
        };
        let params = make_params_with_config(DecisionCommand::Scan, config, false);
        assert!(!params.receipt_enabled);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn test_recording_full_both() {
        let conn = setup_test_db();
        let config = LedgerConfig {
            recording: RecordingMode::Full,
            ..LedgerConfig::default()
        };
        let params = make_params_with_config(DecisionCommand::Scan, config, false);
        assert!(params.receipt_enabled);
        let recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    // =========================================================================
    // Non-targeted (ledger-root) placement
    // =========================================================================

    #[test]
    fn test_recorder_ledger_root_placement_flat() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = DecisionParams {
            command: DecisionCommand::ExcludeSet,
            scope: None,
            command_line: "canon exclude set".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: LedgerConfig::default(),
        };
        let placement = ReceiptPlacement::LedgerRoot {
            root_id: 3,
            root_path: dir.path().to_str().unwrap().to_string(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, Some(&placement));

        let rr = recorder.receipt_ref().expect("receipt_ref should be set");
        assert_eq!(rr.root_id, 3, "receipt root is the ledger root");
        // Flat at the ledger root — no base_dir subdirectory.
        assert_eq!(rr.rel_path, ".canon-ledger/000001-exclude_set.toml");
        assert!(recorder
            .receipt_abs_path()
            .unwrap()
            .ends_with(".canon-ledger/000001-exclude_set.toml"));

        // The DB record points at the same place.
        let d = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(d.receipt_root_id, Some(3));
        assert_eq!(
            d.receipt_rel_path.as_deref(),
            Some(".canon-ledger/000001-exclude_set.toml")
        );
    }
}
