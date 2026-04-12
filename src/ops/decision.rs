use crate::domain::config::LedgerConfig;
use crate::domain::decision::{DecisionCommand, DecisionStatus};
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
    /// Ledger config for receipt path computation (used in Story 2+).
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
    id: Option<i64>, // None if recording is disabled or start failed
    warnings: Vec<String>,
}

impl DecisionRecorder {
    /// Expose the decision ID for receipt writing and source decision_id.
    /// Returns None if recording is disabled or the INSERT failed.
    pub fn decision_id(&self) -> Option<i64> {
        self.id
    }

    /// Insert the initial "started" record.
    /// If record_enabled is false (recording=off, dry-run), returns a no-op recorder.
    /// If the INSERT fails, collects a warning and returns a no-op recorder.
    pub fn start(conn: &Connection, params: &DecisionParams) -> Self {
        if !params.record_enabled {
            return DecisionRecorder {
                id: None,
                warnings: Vec::new(),
            };
        }

        let canon_version = env!("CARGO_PKG_VERSION");

        match repo::decision::insert_started(
            conn,
            params.command.as_str(),
            params.scope.as_deref(),
            &params.command_line,
            params.reason.as_deref(),
            canon_version,
            None, // receipt_root_id — populated in Story 2
            None, // receipt_rel_path — populated in Story 2
        ) {
            Ok(id) => DecisionRecorder {
                id: Some(id),
                warnings: Vec::new(),
            },
            Err(e) => DecisionRecorder {
                id: None,
                warnings: vec![format!("Warning: failed to record decision: {e}")],
            },
        }
    }

    /// Update the record with completion data. No-op if disabled or start failed.
    /// Collects a warning if the UPDATE fails.
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
    }

    /// Drain accumulated warnings. Returns an empty vec if no warnings.
    pub fn take_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.warnings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::config::LedgerConfig;
    use crate::repo::db::open_in_memory_for_test;

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

        let recorder = DecisionRecorder::start(&conn, &params);

        assert!(recorder.id.is_some());
        assert!(recorder.warnings.is_empty());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn recorder_complete_updates_record() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::ExcludeSet, true);
        let mut recorder = DecisionRecorder::start(&conn, &params);

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

        let recorder = DecisionRecorder::start(&conn, &params);

        assert!(recorder.id.is_none());
        assert_eq!(count_decisions(&conn), 0);
    }

    #[test]
    fn recorder_interrupted_sets_status() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Apply, true);
        let mut recorder = DecisionRecorder::start(&conn, &params);

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
        let mut recorder = DecisionRecorder::start(&conn, &params);

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

        let recorder = DecisionRecorder::start(&conn, &params);

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
        let recorder = DecisionRecorder::start(&conn, &params);

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.canon_version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_recorder_record_enabled_creates_row() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let recorder = DecisionRecorder::start(&conn, &params);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn test_recorder_record_disabled_no_row() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let recorder = DecisionRecorder::start(&conn, &params);
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
        let recorder = DecisionRecorder::start(&conn, &params);
        assert!(recorder.decision_id().is_some());
        assert_eq!(count_decisions(&conn), 1);
    }

    #[test]
    fn test_recorder_decision_id_some_when_enabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let recorder = DecisionRecorder::start(&conn, &params);
        assert!(recorder.decision_id().is_some());
    }

    #[test]
    fn test_recorder_decision_id_none_when_disabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let recorder = DecisionRecorder::start(&conn, &params);
        assert!(recorder.decision_id().is_none());
    }

    #[test]
    fn test_recorder_insert_started_receipt_columns_null() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Apply, true);
        let recorder = DecisionRecorder::start(&conn, &params);
        let id = recorder.decision_id().unwrap();
        let d = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(d.receipt_root_id.is_none());
        assert!(d.receipt_rel_path.is_none());
    }

    #[test]
    fn test_recorder_complete_updates() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::ExcludeSet, true);
        let mut recorder = DecisionRecorder::start(&conn, &params);
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts { attempted: Some(5), completed: Some(5), failed: Some(0), skipped: None },
            "Excluded 5 sources",
        );
        let d = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap()).unwrap().unwrap();
        assert_eq!(d.status, "completed");
        assert_eq!(d.count_completed, Some(5));
    }

    #[test]
    fn test_recorder_warnings_collected() {
        let conn = setup_test_db();
        // Start with recording enabled, then complete — no warnings in happy path
        let params = make_params(DecisionCommand::Scan, true);
        let mut recorder = DecisionRecorder::start(&conn, &params);
        assert!(recorder.warnings.is_empty());
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts { attempted: None, completed: None, failed: None, skipped: None },
            "done",
        );
        let warnings = recorder.take_warnings();
        assert!(warnings.is_empty());
    }
}
