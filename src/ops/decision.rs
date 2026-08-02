use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::domain::config::LedgerConfig;
use crate::domain::decision::{DecisionCommand, DecisionStatus};
use crate::domain::scope::DecisionScope;
use crate::domain::trail::{fate_posture, fate_transition};
use crate::ops::receipt::{
    compute_ledger_root_receipt_rel_path, compute_targeted_receipt_rel_path, finalize_receipt,
    write_receipt, ReceiptKind, ReceiptLocus, ReceiptMeta, ReceiptPlacement, ReceiptRef,
};
use crate::repo::{self, Connection};

/// Parameters for starting a decision record.
pub struct DecisionParams {
    pub command: DecisionCommand,
    /// The decision's scope, decomposed to known roots (empty = global). The
    /// recorder derives both the `decisions.scope` display column and the
    /// `decision_scopes` index rows from this — callers never supply raw
    /// strings, so a non-canonical or rootless scope is unrepresentable.
    pub scope: Vec<DecisionScope>,
    pub command_line: String,
    pub reason: Option<String>,
    /// Whether to write a DB decision record. False for recording=off or dry-run.
    pub record_enabled: bool,
    /// Whether to write a receipt file. False unless recording=full and no --no-receipt.
    pub receipt_enabled: bool,
    /// Ledger config for receipt path computation.
    pub ledger_config: LedgerConfig,
}

impl DecisionParams {
    /// Build the shared receipt `[meta]` block from these params.
    ///
    /// The what (`transition`), its `posture`, and the apply-only
    /// `origin_disposition` are all *derived* here from `kind` via the shared
    /// `fate_transition`/`fate_posture` functions — the single derivation site,
    /// so no writer emits a vocabulary literal. `locus` is the receipt's where,
    /// taken from its placement (`(root_id, root_path)`). `manifest` is `Some`
    /// only for apply receipts; other commands pass `None`.
    pub fn receipt_meta(
        &self,
        decision_id: i64,
        status: DecisionStatus,
        summary: &str,
        locus: (i64, &str),
        kind: ReceiptKind,
        manifest: Option<String>,
    ) -> ReceiptMeta {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let (family, aspect) = kind.family_aspect();
        let transition = fate_transition(family, aspect)
            .expect("every receipt kind maps to a transition")
            .as_str()
            .to_string();
        let posture = fate_posture(family, aspect).as_str().to_string();
        ReceiptMeta {
            receipt_version: 1,
            decision_id,
            command: self.command.as_str().to_string(),
            transition,
            posture,
            status: status.as_str().to_string(),
            timestamp,
            scope: scope_display(&self.scope),
            reason: self.reason.clone(),
            summary: summary.to_string(),
            canon_version: env!("CARGO_PKG_VERSION").to_string(),
            command_line: self.command_line.clone(),
            manifest,
            origin_disposition: kind.origin_disposition().map(str::to_string),
            locus: ReceiptLocus {
                path: locus.1.to_string(),
                id: locus.0,
            },
        }
    }
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
    /// Canonical display paths already written to the `decisions.scope` column.
    /// Held so `record_scopes` can backfill scopes discovered after `start()`
    /// (a `scan --add` root) without re-reading the row.
    scope_display: Vec<String>,
    warnings: Vec<String>,
}

/// The `decisions.scope` / `meta.scope` display strings for a typed scope:
/// each scope's canonical path, or `None` when the decision is global (so the
/// column stays SQL `NULL`, matching how global decisions were always stored).
fn scope_display(scope: &[DecisionScope]) -> Option<Vec<String>> {
    if scope.is_empty() {
        None
    } else {
        Some(scope.iter().map(DecisionScope::display_path).collect())
    }
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
                scope_display: Vec::new(),
                warnings: Vec::new(),
            };
        }

        let canon_version = env!("CARGO_PKG_VERSION");
        let scope_display = scope_display(&params.scope).unwrap_or_default();

        let id = match repo::decision::insert_started(
            conn,
            params.command.as_str(),
            (!scope_display.is_empty()).then_some(scope_display.as_slice()),
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
                    scope_display: Vec::new(),
                    warnings: vec![format!("Warning: failed to record decision: {e}")],
                };
            }
        };

        // Populate the durable scope index (decision_scopes) from the resolved scope.
        let mut warnings = populate_decision_scopes(conn, id, params);

        // Compute receipt path if receipts are enabled and context is provided.
        let (receipt_ref, receipt_abs_path) = if params.receipt_enabled {
            if let Some(placement) = placement {
                let (rr, rap, mut receipt_warnings) =
                    compute_and_register_receipt(conn, id, params, placement);
                warnings.append(&mut receipt_warnings);
                (rr, rap)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        DecisionRecorder {
            id: Some(id),
            receipt_ref,
            receipt_abs_path,
            scope_display,
            warnings,
        }
    }

    /// Update the DB record with completion data only — does NOT finalize the
    /// receipt file. Use inside a transaction; write+finalize the receipt file
    /// after commit via `write_receipt_file` + `finalize_receipt_file`. No-op if
    /// disabled or start failed. Collects a warning if the UPDATE fails.
    pub fn complete_db(
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

    /// Write `receipt` to the `.incomplete` file (no DB, no finalize). A write
    /// failure is collected as a warning. No-op if no receipt path was set up.
    pub fn write_receipt_file<T: Serialize>(&mut self, receipt: &T, summary: &str) {
        if let Some(path) = self.receipt_abs_path().map(|p| p.to_owned()) {
            if let Err(e) = write_receipt(&path, receipt, summary) {
                self.push_warning(format!("Receipt write failed: {e:#}"));
            }
        }
    }

    /// Write a receipt to disk immediately (write `.incomplete`, then finalize to
    /// `.toml`), independent of the `start()`-time receipt slot.
    ///
    /// Scan uses this: a deletion receipt's placement (which root lost files) and
    /// existence (only if sources went missing) are known only after the walk, not
    /// at `start()`. The path is computed from `placement` and `command`; the file
    /// is written and finalized in one call (fix-forward — the deletion was already
    /// observed on disk).
    ///
    /// Returns the written receipt's `(root_id, rel_path)` for the caller to index,
    /// or `None` if recording is disabled or a filesystem step failed (a warning is
    /// collected). Only `LedgerRoot` placement is supported — source-local receipts
    /// land flat in that root's `.canon-ledger/`.
    pub fn write_placed_receipt<T: Serialize>(
        &mut self,
        placement: &ReceiptPlacement,
        command: &str,
        receipt: &T,
        summary: &str,
    ) -> Option<ReceiptRef> {
        let decision_id = self.id?;

        let (root_id, root_path) = match placement {
            ReceiptPlacement::LedgerRoot { root_id, root_path } => (*root_id, root_path.clone()),
            ReceiptPlacement::Targeted { .. } => {
                self.warnings.push(
                    "Warning: source-local receipt requires ledger-root placement".to_string(),
                );
                return None;
            }
        };

        let rel_path = compute_ledger_root_receipt_rel_path(decision_id, command);
        let abs_path = PathBuf::from(&root_path).join(&rel_path);

        if let Some(parent) = abs_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                self.warnings.push(format!(
                    "Warning: could not create receipt directory {}: {e}",
                    parent.display()
                ));
                return None;
            }
        }

        if let Err(e) = write_receipt(&abs_path, receipt, summary) {
            self.warnings.push(format!("Receipt write failed: {e:#}"));
            return None;
        }
        if let Err(e) = finalize_receipt(&abs_path) {
            self.warnings
                .push(format!("Warning: failed to finalize receipt: {e}"));
            return None;
        }

        Some(ReceiptRef { root_id, rel_path })
    }

    /// Finalize the receipt file: rename `.incomplete` → `.toml`. A failure is
    /// collected as a warning. No-op if no receipt path was set up.
    pub fn finalize_receipt_file(&mut self) {
        if let Some(ref path) = self.receipt_abs_path {
            if let Err(e) = finalize_receipt(path) {
                self.warnings
                    .push(format!("Warning: failed to finalize receipt: {e}"));
            }
        }
    }

    /// Idempotently record additional typed scopes discovered after `start()`.
    ///
    /// The `start()`-time scope decomposition can only match roots that already
    /// exist. A `canon scan --add` creates its root inside the scan loop, so at
    /// `start()` the scope path matched no root and neither a `decision_scopes`
    /// row nor a `decisions.scope` display entry was written. The loop resolves
    /// each path to a typed `DecisionScope` — including roots it just created —
    /// so passing those here records the index rows *and* backfills the display
    /// column for the new roots. Scopes already written at `start()` are left
    /// untouched (the index insert is `NOT EXISTS`-guarded; display is deduped).
    /// No-op if recording is disabled or `start()` failed; a write failure is
    /// collected as a warning, never fatal.
    pub fn record_scopes(&mut self, conn: &Connection, scopes: &[DecisionScope]) {
        let Some(id) = self.id else {
            return;
        };
        if scopes.is_empty() {
            return;
        }
        let rows: Vec<(i64, String, String)> =
            scopes.iter().map(DecisionScope::index_row).collect();
        if let Err(e) = repo::decision::insert_scopes(conn, id, &rows) {
            self.warnings.push(format!(
                "Warning: failed to update decision scope index: {e}"
            ));
            return;
        }

        // Backfill the display column with the newly-recorded roots' paths.
        let mut changed = false;
        for scope in scopes {
            let display = scope.display_path();
            if !self.scope_display.contains(&display) {
                self.scope_display.push(display);
                changed = true;
            }
        }
        if changed {
            if let Err(e) = repo::decision::update_scope_display(conn, id, &self.scope_display) {
                self.warnings
                    .push(format!("Warning: failed to update decision scope: {e}"));
            }
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
        self.complete_db(conn, status, counts, summary);
        self.finalize_receipt_file();
    }

    /// Write `receipt` (when a receipt path was set up and `receipt` is `Some`),
    /// then complete the record. The write happens before completion so the
    /// `.incomplete` → `.toml` finalize inside `complete()` renames the written
    /// file. A write failure is collected as a warning, never halting the command.
    pub fn complete_with_receipt<T: Serialize>(
        &mut self,
        conn: &Connection,
        status: DecisionStatus,
        counts: DecisionCounts,
        summary: &str,
        receipt: Option<&T>,
    ) {
        if let Some(receipt) = receipt {
            self.write_receipt_file(receipt, summary);
        }
        self.complete(conn, status, counts, summary);
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

/// Write the decision's typed scope to the durable `decision_scopes` index, one
/// row per `(root_id, rel_prefix)`. An empty scope (global op) writes nothing.
/// The scope is already decomposed to known roots by the caller, so this is a
/// straight projection — no re-derivation, no roots fetch. Failure is collected
/// as a warning, never fatal.
fn populate_decision_scopes(
    conn: &Connection,
    decision_id: i64,
    params: &DecisionParams,
) -> Vec<String> {
    if params.scope.is_empty() {
        return Vec::new();
    }

    let rows: Vec<(i64, String, String)> =
        params.scope.iter().map(DecisionScope::index_row).collect();

    match repo::decision::insert_scopes(conn, decision_id, &rows) {
        Ok(()) => Vec::new(),
        Err(e) => vec![format!(
            "Warning: failed to write decision scope index: {e}"
        )],
    }
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
            scope: Vec::new(),
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
            scope: vec![DecisionScope::new(1, "/photos".to_string(), String::new())],
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
            scope: Vec::new(),
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
            scope: Vec::new(),
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
            scope: Vec::new(),
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
            scope: Vec::new(),
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
            scope: Vec::new(),
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

    #[test]
    fn test_complete_with_receipt_writes_finalizes_and_completes() {
        #[derive(serde::Serialize)]
        struct Body {
            note: String,
        }

        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };
        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let path = recorder.receipt_abs_path().unwrap().to_path_buf();

        let body = Body {
            note: "hi".to_string(),
        };
        recorder.complete_with_receipt(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(1),
                failed: Some(0),
                skipped: None,
            },
            "done",
            Some(&body),
        );

        // Receipt written and finalized to .toml in one call.
        assert!(
            path.exists(),
            ".toml should exist after complete_with_receipt"
        );
        assert!(!path.with_extension("incomplete").exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("note = \"hi\""));
        // DB row completed.
        let d = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(d.status, "completed");
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn test_complete_with_receipt_none_writes_no_file() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };
        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let path = recorder.receipt_abs_path().unwrap().to_path_buf();

        // No receipt body → nothing written; complete() still finalizes (no-op rename
        // target absent) and collects that as a warning, but no file appears.
        recorder.complete_with_receipt::<()>(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(0),
                completed: Some(0),
                failed: Some(0),
                skipped: Some(0),
            },
            "nothing",
            None,
        );

        assert!(!path.exists(), "no receipt file should be written for None");
    }

    // =========================================================================
    // record_scopes — new-root scope capture at completion
    // =========================================================================

    fn scope_row_count(conn: &Connection, decision_id: i64, root_id: i64) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM decision_scopes WHERE decision_id = ? AND root_id = ?",
            rusqlite::params![decision_id, root_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn record_scopes_writes_row_for_newly_created_root() {
        // A scan --add creates its root inside the loop: at start() the scope path
        // matched no root, so no scope row was written. record_scopes captures it.
        let conn = setup_test_db();
        let params = DecisionParams {
            command: DecisionCommand::Scan,
            // The --add root doesn't exist at start(), so it decomposes to nothing.
            scope: Vec::new(),
            command_line: "canon scan --add /newdrive/photos --role source".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let decision_id = recorder.decision_id().unwrap();

        // No root existed at start() → nothing populated for it.
        let root_id = repo::insert_test_root(&conn, "/newdrive/photos", "source", false);
        assert_eq!(scope_row_count(&conn, decision_id, root_id), 0);

        recorder.record_scopes(
            &conn,
            &[DecisionScope::new(
                root_id,
                "/newdrive/photos".to_string(),
                String::new(),
            )],
        );

        assert_eq!(scope_row_count(&conn, decision_id, root_id), 1);
        // The index row carries the write-time root_path snapshot.
        let snapshot: Option<String> = conn
            .query_row(
                "SELECT root_path FROM decision_scopes WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(snapshot.as_deref(), Some("/newdrive/photos"));
        // The display column is backfilled with the newly-recorded root's path,
        // so a --add scan's decisions.scope is not left NULL.
        let d = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .unwrap();
        assert_eq!(d.scope, Some(vec!["/newdrive/photos".to_string()]));
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn record_scopes_does_not_duplicate_start_time_rows() {
        // An existing-root scan: start() already wrote the scope row; recording the
        // same resolved pair at completion must not double-insert.
        let conn = setup_test_db();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let params = DecisionParams {
            command: DecisionCommand::Scan,
            scope: vec![DecisionScope::new(
                root_id,
                "/photos".to_string(),
                String::new(),
            )],
            command_line: "canon scan /photos".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let decision_id = recorder.decision_id().unwrap();
        assert_eq!(scope_row_count(&conn, decision_id, root_id), 1);
        // start()-time population snapshots the root path too.
        let snapshot: Option<String> = conn
            .query_row(
                "SELECT root_path FROM decision_scopes WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(snapshot.as_deref(), Some("/photos"));

        recorder.record_scopes(
            &conn,
            &[DecisionScope::new(
                root_id,
                "/photos".to_string(),
                String::new(),
            )],
        );

        assert_eq!(scope_row_count(&conn, decision_id, root_id), 1);
    }

    #[test]
    fn recorded_scope_is_always_canonical_never_relative() {
        // Regression: the original bug recorded a raw "." as a decision scope,
        // unattributable later. The typed contract makes that unrepresentable —
        // decompose drops a rootless "." and keeps only the root-anchored path,
        // and the recorder stores exactly that canonical string in both the
        // decisions.scope column and meta.scope.
        let conn = setup_test_db();
        let roots = vec![crate::domain::root::Root {
            id: 1,
            path: "/vol/photos".to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }];
        let scope =
            DecisionScope::decompose(&[".".to_string(), "/vol/photos/2016".to_string()], &roots);
        let params = DecisionParams {
            command: DecisionCommand::Scan,
            scope,
            command_line: "canon scan .".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };

        let recorder = DecisionRecorder::start(&conn, &params, None);
        let d = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
            .unwrap()
            .unwrap();

        // Only the canonical, root-anchored path survived; "." was never recorded.
        assert_eq!(d.scope, Some(vec!["/vol/photos/2016".to_string()]));
        // meta.scope tells the same canonical story, and nothing relative leaks.
        let meta = params.receipt_meta(
            1,
            DecisionStatus::Completed,
            "s",
            (1, "/vol/photos"),
            ReceiptKind::Deletion,
            None,
        );
        assert_eq!(meta.scope, Some(vec!["/vol/photos/2016".to_string()]));
        for path in meta.scope.unwrap() {
            assert!(path.starts_with('/'), "scope {path:?} is not absolute");
        }
    }

    #[test]
    fn record_scopes_is_noop_when_recording_disabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        assert!(recorder.decision_id().is_none());

        recorder.record_scopes(
            &conn,
            &[DecisionScope::new(1, "/x".to_string(), String::new())],
        );

        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM decision_scopes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(total, 0);
    }
}
