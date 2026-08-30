use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::core::domain::config::LedgerConfig;
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::fate::{fate_posture, fate_transition};
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::receipt::{
    compute_ledger_root_receipt_rel_path, compute_targeted_receipt_rel_path, finalize_receipt,
    write_receipt, ReceiptKind, ReceiptLocus, ReceiptMeta, ReceiptPlacement, ReceiptRef,
};
use crate::core::repo::{self, Connection};

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

impl DecisionCounts {
    /// No counts at all — the shape of an outcome that attempted nothing.
    /// Every column stays SQL `NULL`, so the row cannot be read as having
    /// tried and failed at zero items.
    pub fn none() -> Self {
        DecisionCounts {
            attempted: None,
            completed: None,
            failed: None,
            skipped: None,
        }
    }
}

/// What a decision row's receipt columns currently claim.
///
/// The columns are written prospectively at `start()`, before the artifact they
/// name exists. The recorder is the claim's lifecycle owner — it registers the
/// path, so it is the one that must retract it — and this is what it tracks to
/// know whether a retraction is owed.
#[derive(Debug, PartialEq, Eq)]
enum ReceiptClaim {
    /// Nothing was ever claimed.
    Unclaimed,
    /// A path was registered at `start()`; nothing has been written there.
    Registered,
    /// The registered receipt is on disk, under its `.incomplete` name.
    Written,
    /// The registered receipt is on disk under its final name. Terminal: the
    /// claim is true and settling again must not touch it — a second finalize
    /// would fail on a file that is already where it belongs, and retract a
    /// claim that is on disk.
    Finalized,
    /// The columns were pointed at a durable artifact that is not a receipt —
    /// the retirement book. Not the recorder's to retract.
    Artifact,
}

/// What a decision row's status column currently claims.
///
/// `insert_started` writes `started` before anything happens — a prospective
/// claim about a run that has not finished. The recorder registered it, so the
/// recorder is what settles it at the decision's last act.
#[derive(Debug, PartialEq, Eq)]
enum StatusClaim {
    /// No row exists — recording disabled, or the INSERT failed.
    Unclaimed,
    /// The row says `started`; nothing has settled it.
    Registered,
    /// A terminal status was written. It never walks back: a later failure
    /// leaves this alone rather than re-claiming the row says `started`,
    /// which would point crash recovery at a run that finished. Unlike the
    /// receipt side's `Finalized`, a second settlement is harmless here — a
    /// plain UPDATE, not a rename that would break on its own success.
    Settled,
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
    /// No production caller needs this yet — only exercised via `receipt_ref()` in tests.
    #[allow(dead_code)]
    receipt_ref: Option<ReceiptRef>,
    /// Absolute path to the final `.toml` file, used for write and finalize.
    receipt_abs_path: Option<PathBuf>,
    /// Canonical display paths already written to the `decisions.scope` column.
    /// Held so `record_scopes` can backfill scopes discovered after `start()`
    /// (a `scan --add` root) without re-reading the row.
    scope_display: Vec<String>,
    /// What the row's receipt columns currently claim, so an unwritten claim
    /// can be retracted rather than left pointing at nothing.
    claim: ReceiptClaim,
    /// What the row's status column currently claims, so a row registered as
    /// `started` cannot be left saying so by a run that did reach a last act.
    status_claim: StatusClaim,
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
    #[allow(dead_code)]
    fn receipt_ref(&self) -> Option<&ReceiptRef> {
        self.receipt_ref.as_ref()
    }

    /// Expose the absolute path for receipt writing.
    /// Returns None if receipts are disabled or path computation failed.
    fn receipt_abs_path(&self) -> Option<&std::path::Path> {
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
                claim: ReceiptClaim::Unclaimed,
                status_claim: StatusClaim::Unclaimed,
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
                    claim: ReceiptClaim::Unclaimed,
                    status_claim: StatusClaim::Unclaimed,
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

        let claim = if receipt_abs_path.is_some() {
            ReceiptClaim::Registered
        } else {
            ReceiptClaim::Unclaimed
        };

        DecisionRecorder {
            id: Some(id),
            receipt_ref,
            receipt_abs_path,
            scope_display,
            claim,
            // The INSERT wrote `started`: the row now makes a claim about a run
            // that has not finished, and this is what owes the settlement.
            status_claim: StatusClaim::Registered,
            warnings,
        }
    }

    /// Update the DB record with completion data only — does NOT finalize the
    /// receipt file. Use inside a transaction; write+finalize the receipt file
    /// after commit via `write_receipt_file` + `settle_receipt_claim`. No-op if
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
            // The claim stays registered: the row really is still `started`,
            // and the warning is the record of why. Recording settlement here
            // would be the falsehood the tracking exists to prevent.
            self.warnings
                .push(format!("Warning: failed to update decision record: {e}"));
            return;
        }
        self.status_claim = StatusClaim::Settled;
    }

    /// The decision's last act for a run that declined to do anything.
    ///
    /// A refusal attempted nothing, so the counts stay empty and the receipt
    /// registered at `start()` is retracted — the row must not go on citing a
    /// file that will never be written. Distinct from `interrupted`, which
    /// means a run was cut short after starting, and from a row left
    /// `started`, which means the run never reached a last act at all.
    ///
    /// Deliberately thin over `complete`: what it buys is the vocabulary point,
    /// not new mechanism. No caller spells `Refused` plus four `None`s itself,
    /// and settling the status and the receipt is one act rather than two calls
    /// a caller could half-make.
    pub fn refuse(&mut self, conn: &Connection, summary: &str) {
        self.complete(
            conn,
            DecisionStatus::Refused,
            DecisionCounts::none(),
            summary,
        );
    }

    /// Write `receipt` to the `.incomplete` file (no DB, no finalize). A write
    /// failure is collected as a warning. No-op if no receipt path was set up.
    pub fn write_receipt_file<T: Serialize>(&mut self, receipt: &T, summary: &str) {
        if let Some(path) = self.receipt_abs_path().map(|p| p.to_owned()) {
            match write_receipt(&path, receipt, summary) {
                Ok(()) => self.claim = ReceiptClaim::Written,
                Err(e) => self.push_warning(format!("Receipt write failed: {e:#}")),
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

    /// Settle what the row claims about its receipt: finalize the file that was
    /// written (rename `.incomplete` → `.toml`), or retract a claim whose file
    /// never appeared.
    ///
    /// This is the last act of a decision on every shape that can register a
    /// claim — `complete()` and its terminals, `refuse()` among them, plus the
    /// commit-then-write shape that settles on its own after its transaction. A
    /// path registered at `start()` that was never written is
    /// retracted here: the row would otherwise cite a receipt that does not
    /// exist, and every reader of the trail treats those columns as the index
    /// over what is on disk. Nothing was written, so there is nothing to
    /// finalize either — which is why the old spurious "failed to finalize"
    /// warning on that path is gone rather than silenced.
    ///
    /// Idempotent: settling an already-settled claim does nothing.
    pub fn settle_receipt_claim(&mut self, conn: &Connection) {
        match self.claim {
            ReceiptClaim::Written => {
                if let Some(path) = self.receipt_abs_path.clone() {
                    if let Err(e) = finalize_receipt(&path) {
                        // The body is still on disk under `.incomplete`, but
                        // the columns name the finalized path, and that file
                        // does not exist. A rename that fails leaves the same
                        // missing artifact a failed write does, so it is
                        // settled the same way.
                        self.warnings
                            .push(format!("Warning: failed to finalize receipt: {e}"));
                        self.retract_claim(conn);
                    } else {
                        self.claim = ReceiptClaim::Finalized;
                    }
                }
            }
            ReceiptClaim::Registered => self.retract_claim(conn),
            ReceiptClaim::Unclaimed | ReceiptClaim::Artifact | ReceiptClaim::Finalized => {}
        }
    }

    /// Clear the receipt columns of a claim whose artifact was never written.
    fn retract_claim(&mut self, conn: &Connection) {
        let Some(id) = self.id else {
            return;
        };
        if let Err(e) = repo::decision::update_receipt_path(conn, id, None, None) {
            self.warnings
                .push(format!("Warning: failed to clear the receipt claim: {e}"));
            return;
        }
        self.claim = ReceiptClaim::Unclaimed;
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
        self.settle_receipt_claim(conn);
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

    /// Point the decision's receipt columns at a durable artifact that is not
    /// a receipt file — the retirement ceremony's book. The columns hold "the
    /// decision's durable record", which for every other command happens to be
    /// a receipt; here the artifact exists independent of receipt settings, so
    /// this is gated only on recording being enabled (a disabled recorder is a
    /// no-op). A failure is collected as a warning, never fatal.
    pub fn record_artifact_pointer(&mut self, conn: &Connection, root_id: i64, rel_path: &str) {
        let Some(id) = self.id else {
            return;
        };
        if let Err(e) = repo::decision::update_receipt_path(conn, id, Some(root_id), Some(rel_path))
        {
            self.warnings.push(format!(
                "Warning: failed to record the artifact pointer: {e}"
            ));
            return;
        }
        // The columns now name something that exists independently of any
        // receipt, so the unwritten-claim retraction must leave them alone.
        self.claim = ReceiptClaim::Artifact;
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
    use crate::core::domain::config::{LedgerConfig, RecordingMode};
    use crate::core::repo::db::open_in_memory_for_test;
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
    fn record_artifact_pointer_sets_receipt_columns() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::RootsRetire, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        recorder.record_artifact_pointer(&conn, 7, "retired/photos-backup-2026-08-02");

        let decision = repo::decision::fetch_by_id(&conn, recorder.id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(decision.receipt_root_id, Some(7));
        assert_eq!(
            decision.receipt_rel_path.as_deref(),
            Some("retired/photos-backup-2026-08-02")
        );
        assert!(recorder.take_warnings().is_empty());
    }

    #[test]
    fn record_artifact_pointer_is_a_noop_when_disabled() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::RootsRetire, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);

        recorder.record_artifact_pointer(&conn, 7, "retired/photos-backup-2026-08-02");

        assert_eq!(count_decisions(&conn), 0);
        assert!(recorder.take_warnings().is_empty());
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

    /// A registered claim is retracted when the write it named fails, not only
    /// when no write was attempted. The write failure still surfaces as a
    /// warning — the user is told the receipt is missing — but the row stops
    /// pointing at a file that was never created.
    #[test]
    fn a_failed_receipt_write_clears_the_claim() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let id = recorder.decision_id().unwrap();
        let claimed = recorder.receipt_abs_path().unwrap().to_path_buf();

        // Put a file where the ledger directory has to be, so the write fails.
        let ledger_dir = claimed.parent().unwrap().to_path_buf();
        std::fs::remove_dir_all(&ledger_dir).unwrap();
        std::fs::write(&ledger_dir, b"not a directory").unwrap();
        recorder.write_receipt_file(&toml_body(), "one file");

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(1),
                failed: Some(0),
                skipped: None,
            },
            "one file",
        );

        assert!(
            recorder
                .warnings
                .iter()
                .any(|w| w.contains("Receipt write failed")),
            "the failure must still be reported: {:?}",
            recorder.warnings
        );

        let row = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(row.receipt_root_id, None);
        assert_eq!(row.receipt_rel_path, None);
        assert!(!claimed.exists());
    }

    /// Settling is idempotent, and the finalized case is the one that has to
    /// be: a second finalize would rename a file that is already where it
    /// belongs, fail, and retract a claim whose artifact is on disk — the law
    /// broken from the other side.
    #[test]
    fn settling_a_finalized_claim_again_leaves_it_alone() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let id = recorder.decision_id().unwrap();
        let claimed = recorder.receipt_abs_path().unwrap().to_path_buf();
        recorder.write_receipt_file(&toml_body(), "one file");

        recorder.settle_receipt_claim(&conn);
        recorder.settle_receipt_claim(&conn);

        assert!(claimed.exists());
        assert!(
            recorder.warnings.is_empty(),
            "unexpected warnings: {:?}",
            recorder.warnings
        );
        let row = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(row.receipt_rel_path.is_some(), "claim must survive");
    }

    /// A rename that fails leaves the receipt body under `.incomplete`, so the
    /// finalized path the columns name does not exist. That is the same missing
    /// artifact a failed write leaves, and it is settled the same way — the
    /// asymmetry would otherwise let a `completed` row cite a file that is not
    /// there.
    #[test]
    fn a_failed_finalize_clears_the_claim() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let id = recorder.decision_id().unwrap();
        let claimed = recorder.receipt_abs_path().unwrap().to_path_buf();
        recorder.write_receipt_file(&toml_body(), "one file");

        // Occupy the finalized name with a directory, so the rename fails.
        std::fs::create_dir(&claimed).unwrap();

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(1),
                failed: Some(0),
                skipped: None,
            },
            "one file",
        );

        assert!(
            recorder
                .warnings
                .iter()
                .any(|w| w.contains("failed to finalize")),
            "the failure must still be reported: {:?}",
            recorder.warnings
        );
        let row = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(row.receipt_root_id, None);
        assert_eq!(row.receipt_rel_path, None);
    }

    /// The retraction governs the slot registered at `start()` and nothing
    /// else. Retirement points the same columns at the book, which exists
    /// independently of any receipt — a recorder that then completes must
    /// leave the pointer alone.
    #[test]
    fn an_artifact_pointer_survives_completion() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();
        let ctx = ReceiptPlacement::Targeted {
            archive_root_id: 1,
            archive_root_path: dir.path().to_str().unwrap().to_string(),
            base_dir_rel: String::new(),
        };

        let mut recorder = DecisionRecorder::start(&conn, &params, Some(&ctx));
        let id = recorder.decision_id().unwrap();
        recorder.record_artifact_pointer(&conn, 1, "books/drive-1/story.md");

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(1),
                failed: Some(0),
                skipped: None,
            },
            "retired",
        );

        let row = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(row.receipt_root_id, Some(1));
        assert_eq!(
            row.receipt_rel_path.as_deref(),
            Some("books/drive-1/story.md")
        );
    }

    /// A post-hoc receipt registers no claim at `start()` — scan computes its
    /// path only after the walk, and links it through the scope index rather
    /// than the decision's own columns. Completion must not invent a
    /// finalization for a slot that was never filled: the receipt is already
    /// final on disk, and a spurious warning about it would be the only visible
    /// symptom.
    #[test]
    fn a_post_hoc_receipt_leaves_the_columns_alone() {
        let conn = setup_test_db();
        let dir = tempdir().unwrap();
        let params = make_receipt_params();

        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        let id = recorder.decision_id().unwrap();
        let placed = recorder.write_placed_receipt(
            &ReceiptPlacement::LedgerRoot {
                root_id: 1,
                root_path: dir.path().to_str().unwrap().to_string(),
            },
            "scan",
            &toml_body(),
            "one file",
        );
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(1),
                completed: Some(0),
                failed: Some(0),
                skipped: Some(1),
            },
            "scanned",
        );

        // The placed receipt is on disk and stays there; the decision's own
        // columns were never the place it was recorded.
        let placed = placed.unwrap();
        assert!(dir.path().join(&placed.rel_path).exists());
        let row = repo::decision::fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(row.receipt_root_id, None);
        assert_eq!(row.receipt_rel_path, None);
        assert!(
            recorder.warnings.is_empty(),
            "unexpected warnings: {:?}",
            recorder.warnings
        );
    }

    // =========================================================================
    // The status conjugation
    //
    // `insert_started` writes `started` before anything happens, so the row
    // makes a claim about a run that has not finished. The recorder registered
    // that claim, so the recorder settles it at the decision's last act —
    // confirmed (`completed`), corrected (`partial`/`interrupted`) or retracted
    // (`refused`). A row that keeps `started` is a run that never reached a
    // last act at all, which is the recovery signal the provenance model reads.
    // =========================================================================

    /// Force every later write against `decisions` to fail, so a settlement
    /// that did not happen cannot be recorded as one.
    fn break_the_decisions_table(conn: &Connection) {
        conn.execute("DROP TABLE decisions", []).unwrap();
    }

    /// The ordinary path: a run that reaches its last act settles the claim it
    /// registered.
    #[test]
    fn a_completed_run_settles_its_status_claim() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        assert_eq!(recorder.status_claim, StatusClaim::Registered);

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts::none(),
            "done",
        );

        assert_eq!(recorder.status_claim, StatusClaim::Settled);
    }

    /// A settlement the database refused did not happen. The row really is
    /// still `started`, and the warning the recorder already pushes is the
    /// record of why — so the claim stays registered rather than recording a
    /// settlement that is not on the row.
    #[test]
    fn a_failed_status_update_leaves_the_claim_registered() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        break_the_decisions_table(&conn);

        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts::none(),
            "done",
        );

        assert_eq!(recorder.status_claim, StatusClaim::Registered);
        assert!(
            recorder
                .warnings
                .iter()
                .any(|w| w.contains("failed to update decision record")),
            "the failure must still be reported: {:?}",
            recorder.warnings
        );
    }

    /// Settled is terminal, for the same reason `Finalized` is on the receipt
    /// side: a later failure must not walk a settled row back to a claim that
    /// its status is still `started`, which would point recovery at a run that
    /// finished.
    #[test]
    fn settling_a_settled_status_again_leaves_it_alone() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Scan, true);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        recorder.complete(
            &conn,
            DecisionStatus::Completed,
            DecisionCounts::none(),
            "done",
        );
        assert_eq!(recorder.status_claim, StatusClaim::Settled);

        break_the_decisions_table(&conn);
        recorder.complete(
            &conn,
            DecisionStatus::Interrupted,
            DecisionCounts::none(),
            "second thoughts",
        );

        assert_eq!(recorder.status_claim, StatusClaim::Settled);
    }

    /// No row, no claim. A recorder that never registered anything has nothing
    /// to settle, and `refuse` — the terminal a refusing caller reaches for
    /// without knowing whether recording is on — must be a silent no-op rather
    /// than a write against a decision that does not exist.
    #[test]
    fn a_disabled_recorder_claims_no_status() {
        let conn = setup_test_db();
        let params = make_params(DecisionCommand::Apply, false);
        let mut recorder = DecisionRecorder::start(&conn, &params, None);
        assert_eq!(recorder.status_claim, StatusClaim::Unclaimed);

        recorder.refuse(&conn, "Refused: 1 sources are not readable");

        assert_eq!(recorder.status_claim, StatusClaim::Unclaimed);
        assert_eq!(count_decisions(&conn), 0);
        assert!(
            recorder.take_warnings().is_empty(),
            "a disabled recorder has nothing to warn about"
        );
    }

    /// Exhaustive over the status vocabulary, with no `_` arm: a variant added
    /// later cannot slip in without an answer to "which act writes this, and
    /// does writing it settle the row?". `Started` is the one non-terminal, and
    /// it is here to be classified as such rather than omitted.
    #[test]
    fn every_terminal_status_is_reachable_by_name() {
        for status in [
            DecisionStatus::Started,
            DecisionStatus::Completed,
            DecisionStatus::Partial,
            DecisionStatus::Interrupted,
            DecisionStatus::Refused,
        ] {
            let conn = setup_test_db();
            let params = make_params(DecisionCommand::Apply, true);
            let mut recorder = DecisionRecorder::start(&conn, &params, None);
            assert_eq!(recorder.status_claim, StatusClaim::Registered);

            match status {
                // Not a terminal: it is what `start()` registers, and the only
                // row that keeps it is one whose run never reached a last act.
                DecisionStatus::Started => {
                    let row = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
                        .unwrap()
                        .unwrap();
                    assert_eq!(row.status, DecisionStatus::Started.as_str());
                    assert_eq!(recorder.status_claim, StatusClaim::Registered);
                    continue;
                }
                // The outcomes the caller names, because which word an outcome
                // deserves is caller knowledge — it is what knows its results.
                DecisionStatus::Completed
                | DecisionStatus::Partial
                | DecisionStatus::Interrupted => {
                    recorder.complete(&conn, status, DecisionCounts::none(), "an outcome")
                }
                // The one outcome carrying no caller-specific information, so
                // the recorder owns its whole shape: empty counts, receipt
                // claim retracted.
                DecisionStatus::Refused => recorder.refuse(&conn, "Refused: a pre-flight said no"),
            }

            let row = repo::decision::fetch_by_id(&conn, recorder.decision_id().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(row.status, status.as_str());
            assert_eq!(recorder.status_claim, StatusClaim::Settled);
        }
    }

    /// The smallest thing that serializes to a receipt body.
    fn toml_body() -> std::collections::BTreeMap<String, String> {
        let mut body = std::collections::BTreeMap::new();
        body.insert("summary".to_string(), "one file".to_string());
        body
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

        // Write through the recorder, as production does: what makes a claim
        // finalizable rather than retractable is that the recorder wrote it.
        let receipt_path = recorder.receipt_abs_path().unwrap().to_path_buf();
        let incomplete = receipt_path.with_extension("incomplete");
        recorder.write_receipt_file(&toml_body(), "Applied 1 file");
        assert!(
            incomplete.exists(),
            ".incomplete should exist before complete()"
        );

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
        let roots = vec![crate::core::domain::root::Root {
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
