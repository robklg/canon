//! Performing an apply — carrying the files across and recording what happened.
//!
//! The transfer loop is non-transactional by design: the filesystem cannot roll
//! back alongside the database, so recovery is fix-forward. A decision is opened
//! before the first transfer and closed with the outcome, the receipt names only
//! the transfers that completed, and an interrupted run leaves both saying so.

use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(not(unix))]
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};

use crate::archive::domain::mtime_change;
use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::extraction::{build_extraction_rows, ExtractionItem, OriginDisposition};
use crate::core::domain::format::first_chars;
use crate::core::domain::source::NewSource;
use crate::core::domain::time::now_epoch_secs;
use crate::core::domain::time::zone::RecordedZone;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::fs::{compute_partial_hash, copy_file, move_file, rename_file, MoveOutcome};
use crate::core::ops::receipt::{ReceiptKind, ReceiptPlacement};
use crate::core::repo::{self, Connection};

use super::receipt::{ApplyReceipt, ApplyReceiptItem};

use super::plan::{ensure_parent_dir, staleness_lines, ApplyPlan, ApplyTransfer, StaleSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV
}

impl TransferMode {
    /// What the transfer leaves behind at the origin. Copy leaves the content
    /// standing in two places; Rename and Move both take it away, and the
    /// receipt records only that difference — which of the two carried it is
    /// the transfer's business, not the record's.
    pub fn origin_disposition(self) -> OriginDisposition {
        match self {
            TransferMode::Copy => OriginDisposition::Retained,
            TransferMode::Rename | TransferMode::Move => OriginDisposition::Relocated,
        }
    }
}

// ===========================================================================
// Execute types
// ===========================================================================

/// Outcome of a single transfer operation.
#[derive(Debug)]
pub enum TransferOutcome {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String),
    Error(String),
}

/// Which preflight sweep is running.
///
/// The two sweeps stay separate on purpose: every source is opened first, so an
/// unreadable one is refused before a single file's content has been read. That
/// ordering is what keeps a refusal cheap, and it is why the sweeps are reported
/// as two passes rather than one — a single count over both would have to hide
/// the boundary that makes the refusal cheap.
///
/// Ops names the sweep; the interface owns the words for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationSweep {
    /// Every source is opened, to refuse an unreadable one before reading any.
    Readability,
    /// Every source is measured and read against what the lock file recorded.
    LockAgreement,
}

/// Progress notification for file transfer operations.
/// The interface implements this to display progress, verbose logging, etc.
/// Fire-and-forget — does not affect the operation's behavior.
pub trait TransferProgress {
    /// Called once before the transfer loop begins.
    fn on_start(&self, total: usize);
    /// Called after each transfer completes.
    fn on_transfer(
        &self,
        index: usize,
        total: usize,
        source_path: &str,
        dest_path: &str,
        outcome: &TransferOutcome,
    );
    /// Called when an interrupt is detected: after the current transfer completes, or
    /// before the first one when the press came during the checks.
    fn on_interrupt(&self);
    /// Called once after the transfer loop ends.
    fn on_finish(&self);

    /// Called before a preflight sweep begins, with which sweep it is and how
    /// many sources it will touch.
    ///
    /// Each sweep touches every source on disk before anything moves, which on
    /// a network volume is minutes of otherwise silent work. These three
    /// default to nothing, so an implementation written before the sweeps had
    /// a voice still compiles and still runs.
    fn on_validation_start(&self, _sweep: ValidationSweep, _total: usize) {}
    /// Called after each source in the current sweep, with how many are done.
    fn on_validation_progress(&self, _done: usize) {}
    /// Called once the current sweep ends, however it ended.
    fn on_validation_finish(&self) {}
}

/// Parameters for executing an apply operation.
pub struct ApplyExecuteParams {
    /// Base directory for destination paths (archive root + base_dir from manifest).
    pub base_dir: PathBuf,
    /// Archive root ID for DB registration.
    pub archive_root_id: i64,
    /// How to transfer files.
    pub transfer_mode: TransferMode,
    /// Whether this is a resume operation.
    pub resume: bool,
    /// Interrupt flag — set to true to stop after the current transfer, or before
    /// the first one when it is set during the checks.
    /// If None, the run has a fresh flag nothing sets: no signal handling is
    /// installed (the interface installs it, `setup_interrupt_flag`).
    pub interrupt_flag: Option<Arc<AtomicBool>>,
    /// Sources skipped by --root filter (for summary).
    pub skipped_by_filter: usize,
    /// Manifest display path (for summary and decision record).
    pub manifest_display: String,
    /// Receipt placement for targeted (apply) receipts. None if receipts are disabled.
    pub receipt_ctx: Option<ReceiptPlacement>,
    /// The zone the lock was built in, which the disk check's save times print on.
    pub lock_zone: RecordedZone,
}

/// Result of executing an apply operation.
pub struct ApplyResult {
    pub copied: u64,
    pub renamed: u64,
    pub moved: u64,
    pub skipped_missing: u64,
    pub skipped_stale: Vec<StaleSource>,
    pub errors: Vec<TransferError>,
    /// Resume mode: count of entries already at destination, whether the index
    /// already recorded them or this run proved and recorded them.
    pub already_there: u64,
    /// Resume mode: count of already-there entries where source file still exists.
    pub already_there_source_present: u64,
    /// Resume mode: how many of `already_there` this run proved by hashing the
    /// destination and then recorded (row, receipt item, ledger row).
    pub proven: u64,
    /// Whether the operation was interrupted by Ctrl+C.
    pub interrupted: bool,
    /// Number of files remaining when interrupted.
    pub remaining: usize,
    /// Completion summary message.
    pub summary: String,
    /// Warnings collected during execution (e.g. receipt-write failures).
    /// Drained from the decision recorder; the interface surfaces them.
    pub warnings: Vec<String>,
}

/// An error encountered during a file transfer.
/// Fields are populated by execute_apply but errors are reported eagerly
/// via TransferProgress — the struct exists for post-transfer summary access.
#[allow(dead_code)]
pub struct TransferError {
    pub path: String,
    pub error: String,
}

// ===========================================================================
// Execute function
// ===========================================================================

/// Set up two-tier Ctrl+C handling for an apply.
/// Returns an Arc<AtomicBool> that becomes true on the first SIGINT; the second
/// SIGINT gets the default action, which ends the process.
///
/// The order of the two registrations is the contract: the conditional default
/// reads the flag before the second registration sets it, so the first signal
/// arms and the second fires. Reversed, the first signal would set the flag and
/// then fire the default in the same round.
pub fn setup_interrupt_flag() -> Result<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register_conditional_default(
        signal_hook::consts::SIGINT,
        Arc::clone(&flag),
    )?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&flag))?;
    Ok(flag)
}

/// Execute file transfers from a computed apply plan.
///
/// Performs: source readability checks, batch staleness validation, the transfer
/// loop, and (in resume mode) recording the entries the plan proved were already at
/// their destination. Each transfer is independent (no transaction wrapping) — the
/// operation is idempotent and resume-safe.
///
/// Does NOT handle: manifest parsing, confirmation prompts, dry-run display,
/// output formatting. These are interface concerns.
pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
    decision: Option<&DecisionParams>,
) -> Result<ApplyResult> {
    // `None` means no signal handling: the interface installs the handler
    // (`setup_interrupt_flag`) before the run, so a test that passes nothing
    // never registers a process-wide handler.
    let interrupt_flag = match &params.interrupt_flag {
        Some(flag) => Arc::clone(flag),
        None => Arc::new(AtomicBool::new(false)),
    };

    let mut recorder =
        decision.map(|d| DecisionRecorder::start(conn, d, params.receipt_ctx.as_ref()));

    let mut result = ApplyResult {
        copied: 0,
        renamed: 0,
        moved: 0,
        skipped_missing: 0,
        skipped_stale: Vec::new(),
        errors: Vec::new(),
        already_there: plan.already_archived_count as u64,
        already_there_source_present: plan.resume_already_there_source_present as u64,
        proven: 0,
        interrupted: false,
        remaining: 0,
        summary: String::new(),
        warnings: Vec::new(),
    };

    // --- Source readability pre-check ---
    let mut unreadable: Vec<(String, String)> = Vec::new();
    progress.on_validation_start(ValidationSweep::Readability, plan.transfers.len());
    for (i, transfer) in plan.transfers.iter().enumerate() {
        match File::open(&transfer.source_path) {
            Ok(_) => {}
            // A source that is simply gone is not unreadable; the sweep below
            // names the absence.
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                unreadable.push((
                    transfer.source_path.clone(),
                    "permission denied".to_string(),
                ));
            }
            Err(e) => {
                unreadable.push((transfer.source_path.clone(), e.to_string()));
            }
        }
        progress.on_validation_progress(i + 1);
        if interrupt_flag.load(Ordering::Relaxed) {
            break;
        }
    }
    progress.on_validation_finish();
    // A sweep that stopped early has only a partial list, so it refuses nothing.
    if !unreadable.is_empty() && !interrupt_flag.load(Ordering::Relaxed) {
        // The run refuses before anything moves. This is the decision's last
        // act, so the row settles here: `refused` names what happened, the
        // counts stay empty, and the receipt registered at start() — a file
        // that will never appear — is retracted in the same act.
        if let Some(recorder) = recorder.as_mut() {
            recorder.refuse(
                conn,
                &format!("Refused: {} sources are not readable", unreadable.len()),
            );
        }
        bail!(
            "{} sources are not readable: {}",
            unreadable.len(),
            unreadable
                .iter()
                .take(10)
                .map(|(p, r)| format!("{p} ({r})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // In resume mode, plan.transfers already contains only pending entries.
    // No disk classification needed here — it was done in plan_apply.
    let transfers_to_execute: Vec<&ApplyTransfer> = plan.transfers.iter().collect();

    // --- Batch staleness validation ---
    if !transfers_to_execute.is_empty() {
        let stale = validate_transfers_disk(
            &transfers_to_execute,
            progress,
            &params.lock_zone,
            &interrupt_flag,
        );
        if !stale.is_empty() && !interrupt_flag.load(Ordering::Relaxed) {
            // The other pre-transfer refusal, settled the same way — the two
            // must never diverge, which is what pins them separately.
            if let Some(recorder) = recorder.as_mut() {
                recorder.refuse(
                    conn,
                    &format!(
                        "Refused: {} sources have changed since the manifest was generated",
                        stale.len()
                    ),
                );
            }
            let mut message = format!(
                "{} sources have changed since manifest was generated:",
                stale.len()
            );
            for line in staleness_lines(&stale, &params.manifest_display) {
                message.push('\n');
                message.push_str(&line);
            }
            bail!(message);
        }
    }

    // --- Transfer loop ---
    let total = transfers_to_execute.len();
    let decision_id = recorder.as_ref().and_then(|r| r.decision_id());
    let mut receipt_items: Vec<ApplyReceiptItem> = Vec::new();

    // A press during the checks above ends the run before the first transfer:
    // nothing moves, and the run settles through the ordinary interrupted path
    // with nothing completed. A run that stops here records nothing, the entries
    // a resume proved included: a press at this point is often the user seeing
    // a wrong run, and the receipt's origin disposition comes from the run's mode.
    // The next resume proves and records them.
    let stopped_before_transfer = total > 0 && interrupt_flag.load(Ordering::Relaxed);
    let proven_to_record: &[ApplyTransfer] = if stopped_before_transfer {
        &[]
    } else {
        &plan.resume_already_there
    };
    let to_run: &[&ApplyTransfer] = if stopped_before_transfer {
        progress.on_interrupt();
        result.interrupted = true;
        result.remaining = total;
        &[]
    } else {
        progress.on_start(total);
        &transfers_to_execute
    };

    for (i, transfer) in to_run.iter().enumerate() {
        let (outcome, prev_decision_id) = match execute_single_transfer(
            transfer,
            &params.base_dir,
            params.transfer_mode,
            conn,
            params.archive_root_id,
            decision_id,
            &params.lock_zone,
        ) {
            Ok(pair) => pair,
            Err(e) => (TransferOutcome::Error(format!("{:#}", e)), None),
        };

        match &outcome {
            TransferOutcome::Copied | TransferOutcome::Renamed | TransferOutcome::Moved => {
                receipt_items.push(ApplyReceiptItem {
                    source_root: transfer.source_root_path.clone(),
                    source_rel_path: transfer.source_rel_path.clone(),
                    destination_rel_path: transfer.archive_rel_path.as_str().to_string(),
                    hash: transfer.hash.clone(),
                    size: transfer.size,
                    mtime: transfer.mtime,
                    previous_decision_id: prev_decision_id,
                });
            }
            _ => {}
        }

        match &outcome {
            TransferOutcome::Copied => result.copied += 1,
            TransferOutcome::Renamed => result.renamed += 1,
            TransferOutcome::Moved => result.moved += 1,
            TransferOutcome::SkippedMissing => result.skipped_missing += 1,
            TransferOutcome::SkippedStale(reason) => {
                result.skipped_stale.push(StaleSource {
                    path: transfer.source_path.clone(),
                    reason: reason.clone(),
                });
            }
            TransferOutcome::Error(msg) => {
                result.errors.push(TransferError {
                    path: transfer.source_path.clone(),
                    error: msg.clone(),
                });
            }
        }

        let dest_full = transfer.dest_rel_path.under(&params.base_dir);
        let dest_str = dest_full.display().to_string();
        progress.on_transfer(i, total, &transfer.source_path, &dest_str, &outcome);

        // Check interrupt flag AFTER the complete unit of work (file + DB + count)
        if interrupt_flag.load(Ordering::Relaxed) {
            progress.on_interrupt();
            result.interrupted = true;
            result.remaining = total - (i + 1);
            break;
        }
    }

    progress.on_finish();

    // --- Resume mode: record the entries this run proved were already there ---
    //
    // The plan lists only entries whose destination it hashed to the lock's
    // content and that no present row records. No earlier receipt lists them: a
    // killed run wrote none, and a run whose index write failed reported an
    // error and listed nothing for that file. So this run is the one that
    // records them — the row, a receipt item, and (through the extraction block
    // below, with no code of its own) the ledger row. Without the item,
    // `ledger reindex` could not rebuild the row and the card could not say
    // where the file came from.
    //
    // An entry the index already records is not here: it is counted and left
    // alone, its row keeping the decision that put it there.
    if params.resume {
        for transfer in proven_to_record {
            let dest_path = transfer.dest_rel_path.under(&params.base_dir);
            let recorded = crate::archive::repo::fetch_decision_id_at_path(
                conn,
                params.archive_root_id,
                transfer.archive_rel_path.as_str(),
            )
            .and_then(|previous| {
                register_destination(
                    conn,
                    transfer,
                    &dest_path,
                    params.archive_root_id,
                    decision_id,
                )
                .map(|()| previous)
            });
            match recorded {
                Ok(previous_decision_id) => {
                    receipt_items.push(ApplyReceiptItem {
                        source_root: transfer.source_root_path.clone(),
                        source_rel_path: transfer.source_rel_path.clone(),
                        destination_rel_path: transfer.archive_rel_path.as_str().to_string(),
                        hash: transfer.hash.clone(),
                        size: transfer.size,
                        mtime: transfer.mtime,
                        previous_decision_id,
                    });
                    result.proven += 1;
                }
                Err(e) => result.errors.push(TransferError {
                    path: transfer.source_path.clone(),
                    error: format!(
                        "{} is in the archive, but the index was not updated: {e:#}",
                        dest_path.display()
                    ),
                }),
            }
        }
    }

    // --- Extraction ledger: record what this run drew from each source root ---
    //
    // Gated on a live decision id (which implies record_enabled — Records mode
    // gets a live ledger too, receipts or not) and a non-empty completed set
    // (the 0-item convention). Written after the DB mutations above and
    // independent of receipt file writing — apply is non-transactional
    // fix-forward by ADR, so a crash before this point is healed by a later
    // `ledger reindex`.
    //
    // Every failure here is best-effort: the transfers are already done and
    // persisted, so bookkeeping must never turn a completed apply into a
    // failed one. Each gap warns rather than propagating — `ledger reindex`
    // heals it from the receipt later, and the warning says so meanwhile.
    if let Some(decision_id) = decision_id {
        if !receipt_items.is_empty() {
            for warning in record_extractions(conn, &receipt_items, params, decision_id) {
                if let Some(recorder) = recorder.as_mut() {
                    recorder.push_warning(warning);
                }
            }
        }
    }

    // Compose summary
    result.summary = if result.interrupted {
        format!(
            "Applied {}: {} copied, {} renamed, {} moved, {} errors. Interrupted — {} files remaining.",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.errors.len(), result.remaining
        )
    } else if params.resume {
        format!(
            "Applied {} (--resume): {} copied, {} renamed, {} moved, {} already at destination, {} errors",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.already_there, result.errors.len()
        )
    } else {
        format!(
            "Applied {}: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (stale), {} skipped (filtered), {} errors",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.skipped_missing,
            result.skipped_stale.len(), params.skipped_by_filter, result.errors.len()
        )
    };

    if let Some(recorder) = recorder.as_mut() {
        // A proven entry is work this run did: it counts as attempted and completed
        // so the row agrees with the receipt that lists it.
        let total = (plan.transfers.len() + proven_to_record.len()) as i64;
        let completed = (result.copied + result.renamed + result.moved + result.proven) as i64;
        let failed = result.errors.len() as i64;
        let skipped = (result.skipped_missing as i64)
            + (result.skipped_stale.len() as i64)
            + (params.skipped_by_filter as i64);
        let status = if result.interrupted {
            DecisionStatus::Interrupted
        } else if !result.errors.is_empty() {
            DecisionStatus::Partial
        } else {
            DecisionStatus::Completed
        };

        // Build the receipt only when there's something to record, a live decision
        // id exists, and a placement is known (the placement is the receipt's
        // locus). A placement-less receipt was never written to disk anyway, so
        // gating here is behavior-identical and lets the locus be non-optional.
        let receipt = match (
            decision,
            recorder.decision_id(),
            params.receipt_ctx.as_ref(),
        ) {
            (Some(d), Some(did), Some(placement)) if !receipt_items.is_empty() => {
                Some(ApplyReceipt {
                    meta: d.receipt_meta(
                        did,
                        status,
                        &result.summary,
                        placement.locus_root(),
                        ReceiptKind::Apply(params.transfer_mode.origin_disposition()),
                        Some(params.manifest_display.clone()),
                    ),
                    items: receipt_items,
                })
            }
            _ => None,
        };

        recorder.complete_with_receipt(
            conn,
            status,
            DecisionCounts {
                attempted: Some(total),
                completed: Some(completed),
                failed: Some(failed),
                skipped: Some(skipped),
            },
            &result.summary,
            receipt.as_ref(),
        );
        result.warnings = recorder.take_warnings();
    }

    Ok(result)
}

/// Record what this apply drew from each source root into the extraction
/// ledger, returning any warnings rather than failing the run.
///
/// Best-effort by design: the transfers this summarizes are already on disk
/// and in the DB, so no bookkeeping failure here may propagate — an
/// unrecorded row is a gap `ledger reindex` heals from the receipt, while a
/// returned `Err` would report a completed apply as a failed one. Every gap
/// warns; none is silent.
fn record_extractions(
    conn: &Connection,
    receipt_items: &[ApplyReceiptItem],
    params: &ApplyExecuteParams,
    decision_id: i64,
) -> Vec<String> {
    let roots = match repo::root::fetch_all(conn) {
        Ok(roots) => roots,
        Err(e) => {
            return vec![format!(
                "Warning: could not read roots to record the extraction ledger: {e} \
                 (run `canon ledger reindex` to backfill)"
            )]
        }
    };
    let Some(archive_root) = roots.iter().find(|r| r.id == params.archive_root_id) else {
        return vec![format!(
            "Warning: archive root #{} not found when recording the extraction ledger \
             (run `canon ledger reindex` to backfill)",
            params.archive_root_id
        )];
    };

    let items: Vec<ExtractionItem> = receipt_items
        .iter()
        .map(|item| ExtractionItem {
            source_root: &item.source_root,
            source_rel_path: &item.source_rel_path,
            destination_rel_path: &item.destination_rel_path,
            size: item.size,
        })
        .collect();
    let disposition = match params.transfer_mode {
        TransferMode::Copy => OriginDisposition::Retained,
        TransferMode::Move | TransferMode::Rename => OriginDisposition::Relocated,
    };
    let (rows, unknown_roots) = build_extraction_rows(
        &items,
        &roots,
        (Some(params.archive_root_id), &archive_root.path),
        Some(disposition),
        decision_id,
    );

    let mut warnings = Vec::new();
    // Delete-then-insert as one atomic pair: a concurrent `canon trail` must
    // never read a half-replaced decision. The apply flow itself stays
    // non-transactional (fix-forward); this brackets only the index write.
    // This opens its own transaction, so a caller must not already hold one:
    // the nested begin would fail, and the failure is downgraded to a warning
    // below, leaving the index quietly unwritten.
    let write = || -> anyhow::Result<()> {
        let tx = conn.unchecked_transaction()?;
        repo::decision::replace_extractions(&tx, &rows)?;
        tx.commit()?;
        Ok(())
    };
    if let Err(e) = write() {
        warnings.push(format!(
            "Warning: failed to record extraction ledger: {e} \
             (run `canon ledger reindex` to backfill)"
        ));
    }
    if !unknown_roots.is_empty() {
        warnings.push(format!(
            "Warning: {} source root(s) not recognized for the extraction ledger: {}",
            unknown_roots.len(),
            unknown_roots.join(", ")
        ));
    }
    warnings
}

// ===========================================================================
// Execute helpers (private)
// ===========================================================================

/// Execute a single file transfer. Returns the outcome and the previous decision_id
/// for the destination path (None if no prior source existed there).
fn execute_single_transfer(
    transfer: &ApplyTransfer,
    base_dir: &Path,
    transfer_mode: TransferMode,
    conn: &Connection,
    archive_root_id: i64,
    decision_id: Option<i64>,
    clock: &RecordedZone,
) -> Result<(TransferOutcome, Option<i64>)> {
    let src_path = Path::new(&transfer.source_path);
    let dest_path = transfer.dest_rel_path.under(base_dir);

    // Check if source exists
    if !src_path.exists() {
        return Ok((TransferOutcome::SkippedMissing, None));
    }

    // Per-transfer staleness validation (catches race conditions)
    if let Err(reason) = validate_source_state(transfer, clock) {
        return Ok((TransferOutcome::SkippedStale(reason), None));
    }

    // Capture previous decision_id before the transfer overwrites the destination record
    let prev_decision_id = crate::archive::repo::fetch_decision_id_at_path(
        conn,
        archive_root_id,
        transfer.archive_rel_path.as_str(),
    )?;

    // Create parent directories
    ensure_parent_dir(&dest_path)?;

    match transfer_mode {
        TransferMode::Copy => {
            copy_file(src_path, &dest_path, true)?;
            register_destination(conn, transfer, &dest_path, archive_root_id, decision_id)
                .map_err(|e| index_not_updated("copied", &dest_path, e))?;
            Ok((TransferOutcome::Copied, prev_decision_id))
        }
        TransferMode::Rename => {
            rename_file(src_path, &dest_path, true)?;
            record_relocation(conn, transfer, &dest_path, archive_root_id, decision_id)
                .map_err(|e| index_not_updated("moved", &dest_path, e))?;
            Ok((TransferOutcome::Renamed, prev_decision_id))
        }
        TransferMode::Move => match move_file(src_path, &dest_path, true)? {
            MoveOutcome::Renamed => {
                record_relocation(conn, transfer, &dest_path, archive_root_id, decision_id)
                    .map_err(|e| index_not_updated("moved", &dest_path, e))?;
                Ok((TransferOutcome::Renamed, prev_decision_id))
            }
            MoveOutcome::CopiedAndDeleted => {
                clear_origin_and_register(conn, transfer, &dest_path, archive_root_id, decision_id)
                    .map_err(|e| index_not_updated("moved", &dest_path, e))?;
                Ok((TransferOutcome::Moved, prev_decision_id))
            }
        },
    }
}

/// Clear the origin before registering the destination. A crash between the two
/// reports the file at neither end, which a later scan heals; the other order
/// reports it at both, which reads as a duplicate and invites acting on it.
fn clear_origin_and_register(
    conn: &Connection,
    transfer: &ApplyTransfer,
    dest_path: &Path,
    archive_root_id: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    mark_source_not_present(conn, transfer.source_id, decision_id)?;
    register_destination(conn, transfer, dest_path, archive_root_id, decision_id)
}

/// Register the file now standing at the destination: a new row, or the row
/// already at that path revived with this content (`insert_destination`).
fn register_destination(
    conn: &Connection,
    transfer: &ApplyTransfer,
    dest_path: &Path,
    archive_root_id: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    let new_source = build_new_source(
        dest_path,
        archive_root_id,
        transfer.archive_rel_path.as_str(),
        transfer.object_id,
        &transfer.partial_hash,
        decision_id,
    )?;
    crate::archive::repo::insert_destination(conn, &new_source)?;
    Ok(())
}

/// Record a relocation the filesystem has already carried out.
///
/// The origin row can take the destination path only where no row holds it:
/// `UNIQUE(root_id, rel_path)` refuses otherwise, and a row stands there when the
/// path was deleted (a tombstone, `present = 0`) or when a stale present row is
/// left over. Where one does, the origin is cleared and the destination is
/// registered through the route a move across devices already takes.
fn record_relocation(
    conn: &Connection,
    transfer: &ApplyTransfer,
    dest_path: &Path,
    archive_root_id: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    if repo::source::source_exists_at_path(
        conn,
        archive_root_id,
        transfer.archive_rel_path.as_str(),
    )? {
        clear_origin_and_register(conn, transfer, dest_path, archive_root_id, decision_id)
    } else {
        relocate_source(
            conn,
            transfer.source_id,
            archive_root_id,
            transfer.archive_rel_path.as_str(),
            decision_id,
        )
    }
}

/// The error for an index write that failed after the file was already placed.
/// It says where the file now is, because the file is the only thing that is
/// certain; `--resume` registers a destination the index does not record.
fn index_not_updated(verb: &str, dest_path: &Path, cause: anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "{verb} to {}, but the index was not updated: {cause:#}. `--resume` registers it.",
        dest_path.display()
    )
}

/// Validate that a source file on disk matches the state recorded in the transfer.
fn validate_source_state(
    transfer: &ApplyTransfer,
    clock: &RecordedZone,
) -> std::result::Result<(), String> {
    let meta = match fs::metadata(&transfer.source_path) {
        Ok(m) => m,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err("file not found".to_string());
        }
        Err(e) => {
            return Err(format!("cannot stat: {e}"));
        }
    };

    let mut mismatches = Vec::new();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let current_size = meta.size() as i64;
        let current_mtime = meta.mtime();

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(mtime_change(transfer.mtime, current_mtime, clock));
        }
    }

    #[cfg(not(unix))]
    {
        let current_size = meta.len() as i64;
        let current_mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(mtime_change(transfer.mtime, current_mtime, clock));
        }
    }

    // Partial hash check — recompute from disk and compare to lock
    let current_hash = compute_partial_hash(Path::new(&transfer.source_path), transfer.size as u64)
        .map_err(|e| format!("failed to compute partial hash: {e}"))?;
    if current_hash != transfer.partial_hash {
        mismatches.push(format!(
            "partial hash mismatch: {}... → {}...",
            first_chars(&transfer.partial_hash, 16),
            first_chars(&current_hash, 16)
        ));
    }

    if !mismatches.is_empty() {
        Err(mismatches.join(", "))
    } else {
        Ok(())
    }
}

/// Batch validate source file states against disk.
fn validate_transfers_disk(
    transfers: &[&ApplyTransfer],
    progress: &dyn TransferProgress,
    clock: &RecordedZone,
    interrupt: &AtomicBool,
) -> Vec<StaleSource> {
    let mut stale = Vec::new();
    progress.on_validation_start(ValidationSweep::LockAgreement, transfers.len());
    for (i, transfer) in transfers.iter().enumerate() {
        if let Err(reason) = validate_source_state(transfer, clock) {
            stale.push(StaleSource {
                path: transfer.source_path.clone(),
                reason,
            });
        }
        progress.on_validation_progress(i + 1);
        if interrupt.load(Ordering::Relaxed) {
            break;
        }
    }
    progress.on_validation_finish();
    stale
}

/// Relocate an existing source to a new location (for rename/move on same device).
/// Updates the source row in-place since the inode remains the same.
fn relocate_source(
    conn: &Connection,
    source_id: i64,
    archive_root_id: i64,
    rel_path: &str,
    decision_id: Option<i64>,
) -> Result<()> {
    let now = now_epoch_secs();
    crate::archive::repo::update_location(
        conn,
        source_id,
        archive_root_id,
        rel_path,
        now,
        decision_id,
    )
}

/// Mark a source as no longer present (for cross-device move after deletion).
///
/// Apply *caused* the source to vanish (copy-then-delete move), so this transition is
/// decision-linked to apply's own decision — passed through to `mark_missing`.
fn mark_source_not_present(
    conn: &Connection,
    source_id: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    let now = now_epoch_secs();
    repo::source::mark_missing(conn, &[source_id], now, decision_id)?;
    Ok(())
}

/// Build a NewSource from destination file metadata for DB registration.
#[cfg(unix)]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
    decision_id: Option<i64>,
) -> Result<NewSource> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(dest_path).with_context(|| {
        format!(
            "Failed to read metadata for registration: {}",
            dest_path.display()
        )
    })?;
    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.size() as i64,
        mtime: meta.mtime(),
        partial_hash: partial_hash.to_string(),
        object_id,
        device: Some(meta.dev() as i64),
        inode: Some(meta.ino() as i64),
        decision_id,
    })
}

#[cfg(not(unix))]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
    decision_id: Option<i64>,
) -> Result<NewSource> {
    let meta = fs::metadata(dest_path).with_context(|| {
        format!(
            "Failed to read metadata for registration: {}",
            dest_path.display()
        )
    })?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.len() as i64,
        mtime,
        partial_hash: partial_hash.to_string(),
        object_id,
        device: None,
        inode: None,
        decision_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::path::ContainedPath;
    use crate::core::domain::time::zone::{ReadTimesOn, UserZone};

    /// The zone a test lock records: the reference zone, never the machine's.
    fn lock_zone() -> crate::core::domain::time::zone::RecordedZone {
        crate::core::domain::time::zone::parse_zone_name("Europe/Amsterdam").unwrap()
    }
    use crate::core::testing::{insert_object, insert_root, setup_test_db};

    use super::super::plan::ApplyViolations;

    // =========================================================================
    // Origin disposition
    // =========================================================================

    /// The three transfer modes collapse to two dispositions: only whether the
    /// origin still holds the content is recorded. Rename and Move differ in
    /// how they move it, never in what they leave behind.
    #[test]
    fn transfer_modes_collapse_to_two_origin_dispositions() {
        assert_eq!(
            TransferMode::Copy.origin_disposition(),
            OriginDisposition::Retained
        );
        assert_eq!(
            TransferMode::Rename.origin_disposition(),
            OriginDisposition::Relocated
        );
        assert_eq!(
            TransferMode::Move.origin_disposition(),
            OriginDisposition::Relocated
        );
    }

    // =========================================================================
    // validate_source_state tests
    // =========================================================================

    fn make_transfer_for_file(
        path: &Path,
        size: i64,
        mtime: i64,
        partial_hash: &str,
    ) -> ApplyTransfer {
        ApplyTransfer {
            source_id: 1,
            source_path: path.display().to_string(),
            source_root_path: String::new(),
            source_rel_path: path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            dest_rel_path: ContainedPath::new("dest.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("dest.jpg").unwrap(),
            object_id: Some(1),
            partial_hash: partial_hash.to_string(),
            size,
            mtime,
            hash: None,
        }
    }

    #[test]
    fn validate_unchanged_file() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        f.flush().unwrap();

        let meta = std::fs::metadata(f.path()).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = { (meta.len() as i64, 0i64) };

        let hash = compute_partial_hash(f.path(), size as u64).unwrap();
        let transfer = make_transfer_for_file(f.path(), size, mtime, &hash);
        assert!(validate_source_state(&transfer, &lock_zone()).is_ok());
    }

    #[test]
    fn validate_missing_file() {
        let transfer = make_transfer_for_file(
            Path::new("/nonexistent/file.jpg"),
            1000,
            1704067200,
            "testhash",
        );
        let err = validate_source_state(&transfer, &lock_zone()).unwrap_err();
        assert!(
            err.contains("not found"),
            "expected 'not found', got: {err}"
        );
    }

    #[test]
    fn validate_size_changed() {
        // Create file, record metadata, then change it
        let f = tempfile::NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        std::fs::write(&path, b"short").unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        #[cfg(unix)]
        let (orig_size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (orig_size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&path, orig_size as u64).unwrap();

        // Now write more data so size changes
        std::fs::write(&path, b"much longer content here").unwrap();

        let transfer = make_transfer_for_file(&path, orig_size, mtime, &hash);
        let err = validate_source_state(&transfer, &lock_zone()).unwrap_err();
        assert!(err.contains("size"), "expected 'size' in error, got: {err}");
    }

    #[test]
    fn validate_hash_changed() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        f.flush().unwrap();

        let meta = std::fs::metadata(f.path()).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = { (meta.len() as i64, 0i64) };

        // Correct size/mtime but wrong hash
        let transfer = make_transfer_for_file(f.path(), size, mtime, "wrong_hash_value_here");
        let err = validate_source_state(&transfer, &lock_zone()).unwrap_err();
        assert!(
            err.contains("partial hash"),
            "expected 'partial hash' in error, got: {err}"
        );
    }

    #[test]
    fn validate_hash_changed_reports_a_multibyte_hash_without_panicking() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        f.flush().unwrap();

        let meta = std::fs::metadata(f.path()).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = { (meta.len() as i64, 0i64) };

        // A lock file is ordinary text on disk and nothing enforces that a
        // recorded hash is hex. Truncating this one for the message must count
        // characters: its sixteenth byte falls inside a character.
        let transfer = make_transfer_for_file(f.path(), size, mtime, "日本語日本語日本語");
        let err = validate_source_state(&transfer, &lock_zone()).unwrap_err();
        assert!(
            err.contains("partial hash"),
            "expected 'partial hash' in error, got: {err}"
        );
    }

    // =========================================================================
    // execute_single_transfer tests (integration, uses tempfiles)
    // =========================================================================

    #[test]
    fn execute_copy_creates_file() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&src_file).unwrap();
        f.write_all(b"photo data").unwrap();
        drop(f);

        let meta = std::fs::metadata(&src_file).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&src_file, size as u64).unwrap();

        let obj_id = insert_object(&conn, "abc123", false);

        let dest_dir = tempfile::tempdir().unwrap();
        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: Some(obj_id),
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let (outcome, _prev) = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            None,
            &lock_zone(),
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::Copied));
        assert!(dest_dir.path().join("photo.jpg").exists());
    }

    #[test]
    fn execute_copy_noclobber() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        let dest_dir = tempfile::tempdir().unwrap();
        // Pre-create destination
        std::fs::File::create(dest_dir.path().join("photo.jpg")).unwrap();

        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: Some(1),
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let result = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            None,
            &lock_zone(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    /// Every transfer mode passes noclobber, and that flag is what stands
    /// between a re-run and an overwritten archive original — the reason the
    /// planner is allowed to skip an archive hash check. Copy is covered
    /// above; these cover the two modes that also remove the source.
    fn noclobber_refuses_an_occupied_destination(mode: TransferMode) {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        let dest_dir = tempfile::tempdir().unwrap();
        std::fs::File::create(dest_dir.path().join("photo.jpg"))
            .unwrap()
            .write_all(b"original")
            .unwrap();

        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: Some(1),
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let result = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            mode,
            &conn,
            archive_root,
            None,
            &lock_zone(),
        );
        assert!(
            result.is_err(),
            "{mode:?} overwrote an occupied destination"
        );
        assert!(result.unwrap_err().to_string().contains("already exists"));

        // The occupant is untouched and the source is still where it was.
        let occupant = std::fs::read(dest_dir.path().join("photo.jpg")).unwrap();
        assert_eq!(occupant, b"original");
        assert!(src_file.exists());
    }

    #[test]
    fn execute_rename_noclobber() {
        noclobber_refuses_an_occupied_destination(TransferMode::Rename);
    }

    #[test]
    fn execute_move_noclobber() {
        noclobber_refuses_an_occupied_destination(TransferMode::Move);
    }

    #[test]
    fn execute_source_missing() {
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let dest_dir = tempfile::tempdir().unwrap();

        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: "/nonexistent/photo.jpg".to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let (outcome, _prev) = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            None,
            &lock_zone(),
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::SkippedMissing));
    }

    // =========================================================================
    // decision_id threading and previous_decision_id capture
    // =========================================================================

    #[test]
    fn execute_copy_sets_decision_id_on_destination() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        let dest_dir = tempfile::tempdir().unwrap();
        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: None,
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let decision_id = Some(77i64);
        execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            decision_id,
            &lock_zone(),
        )
        .unwrap();

        // Destination source should have decision_id = 77
        let stored_decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = 'photo.jpg'",
                rusqlite::params![archive_root],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_decision_id, decision_id);
    }

    #[test]
    fn execute_copy_re_apply_updates_decision_id() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        let dest_dir = tempfile::tempdir().unwrap();
        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: None,
            partial_hash: hash.clone(),
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        // First apply: decision 10
        execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            Some(10),
            &lock_zone(),
        )
        .unwrap();

        // Recreate source file for second apply (clobber protection requires dest be renamed/moved)
        let dest_file = dest_dir.path().join("photo.jpg");
        std::fs::remove_file(&dest_file).unwrap();
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data2")
            .unwrap();
        let meta2 = std::fs::metadata(&src_file).unwrap();
        #[cfg(unix)]
        let (size2, mtime2) = {
            use std::os::unix::fs::MetadataExt;
            (meta2.size() as i64, meta2.mtime())
        };
        #[cfg(not(unix))]
        let (size2, mtime2) = (meta2.len() as i64, 0i64);
        let hash2 = compute_partial_hash(&src_file, size2 as u64).unwrap();

        let transfer2 = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: None,
            partial_hash: hash2,
            size: size2,
            mtime: mtime2,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        // Second apply: decision 20 — also captures previous_decision_id = 10
        let (outcome, prev_decision_id) = execute_single_transfer(
            &transfer2,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            Some(20),
            &lock_zone(),
        )
        .unwrap();
        assert!(matches!(outcome, TransferOutcome::Copied));
        assert_eq!(prev_decision_id, Some(10));

        // DB should now show decision_id = 20
        let stored: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = 'photo.jpg'",
                rusqlite::params![archive_root],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, Some(20));
    }

    #[test]
    fn execute_copy_previous_decision_id_none_for_new_destination() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        let dest_dir = tempfile::tempdir().unwrap();
        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
            object_id: None,
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        // Fresh destination — no prior record
        let (_outcome, prev_decision_id) = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
            Some(5),
            &lock_zone(),
        )
        .unwrap();
        assert_eq!(prev_decision_id, None);
    }

    // =========================================================================
    // Interrupt flag tests
    // =========================================================================

    /// No-op progress implementation for testing.
    ///
    /// It implements only the four transfer-loop methods, which is what makes
    /// it the standing check that the validation methods stay defaulted: give
    /// any of them a body in the trait and this — and every caller like it —
    /// stops compiling.
    struct NoopProgress;

    impl TransferProgress for NoopProgress {
        fn on_start(&self, _total: usize) {}
        fn on_transfer(
            &self,
            _index: usize,
            _total: usize,
            _source_path: &str,
            _dest_path: &str,
            _outcome: &TransferOutcome,
        ) {
        }
        fn on_interrupt(&self) {}
        fn on_finish(&self) {}
    }

    /// A press that lands while the first file is in transfer: the flag is set
    /// when that file ends, as the signal handler would set it.
    struct PressAfterFirstFile(Arc<AtomicBool>);

    impl TransferProgress for PressAfterFirstFile {
        fn on_start(&self, _total: usize) {}
        fn on_transfer(
            &self,
            _index: usize,
            _total: usize,
            _source_path: &str,
            _dest_path: &str,
            _outcome: &TransferOutcome,
        ) {
            self.0.store(true, Ordering::Relaxed);
        }
        fn on_interrupt(&self) {}
        fn on_finish(&self) {}
    }

    #[test]
    fn test_execute_apply_respects_interrupt_flag() {
        use std::io::Write;

        let conn = setup_test_db();
        let _root_id = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);

        // Create two real source files
        let src_dir = tempfile::tempdir().unwrap();
        let src1 = src_dir.path().join("a.jpg");
        let src2 = src_dir.path().join("b.jpg");
        std::fs::File::create(&src1)
            .unwrap()
            .write_all(b"data1")
            .unwrap();
        std::fs::File::create(&src2)
            .unwrap()
            .write_all(b"data2")
            .unwrap();

        let meta1 = std::fs::metadata(&src1).unwrap();
        let meta2 = std::fs::metadata(&src2).unwrap();
        #[cfg(unix)]
        let ((size1, mtime1), (size2, mtime2)) = {
            use std::os::unix::fs::MetadataExt;
            (
                (meta1.size() as i64, meta1.mtime()),
                (meta2.size() as i64, meta2.mtime()),
            )
        };
        #[cfg(not(unix))]
        let ((size1, mtime1), (size2, mtime2)) =
            { ((meta1.len() as i64, 0i64), (meta2.len() as i64, 0i64)) };
        let hash1 = compute_partial_hash(&src1, size1 as u64).unwrap();
        let hash2 = compute_partial_hash(&src2, size2 as u64).unwrap();

        let dest_dir = tempfile::tempdir().unwrap();

        let plan = ApplyPlan {
            transfers: vec![
                ApplyTransfer {
                    source_id: 1,
                    source_path: src1.display().to_string(),
                    dest_rel_path: ContainedPath::new("a.jpg").unwrap(),
                    archive_rel_path: ContainedPath::new("a.jpg").unwrap(),
                    object_id: Some(obj1),
                    partial_hash: hash1,
                    size: size1,
                    mtime: mtime1,
                    source_root_path: String::new(),
                    source_rel_path: String::new(),
                    hash: None,
                },
                ApplyTransfer {
                    source_id: 2,
                    source_path: src2.display().to_string(),
                    dest_rel_path: ContainedPath::new("b.jpg").unwrap(),
                    archive_rel_path: ContainedPath::new("b.jpg").unwrap(),
                    object_id: Some(obj2),
                    partial_hash: hash2,
                    size: size2,
                    mtime: mtime2,
                    source_root_path: String::new(),
                    source_rel_path: String::new(),
                    hash: None,
                },
            ],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        // The flag is set while the first transfer is in flight.
        let flag = Arc::new(AtomicBool::new(false));

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: dest_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(Arc::clone(&flag)),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };

        let result =
            execute_apply(&conn, &plan, &params, &PressAfterFirstFile(flag), None).unwrap();

        // The first transfer finishes, then the loop breaks
        assert!(result.interrupted);
        assert_eq!(result.remaining, 1); // 2 total - 1 completed = 1 remaining
        assert_eq!(result.copied, 1);
        // First file should exist, second should not
        assert!(dest_dir.path().join("a.jpg").exists());
        assert!(!dest_dir.path().join("b.jpg").exists());
    }

    #[test]
    fn test_execute_apply_interrupt_empty_plan() {
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let dest_dir = tempfile::tempdir().unwrap();

        let plan = ApplyPlan {
            transfers: vec![],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: dest_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(flag),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };

        let result = execute_apply(&conn, &plan, &params, &NoopProgress, None).unwrap();

        // No transfers, so not interrupted (loop never runs)
        assert!(!result.interrupted);
        assert_eq!(result.remaining, 0);
    }

    // =========================================================================
    // Apply receipt integration
    // =========================================================================

    fn make_decision_params(receipt_enabled: bool) -> DecisionParams {
        use crate::core::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
        DecisionParams {
            command: crate::core::domain::decision::DecisionCommand::Apply,
            scope: Vec::new(),
            command_line: "canon apply test.toml".to_string(),
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
            zone: ReadTimesOn::of_user(&UserZone::utc()),
        }
    }

    #[test]
    fn test_apply_receipt_written_on_completion() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 1);

        // Receipt should exist in .canon-ledger/
        let receipt_files: Vec<_> = std::fs::read_dir(archive_dir.path().join(".canon-ledger"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "toml").unwrap_or(false))
            .collect();
        assert_eq!(receipt_files.len(), 1, "Expected one .toml receipt file");

        let receipt_content = std::fs::read_to_string(receipt_files[0].path()).unwrap();
        assert!(receipt_content.contains("# Canon Decision Receipt"));
        assert!(receipt_content.contains("[meta]"));
        assert!(receipt_content.contains("[[items]]"));
        assert!(receipt_content.contains("source_rel_path = \"photo.jpg\""));
        assert!(receipt_content.contains("destination_rel_path = \"photo.jpg\""));
        assert!(receipt_content.contains("manifest = \"test.toml\""));
    }

    // =========================================================================
    // Extraction ledger recording
    // =========================================================================

    fn latest_decision_id(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT id FROM decisions ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// Build a single-transfer plan for one source root → one archive root,
    /// with real file bytes on disk (extraction rows need real sizes).
    fn single_transfer_plan(
        src_dir: &Path,
        rel_path: &str,
        archive_rel_path: &str,
        contents: &[u8],
    ) -> (ApplyPlan, i64, i64) {
        use std::io::Write;
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.display().to_string(),
                source_rel_path: rel_path.to_string(),
                dest_rel_path: ContainedPath::new(archive_rel_path).unwrap(),
                archive_rel_path: ContainedPath::new(archive_rel_path).unwrap(),
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
            resume_content_mismatches: vec![],
        };
        (plan, size, mtime)
    }

    /// Records every progress call in order, so a test can read what a run
    /// said rather than that it said something.
    #[derive(Default)]
    struct RecordingProgress {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl TransferProgress for RecordingProgress {
        fn on_start(&self, _total: usize) {}
        fn on_transfer(
            &self,
            _index: usize,
            _total: usize,
            _source_path: &str,
            _dest_path: &str,
            _outcome: &TransferOutcome,
        ) {
        }
        fn on_interrupt(&self) {}
        fn on_finish(&self) {}

        fn on_validation_start(&self, sweep: ValidationSweep, total: usize) {
            self.events
                .borrow_mut()
                .push(format!("start {sweep:?} {total}"));
        }
        fn on_validation_progress(&self, done: usize) {
            self.events.borrow_mut().push(format!("step {done}"));
        }
        fn on_validation_finish(&self) {
            self.events.borrow_mut().push("finish".to_string());
        }
    }

    /// The preflight reads every source before anything moves — minutes on a
    /// network volume. It reports each sweep it starts and each source it
    /// finishes, so a long wait is legible as work rather than as a stall.
    ///
    /// The two sweeps stay two: the readability pass refuses before any
    /// content is read, and a single merged count would have to hide the
    /// boundary that keeps that refusal cheap.
    #[test]
    fn validation_progress_is_reported_per_file() {
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

        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"hi");
        let (second, _, _) = single_transfer_plan(src_dir.path(), "b.jpg", "b.jpg", b"there");
        plan.transfers.extend(second.transfers);

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let progress = RecordingProgress::default();
        let result = execute_apply(&conn, &plan, &params, &progress, None).unwrap();
        assert_eq!(result.copied, 2);

        assert_eq!(
            progress.events.borrow().as_slice(),
            [
                "start Readability 2",
                "step 1",
                "step 2",
                "finish",
                "start LockAgreement 2",
                "step 1",
                "step 2",
                "finish",
            ]
        );
    }

    #[test]
    fn test_execute_apply_records_extraction_row_copy_mode() {
        let conn = setup_test_db();
        let src_dir = tempfile::tempdir().unwrap();
        let source_root = insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let (plan, size, _mtime) = single_transfer_plan(
            src_dir.path(),
            "2016/italy/a.jpg",
            "2016/Italy/a.jpg",
            b"hi",
        );

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 1);

        let decision_id = latest_decision_id(&conn);
        let rows = repo::decision::fetch_extractions_by_decisions(&conn, &[decision_id]).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.root_id, source_root);
        assert_eq!(row.root_path, src_dir.path().display().to_string());
        assert_eq!(row.rel_prefix, "2016/italy");
        assert_eq!(row.files, 1);
        assert_eq!(row.bytes, Some(size));
        assert_eq!(row.destination_root_id, Some(archive_root));
        assert_eq!(
            row.destination_path,
            format!("{}/2016/Italy", archive_dir.path().display())
        );
        assert_eq!(row.disposition, Some(OriginDisposition::Retained));
    }

    #[test]
    fn test_execute_apply_resume_registers_entries_already_at_the_destination() {
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

        // A resumed run finds the file already copied. Nothing is transferred;
        // the destination row is what the run still owes. Registration reads the
        // file standing at the destination, so the file stands there.
        let (mut plan, _size, _mtime) =
            single_transfer_plan(src_dir.path(), "2016/a.jpg", "2016/a.jpg", b"hi");
        std::fs::create_dir_all(archive_dir.path().join("2016")).unwrap();
        std::fs::write(archive_dir.path().join("2016/a.jpg"), b"hi").unwrap();
        plan.resume_already_there = std::mem::take(&mut plan.transfers);
        plan.resume_already_there_source_present = 1;
        plan.already_archived_count = 1;

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: true,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();

        assert_eq!(result.copied, 0);
        let failed: Vec<&str> = result.errors.iter().map(|e| e.error.as_str()).collect();
        assert!(failed.is_empty(), "registration failed: {failed:?}");

        let decision_id = latest_decision_id(&conn);
        let row = repo::source::fetch_by_path(&conn, archive_root, "2016/a.jpg")
            .unwrap()
            .expect("the already-there entry should be registered at the destination");
        assert_eq!(row.decision_id, Some(decision_id));
    }

    /// A resume over one entry, planned for real and then executed for real.
    /// The destination holds `bytes` (the lock's own, so the entry can be
    /// proven); `registered` puts a present row there for the lock's object
    /// with a decision, device, inode and `basis_rev` of its own; `origin`
    /// says whether the source file is still on disk.
    struct ResumeRun {
        conn: Connection,
        archive_root: i64,
        result: ApplyResult,
        decision_id: i64,
        dest: std::path::PathBuf,
        _dirs: (tempfile::TempDir, tempfile::TempDir),
    }

    fn resume_over(bytes: &[u8], registered: bool, origin: bool) -> ResumeRun {
        use crate::archive::domain::LockEntry;
        use crate::archive::ops::plan::{plan_apply, ApplyPlanParams};
        use crate::core::testing::insert_source_with_metadata;
        use crate::expr::Unmeasured;
        use sha2::{Digest, Sha256};

        let src_dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::write(&src_file, bytes).unwrap();
        let dest = archive_dir.path().join("photo.jpg");
        std::fs::write(&dest, bytes).unwrap();

        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        let object = insert_object(&conn, "locked", false);
        let source_id = insert_source_with_metadata(
            &conn,
            src_root,
            "photo.jpg",
            Some(object),
            bytes.len() as i64,
            1704067200,
        );
        if registered {
            let id = insert_source_with_metadata(
                &conn,
                archive_root,
                "photo.jpg",
                Some(object),
                bytes.len() as i64,
                1704067200,
            );
            conn.execute(
                "UPDATE sources SET decision_id = 77, device = 5, inode = 6, basis_rev = 3 WHERE id = ?",
                [id],
            )
            .unwrap();
        }
        if !origin {
            std::fs::remove_file(&src_file).unwrap();
        }

        let entry = LockEntry {
            id: source_id,
            root_id: src_root,
            path: src_file.display().to_string(),
            scope_rel_path: None,
            device: 0,
            inode: 0,
            size: bytes.len() as i64,
            mtime: 1704067200,
            partial_hash: "testhash".to_string(),
            object_id: Some(object),
            hash_type: Some("sha256".to_string()),
            hash_value: Some(format!("{:x}", Sha256::digest(bytes))),
        };
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = crate::expr::read_pattern(&conn, "{filename}")
            .unwrap()
            .unwrap();
        let root_paths: std::collections::HashMap<i64, String> = repo::root::fetch_all(&conn)
            .unwrap()
            .into_iter()
            .map(|r| (r.id, r.path))
            .collect();
        let base_dir = ContainedPath::default();
        let zone = lock_zone();
        let plan = plan_apply(
            &mut conn,
            &ApplyPlanParams {
                sources: &sources,
                pattern: &pattern,
                unmeasured: Unmeasured::NoScopeRecorded,
                pattern_needs_a_scope_it_lacks: false,
                root_paths: &root_paths,
                lock_zone: &zone,
                archive_root_id: archive_root,
                base_dir: &base_dir,
                resume: true,
                progress: None,
            },
        )
        .unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: true,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        let decision_id = latest_decision_id(&conn);
        ResumeRun {
            conn,
            archive_root,
            result,
            decision_id,
            dest,
            _dirs: (src_dir, archive_dir),
        }
    }

    /// An entry the index already records is counted and left alone: its row
    /// keeps the decision that put it there, and its device, inode and
    /// `basis_rev`. Re-registering it would stamp this run over an earlier,
    /// correct act.
    #[test]
    fn a_resume_leaves_a_registered_row_untouched() {
        let run = resume_over(b"hello world", true, true);
        assert_eq!(run.result.already_there, 1);
        assert_eq!(run.result.proven, 0);
        let row = repo::source::fetch_by_path(&run.conn, run.archive_root, "photo.jpg")
            .unwrap()
            .unwrap();
        assert_eq!(row.decision_id, Some(77));
        assert_eq!((row.device, row.inode), (5, 6));
        assert_eq!(row.basis_rev, 3);
        assert_ne!(Some(run.decision_id), row.decision_id);
    }

    /// An entry no row records, whose destination hashes to the lock's content,
    /// is registered by this run from the file itself: its real device and save
    /// time, not the lock's. The origin need not still stand — the file at the
    /// destination is what was shown to hold the content.
    #[test]
    fn a_resume_proves_and_registers_an_unregistered_destination() {
        for origin in [true, false] {
            let run = resume_over(b"hello world", false, origin);
            assert_eq!(run.result.proven, 1, "origin present: {origin}");
            assert_eq!(run.result.already_there, 1);
            assert!(run.result.errors.is_empty());
            let row = repo::source::fetch_by_path(&run.conn, run.archive_root, "photo.jpg")
                .unwrap()
                .unwrap();
            assert_eq!(row.decision_id, Some(run.decision_id));
            assert!(row.object_id.is_some());
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let meta = std::fs::metadata(&run.dest).unwrap();
                assert_eq!(row.device, meta.dev() as i64, "the real device");
                assert_eq!(row.inode, meta.ino() as i64);
                assert_eq!(row.mtime, meta.mtime(), "the file's own modification time");
            }
        }
    }

    #[test]
    fn test_execute_apply_records_extraction_row_move_mode_relocated() {
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

        let (plan, _size, _mtime) =
            single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"hello");

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Move,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert!(result.moved == 1 || result.renamed == 1);

        let decision_id = latest_decision_id(&conn);
        let rows = repo::decision::fetch_extractions_by_decisions(&conn, &[decision_id]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].disposition, Some(OriginDisposition::Relocated));
    }

    #[test]
    fn test_execute_apply_records_extraction_row_per_source_root() {
        let conn = setup_test_db();
        let src_dir_a = tempfile::tempdir().unwrap();
        let root_a = insert_root(&conn, src_dir_a.path().to_str().unwrap(), "source", false);
        let src_dir_b = tempfile::tempdir().unwrap();
        let root_b = insert_root(&conn, src_dir_b.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let (plan_a, _, _) = single_transfer_plan(src_dir_a.path(), "x/1.jpg", "out/1.jpg", b"a");
        let (plan_b, _, _) = single_transfer_plan(src_dir_b.path(), "y/2.jpg", "out/2.jpg", b"bb");
        let plan = ApplyPlan {
            transfers: plan_a
                .transfers
                .into_iter()
                .chain(plan_b.transfers)
                .collect(),
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 2);

        let decision_id = latest_decision_id(&conn);
        let rows = repo::decision::fetch_extractions_by_decisions(&conn, &[decision_id]).unwrap();
        assert_eq!(rows.len(), 2);
        let a = rows.iter().find(|r| r.root_id == root_a).unwrap();
        let b = rows.iter().find(|r| r.root_id == root_b).unwrap();
        assert_eq!(a.rel_prefix, "x");
        assert_eq!(a.files, 1);
        assert_eq!(b.rel_prefix, "y");
        assert_eq!(b.files, 1);
        // destination_path is decision-wide, shared across both rows.
        assert_eq!(a.destination_path, b.destination_path);
    }

    #[test]
    fn test_execute_apply_zero_completed_transfers_records_no_extraction_rows() {
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        // No transfers at all: the 0-item convention — nothing completed,
        // so no extraction row, even though recording is enabled.
        let plan = ApplyPlan {
            transfers: vec![],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 0);

        let decision_id = latest_decision_id(&conn);
        let rows = repo::decision::fetch_extractions_by_decisions(&conn, &[decision_id]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_execute_apply_records_mode_still_writes_extraction_rows() {
        // Records mode: receipts off, DB recording on — the extraction ledger
        // is gated on record_enabled, not receipt_enabled, so it still writes.
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"records");

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None, // Records mode never builds a receipt_ctx
        };
        let decision = make_decision_params(false); // receipt_enabled = false
        assert!(!decision.receipt_enabled);
        assert!(decision.record_enabled);
        execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();

        let decision_id = latest_decision_id(&conn);
        let rows = repo::decision::fetch_extractions_by_decisions(&conn, &[decision_id]).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "Records mode should still record extraction rows"
        );
    }

    #[test]
    fn test_execute_apply_no_decision_records_no_extraction_rows() {
        // No DecisionParams at all (the dry-run / disabled-recording shape at
        // this layer): no decision id exists to attribute rows to.
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"dryrun");

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };
        execute_apply(&conn, &plan, &params, &NoopProgress, None).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM decision_extractions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_apply_receipt_write_failure_surfaces_warning() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        // A regular file standing where the receipt's archive root should be:
        // create_dir_all under it fails, forcing receipt setup to fail.
        let blocker = archive_dir.path().join("blocker");
        std::fs::File::create(&blocker)
            .unwrap()
            .write_all(b"x")
            .unwrap();

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        // Placement points at a file, not a directory → receipt setup fails,
        // but the transfer (which uses base_dir) still succeeds.
        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: blocker.display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();

        // Transfer succeeded, but the receipt failure is surfaced — not swallowed.
        assert_eq!(result.copied, 1);
        assert!(
            !result.warnings.is_empty(),
            "a receipt setup failure should surface a warning"
        );
        assert!(
            result.warnings.iter().any(|w| w.contains("receipt")),
            "warning should mention the receipt: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_apply_no_receipt_when_disabled() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        // receipt_ctx is None — no receipt written
        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };

        let decision = make_decision_params(false);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 1);

        // No .canon-ledger directory should exist
        assert!(!archive_dir.path().join(".canon-ledger").exists());
    }

    #[test]
    fn test_apply_receipt_db_fields_populated() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();

        // Verify DB receipt fields are populated
        let (receipt_root_id, receipt_rel_path): (Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT receipt_root_id, receipt_rel_path FROM decisions ORDER BY id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(receipt_root_id, Some(archive_root));
        assert!(receipt_rel_path.is_some());
        let rel_path = receipt_rel_path.unwrap();
        assert!(
            rel_path.contains(".canon-ledger"),
            "rel_path should contain .canon-ledger: {rel_path}"
        );
        assert!(
            rel_path.ends_with("-apply.toml"),
            "rel_path should end with -apply.toml: {rel_path}"
        );
    }

    #[test]
    fn test_apply_receipt_alongside_layout() {
        use crate::core::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
        use std::io::Write;

        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        // Create base_dir inside archive
        let base_dir = archive_dir.path().join("Media/2024");
        std::fs::create_dir_all(&base_dir).unwrap();

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("Media/2024/photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: "Media/2024".to_string(),
        };

        let decision = DecisionParams {
            command: crate::core::domain::decision::DecisionCommand::Apply,
            scope: Vec::new(),
            command_line: "canon apply test.toml".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: LedgerConfig {
                recording: RecordingMode::Full,
                layout: ReceiptLayout::Alongside,
                root: None,
            },
            zone: ReadTimesOn::of_user(&UserZone::utc()),
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: base_dir.clone(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 1);

        // Alongside: receipt at {base_dir_rel}/.canon-ledger/{id}-apply.toml
        let ledger_dir = archive_dir.path().join("Media/2024/.canon-ledger");
        assert!(
            ledger_dir.exists(),
            ".canon-ledger should exist under base_dir"
        );
        let receipt_files: Vec<_> = std::fs::read_dir(&ledger_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "toml").unwrap_or(false))
            .collect();
        assert_eq!(
            receipt_files.len(),
            1,
            "Expected one receipt in alongside layout"
        );
    }

    /// The pre-transfer bail is one of three staleness printers, and the only
    /// one whose output a test can reach — the other two speak through
    /// `eprintln!`. It must list the sources it refuses over, because the
    /// remedy below the listing says "just these files" and needs a "these" to
    /// mean.
    ///
    /// What this pins is that the site prints the shared body rather than a
    /// second spelling of it: break the composer and this fails, inline a
    /// listing here and it fails too.
    #[test]
    fn the_pre_transfer_bail_prints_the_shared_staleness_body() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
            .unwrap();

        // The lock remembers a size the file no longer has: stale on disk,
        // which is what the batch sweep before the transfer loop refuses on.
        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                object_id: None,
                partial_hash: "deadbeef".to_string(),
                size: 999_999,
                mtime: 0,
                hash: None,
            }],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "trip.toml".to_string(),
            receipt_ctx: None,
        };

        let err = match execute_apply(&conn, &plan, &params, &NoopProgress, None) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a stale source must refuse the run before any transfer"),
        };

        let stale = vec![StaleSource {
            path: src_file.display().to_string(),
            reason: String::new(),
        }];
        for line in staleness_lines(&stale, "trip.toml") {
            if line.is_empty() {
                continue;
            }
            assert!(
                err.contains(line.trim_end()),
                "the bail carries the shared body's line {line:?}:\n{err}"
            );
        }
        assert!(
            err.contains(&src_file.display().to_string()),
            "and names the file it refused over: {err}"
        );
    }

    #[test]
    fn test_apply_no_receipt_when_all_transfers_error() {
        use std::io::Write;
        // Source passes staleness but dest already exists → copy error → no receipt items
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"image data")
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

        // Pre-create destination so copy_file fails with "already exists"
        std::fs::File::create(archive_dir.path().join("photo.jpg")).unwrap();

        let plan = ApplyPlan {
            transfers: vec![ApplyTransfer {
                source_id: 1,
                source_path: src_file.display().to_string(),
                source_root_path: src_dir.path().display().to_string(),
                source_rel_path: "photo.jpg".to_string(),
                dest_rel_path: ContainedPath::new("photo.jpg").unwrap(),
                archive_rel_path: ContainedPath::new("photo.jpg").unwrap(),
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
            resume_content_mismatches: vec![],
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(
            result.errors.len(),
            1,
            "Transfer should error on existing dest"
        );
        assert_eq!(result.copied, 0);

        // No receipt .toml should exist (no completed transfers → empty items → skipped)
        let ledger_dir = archive_dir.path().join(".canon-ledger");
        if ledger_dir.exists() {
            let receipt_toml_files: Vec<_> = std::fs::read_dir(&ledger_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "toml").unwrap_or(false))
                .collect();
            assert_eq!(
                receipt_toml_files.len(),
                0,
                "No receipt .toml when all transfers error"
            );
        }

        // The row must not claim what the disk does not hold. The counts are
        // the claim's own explanation: nothing completed, so nothing was
        // receipted.
        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.receipt_root_id, None);
        assert_eq!(row.receipt_rel_path, None);
        assert_eq!(row.count_completed, Some(0));
        assert_eq!(row.count_failed, Some(1));
    }

    /// A decision's receipt columns are written at `start()`, before the file
    /// they name exists. An apply that completes no transfer writes no receipt,
    /// so the claim must be retracted: the trail reads those columns as the
    /// index over what is on disk, and a pointer into nothing is worse than no
    /// pointer at all.
    #[test]
    fn a_zero_transfer_apply_claims_no_receipt() {
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");
        // Occupy the destination so the one transfer fails.
        std::fs::File::create(archive_dir.path().join("photo.jpg")).unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 0);

        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.receipt_root_id, None, "claim must be retracted");
        assert_eq!(row.receipt_rel_path, None, "claim must be retracted");
        assert_eq!(row.count_attempted, Some(1));
        assert_eq!(row.count_completed, Some(0));
    }

    /// Apply registers its receipt path before its pre-transfer checks run, and
    /// those checks can refuse the whole run. The row must not go on citing a
    /// receipt that will never be written. The staleness refusal is the routine
    /// way a user meets this: its own message tells them to rescan and refresh.
    #[test]
    fn an_abandoned_apply_claims_no_receipt() {
        use std::io::Write;
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");
        // Change the file behind the plan, so the lock-agreement sweep refuses.
        std::fs::File::create(src_dir.path().join("photo.jpg"))
            .unwrap()
            .write_all(b"different data entirely")
            .unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let outcome = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision));
        let err = match outcome {
            Err(e) => e,
            Ok(_) => panic!("a stale source must refuse the run"),
        };
        assert!(err.to_string().contains("have changed"), "{err}");

        // Non-vacuity: registering the claim is what creates the ledger
        // directory, so its presence witnesses that there was a claim to
        // retract rather than nothing to find.
        assert!(
            archive_dir.path().join(".canon-ledger").exists(),
            "the run must have registered a claim for this test to mean anything"
        );

        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.receipt_root_id, None, "claim must not outlive the run");
        assert_eq!(row.receipt_rel_path, None, "claim must not outlive the run");
        // The refusal *is* the decision's last act, so both of the row's
        // prospective claims settle in it. `started` is reserved for a run
        // whose outcome was never written down — a kill, a crash, a power cut,
        // or a settlement the database refused — and a deliberate no must not
        // read as one of those.
        assert_eq!(row.status, "refused");
    }

    /// The other refusal, which no test reached before: a source that cannot be
    /// opened. Both pre-transfer refusals must settle the claim, and covering
    /// only the one that is easy to stage would leave the other free to rot.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_source_also_settles_the_claim() {
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");
        // A symlink pointing at itself: opening it fails with a loop error,
        // which is neither "not found" nor a permission problem — the arm that
        // chmod cannot reach when the tests run as root.
        let path = src_dir.path().join("photo.jpg");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&path, &path).unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let outcome = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision));
        let err = match outcome {
            Err(e) => e,
            Ok(_) => panic!("an unreadable source must refuse the run"),
        };
        assert!(err.to_string().contains("not readable"), "{err}");

        assert!(archive_dir.path().join(".canon-ledger").exists());
        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.receipt_root_id, None, "claim must not outlive the run");
        assert_eq!(row.receipt_rel_path, None, "claim must not outlive the run");
    }

    // =========================================================================
    // The status conjugation: a run that refuses settles its own row
    // =========================================================================

    /// Stage the staleness refusal — a plan whose one source changed under it,
    /// driven to the refusal — and hand back the world it left behind. The
    /// temp dirs are returned because dropping them deletes the ledger the
    /// non-vacuity witness reads.
    fn refused_by_staleness() -> (Connection, tempfile::TempDir, tempfile::TempDir) {
        use std::io::Write;
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");
        std::fs::File::create(src_dir.path().join("photo.jpg"))
            .unwrap()
            .write_all(b"different data entirely")
            .unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let outcome = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision));
        let err = match outcome {
            Err(e) => e,
            Ok(_) => panic!("a stale source must refuse the run"),
        };
        assert!(err.to_string().contains("have changed"), "{err}");

        // Non-vacuity: registering the receipt claim is what creates the ledger
        // directory, so its presence witnesses that a decision row was opened
        // and had something to settle.
        assert!(
            archive_dir.path().join(".canon-ledger").exists(),
            "the run must have opened a decision for these tests to mean anything"
        );
        (conn, archive_dir, src_dir)
    }

    /// `started` is a claim registered before the run finishes, and the
    /// provenance model reads a surviving `started` row as a run that never
    /// reached a last act — a kill, a crash, a power cut. A refusal *is* a last
    /// act: the run was told no and stopped. Leaving the row `started` puts a
    /// deliberate no in the same bucket as a power cut and sends the user
    /// looking for damage that was never done.
    #[test]
    fn a_refused_apply_is_never_left_started() {
        let (conn, _archive_dir, _src_dir) = refused_by_staleness();

        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .expect("decision row");
        assert_eq!(row.status, "refused");
        assert_eq!(
            row.summary.as_deref(),
            Some("Refused: 1 sources have changed since the manifest was generated"),
            "the durable record says which refusal this was"
        );
    }

    /// The other pre-transfer refusal, on its own pin. Apply has two, and a fix
    /// applied to whichever one is easy to stage leaves the other free to rot —
    /// which is exactly the shape the receipt half of this law was already
    /// caught in.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_source_refusal_is_never_left_started() {
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");
        // A symlink pointing at itself: opening it fails with a loop error,
        // which is neither "not found" nor a permission problem — the arm that
        // chmod cannot reach when the tests run as root.
        let path = src_dir.path().join("photo.jpg");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&path, &path).unwrap();

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let outcome = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision));
        let err = match outcome {
            Err(e) => e,
            Ok(_) => panic!("an unreadable source must refuse the run"),
        };
        assert!(err.to_string().contains("not readable"), "{err}");

        assert!(archive_dir.path().join(".canon-ledger").exists());
        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .expect("decision row");
        assert_eq!(row.status, "refused");
        assert_eq!(
            row.summary.as_deref(),
            Some("Refused: 1 sources are not readable"),
        );
    }

    /// A refusal attempted nothing, and the row must not imply otherwise. Zero
    /// attempted is a different claim from no attempt: `trail show` reads the
    /// counts to explain a missing receipt, and a run that tried one file and
    /// completed none is a failure, not a refusal.
    #[test]
    fn a_refusal_records_no_counts() {
        let (conn, _archive_dir, _src_dir) = refused_by_staleness();

        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .expect("decision row");
        assert_eq!(row.count_attempted, None);
        assert_eq!(row.count_completed, None);
        assert_eq!(row.count_failed, None);
        assert_eq!(row.count_skipped, None);
    }

    /// The two prospective claims settle together in the one act. Composing the
    /// refusal over `complete` is what makes half-settling unreachable rather
    /// than merely unintended: settling one and not the other leaves the row
    /// either citing a receipt that will never exist, or claiming a run that
    /// never ended.
    #[test]
    fn a_refusal_also_retracts_the_receipt_claim() {
        let (conn, _archive_dir, _src_dir) = refused_by_staleness();

        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .expect("decision row");
        assert_eq!(row.status, "refused");
        assert_eq!(row.receipt_root_id, None);
        assert_eq!(row.receipt_rel_path, None);
    }

    /// The positive half of the same law: a receipt that was written stays
    /// claimed, and the file the row names is on disk.
    #[test]
    fn a_completed_apply_keeps_its_claim() {
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "photo.jpg", "photo.jpg", b"data");

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        assert_eq!(result.copied, 1);

        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        let rel_path = row.receipt_rel_path.expect("claim kept");
        assert_eq!(row.receipt_root_id, Some(archive_root));
        assert!(
            archive_dir.path().join(&rel_path).exists(),
            "the row names a file that is not there: {rel_path}"
        );
    }

    #[test]
    fn test_apply_receipt_interrupted_contains_only_completed() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        let src_dir = tempfile::tempdir().unwrap();
        let src1 = src_dir.path().join("a.jpg");
        let src2 = src_dir.path().join("b.jpg");
        std::fs::File::create(&src1)
            .unwrap()
            .write_all(b"data1")
            .unwrap();
        std::fs::File::create(&src2)
            .unwrap()
            .write_all(b"data2")
            .unwrap();

        let meta1 = std::fs::metadata(&src1).unwrap();
        let meta2 = std::fs::metadata(&src2).unwrap();
        #[cfg(unix)]
        let (size1, mtime1) = {
            use std::os::unix::fs::MetadataExt;
            (meta1.size() as i64, meta1.mtime())
        };
        #[cfg(not(unix))]
        let (size1, mtime1) = (meta1.len() as i64, 0i64);
        #[cfg(unix)]
        let (size2, mtime2) = {
            use std::os::unix::fs::MetadataExt;
            (meta2.size() as i64, meta2.mtime())
        };
        #[cfg(not(unix))]
        let (size2, mtime2) = (meta2.len() as i64, 0i64);
        let hash1 = compute_partial_hash(&src1, size1 as u64).unwrap();
        let hash2 = compute_partial_hash(&src2, size2 as u64).unwrap();
        let obj1 = insert_object(&conn, "hash_a", false);
        let obj2 = insert_object(&conn, "hash_b", false);

        let plan = ApplyPlan {
            transfers: vec![
                ApplyTransfer {
                    source_id: 1,
                    source_path: src1.display().to_string(),
                    source_root_path: src_dir.path().display().to_string(),
                    source_rel_path: "a.jpg".to_string(),
                    dest_rel_path: ContainedPath::new("a.jpg").unwrap(),
                    archive_rel_path: ContainedPath::new("a.jpg").unwrap(),
                    object_id: Some(obj1),
                    partial_hash: hash1,
                    size: size1,
                    mtime: mtime1,
                    hash: None,
                },
                ApplyTransfer {
                    source_id: 2,
                    source_path: src2.display().to_string(),
                    source_root_path: src_dir.path().display().to_string(),
                    source_rel_path: "b.jpg".to_string(),
                    dest_rel_path: ContainedPath::new("b.jpg").unwrap(),
                    archive_rel_path: ContainedPath::new("b.jpg").unwrap(),
                    object_id: Some(obj2),
                    partial_hash: hash2,
                    size: size2,
                    mtime: mtime2,
                    hash: None,
                },
            ],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
            resume_content_mismatches: vec![],
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        // The flag is set while the first transfer is in flight — it completes,
        // then the loop breaks
        let flag = Arc::new(AtomicBool::new(false));

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(Arc::clone(&flag)),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        let result = execute_apply(
            &conn,
            &plan,
            &params,
            &PressAfterFirstFile(flag),
            Some(&decision),
        )
        .unwrap();
        assert!(result.interrupted);
        assert_eq!(result.copied, 1); // Only first transfer completed

        // Receipt should exist with exactly 1 item (the completed transfer)
        let ledger_dir = archive_dir.path().join(".canon-ledger");
        assert!(ledger_dir.exists());
        let receipt_files: Vec<_> = std::fs::read_dir(&ledger_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "toml").unwrap_or(false))
            .collect();
        assert_eq!(
            receipt_files.len(),
            1,
            "Receipt should be finalized even on interrupt"
        );

        let content = std::fs::read_to_string(receipt_files[0].path()).unwrap();
        // Should contain exactly one [[items]] section
        let items_count = content.matches("[[items]]").count();
        assert_eq!(
            items_count, 1,
            "Interrupted receipt should contain only completed items"
        );
        assert!(
            content.contains("a.jpg"),
            "Should contain the completed transfer"
        );
        assert!(
            !content.contains("b.jpg"),
            "Should NOT contain the interrupted transfer"
        );
    }

    /// The retraction governs claim-versus-existence, not how a run ended. An
    /// interrupted apply that completed transfers has a receipt naming them, so
    /// its claim stands.
    #[test]
    fn an_interrupted_apply_with_completed_transfers_keeps_its_receipt() {
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

        let (plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"data");

        let flag = Arc::new(AtomicBool::new(false));
        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            // Set when the first transfer ends: the loop stops after it.
            interrupt_flag: Some(Arc::clone(&flag)),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(
            &conn,
            &plan,
            &params,
            &PressAfterFirstFile(flag),
            Some(&decision),
        )
        .unwrap();
        assert_eq!(result.copied, 1);
        assert!(result.interrupted);

        let decision_id = latest_decision_id(&conn);
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.receipt_root_id, Some(archive_root));
        let rel_path = row.receipt_rel_path.expect("claim kept on interrupt");
        assert!(archive_dir.path().join(&rel_path).exists());
    }

    #[test]
    fn test_rename_transfer_sets_decision_id() {
        use std::io::Write;
        let conn = setup_test_db();
        let src_root = insert_root(&conn, "/photos", "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );

        // Create a source file inside the archive dir (rename = same filesystem)
        let src_file = archive_dir.path().join("original.jpg");
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
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

        // Insert source record in DB so relocate_source has something to update
        let source_id = {
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
                 basis_rev, scanned_at, last_seen_at, present, excluded)
                 VALUES (?, 'original.jpg', 0, 0, ?, ?, ?, 0, 0, 0, 1, 0)",
                rusqlite::params![src_root, size, mtime, hash],
            )
            .unwrap();
            conn.last_insert_rowid()
        };

        let dest_dir = archive_dir.path().join("dest");
        std::fs::create_dir_all(&dest_dir).unwrap();

        let transfer = ApplyTransfer {
            source_id,
            source_path: src_file.display().to_string(),
            source_root_path: archive_dir.path().display().to_string(),
            source_rel_path: "original.jpg".to_string(),
            dest_rel_path: ContainedPath::new("renamed.jpg").unwrap(),
            archive_rel_path: ContainedPath::new("dest/renamed.jpg").unwrap(),
            object_id: None,
            partial_hash: hash,
            size,
            mtime,
            hash: None,
        };

        let (outcome, _prev) = execute_single_transfer(
            &transfer,
            &dest_dir,
            TransferMode::Rename,
            &conn,
            archive_root,
            Some(42),
            &lock_zone(),
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::Renamed));

        // Verify decision_id was set on the relocated source
        let stored_decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                rusqlite::params![source_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stored_decision_id,
            Some(42),
            "Rename should set decision_id"
        );
    }

    // =========================================================================
    // A move onto a path that already holds a row
    // =========================================================================

    /// What stands in the index at the destination before the run.
    #[derive(Clone, Copy)]
    enum DestRow {
        /// A row the index holds as deleted (`present = 0`).
        Tombstone,
        /// A row still marked present, carrying a different object.
        StalePresent,
    }

    struct OccupiedRun {
        conn: Connection,
        result: ApplyResult,
        archive_dir: tempfile::TempDir,
        _src_dir: tempfile::TempDir,
        archive_root: i64,
        origin_id: i64,
        object_id: i64,
    }

    /// Run one real transfer of `a.jpg` into `dest/a.jpg` in `mode`, with a row already
    /// in the index at the destination path and no file there.
    fn run_onto_a_row(mode: TransferMode, row: DestRow, resume: bool) -> OccupiedRun {
        let conn = setup_test_db();
        let src_dir = tempfile::tempdir().unwrap();
        let src_root = insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        let object_id = insert_object(&conn, "aaaa", false);
        let other_object = insert_object(&conn, "bbbb", false);

        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "dest/a.jpg", b"data");
        let origin_id = crate::core::testing::insert_source(&conn, src_root, "a.jpg", None);
        plan.transfers[0].source_id = origin_id;
        plan.transfers[0].object_id = Some(object_id);

        let present = matches!(row, DestRow::StalePresent) as i64;
        conn.execute(
            "UPDATE sources SET present = ? WHERE id = ?",
            rusqlite::params![
                present,
                crate::core::testing::insert_source(
                    &conn,
                    archive_root,
                    "dest/a.jpg",
                    Some(other_object)
                )
            ],
        )
        .unwrap();
        let standing: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = 'dest/a.jpg' AND present = ?",
                rusqlite::params![archive_root, present],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(standing, 1, "the row the scenario needs is in place");

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: mode,
            resume,
            interrupt_flag: None,
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        OccupiedRun {
            conn,
            result,
            archive_dir,
            _src_dir: src_dir,
            archive_root,
            origin_id,
            object_id,
        }
    }

    /// The index agrees with the disk after a move onto a path that held a row.
    fn assert_move_recorded(run: &OccupiedRun) {
        assert!(
            run.result.errors.is_empty(),
            "a move onto a held path must not error"
        );
        assert!(run.archive_dir.path().join("dest/a.jpg").exists());
        let decision_id = latest_decision_id(&run.conn);

        let (present, object, stamp): (i64, Option<i64>, Option<i64>) = run
            .conn
            .query_row(
                "SELECT present, object_id, decision_id FROM sources
                 WHERE root_id = ? AND rel_path = 'dest/a.jpg'",
                rusqlite::params![run.archive_root],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(present, 1, "the destination row is present");
        assert_eq!(object, Some(run.object_id), "with this content");
        assert_eq!(stamp, Some(decision_id), "and this decision");

        let (origin_present, origin_stamp): (i64, Option<i64>) = run
            .conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                rusqlite::params![run.origin_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(origin_present, 0, "the origin row is absent");
        assert_eq!(origin_stamp, Some(decision_id), "stamped by this decision");

        let extractions: i64 = run
            .conn
            .query_row(
                "SELECT COUNT(*) FROM decision_extractions WHERE decision_id = ?",
                rusqlite::params![decision_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(extractions, 1, "the extraction row is recorded");

        let row = repo::decision::fetch_by_id(&run.conn, decision_id)
            .unwrap()
            .expect("decision row");
        let receipt = std::fs::read_to_string(
            run.archive_dir
                .path()
                .join(row.receipt_rel_path.expect("receipt written")),
        )
        .unwrap();
        assert!(receipt.contains("dest/a.jpg"), "the receipt lists the file");
    }

    #[test]
    fn a_move_onto_a_deleted_path_succeeds() {
        let run = run_onto_a_row(TransferMode::Move, DestRow::Tombstone, false);
        assert_move_recorded(&run);
    }

    #[test]
    fn a_rename_onto_a_deleted_path_succeeds() {
        let run = run_onto_a_row(TransferMode::Rename, DestRow::Tombstone, false);
        assert_move_recorded(&run);
    }

    #[test]
    fn a_move_over_a_stale_present_row_succeeds() {
        // The state `--resume --move` meets: a present row carrying another object, its
        // file deleted by hand, and nothing in the way of the move.
        let run = run_onto_a_row(TransferMode::Move, DestRow::StalePresent, true);
        assert_move_recorded(&run);
    }

    #[test]
    fn a_rename_over_a_stale_present_row_succeeds() {
        let run = run_onto_a_row(TransferMode::Rename, DestRow::StalePresent, true);
        assert_move_recorded(&run);
    }

    /// A plain copy onto a path whose row was deleted records the destination under
    /// this decision and leaves the origin's row alone, as the move and rename cases
    /// beside it record theirs.
    #[test]
    fn a_copy_onto_a_deleted_path_succeeds() {
        let run = run_onto_a_row(TransferMode::Copy, DestRow::Tombstone, false);
        assert!(
            run.result.errors.is_empty(),
            "a copy onto a held path must not error"
        );
        assert!(run.archive_dir.path().join("dest/a.jpg").exists());
        let decision_id = latest_decision_id(&run.conn);

        let (present, object, stamp): (i64, Option<i64>, Option<i64>) = run
            .conn
            .query_row(
                "SELECT present, object_id, decision_id FROM sources
                 WHERE root_id = ? AND rel_path = 'dest/a.jpg'",
                rusqlite::params![run.archive_root],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(present, 1);
        assert_eq!(object, Some(run.object_id));
        assert_eq!(stamp, Some(decision_id));

        let origin_present: i64 = run
            .conn
            .query_row(
                "SELECT present FROM sources WHERE id = ?",
                rusqlite::params![run.origin_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(origin_present, 1, "a copy leaves the origin row present");
    }

    #[test]
    fn a_relocation_with_no_row_at_the_destination_keeps_its_one_row() {
        // The allowed case beside the three above: the ordinary same-device move
        // re-points the origin's own row, and no second row appears.
        let conn = setup_test_db();
        let src_dir = tempfile::tempdir().unwrap();
        let src_root = insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "dest/a.jpg", b"data");
        let origin_id = crate::core::testing::insert_source(&conn, src_root, "a.jpg", None);
        plan.transfers[0].source_id = origin_id;
        let dest_dir = archive_dir.path().join("dest");
        std::fs::create_dir_all(&dest_dir).unwrap();

        let (outcome, _) = execute_single_transfer(
            &plan.transfers[0],
            archive_dir.path(),
            TransferMode::Move,
            &conn,
            archive_root,
            Some(7),
            &lock_zone(),
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::Renamed));
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM sources", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
        let (root, rel): (i64, String) = conn
            .query_row(
                "SELECT root_id, rel_path FROM sources WHERE id = ?",
                rusqlite::params![origin_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((root, rel.as_str()), (archive_root, "dest/a.jpg"));
    }

    #[test]
    fn a_failed_index_write_after_a_move_says_where_the_file_is() {
        let conn = setup_test_db();
        let src_dir = tempfile::tempdir().unwrap();
        let src_root = insert_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_dir = tempfile::tempdir().unwrap();
        let archive_root = insert_root(
            &conn,
            archive_dir.path().to_str().unwrap(),
            "archive",
            false,
        );
        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "dest/a.jpg", b"data");
        let origin_id = crate::core::testing::insert_source(&conn, src_root, "a.jpg", None);
        plan.transfers[0].source_id = origin_id;
        std::fs::create_dir_all(archive_dir.path().join("dest")).unwrap();

        // The row the trigger refuses to update exists before it is planted.
        let matching: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE id = ?",
                [origin_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(matching, 1);
        conn.execute_batch(
            "CREATE TRIGGER refuse_relocation BEFORE UPDATE ON sources
             BEGIN SELECT RAISE(ABORT, 'database is locked'); END;",
        )
        .unwrap();

        let err = execute_single_transfer(
            &plan.transfers[0],
            archive_dir.path(),
            TransferMode::Move,
            &conn,
            archive_root,
            Some(7),
            &lock_zone(),
        )
        .unwrap_err();

        let said = format!("{err:#}");
        assert!(archive_dir.path().join("dest/a.jpg").exists());
        assert!(said.contains("moved to"), "{said}");
        assert!(said.contains("dest/a.jpg"), "{said}");
        assert!(said.contains("the index was not updated"), "{said}");
        assert!(said.contains("database is locked"), "{said}");
        assert!(said.contains("--resume"), "{said}");
    }

    // =========================================================================
    // Ctrl-C
    // =========================================================================

    /// The one test in this binary that installs the handler and raises a real
    /// SIGINT: the first press sets the flag and the process survives it. It
    /// raises exactly once — a second press is the default action and would end
    /// the test run — and no other test in this binary installs a handler
    /// (`ApplyOptions::handle_interrupts` is false for the interface tests that call `run`).
    #[test]
    fn a_first_interrupt_sets_the_flag_and_a_process_survives_it() {
        let flag = setup_interrupt_flag().unwrap();
        assert!(!flag.load(Ordering::Relaxed), "nothing has been pressed");

        signal_hook::low_level::raise(signal_hook::consts::SIGINT).unwrap();

        assert!(
            flag.load(Ordering::Relaxed),
            "the first press must set the flag"
        );
    }

    /// The signal test above is the only test in this binary that installs a handler
    /// because `setup_interrupt_flag` has exactly two callers: the interface's `run`
    /// (behind `ApplyOptions::handle_interrupts`) and that test. A third caller, in a
    /// test or not, could set the signal test's neighbours' flags with its real press.
    /// The list is exact in both directions, so a caller added or removed fails here.
    ///
    /// **Blind spots, named:** it counts the spelling of a direct call. A call through an
    /// alias (`use … as`) or a function pointer, and a handler registered by calling the
    /// signal crate directly, pass unseen.
    #[test]
    fn only_the_interface_and_the_signal_test_install_the_handler() {
        fn collect(dir: &Path, found: &mut std::collections::BTreeMap<String, usize>, root: &Path) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    collect(&path, found, root);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    // Built in two parts so this file does not count itself.
                    let call = concat!("setup_interrupt_flag", "()");
                    let definition = concat!("fn setup_interrupt_flag", "()");
                    let text = std::fs::read_to_string(&path).unwrap();
                    let callers = text.matches(call).count() - text.matches(definition).count();
                    if callers > 0 {
                        let rel = path.strip_prefix(root).unwrap().display().to_string();
                        found.insert(rel, callers);
                    }
                }
            }
        }

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = std::collections::BTreeMap::new();
        collect(&root, &mut found, &root);
        let expected: std::collections::BTreeMap<String, usize> = [
            ("archive/cli/apply.rs".to_string(), 1),
            ("archive/ops/execute.rs".to_string(), 1),
        ]
        .into();
        assert_eq!(found, expected);
    }

    /// Two real files, a flag already set (a press during the checks), and a
    /// decision recorded: the run ends before the first transfer.
    fn run_pressed_during_the_checks() -> (
        Connection,
        ApplyResult,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
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
        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"aaaa");
        let (second, _, _) = single_transfer_plan(src_dir.path(), "b.jpg", "b.jpg", b"bbbb");
        plan.transfers.extend(second.transfers);

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Move,
            resume: false,
            interrupt_flag: Some(Arc::new(AtomicBool::new(true))),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        (conn, result, src_dir, archive_dir)
    }

    #[test]
    fn an_interrupt_during_the_checks_moves_nothing() {
        let (_conn, result, src_dir, archive_dir) = run_pressed_during_the_checks();

        assert!(result.interrupted);
        assert_eq!(result.remaining, 2, "both transfers are still to do");
        assert_eq!(result.copied + result.renamed + result.moved, 0);
        assert!(result.errors.is_empty());
        assert!(src_dir.path().join("a.jpg").exists());
        assert!(src_dir.path().join("b.jpg").exists());
        assert!(!archive_dir.path().join("a.jpg").exists());
        assert!(!archive_dir.path().join("b.jpg").exists());
        assert!(
            result.summary.contains("Interrupted — 2 files remaining."),
            "{}",
            result.summary
        );
    }

    #[test]
    fn an_interrupt_during_the_checks_settles_the_row_interrupted_with_no_receipt() {
        let (conn, _result, _src, _archive) = run_pressed_during_the_checks();

        let decision_id = latest_decision_id(&conn);
        let status: String = conn
            .query_row(
                "SELECT status FROM decisions WHERE id = ?",
                [decision_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "interrupted");
        let row = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .expect("decision row");
        assert_eq!(row.count_completed, Some(0));
        assert_eq!(row.receipt_rel_path, None, "no receipt, so no claim");
        assert_eq!(row.receipt_root_id, None);
    }

    /// A resume holding one pending transfer and one entry it proved was already
    /// at its destination, run with the flag as given.
    fn resume_with_a_proven_entry(
        pressed: bool,
    ) -> (Connection, ApplyResult, i64, tempfile::TempDir) {
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
        let (mut plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"aaaa");
        let (mut proven, _, _) = single_transfer_plan(src_dir.path(), "b.jpg", "b.jpg", b"bbbb");
        std::fs::write(archive_dir.path().join("b.jpg"), b"bbbb").unwrap();
        plan.resume_already_there = std::mem::take(&mut proven.transfers);
        plan.resume_already_there_source_present = 1;
        plan.already_archived_count = 1;

        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: true,
            interrupt_flag: Some(Arc::new(AtomicBool::new(pressed))),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(ReceiptPlacement::Targeted {
                archive_root_id: archive_root,
                archive_root_path: archive_dir.path().display().to_string(),
                base_dir_rel: String::new(),
            }),
        };
        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
        (conn, result, archive_root, archive_dir)
    }

    /// A press before the transfer loop records nothing, the entries a resume
    /// proved included: a run that reached the loop finishes its record, and a
    /// run that did not makes none. The allowed twin is the same state with no
    /// press: the transfer runs and the proven entry is recorded beside it.
    #[test]
    fn a_press_before_the_loop_records_no_proven_entry() {
        let (conn, result, archive_root, _dir) = resume_with_a_proven_entry(true);
        assert!(result.interrupted);
        assert_eq!(result.proven, 0);
        assert!(repo::source::fetch_by_path(&conn, archive_root, "b.jpg")
            .unwrap()
            .is_none());
        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .unwrap();
        assert_eq!(row.count_attempted, Some(1), "only the pending transfer");
        assert_eq!(row.count_completed, Some(0));
        assert_eq!(row.receipt_rel_path, None, "no receipt, so no claim");

        let (conn, result, archive_root, _dir) = resume_with_a_proven_entry(false);
        assert!(!result.interrupted);
        assert_eq!(result.copied, 1);
        assert_eq!(result.proven, 1);
        assert!(repo::source::fetch_by_path(&conn, archive_root, "b.jpg")
            .unwrap()
            .is_some());
        let row = repo::decision::fetch_by_id(&conn, latest_decision_id(&conn))
            .unwrap()
            .unwrap();
        assert_eq!(row.count_completed, Some(2));
        assert!(row.receipt_rel_path.is_some());
    }

    #[test]
    fn a_run_nobody_interrupts_still_transfers_everything() {
        // The allowed case beside the two refusals above, in the state the
        // production caller is in: a handler's flag, never pressed.
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
        let (plan, _, _) = single_transfer_plan(src_dir.path(), "a.jpg", "a.jpg", b"aaaa");
        let params = ApplyExecuteParams {
            lock_zone: lock_zone(),
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(Arc::new(AtomicBool::new(false))),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: None,
        };

        let result = execute_apply(&conn, &plan, &params, &NoopProgress, None).unwrap();

        assert!(!result.interrupted);
        assert_eq!(result.copied, 1);
        assert!(archive_dir.path().join("a.jpg").exists());
    }
}
