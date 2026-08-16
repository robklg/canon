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
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use crate::core::domain::extraction::{build_extraction_rows, ExtractionItem, OriginDisposition};
use crate::domain::decision::DecisionStatus;
use crate::domain::format::first_chars;
use crate::domain::source::NewSource;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::ops::fs::{
    compute_partial_hash, copy_file, ensure_parent_dir, move_file, rename_file, MoveOutcome,
};
use crate::ops::receipt::{ApplyReceipt, ApplyReceiptItem, ReceiptKind, ReceiptPlacement};
use crate::repo::{self, Connection};

use super::plan::{ApplyPlan, ApplyTransfer, StaleSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV
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
    /// Called when an interrupt is detected after the current transfer completes.
    fn on_interrupt(&self);
    /// Called once after the transfer loop ends.
    fn on_finish(&self);
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
    /// Interrupt flag — set to true to stop after current transfer.
    /// If None, signal handling is set up automatically.
    pub interrupt_flag: Option<Arc<AtomicBool>>,
    /// Sources skipped by --root filter (for summary).
    pub skipped_by_filter: usize,
    /// Manifest display path (for summary and decision record).
    pub manifest_display: String,
    /// Receipt placement for targeted (apply) receipts. None if receipts are disabled.
    pub receipt_ctx: Option<ReceiptPlacement>,
}

/// Result of executing an apply operation.
pub struct ApplyResult {
    pub copied: u64,
    pub renamed: u64,
    pub moved: u64,
    pub skipped_missing: u64,
    pub skipped_stale: Vec<StaleSource>,
    pub errors: Vec<TransferError>,
    /// Resume mode: count of entries already at destination (registered in DB during execute).
    pub already_there: u64,
    /// Resume mode: count of already-there entries where source file still exists.
    pub already_there_source_present: u64,
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

/// Execute file transfers from a computed apply plan.
///
/// Performs: source readability checks, resume disk classification (if resume mode),
/// batch staleness validation, and the transfer loop. Each transfer is independent
/// (no transaction wrapping) — the operation is idempotent and resume-safe.
///
/// Does NOT handle: manifest parsing, confirmation prompts, dry-run display,
/// output formatting. These are interface concerns.
/// Set up two-tier Ctrl+C handling for the transfer loop.
/// Returns an Arc<AtomicBool> that becomes true on first SIGINT.
/// Second SIGINT gets default OS termination.
fn setup_interrupt_flag() -> Result<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register_conditional_default(
        signal_hook::consts::SIGINT,
        Arc::clone(&flag),
    )?;
    Ok(flag)
}

pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
    decision: Option<&DecisionParams>,
) -> Result<ApplyResult> {
    let mut recorder =
        decision.map(|d| DecisionRecorder::start(conn, d, params.receipt_ctx.as_ref()));

    let interrupt_flag = match &params.interrupt_flag {
        Some(flag) => Arc::clone(flag),
        None => setup_interrupt_flag()?,
    };

    let mut result = ApplyResult {
        copied: 0,
        renamed: 0,
        moved: 0,
        skipped_missing: 0,
        skipped_stale: Vec::new(),
        errors: Vec::new(),
        already_there: plan.already_archived_count as u64,
        already_there_source_present: plan.resume_already_there_source_present as u64,
        interrupted: false,
        remaining: 0,
        summary: String::new(),
        warnings: Vec::new(),
    };

    // --- Source readability pre-check ---
    let mut unreadable: Vec<(String, String)> = Vec::new();
    for transfer in &plan.transfers {
        match File::open(&transfer.source_path) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
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
    }
    if !unreadable.is_empty() {
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
        let stale = validate_transfers_disk(&transfers_to_execute);
        if !stale.is_empty() {
            bail!(
                "{} sources have changed since manifest was generated. \
                 Run `canon scan` then `cluster refresh` to regenerate the lock file. \
                 First: {} ({})",
                stale.len(),
                stale[0].path,
                stale[0].reason
            );
        }
    }

    // --- Transfer loop ---
    let total = transfers_to_execute.len();
    progress.on_start(total);
    let decision_id = recorder.as_ref().and_then(|r| r.decision_id());
    let mut receipt_items: Vec<ApplyReceiptItem> = Vec::new();

    for (i, transfer) in transfers_to_execute.iter().enumerate() {
        let (outcome, prev_decision_id) = match execute_single_transfer(
            transfer,
            &params.base_dir,
            params.transfer_mode,
            conn,
            params.archive_root_id,
            decision_id,
        ) {
            Ok(pair) => pair,
            Err(e) => (TransferOutcome::Error(format!("{:#}", e)), None),
        };

        match &outcome {
            TransferOutcome::Copied | TransferOutcome::Renamed | TransferOutcome::Moved => {
                receipt_items.push(ApplyReceiptItem {
                    source_root: transfer.source_root_path.clone(),
                    source_rel_path: transfer.source_rel_path.clone(),
                    destination_rel_path: transfer.archive_rel_path.clone(),
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

        let dest_full = params.base_dir.join(&transfer.dest_rel_path);
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

    // --- Resume mode: register "already there" entries in DB ---
    //
    // These rows are stamped with this decision, but no receipt item is added
    // for them and they are not counted into the extraction ledger below —
    // the transfer that put the files there belongs to an earlier decision,
    // whose receipt already lists them. One consequence is worth knowing
    // before changing either side: rebuilding the ledger from receipts on
    // disk cannot re-derive these rows, because this decision's receipt does
    // not mention them.
    if params.resume && !plan.resume_already_there.is_empty() {
        for transfer in &plan.resume_already_there {
            let new_source = build_new_source_from_lock(
                params.archive_root_id,
                &transfer.archive_rel_path,
                transfer.object_id,
                &transfer.partial_hash,
                transfer.size,
                transfer.mtime,
                decision_id,
            );
            if let Err(e) = crate::archive::repo::insert_destination(conn, &new_source) {
                result.errors.push(TransferError {
                    path: transfer.source_path.clone(),
                    error: format!("Failed to register in DB: {e}"),
                });
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
        let total = plan.transfers.len() as i64;
        let completed = (result.copied + result.renamed + result.moved) as i64;
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
                        ReceiptKind::Apply(params.transfer_mode),
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

/// Build a NewSource from lock entry data for DB registration.
/// Used for "already there" entries in resume mode — no disk read needed.
fn build_new_source_from_lock(
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
    size: i64,
    mtime: i64,
    decision_id: Option<i64>,
) -> NewSource {
    NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size,
        mtime,
        partial_hash: partial_hash.to_string(),
        object_id,
        device: None,
        inode: None,
        decision_id,
    }
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
) -> Result<(TransferOutcome, Option<i64>)> {
    let src_path = Path::new(&transfer.source_path);
    let dest_path = base_dir.join(&transfer.dest_rel_path);

    // Check if source exists
    if !src_path.exists() {
        return Ok((TransferOutcome::SkippedMissing, None));
    }

    // Per-transfer staleness validation (catches race conditions)
    if let Err(reason) = validate_source_state(transfer) {
        return Ok((TransferOutcome::SkippedStale(reason), None));
    }

    // Capture previous decision_id before the transfer overwrites the destination record
    let prev_decision_id = crate::archive::repo::fetch_decision_id_at_path(
        conn,
        archive_root_id,
        &transfer.archive_rel_path,
    )?;

    // Create parent directories
    ensure_parent_dir(&dest_path)?;

    match transfer_mode {
        TransferMode::Copy => {
            copy_file(src_path, &dest_path, true)?;
            let new_source = build_new_source(
                &dest_path,
                archive_root_id,
                &transfer.archive_rel_path,
                transfer.object_id,
                &transfer.partial_hash,
                decision_id,
            )?;
            crate::archive::repo::insert_destination(conn, &new_source)?;
            Ok((TransferOutcome::Copied, prev_decision_id))
        }
        TransferMode::Rename => {
            rename_file(src_path, &dest_path, true)?;
            relocate_source(
                conn,
                transfer.source_id,
                archive_root_id,
                &transfer.archive_rel_path,
                decision_id,
            )?;
            Ok((TransferOutcome::Renamed, prev_decision_id))
        }
        TransferMode::Move => match move_file(src_path, &dest_path, true)? {
            MoveOutcome::Renamed => {
                relocate_source(
                    conn,
                    transfer.source_id,
                    archive_root_id,
                    &transfer.archive_rel_path,
                    decision_id,
                )?;
                Ok((TransferOutcome::Renamed, prev_decision_id))
            }
            MoveOutcome::CopiedAndDeleted => {
                // Clear the origin before registering the destination. A crash
                // between the two then reports the file at neither end, which a
                // later scan heals. The other order would report it at both,
                // which reads as a duplicate and invites acting on it.
                mark_source_not_present(conn, transfer.source_id, decision_id)?;
                let new_source = build_new_source(
                    &dest_path,
                    archive_root_id,
                    &transfer.archive_rel_path,
                    transfer.object_id,
                    &transfer.partial_hash,
                    decision_id,
                )?;
                crate::archive::repo::insert_destination(conn, &new_source)?;
                Ok((TransferOutcome::Moved, prev_decision_id))
            }
        },
    }
}

/// Validate that a source file on disk matches the state recorded in the transfer.
fn validate_source_state(transfer: &ApplyTransfer) -> std::result::Result<(), String> {
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
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
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
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
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
fn validate_transfers_disk(transfers: &[&ApplyTransfer]) -> Vec<StaleSource> {
    let mut stale = Vec::new();
    for transfer in transfers {
        if let Err(reason) = validate_source_state(transfer) {
            stale.push(StaleSource {
                path: transfer.source_path.clone(),
                reason,
            });
        }
    }
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
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
    use crate::ops::test_helpers::{insert_object, insert_root, setup_test_db};

    use super::super::plan::ApplyViolations;

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
            dest_rel_path: "dest.jpg".to_string(),
            archive_rel_path: "dest.jpg".to_string(),
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
        assert!(validate_source_state(&transfer).is_ok());
    }

    #[test]
    fn validate_missing_file() {
        let transfer = make_transfer_for_file(
            Path::new("/nonexistent/file.jpg"),
            1000,
            1704067200,
            "testhash",
        );
        let err = validate_source_state(&transfer).unwrap_err();
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
        let err = validate_source_state(&transfer).unwrap_err();
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
        let err = validate_source_state(&transfer).unwrap_err();
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
        let err = validate_source_state(&transfer).unwrap_err();
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: hash,
            size,
            mtime,
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        };

        let result =
            execute_single_transfer(&transfer, dest_dir.path(), mode, &conn, archive_root, None);
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
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
        )
        .unwrap();
        assert_eq!(prev_decision_id, None);
    }

    // =========================================================================
    // Interrupt flag tests
    // =========================================================================

    /// No-op progress implementation for testing.
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
                    dest_rel_path: "a.jpg".to_string(),
                    archive_rel_path: "a.jpg".to_string(),
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
                    dest_rel_path: "b.jpg".to_string(),
                    archive_rel_path: "b.jpg".to_string(),
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
        };

        // Pre-set the interrupt flag before calling execute
        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
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

        // Flag was pre-set, so first transfer executes then loop breaks
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
        };

        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
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
        use crate::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
        DecisionParams {
            command: crate::domain::decision::DecisionCommand::Apply,
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "photo.jpg".to_string(),
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
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
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
        };
        (plan, size, mtime)
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
        // the destination row is what the run still owes.
        let (mut plan, _size, _mtime) =
            single_transfer_plan(src_dir.path(), "2016/a.jpg", "2016/a.jpg", b"hi");
        plan.resume_already_there = std::mem::take(&mut plan.transfers);
        plan.resume_already_there_source_present = 1;
        plan.already_archived_count = 1;

        let params = ApplyExecuteParams {
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
        };

        let params = ApplyExecuteParams {
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
        };

        let params = ApplyExecuteParams {
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "photo.jpg".to_string(),
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
        };

        // Placement points at a file, not a directory → receipt setup fails,
        // but the transfer (which uses base_dir) still succeeds.
        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: blocker.display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "photo.jpg".to_string(),
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
        };

        // receipt_ctx is None — no receipt written
        let params = ApplyExecuteParams {
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "photo.jpg".to_string(),
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
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
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
        use crate::domain::config::{LedgerConfig, ReceiptLayout, RecordingMode};
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "Media/2024/photo.jpg".to_string(),
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
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: "Media/2024".to_string(),
        };

        let decision = DecisionParams {
            command: crate::domain::decision::DecisionCommand::Apply,
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
        };

        let params = ApplyExecuteParams {
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
                dest_rel_path: "photo.jpg".to_string(),
                archive_rel_path: "photo.jpg".to_string(),
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
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        let params = ApplyExecuteParams {
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
                    dest_rel_path: "a.jpg".to_string(),
                    archive_rel_path: "a.jpg".to_string(),
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
                    dest_rel_path: "b.jpg".to_string(),
                    archive_rel_path: "b.jpg".to_string(),
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
        };

        let receipt_ctx = ReceiptPlacement::Targeted {
            archive_root_id: archive_root,
            archive_root_path: archive_dir.path().display().to_string(),
            base_dir_rel: String::new(),
        };

        // Pre-set interrupt flag — first transfer completes, then loop breaks
        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
            base_dir: archive_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(flag),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
            receipt_ctx: Some(receipt_ctx),
        };

        let decision = make_decision_params(true);
        let result = execute_apply(&conn, &plan, &params, &NoopProgress, Some(&decision)).unwrap();
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
            dest_rel_path: "renamed.jpg".to_string(),
            archive_rel_path: "dest/renamed.jpg".to_string(),
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
}
