//! The exclude subsystem's single transaction site. `run_exclusion` is the
//! one implementation of the write-path-atomicity ADR that all seven execute
//! paths share — the only `conn.transaction()` call and the only receipt-file
//! write site in the subsystem.

use anyhow::Result;
use serde::Serialize;

use super::types::ReceiptDestination;
use crate::core::domain::decision::DecisionStatus;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::repo::Connection;

/// The outcome of one exclusion run: the summary as it was recorded, and the
/// warnings the caller must surface.
pub(super) struct ExclusionRun {
    /// The composed summary — printed, recorded on the decision row, and
    /// narrated by the trail: one composition, three uses. It carries the
    /// receipt gap's reason when there is one, so the row explains its own
    /// empty receipt columns.
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Run an exclusion execution transactionally. Opens a transaction, performs the
/// DB mutations via `mutate` (which also builds the receipt body from the
/// now-known `decision_id`), completes the decision record inside the
/// transaction, commits, then writes + finalizes the receipt file *after* commit.
/// Returns the recorded summary and accumulated warnings.
///
/// `destination` carries the placement and, when there is none, the reason. The
/// reason is joined onto `summary` here — the one place the join is spelled —
/// and never onto the receipt body's own copy: a gap means no placement, and no
/// placement means the `mutate` closure builds no receipt at all, so the two can
/// never disagree.
///
/// Per the write-path-atomicity ADR: the transaction covers DB mutations only.
/// The receipt file is a durable artifact written after the DB is committed, so a
/// receipt-write failure is a warning, never a rollback. A failure inside
/// `mutate` (or the commit) drops the transaction — the `started` decision row
/// and any partial flips roll back together, leaving no half-state.
pub(super) fn run_exclusion<T, F>(
    conn: &mut Connection,
    decision: Option<&DecisionParams>,
    destination: &ReceiptDestination,
    has_items: bool,
    counts: DecisionCounts,
    summary: &str,
    mutate: F,
) -> Result<ExclusionRun>
where
    T: Serialize,
    F: FnOnce(&Connection, Option<i64>) -> Result<Option<T>>,
{
    // A run that records nothing has no row to explain, so it states no gap.
    let summary = match destination.gap.as_deref() {
        Some(reason) if has_items => format!("{summary} — {reason}"),
        _ => summary.to_string(),
    };
    let summary = summary.as_str();

    let tx = conn.transaction()?;

    // Empty plans don't record — no transition, no receipt.
    let mut recorder = if has_items {
        decision.map(|d| DecisionRecorder::start(&tx, d, destination.placement.as_ref()))
    } else {
        None
    };
    let decision_id = recorder.as_ref().and_then(|r| r.decision_id());

    let receipt = mutate(&tx, decision_id)?;

    if let Some(r) = recorder.as_mut() {
        r.complete_db(&tx, DecisionStatus::Completed, counts, summary);
    }
    tx.commit()?;

    // Receipt file (durable artifact) is written after the DB is committed.
    let warnings = match recorder {
        Some(mut r) => {
            if let Some(receipt) = receipt.as_ref() {
                r.write_receipt_file(receipt, summary);
            }
            r.settle_receipt_claim(conn);
            r.take_warnings()
        }
        None => Vec::new(),
    };
    Ok(ExclusionRun {
        summary: summary.to_string(),
        warnings,
    })
}
