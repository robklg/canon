//! The exclude subsystem's single transaction site. `run_exclusion` is the
//! one implementation of the write-path-atomicity ADR that all seven execute
//! paths share — the only `conn.transaction()` call and the only receipt-file
//! write site in the subsystem.

use anyhow::Result;
use serde::Serialize;

use crate::core::domain::decision::DecisionStatus;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::receipt::ReceiptPlacement;
use crate::core::repo::Connection;

/// Run an exclusion execution transactionally. Opens a transaction, performs the
/// DB mutations via `mutate` (which also builds the receipt body from the
/// now-known `decision_id`), completes the decision record inside the
/// transaction, commits, then writes + finalizes the receipt file *after* commit.
/// Returns accumulated warnings.
///
/// Per the write-path-atomicity ADR: the transaction covers DB mutations only.
/// The receipt file is a durable artifact written after the DB is committed, so a
/// receipt-write failure is a warning, never a rollback. A failure inside
/// `mutate` (or the commit) drops the transaction — the `started` decision row
/// and any partial flips roll back together, leaving no half-state.
pub(super) fn run_exclusion<T, F>(
    conn: &mut Connection,
    decision: Option<&DecisionParams>,
    placement: Option<&ReceiptPlacement>,
    has_items: bool,
    counts: DecisionCounts,
    summary: &str,
    mutate: F,
) -> Result<Vec<String>>
where
    T: Serialize,
    F: FnOnce(&Connection, Option<i64>) -> Result<Option<T>>,
{
    let tx = conn.transaction()?;

    // Empty plans don't record — no transition, no receipt.
    let mut recorder = if has_items {
        decision.map(|d| DecisionRecorder::start(&tx, d, placement))
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
    Ok(warnings)
}
