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
use crate::expr::SelectionNotes;

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

/// The summary an exclusion records: its head line, the receipt gap's reason joined onto it
/// where there is one — the one place that join is spelled — then the plan's note lines
/// under it (`SelectionNotes::under`). The head and the gap stay the first line, which the trail narrates;
/// the notes qualify the count, below it.
pub(super) fn recorded_summary(head: &str, gap: Option<&str>, notes: &SelectionNotes) -> String {
    match gap {
        Some(reason) => notes.under(&format!("{head} — {reason}")),
        None => notes.under(head),
    }
}

/// Run an exclusion execution transactionally. Opens a transaction, performs the
/// DB mutations via `mutate` (which also builds the receipt body from the
/// now-known `decision_id`), completes the decision record inside the
/// transaction, commits, then writes + finalizes the receipt file *after* commit.
/// Returns the recorded summary and accumulated warnings.
///
/// `destination` carries the placement and, when there is none, the reason. The
/// reason is joined onto `head` here, through [`recorded_summary`], and never
/// onto the receipt body's own copy: a gap means no placement, and no placement
/// means the `mutate` closure builds no receipt at all, so the two can never
/// disagree. `head` is the summary's first line, the whole of it for a
/// single-target act, and `notes` are the plan's lines, below the head in both.
///
/// Per the write-path-atomicity ADR: the transaction covers DB mutations only.
/// The receipt file is a durable artifact written after the DB is committed, so a
/// receipt-write failure is a warning, never a rollback. A failure inside
/// `mutate` (or the commit) drops the transaction — the `started` decision row
/// and any partial flips roll back together, leaving no half-state.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_exclusion<T, F>(
    conn: &mut Connection,
    decision: Option<&DecisionParams>,
    destination: &ReceiptDestination,
    has_items: bool,
    counts: DecisionCounts,
    head: &str,
    notes: &SelectionNotes,
    mutate: F,
) -> Result<ExclusionRun>
where
    T: Serialize,
    F: FnOnce(&Connection, Option<i64>) -> Result<Option<T>>,
{
    // A run that records nothing has no row to explain, so it states no gap.
    let gap = destination.gap.as_deref().filter(|_| has_items);
    let summary = recorded_summary(head, gap, notes);
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
