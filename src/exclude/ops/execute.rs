//! Execute functions for exclude: `execute_set`, `execute_clear`,
//! `execute_duplicates`, `execute_set_objects`. Each performs the writes for
//! its plan counterpart via `run_exclusion` (the shared transaction runner).

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::format_count;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::receipt::ReceiptKind;
use crate::core::repo::Connection;
use crate::exclude::repo as exclude_repo;

use super::receipt::{
    counts_all, duplicate_receipt_groups, exclude_receipt_items, object_stamp_set_entries,
    DuplicatesReceipt, ExcludeReceipt, ObjectExcludeEntry, ObjectExcludeReceipt,
};
use super::runner::{recorded_summary, run_exclusion};
use super::types::{
    ExcludeClearPlan, ExcludeDuplicatesPlan, ExcludeSetObjectsPlan, ExcludeSetPlan,
    ReceiptDestination,
};
use crate::core::domain::decision::DecisionStatus;

/// Refuse, inside the transaction, if a door has closed since the plan was
/// made.
///
/// **The backstop for a root parked by another process between plan and
/// execute** — retirement's `plan_bind` re-check is the shape. The planning
/// selection never offers a parked source, so a plan cannot name one; what it
/// cannot rule out is the user closing a door while the confirmation prompt
/// stands. Raised from inside `mutate`, so the `started` row and any flips
/// already made roll back together, exactly as any other failure there does.
///
/// Asked of the **root paths the plan carries**, which is what makes it cheap:
/// a source does not change root mid-run, so the roots table alone answers it
/// and no source needs re-fetching.
///
/// **Where it does not apply**: object exclusion, which is about the object
/// everywhere and proceeds with parked sharers counted rather than filtered
/// (the stamp-set is the receipt-set); and the single-target arms, whose
/// check-outside/execute-inside window this subsystem accepts by name.
fn refuse_if_a_door_closed<'a>(
    tx: &Connection,
    roots_touched: impl Iterator<Item = &'a str>,
) -> Result<()> {
    let roots = crate::core::repo::root::fetch_all(tx)?;
    for path in roots_touched {
        if let Some(parked) = roots
            .iter()
            .find(|r| r.path == path)
            .and_then(crate::core::domain::root::Root::parked)
        {
            return Err(crate::core::domain::root::DoorRefused::at(
                &parked,
                crate::core::domain::root::DoorVerb::Refused,
            )
            .into());
        }
    }
    Ok(())
}

/// Result of an exclude-set execution.
#[allow(dead_code)]
pub struct ExcludeSetResult {
    pub count: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Execute an exclude-set plan — marks sources as excluded, records the
/// decision_id on each, and writes a receipt.
pub fn execute_set(
    conn: &mut Connection,
    plan: &ExcludeSetPlan,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSetResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.items.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let head = format!("Excluded {} {noun}", format_count(count));
    let summary = recorded_summary(&head, None, &plan.notes);

    let run = run_exclusion(
        conn,
        decision,
        destination,
        has_items,
        counts_all(count),
        &head,
        &plan.notes,
        |tx, decision_id| {
            refuse_if_a_door_closed(tx, plan.items.iter().map(|i| i.root.as_str()))?;
            exclude_repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) if has_items => Some(ExcludeReceipt {
                        meta: d.receipt_meta(
                            did,
                            DecisionStatus::Completed,
                            &summary,
                            p.locus_root(),
                            ReceiptKind::ExcludeSet,
                            None,
                        ),
                        items: exclude_receipt_items(&plan.items),
                    }),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeSetResult {
        count,
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Result of an exclude-clear execution.
#[allow(dead_code)]
pub struct ExcludeClearResult {
    pub count: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Execute an exclude-clear plan — clears source-level exclusion, records the
/// decision_id on each, and writes a receipt.
pub fn execute_clear(
    conn: &mut Connection,
    plan: &ExcludeClearPlan,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeClearResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.items.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let head = format!("Cleared exclusions for {} {noun}", format_count(count));
    let summary = recorded_summary(&head, None, &plan.notes);

    let run = run_exclusion(
        conn,
        decision,
        destination,
        has_items,
        counts_all(count),
        &head,
        &plan.notes,
        |tx, decision_id| {
            refuse_if_a_door_closed(tx, plan.items.iter().map(|i| i.root.as_str()))?;
            exclude_repo::source::batch_set_excluded(tx, &source_ids, false, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) if has_items => Some(ExcludeReceipt {
                        meta: d.receipt_meta(
                            did,
                            DecisionStatus::Completed,
                            &summary,
                            p.locus_root(),
                            ReceiptKind::Restore,
                            None,
                        ),
                        items: exclude_receipt_items(&plan.items),
                    }),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeClearResult {
        count,
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Result of a duplicate exclusion execution.
#[allow(dead_code)]
pub struct ExcludeDuplicatesResult {
    pub count: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Execute a duplicate exclusion plan — marks the excluded copies, records the
/// decision_id on each, and writes a grouped receipt.
pub fn execute_duplicates(
    conn: &mut Connection,
    plan: &ExcludeDuplicatesPlan,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeDuplicatesResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.groups.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let head = format!("Excluded {} {noun}", format_count(count));
    let summary = recorded_summary(&head, None, &plan.notes);

    let run = run_exclusion(
        conn,
        decision,
        destination,
        has_items,
        counts_all(count),
        &head,
        &plan.notes,
        |tx, decision_id| {
            refuse_if_a_door_closed(
                tx,
                plan.groups
                    .iter()
                    .flat_map(|g| g.excluded.iter().map(|i| i.root.as_str())),
            )?;
            exclude_repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) if has_items => Some(DuplicatesReceipt {
                        meta: d.receipt_meta(
                            did,
                            DecisionStatus::Completed,
                            &summary,
                            p.locus_root(),
                            ReceiptKind::ExcludeDuplicates,
                            None,
                        ),
                        groups: duplicate_receipt_groups(&plan.groups),
                    }),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeDuplicatesResult {
        count,
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Result of an object exclusion execution.
#[allow(dead_code)]
pub struct ExcludeSetObjectsResult {
    pub count: usize,
    pub total_source_count: usize,
    pub total_archive_count: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Execute an object exclusion plan — marks objects as excluded, records the
/// decision_id on every source sharing each object, and writes a receipt.
pub fn execute_set_objects(
    conn: &mut Connection,
    plan: &ExcludeSetObjectsPlan,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSetObjectsResult> {
    let count = plan.objects.len();
    let has_items = !plan.objects.is_empty();
    let total_in_source_roots = plan.total_source_count - plan.total_archive_count;
    let head = format!(
        "Excluded {} objects affecting {} sources ({} in source roots, {} in archives)",
        count, plan.total_source_count, total_in_source_roots, plan.total_archive_count
    );
    let summary = recorded_summary(&head, None, &plan.notes);

    let run = run_exclusion(
        conn,
        decision,
        destination,
        has_items,
        counts_all(count),
        &head,
        &plan.notes,
        |tx, decision_id| {
            // Capture each object's stamp-set before stamping: the receipt must
            // list exactly the sources the stamp touches, with pre-stamp
            // `previous_decision_id`s.
            let mut stamp_sets = match (decision, decision_id) {
                (Some(_), Some(_)) => {
                    let ids: Vec<i64> = plan.objects.iter().map(|o| o.object_id).collect();
                    exclude_repo::source::fetch_object_sharers_for_receipt(tx, &ids)?
                }
                _ => HashMap::new(),
            };
            for entry in &plan.objects {
                exclude_repo::object::set_excluded(tx, entry.object_id, true)?;
                exclude_repo::source::set_decision_id_by_object(tx, entry.object_id, decision_id)?;
            }
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) if has_items => Some(ObjectExcludeReceipt {
                        meta: d.receipt_meta(
                            did,
                            DecisionStatus::Completed,
                            &summary,
                            p.locus_root(),
                            ReceiptKind::ExcludeObject,
                            None,
                        ),
                        objects: plan
                            .objects
                            .iter()
                            .map(|o| ObjectExcludeEntry {
                                hash: o.hash.clone(),
                                sources: object_stamp_set_entries(
                                    stamp_sets.remove(&o.object_id).unwrap_or_default(),
                                ),
                            })
                            .collect(),
                    }),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeSetObjectsResult {
        count,
        total_source_count: plan.total_source_count,
        total_archive_count: plan.total_archive_count,
        summary: run.summary,
        warnings: run.warnings,
    })
}
