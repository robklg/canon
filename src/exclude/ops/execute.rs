//! Execute functions for exclude: `execute_set`, `execute_clear`,
//! `execute_duplicates`, `execute_set_objects`. Each performs the writes for
//! its plan counterpart via `run_exclusion` (the shared transaction runner).

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::format_count;
use crate::exclude::repo as exclude_repo;
use crate::ops::decision::DecisionParams;
use crate::ops::receipt::{
    DuplicatesReceipt, ExcludeReceipt, ObjectExcludeEntry, ObjectExcludeReceipt, ReceiptKind,
    ReceiptPlacement,
};
use crate::repo::Connection;

use super::receipt::{
    counts_all, duplicate_receipt_groups, exclude_receipt_items, object_stamp_set_entries,
};
use super::runner::run_exclusion;
use super::types::{
    ExcludeClearPlan, ExcludeDuplicatesPlan, ExcludeSetObjectsPlan, ExcludeSetPlan,
};
use crate::core::domain::decision::DecisionStatus;

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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSetResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.items.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let summary = format!("Excluded {} {noun}", format_count(count));

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        has_items,
        counts_all(count),
        &summary,
        |tx, decision_id| {
            exclude_repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(match (decision, decision_id, placement) {
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
            })
        },
    )?;

    Ok(ExcludeSetResult {
        count,
        summary,
        warnings,
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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeClearResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.items.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let summary = format!("Cleared exclusions for {} {noun}", format_count(count));

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        has_items,
        counts_all(count),
        &summary,
        |tx, decision_id| {
            exclude_repo::source::batch_set_excluded(tx, &source_ids, false, decision_id)?;
            Ok(match (decision, decision_id, placement) {
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
            })
        },
    )?;

    Ok(ExcludeClearResult {
        count,
        summary,
        warnings,
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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeDuplicatesResult> {
    let source_ids = plan.source_ids();
    let count = source_ids.len();
    let has_items = !plan.groups.is_empty();
    let noun = if count == 1 { "source" } else { "sources" };
    let summary = format!("Excluded {} {noun}", format_count(count));

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        has_items,
        counts_all(count),
        &summary,
        |tx, decision_id| {
            exclude_repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(match (decision, decision_id, placement) {
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
            })
        },
    )?;

    Ok(ExcludeDuplicatesResult {
        count,
        summary,
        warnings,
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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSetObjectsResult> {
    let count = plan.objects.len();
    let has_items = !plan.objects.is_empty();
    let total_in_source_roots = plan.total_source_count - plan.total_archive_count;
    let summary = format!(
        "Excluded {} objects affecting {} sources ({} in source roots, {} in archives)",
        count, plan.total_source_count, total_in_source_roots, plan.total_archive_count
    );

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        has_items,
        counts_all(count),
        &summary,
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
            Ok(match (decision, decision_id, placement) {
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
            })
        },
    )?;

    Ok(ExcludeSetObjectsResult {
        count,
        total_source_count: plan.total_source_count,
        total_archive_count: plan.total_archive_count,
        summary,
        warnings,
    })
}
