//! Receipt-body mappers for exclude: pure functions turning plan/stamp-set
//! data into the durable receipt document types. `run_exclusion` (the
//! transaction/commit/write-file orchestration these mappers feed) lives in
//! `runner.rs` — kept separate so the subsystem's one transaction site isn't
//! buried under six small mappers.

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::source::Source;
use crate::core::repo::{self, Connection};
use crate::exclude::repo as exclude_repo;
use crate::ops::decision::{DecisionCounts, DecisionParams};
use crate::ops::receipt::{
    DuplicateExcludedEntry, DuplicateGroup, DuplicateKeptEntry, ExcludeReceiptItem,
    ObjectExcludeEntry, ObjectExcludeReceipt, ObjectSourceReceiptEntry, ReceiptKind,
};

use super::types::{DuplicateGroupData, ExcludeItemData};

/// Counts for an all-succeeded decision (attempted == completed == n, no
/// failures or skips). Records `Some(0)` for failed/skipped to match apply's
/// convention (explicit zero, not NULL).
pub(super) fn counts_all(n: usize) -> DecisionCounts {
    DecisionCounts {
        attempted: Some(n as i64),
        completed: Some(n as i64),
        failed: Some(0),
        skipped: Some(0),
    }
}

pub(super) fn exclude_receipt_items(items: &[ExcludeItemData]) -> Vec<ExcludeReceiptItem> {
    items
        .iter()
        .map(|i| ExcludeReceiptItem {
            root: i.root.clone(),
            rel_path: i.rel_path.clone(),
            hash: i.hash.clone(),
            size: i.size,
            mtime: i.mtime,
            previous_decision_id: i.previous_decision_id,
        })
        .collect()
}

pub(super) fn duplicate_receipt_groups(groups: &[DuplicateGroupData]) -> Vec<DuplicateGroup> {
    groups
        .iter()
        .map(|g| DuplicateGroup {
            hash: g.hash.clone(),
            kept: g
                .kept
                .iter()
                .map(|k| DuplicateKeptEntry {
                    root: k.root.clone(),
                    rel_path: k.rel_path.clone(),
                    size: k.size,
                    mtime: k.mtime,
                })
                .collect(),
            excluded: g
                .excluded
                .iter()
                .map(|e| DuplicateExcludedEntry {
                    root: e.root.clone(),
                    rel_path: e.rel_path.clone(),
                    size: e.size,
                    mtime: e.mtime,
                    previous_decision_id: e.previous_decision_id,
                })
                .collect(),
        })
        .collect()
}

/// Map an object's stamp-set (from `fetch_object_sharers_for_receipt`) into
/// receipt entries, sorted role DESC then root/rel_path — the same ordering as
/// the ceremony display. The entries mirror exactly the set
/// `set_decision_id_by_object` touches, including tombstone rows (marked
/// `present = false`), so the stamp is reconstructable from disk.
pub(super) fn object_stamp_set_entries(
    mut sharers: Vec<exclude_repo::source::ObjectReceiptSource>,
) -> Vec<ObjectSourceReceiptEntry> {
    sharers.sort_by(|a, b| {
        b.root_role
            .cmp(&a.root_role)
            .then_with(|| a.root_path.cmp(&b.root_path))
            .then_with(|| a.rel_path.cmp(&b.rel_path))
    });
    sharers
        .into_iter()
        .map(|s| ObjectSourceReceiptEntry {
            root: s.root_path,
            rel_path: s.rel_path,
            size: s.size,
            mtime: s.mtime,
            present: s.present,
            previous_decision_id: s.previous_decision_id,
        })
        .collect()
}

/// Build a single-object exclusion receipt (set-object / clear-object). `kind`
/// distinguishes the two (`ExcludeObject` vs `RestoreObject`); `locus` is the
/// receipt's anchoring root, from its placement.
pub(super) fn object_exclude_receipt(
    decision: &DecisionParams,
    decision_id: i64,
    locus: (i64, &str),
    kind: ReceiptKind,
    hash: &str,
    sources: Vec<ObjectSourceReceiptEntry>,
    summary: &str,
) -> ObjectExcludeReceipt {
    ObjectExcludeReceipt {
        meta: decision.receipt_meta(
            decision_id,
            DecisionStatus::Completed,
            summary,
            locus,
            kind,
            None,
        ),
        objects: vec![ObjectExcludeEntry {
            hash: hash.to_string(),
            sources,
        }],
    }
}

/// Build an `ExcludeItemData` for a single source, resolving its content hash.
pub(super) fn item_for_source(conn: &Connection, source: &Source) -> Result<ExcludeItemData> {
    let objects = match source.object_id {
        Some(oid) => repo::object::batch_fetch_by_ids(conn, &[oid])?,
        None => HashMap::new(),
    };
    Ok(ExcludeItemData::from_source(source, &objects))
}
