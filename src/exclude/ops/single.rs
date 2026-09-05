//! Single-target check/execute functions for exclude: validate-then-act pairs
//! for excluding/clearing one source or object by id, path, or hash. Each
//! `check_*` validates and returns display data; the paired `execute_*`
//! performs the write via `run_exclusion`.

use anyhow::Result;

use crate::core::domain::decision::DecisionStatus;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::receipt::ReceiptKind;
use crate::core::repo::{self, Connection};
use crate::exclude::repo as exclude_repo;

use super::receipt::{
    counts_all, exclude_receipt_items, item_for_source, object_exclude_receipt,
    object_stamp_set_entries, ExcludeReceipt,
};
use super::runner::run_exclusion;
use super::types::{object_source_info, ExcludeItemData, ObjectSourceInfo, ReceiptDestination};

/// Outcome of validating a single source for exclusion.
#[derive(Debug)]
pub enum SourceExclusionCheck {
    /// Source found and eligible for exclusion. Carries the receipt-capable item.
    Ready { item: ExcludeItemData },
    /// Source is already excluded (at source or object level).
    AlreadyExcluded { path: String },
}

/// Outcome of validating a single object for exclusion.
#[derive(Debug)]
pub enum ObjectExclusionCheck {
    /// Object found and eligible for exclusion.
    Ready {
        object_id: i64,
        hash_prefix: String,
        /// Full content hash formatted as `sha256:{value}` (for the receipt).
        hash: String,
        sources: Vec<ObjectSourceInfo>,
    },
    /// Object is already excluded.
    AlreadyExcluded { hash_prefix: String },
}

/// Outcome of validating a single object for clearing exclusion.
#[derive(Debug)]
pub enum ObjectClearCheck {
    /// Object found and currently excluded — eligible for clearing.
    Ready {
        object_id: i64,
        hash_prefix: String,
        /// Full content hash formatted as `sha256:{value}` (for the receipt).
        hash: String,
    },
    /// Object is not excluded.
    NotExcluded { hash_prefix: String },
}

/// Entry in the excluded objects list.
pub struct ExcludedObjectEntry {
    pub object_id: i64,
    pub hash_prefix: String,
    pub source_count: usize,
}

/// Validate that a source can be excluded by its database ID.
///
/// Errors if the source is not found or not present.
pub fn check_set_source_by_id(conn: &Connection, source_id: i64) -> Result<SourceExclusionCheck> {
    let sources = repo::source::batch_fetch_by_ids(conn, &[source_id])?;
    let Some(source) = sources.get(&source_id) else {
        anyhow::bail!("Source with id {source_id} not found or not present");
    };

    if source.is_excluded() {
        return Ok(SourceExclusionCheck::AlreadyExcluded {
            path: source.path(),
        });
    }

    let item = item_for_source(conn, source)?;
    Ok(SourceExclusionCheck::Ready { item })
}

/// Validate that a source can be excluded by its root and relative path.
///
/// Errors if no source exists at the given path.
/// `display_path` is used in error messages (the user-visible path).
pub fn check_set_source_by_path(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    display_path: &str,
) -> Result<SourceExclusionCheck> {
    let Some(source) = repo::source::fetch_by_path(conn, root_id, rel_path)? else {
        anyhow::bail!("No source found for path: {display_path}");
    };

    if source.is_excluded() {
        return Ok(SourceExclusionCheck::AlreadyExcluded {
            path: source.path(),
        });
    }

    let item = item_for_source(conn, &source)?;
    Ok(SourceExclusionCheck::Ready { item })
}

/// Result of excluding a single source.
#[allow(dead_code)]
pub struct ExcludeSourceResult {
    pub source_id: i64,
    pub path: String,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Exclude a single source, recording the decision_id and writing a one-item receipt.
///
/// `item` comes from the preceding check (`check_set_source_by_id`/`by_path`).
pub fn execute_set_source(
    conn: &mut Connection,
    item: &ExcludeItemData,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSourceResult> {
    let path = item.path();
    let summary = format!("Excluded: {path}");

    let run = run_exclusion(
        conn,
        decision,
        destination,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            exclude_repo::source::set_excluded(tx, item.source_id, true, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) => Some(ExcludeReceipt {
                        meta: d.receipt_meta(
                            did,
                            DecisionStatus::Completed,
                            &summary,
                            p.locus_root(),
                            ReceiptKind::ExcludeSet,
                            None,
                        ),
                        items: exclude_receipt_items(std::slice::from_ref(item)),
                    }),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeSourceResult {
        source_id: item.source_id,
        path,
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Validate that an object can be excluded by its hash.
///
/// Errors if no object exists with the given hash.
pub fn check_set_object_by_hash(conn: &Connection, hash: &str) -> Result<ObjectExclusionCheck> {
    let Some(object) = repo::object::fetch_by_hash(conn, hash)? else {
        anyhow::bail!("No object found with hash: {hash}");
    };

    let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();

    if object.is_excluded() {
        return Ok(ObjectExclusionCheck::AlreadyExcluded { hash_prefix });
    }

    let sources = fetch_object_sources(conn, object.id)?;
    Ok(ObjectExclusionCheck::Ready {
        object_id: object.id,
        hash_prefix,
        hash: format!("{}:{}", object.hash_type, object.hash_value),
        sources,
    })
}

/// Validate that an object can be excluded by looking up a source file path.
///
/// Errors if no source at path, source is unhashed, or file is empty (size=0).
/// `display_path` is used in error messages.
pub fn check_set_object_by_file(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    display_path: &str,
) -> Result<ObjectExclusionCheck> {
    let Some(source) = repo::source::fetch_by_path(conn, root_id, rel_path)? else {
        anyhow::bail!(
            "No hashed source found for path: {display_path}\n  (File must be scanned and hashed first)"
        );
    };

    let Some(object_id) = source.object_id else {
        anyhow::bail!(
            "No hashed source found for path: {display_path}\n  (File must be scanned and hashed first)"
        );
    };

    let objects = repo::object::batch_fetch_by_ids(conn, &[object_id])?;
    let Some(object) = objects.get(&object_id) else {
        anyhow::bail!(
            "No hashed source found for path: {display_path}\n  (File must be scanned and hashed first)"
        );
    };

    // The contentless law: a path names one file, but its object is the one
    // every empty file shares — an exclusion keyed on it would dismiss them
    // all. Refused; `--hash` states that intent explicitly.
    if source.is_contentless() {
        anyhow::bail!(
            "Cannot exclude empty file via path (all empty files share the same hash).\n  \
             Use --hash {} to explicitly exclude all empty files.",
            object.hash_value
        );
    }

    let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();

    if object.is_excluded() {
        return Ok(ObjectExclusionCheck::AlreadyExcluded { hash_prefix });
    }

    let sources = fetch_object_sources(conn, object_id)?;
    Ok(ObjectExclusionCheck::Ready {
        object_id,
        hash_prefix,
        hash: format!("{}:{}", object.hash_type, object.hash_value),
        sources,
    })
}

/// Result of excluding a single object.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ExcludeObjectResult {
    pub object_id: i64,
    pub hash_prefix: String,
    pub source_count: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Exclude a single object, recording the decision_id on every source sharing
/// it and writing a one-object receipt.
///
/// The `hash_prefix`/`hash`/`sources` come from the preceding check
/// (`check_set_object_by_hash` or `check_set_object_by_file`).
pub fn execute_set_object(
    conn: &mut Connection,
    object_id: i64,
    hash_prefix: &str,
    hash: &str,
    sources: &[ObjectSourceInfo],
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeObjectResult> {
    let summary = format!("Excluded object: {hash_prefix}...");

    let run = run_exclusion(
        conn,
        decision,
        destination,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            // Capture the stamp-set before stamping (pre-stamp provenance links).
            let stamp_set = match (decision, decision_id) {
                (Some(_), Some(_)) => {
                    exclude_repo::source::fetch_object_sharers_for_receipt(tx, &[object_id])?
                        .remove(&object_id)
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };
            exclude_repo::object::set_excluded(tx, object_id, true)?;
            exclude_repo::source::set_decision_id_by_object(tx, object_id, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) => Some(object_exclude_receipt(
                        d,
                        did,
                        p.locus_root(),
                        ReceiptKind::ExcludeObject,
                        hash,
                        object_stamp_set_entries(stamp_set),
                        &summary,
                    )),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ExcludeObjectResult {
        object_id,
        hash_prefix: hash_prefix.to_string(),
        source_count: sources.len(),
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Validate that an object exclusion can be cleared by its hash.
///
/// Errors if no object exists with the given hash.
pub fn check_clear_object(conn: &Connection, hash: &str) -> Result<ObjectClearCheck> {
    let Some(object) = repo::object::fetch_by_hash(conn, hash)? else {
        anyhow::bail!("No object found with hash: {hash}");
    };

    let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();

    if !object.is_excluded() {
        return Ok(ObjectClearCheck::NotExcluded { hash_prefix });
    }

    Ok(ObjectClearCheck::Ready {
        object_id: object.id,
        hash_prefix,
        hash: format!("{}:{}", object.hash_type, object.hash_value),
    })
}

/// Result of clearing exclusion from a single object.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ClearObjectResult {
    pub object_id: i64,
    pub hash_prefix: String,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Clear exclusion from a single object, recording the decision_id on every
/// source sharing it and writing a one-object receipt.
///
/// The `hash_prefix`/`hash` come from the preceding check
/// (`check_clear_object`); the receipt captures the stamp-set itself.
pub fn execute_clear_object(
    conn: &mut Connection,
    object_id: i64,
    hash_prefix: &str,
    hash: &str,
    destination: &ReceiptDestination,
    decision: Option<&DecisionParams>,
) -> Result<ClearObjectResult> {
    let summary = format!("Cleared exclusion from object: {hash_prefix}...");

    let run = run_exclusion(
        conn,
        decision,
        destination,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            // Capture the stamp-set before stamping (pre-stamp provenance links).
            let stamp_set = match (decision, decision_id) {
                (Some(_), Some(_)) => {
                    exclude_repo::source::fetch_object_sharers_for_receipt(tx, &[object_id])?
                        .remove(&object_id)
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };
            exclude_repo::object::set_excluded(tx, object_id, false)?;
            exclude_repo::source::set_decision_id_by_object(tx, object_id, decision_id)?;
            Ok(
                match (decision, decision_id, destination.placement.as_ref()) {
                    (Some(d), Some(did), Some(p)) => Some(object_exclude_receipt(
                        d,
                        did,
                        p.locus_root(),
                        ReceiptKind::RestoreObject,
                        hash,
                        object_stamp_set_entries(stamp_set),
                        &summary,
                    )),
                    _ => None,
                },
            )
        },
    )?;

    Ok(ClearObjectResult {
        object_id,
        hash_prefix: hash_prefix.to_string(),
        summary: run.summary,
        warnings: run.warnings,
    })
}

/// Fetch source display info for an object.
///
/// Returns present sources sorted by role DESC, root_path, rel_path.
/// Maps to `ObjectSourceInfo` for display.
pub fn fetch_object_sources(conn: &Connection, object_id: i64) -> Result<Vec<ObjectSourceInfo>> {
    let sources_map = repo::source::fetch_sources_by_object_ids(conn, &[object_id])?;
    let mut sources: Vec<_> = sources_map.get(&object_id).cloned().unwrap_or_default();

    // Sort: role DESC (source before archive), root_path, rel_path
    sources.sort_by(|a, b| {
        b.root_role
            .cmp(&a.root_role)
            .then_with(|| a.root_path.cmp(&b.root_path))
            .then_with(|| a.rel_path.cmp(&b.rel_path))
    });

    Ok(sources.iter().map(object_source_info).collect())
}

/// List all excluded objects with source counts.
pub fn list_excluded_objects(conn: &Connection) -> Result<Vec<ExcludedObjectEntry>> {
    let excluded = exclude_repo::object::fetch_excluded(conn)?;

    if excluded.is_empty() {
        return Ok(Vec::new());
    }

    let object_ids: Vec<i64> = excluded.iter().map(|o| o.id).collect();
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids)?;

    Ok(excluded
        .iter()
        .map(|object| {
            let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();
            let source_count = sources_by_object
                .get(&object.id)
                .map(|sources| sources.len())
                .unwrap_or(0);
            ExcludedObjectEntry {
                object_id: object.id,
                hash_prefix,
                source_count,
            }
        })
        .collect())
}
