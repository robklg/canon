//! Exclude operations — plan/execute for exclusion management.
//!
//! Provides plan/execute functions for `exclude set`, `exclude clear`,
//! `exclude duplicates`, and `exclude set --objects`. Plan functions compute
//! what would happen (no side effects), returning typed plan structs with all
//! data needed for display and confirmation. Execute functions perform the writes.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::Serialize;

use crate::domain::decision::DecisionStatus;
use crate::domain::exclusion::find_excludable_duplicates;
use crate::domain::format_count;
use crate::domain::include::IncludeSet;
use crate::domain::object::Object;
use crate::domain::path::path_is_under;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::expr::filter::{self, Filter};
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::ops::receipt::{
    DuplicateExcludedEntry, DuplicateGroup, DuplicateKeptEntry, DuplicatesReceipt, ExcludeReceipt,
    ExcludeReceiptItem, ObjectExcludeEntry, ObjectExcludeReceipt, ObjectSourceReceiptEntry,
    ReceiptPlacement,
};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

/// Receipt-capable per-source detail carried through exclusion plans.
///
/// Single source of truth for both display (via `path()`) and the durable
/// receipt item. `source_ids()` / `paths()` accessors on the plans derive from
/// these, replacing the old parallel `source_ids` / `paths` vectors.
#[derive(Debug)]
pub struct ExcludeItemData {
    pub source_id: i64,
    /// Source root path (absolute).
    pub root: String,
    pub rel_path: String,
    /// Content hash formatted as `sha256:{value}`; `None` if unhashed.
    pub hash: Option<String>,
    pub size: i64,
    pub mtime: i64,
    /// The source's `decision_id` before this op — the predecessor in the chain.
    pub previous_decision_id: Option<i64>,
}

impl ExcludeItemData {
    /// Build from a fetched source, resolving the content hash from `objects`.
    fn from_source(s: &Source, objects: &HashMap<i64, Object>) -> Self {
        let hash = s
            .object_id
            .and_then(|oid| objects.get(&oid))
            .map(|o| format!("{}:{}", o.hash_type, o.hash_value));
        ExcludeItemData {
            source_id: s.id,
            root: s.root_path.clone(),
            rel_path: s.rel_path.clone(),
            hash,
            size: s.size,
            mtime: s.mtime,
            previous_decision_id: s.decision_id,
        }
    }

    /// Full absolute path, mirroring `Source::path()` (handles empty rel_path).
    pub fn path(&self) -> String {
        if self.rel_path.is_empty() {
            self.root.clone()
        } else {
            format!("{}/{}", self.root, self.rel_path)
        }
    }
}

/// One duplicate group in an `exclude duplicates` plan: the content hash, the
/// kept copies (under the prefer prefix — no state transition), and the
/// excluded sources (the duplicates being marked excluded).
pub struct DuplicateGroupData {
    pub hash: String,
    pub kept: Vec<ExcludeItemData>,
    pub excluded: Vec<ExcludeItemData>,
}

/// Parameters for planning a source exclusion set operation.
pub struct ExcludeSetParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding sources. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetPlan {
    /// Sources to exclude, with receipt-capable detail.
    pub items: Vec<ExcludeItemData>,
    /// Distinct root count across sources to exclude.
    pub root_count: usize,
    /// Sources with no archived copy (unhashed or not in any archive root).
    pub not_archived_count: usize,
}

impl ExcludeSetPlan {
    pub fn source_ids(&self) -> Vec<i64> {
        self.items.iter().map(|i| i.source_id).collect()
    }
    pub fn paths(&self) -> Vec<String> {
        self.items.iter().map(|i| i.path()).collect()
    }
}

/// Parameters for planning a source exclusion clear operation.
pub struct ExcludeClearParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for clearing source-level exclusions.
pub struct ExcludeClearPlan {
    /// Sources to clear exclusion from, with receipt-capable detail.
    pub items: Vec<ExcludeItemData>,
    /// Distinct root count across sources to clear.
    pub root_count: usize,
}

impl ExcludeClearPlan {
    pub fn source_ids(&self) -> Vec<i64> {
        self.items.iter().map(|i| i.source_id).collect()
    }
    pub fn paths(&self) -> Vec<String> {
        self.items.iter().map(|i| i.path()).collect()
    }
}

/// Parameters for planning a duplicate exclusion operation.
pub struct ExcludeDuplicatesParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
    pub prefer_prefix: String,
}

/// Computed plan for excluding duplicate sources. Contains all data the
/// interface needs for dry-run display and confirmation — no further
/// queries needed.
pub struct ExcludeDuplicatesPlan {
    /// Duplicate groups: each carries the kept copy/copies and the excluded
    /// sources. Source-of-truth for both display and the duplicates receipt.
    pub groups: Vec<DuplicateGroupData>,
    /// Distinct object groups being excluded (count of unique object_ids).
    pub group_count: usize,
    /// The prefer path used for duplicate resolution.
    pub prefer_prefix: String,
    /// Total sources in scope (before duplicate analysis).
    pub scope_count: usize,
    /// Sources skipped because they have no object_id (unhashed).
    pub skipped_no_hash: usize,
    /// Sources skipped because they're already in the prefer path.
    pub skipped_in_prefer: usize,
    /// Sources skipped because no copy exists in prefer path.
    pub skipped_not_covered: usize,
    /// Sources skipped because multiple copies exist in prefer path.
    pub skipped_multiple: usize,
}

impl ExcludeDuplicatesPlan {
    /// The excluded sources across all groups (the duplicates being marked).
    pub fn source_ids(&self) -> Vec<i64> {
        self.groups
            .iter()
            .flat_map(|g| g.excluded.iter().map(|i| i.source_id))
            .collect()
    }
    pub fn paths(&self) -> Vec<String> {
        self.groups
            .iter()
            .flat_map(|g| g.excluded.iter().map(|i| i.path()))
            .collect()
    }
}

/// Parameters for planning an object exclusion operation.
pub struct ExcludeSetObjectsParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding objects. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetObjectsPlan {
    /// Objects to exclude, with display data.
    pub objects: Vec<ObjectPlanEntry>,
    /// Total source count across all objects.
    pub total_source_count: usize,
    /// Total archive source count across all objects.
    pub total_archive_count: usize,
    /// Sources skipped because they have no hash.
    pub skipped_no_hash: usize,
    /// Empty files skipped (size = 0).
    pub skipped_empty: usize,
    /// Objects already excluded.
    pub skipped_already_excluded: usize,
}

/// A single object entry in the exclusion plan.
pub struct ObjectPlanEntry {
    pub object_id: i64,
    /// Hash prefix for display (first 16 chars).
    pub hash_prefix: String,
    /// Full content hash formatted as `sha256:{value}` (for the receipt).
    pub hash: String,
    /// Sources linked to this object (sorted: role DESC, root_path, rel_path).
    pub sources: Vec<ObjectSourceInfo>,
}

/// Source info for object exclusion display and receipts.
#[derive(Debug)]
pub struct ObjectSourceInfo {
    pub path: String,
    pub is_archive: bool,
}

// ============================================================================
// Plan functions
// ============================================================================

/// Compute what `exclude set` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters via `select_sources()`,
/// then computes confirmation data (root count, archive coverage).
pub fn plan_set(conn: &mut Connection, params: &ExcludeSetParams) -> Result<ExcludeSetPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet::default(),
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = selection::select_sources(conn, &sel_params)?;

    // select_sources() with default IncludeSet already filters out excluded sources
    let sources = selection.sources;

    if sources.is_empty() {
        return Ok(ExcludeSetPlan {
            items: Vec::new(),
            root_count: 0,
            not_archived_count: 0,
        });
    }

    // Compute archive coverage
    let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
    let archived_set = repo::object::batch_check_archived(conn, &object_ids, None)?;

    let not_archived_count = sources
        .iter()
        .filter(|s| match s.object_id {
            None => true, // unhashed counts as not archived
            Some(oid) => !archived_set.contains(&oid),
        })
        .count();

    let root_ids: HashSet<i64> = sources.iter().map(|s| s.root_id).collect();

    // Resolve hashes for receipt-capable items (one batch fetch).
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
    let items: Vec<ExcludeItemData> = sources
        .iter()
        .map(|s| ExcludeItemData::from_source(s, &objects))
        .collect();

    Ok(ExcludeSetPlan {
        items,
        root_count: root_ids.len(),
        not_archived_count,
    })
}

/// Compute what `exclude clear` would do — no side effects.
///
/// Finds source-level excluded sources (`s.excluded == true`, NOT object-level)
/// matching scope and filters. Uses its own selection logic rather than
/// `select_sources()` — finding sources marked for clearing is a different
/// contract from finding visible sources.
pub fn plan_clear(conn: &mut Connection, params: &ExcludeClearParams) -> Result<ExcludeClearPlan> {
    let roots = repo::root::fetch_all(conn)?;
    let source_root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    if source_root_ids.is_empty() {
        return Ok(ExcludeClearPlan {
            items: Vec::new(),
            root_count: 0,
        });
    }

    let all_sources = repo::source::batch_fetch_by_roots(conn, &source_root_ids)?;

    // Filter for scope match and source-level exclusion only.
    // Uses s.excluded (source-level flag), NOT s.is_excluded() which includes
    // object-level. clear() must only clear source-level exclusions.
    let filtered: Vec<_> = all_sources
        .into_iter()
        .filter(|s| params.scopes.is_empty() || s.matches_scope(&params.scopes))
        .filter(|s| s.excluded)
        .collect();

    // Apply --where filters if present
    let filtered = if params.filters.is_empty() {
        filtered
    } else {
        let ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
        let filtered_ids: HashSet<i64> = filter::apply_filters(conn, &ids, &params.filters)?
            .source_ids
            .into_iter()
            .collect();
        filtered
            .into_iter()
            .filter(|s| filtered_ids.contains(&s.id))
            .collect()
    };

    let root_ids: HashSet<i64> = filtered.iter().map(|s| s.root_id).collect();

    // Resolve hashes for receipt-capable items (one batch fetch).
    let object_ids: Vec<i64> = filtered.iter().filter_map(|s| s.object_id).collect();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
    let items: Vec<ExcludeItemData> = filtered
        .iter()
        .map(|s| ExcludeItemData::from_source(s, &objects))
        .collect();

    Ok(ExcludeClearPlan {
        items,
        root_count: root_ids.len(),
    })
}

/// Compute what `exclude duplicates` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters, runs duplicate
/// analysis via `find_excludable_duplicates()`, and computes confirmation
/// data (group count, skip statistics).
pub fn plan_duplicates(
    conn: &mut Connection,
    params: &ExcludeDuplicatesParams,
) -> Result<ExcludeDuplicatesPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet::default(),
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = selection::select_sources(conn, &sel_params)?;
    let scope_count = selection.sources.len();

    if selection.sources.is_empty() {
        return Ok(ExcludeDuplicatesPlan {
            groups: Vec::new(),
            group_count: 0,
            prefer_prefix: params.prefer_prefix.clone(),
            scope_count: 0,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        });
    }

    // Build lookup map for source objects
    let source_map: HashMap<i64, &Source> = selection.sources.iter().map(|s| (s.id, s)).collect();

    // Collect unique object_ids for duplicate lookup
    let object_ids: Vec<i64> = selection
        .sources
        .iter()
        .filter_map(|s| s.object_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Fetch all sources that share these objects (potential duplicates)
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids)?;

    // Use pure domain function to determine what to exclude
    let result = find_excludable_duplicates(
        &selection.sources,
        &sources_by_object,
        &params.prefer_prefix,
    );

    // Resolve hashes for receipt-capable items (one batch fetch).
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;

    // Reconstruct duplicate groups: group the excluded sources by object_id
    // (preserving to_exclude order), and pull the kept copy/copies — the
    // non-excluded sources under the prefer prefix — from sources_by_object.
    let mut group_order: Vec<i64> = Vec::new();
    let mut excluded_by_obj: HashMap<i64, Vec<ExcludeItemData>> = HashMap::new();
    for &id in &result.to_exclude {
        let Some(source) = source_map.get(&id) else {
            continue;
        };
        let Some(oid) = source.object_id else {
            continue;
        };
        if !excluded_by_obj.contains_key(&oid) {
            group_order.push(oid);
        }
        excluded_by_obj
            .entry(oid)
            .or_default()
            .push(ExcludeItemData::from_source(source, &objects));
    }

    let mut groups = Vec::with_capacity(group_order.len());
    for oid in group_order {
        let excluded = excluded_by_obj.remove(&oid).unwrap_or_default();
        let hash = objects
            .get(&oid)
            .map(|o| format!("{}:{}", o.hash_type, o.hash_value))
            .unwrap_or_default();
        let mut kept: Vec<ExcludeItemData> = sources_by_object
            .get(&oid)
            .map(|ss| {
                ss.iter()
                    .filter(|s| !s.is_excluded() && path_is_under(&s.path(), &params.prefer_prefix))
                    .map(|s| ExcludeItemData::from_source(s, &objects))
                    .collect()
            })
            .unwrap_or_default();
        // Deterministic order for a diff-able receipt (DB row order is not stable).
        kept.sort_by(|a, b| {
            a.root
                .cmp(&b.root)
                .then_with(|| a.rel_path.cmp(&b.rel_path))
        });
        groups.push(DuplicateGroupData {
            hash,
            kept,
            excluded,
        });
    }

    let group_count = groups.len();

    Ok(ExcludeDuplicatesPlan {
        groups,
        group_count,
        prefer_prefix: params.prefer_prefix.clone(),
        scope_count,
        skipped_no_hash: result.skipped_no_hash,
        skipped_in_prefer: result.skipped_in_prefer,
        skipped_not_covered: result.skipped_not_covered,
        skipped_multiple: result.skipped_multiple,
    })
}

/// Compute what `exclude set --objects` would do — no side effects.
///
/// Selects sources matching scope and filters (including already-excluded
/// sources), collects their objects, filters out unhashed/empty/already-excluded,
/// and computes display data per object.
pub fn plan_set_objects(
    conn: &mut Connection,
    params: &ExcludeSetObjectsParams,
) -> Result<ExcludeSetObjectsPlan> {
    let sel_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet {
            excluded: true,
            archived: false,
        },
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceOnly,
    };
    let selection = selection::select_sources(conn, &sel_params)?;

    if selection.sources.is_empty() {
        return Ok(ExcludeSetObjectsPlan {
            objects: vec![],
            total_source_count: 0,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        });
    }

    // Collect unique object_ids from selected sources, counting skips
    let mut seen_objects: HashSet<i64> = HashSet::new();
    let mut object_ids_to_check: Vec<i64> = Vec::new();
    let mut skipped_no_hash = 0;
    let mut skipped_empty = 0;

    for source in &selection.sources {
        let Some(object_id) = source.object_id else {
            skipped_no_hash += 1;
            continue;
        };
        if !seen_objects.insert(object_id) {
            continue;
        }
        // Empty files all share the same hash — skip to prevent excluding all empty files
        if source.size == 0 {
            skipped_empty += 1;
            continue;
        }
        object_ids_to_check.push(object_id);
    }

    if object_ids_to_check.is_empty() {
        return Ok(ExcludeSetObjectsPlan {
            objects: vec![],
            total_source_count: 0,
            total_archive_count: 0,
            skipped_no_hash,
            skipped_empty,
            skipped_already_excluded: 0,
        });
    }

    // Batch fetch objects to check exclusion status
    let objects_map = repo::object::batch_fetch_by_ids(conn, &object_ids_to_check)?;

    // Batch fetch all sources per object for display
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids_to_check)?;

    // Build plan entries, filtering out already-excluded objects
    let mut objects: Vec<ObjectPlanEntry> = Vec::new();
    let mut total_source_count = 0;
    let mut total_archive_count = 0;
    let mut skipped_already_excluded = 0;

    for &object_id in &object_ids_to_check {
        let Some(object) = objects_map.get(&object_id) else {
            continue;
        };

        if object.is_excluded() {
            skipped_already_excluded += 1;
            continue;
        }

        let hash_prefix = object.hash_value[..16.min(object.hash_value.len())].to_string();
        let hash = format!("{}:{}", object.hash_type, object.hash_value);

        // Get and sort sources for this object
        let mut obj_sources: Vec<_> = sources_by_object
            .get(&object_id)
            .cloned()
            .unwrap_or_default();

        // Sort: role DESC (source before archive), root_path, rel_path
        obj_sources.sort_by(|a, b| {
            b.root_role
                .cmp(&a.root_role)
                .then_with(|| a.root_path.cmp(&b.root_path))
                .then_with(|| a.rel_path.cmp(&b.rel_path))
        });

        let sources: Vec<ObjectSourceInfo> = obj_sources.iter().map(object_source_info).collect();

        let archive_count = sources.iter().filter(|s| s.is_archive).count();
        total_archive_count += archive_count;
        total_source_count += sources.len();

        objects.push(ObjectPlanEntry {
            object_id,
            hash_prefix,
            hash,
            sources,
        });
    }

    Ok(ExcludeSetObjectsPlan {
        objects,
        total_source_count,
        total_archive_count,
        skipped_no_hash,
        skipped_empty,
        skipped_already_excluded,
    })
}

// ============================================================================
// Receipt helpers
// ============================================================================

/// Counts for an all-succeeded decision (attempted == completed == n, no
/// failures or skips). Records `Some(0)` for failed/skipped to match apply's
/// convention (explicit zero, not NULL).
fn counts_all(n: usize) -> DecisionCounts {
    DecisionCounts {
        attempted: Some(n as i64),
        completed: Some(n as i64),
        failed: Some(0),
        skipped: Some(0),
    }
}

fn exclude_receipt_items(items: &[ExcludeItemData]) -> Vec<ExcludeReceiptItem> {
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

fn duplicate_receipt_groups(groups: &[DuplicateGroupData]) -> Vec<DuplicateGroup> {
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
fn object_stamp_set_entries(
    mut sharers: Vec<repo::source::ObjectReceiptSource>,
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

/// Build a single-object exclusion receipt (set-object / clear-object).
fn object_exclude_receipt(
    decision: &DecisionParams,
    decision_id: i64,
    hash: &str,
    sources: Vec<ObjectSourceReceiptEntry>,
    summary: &str,
) -> ObjectExcludeReceipt {
    ObjectExcludeReceipt {
        meta: decision.receipt_meta(decision_id, DecisionStatus::Completed, summary, None),
        objects: vec![ObjectExcludeEntry {
            hash: hash.to_string(),
            sources,
        }],
    }
}

/// Build an `ExcludeItemData` for a single source, resolving its content hash.
fn item_for_source(conn: &Connection, source: &Source) -> Result<ExcludeItemData> {
    let objects = match source.object_id {
        Some(oid) => repo::object::batch_fetch_by_ids(conn, &[oid])?,
        None => HashMap::new(),
    };
    Ok(ExcludeItemData::from_source(source, &objects))
}

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
fn run_exclusion<T, F>(
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
            r.finalize_receipt_file();
            r.take_warnings()
        }
        None => Vec::new(),
    };
    Ok(warnings)
}

// ============================================================================
// Execute functions
// ============================================================================

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
            repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) if has_items => Some(ExcludeReceipt {
                    meta: d.receipt_meta(did, DecisionStatus::Completed, &summary, None),
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
            repo::source::batch_set_excluded(tx, &source_ids, false, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) if has_items => Some(ExcludeReceipt {
                    meta: d.receipt_meta(did, DecisionStatus::Completed, &summary, None),
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
            repo::source::batch_set_excluded(tx, &source_ids, true, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) if has_items => Some(DuplicatesReceipt {
                    meta: d.receipt_meta(did, DecisionStatus::Completed, &summary, None),
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
                    repo::source::fetch_object_sharers_for_receipt(tx, &ids)?
                }
                _ => HashMap::new(),
            };
            for entry in &plan.objects {
                repo::object::set_excluded(tx, entry.object_id, true)?;
                repo::source::set_decision_id_by_object(tx, entry.object_id, decision_id)?;
            }
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) if has_items => Some(ObjectExcludeReceipt {
                    meta: d.receipt_meta(did, DecisionStatus::Completed, &summary, None),
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

// ============================================================================
// Single-target check/execute functions
// ============================================================================

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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeSourceResult> {
    let path = item.path();
    let summary = format!("Excluded: {path}");

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            repo::source::set_excluded(tx, item.source_id, true, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) => Some(ExcludeReceipt {
                    meta: d.receipt_meta(did, DecisionStatus::Completed, &summary, None),
                    items: exclude_receipt_items(std::slice::from_ref(item)),
                }),
                _ => None,
            })
        },
    )?;

    Ok(ExcludeSourceResult {
        source_id: item.source_id,
        path,
        summary,
        warnings,
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

    // Safety: refuse to exclude empty files via path lookup
    if source.size == 0 {
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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ExcludeObjectResult> {
    let summary = format!("Excluded object: {hash_prefix}...");

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            // Capture the stamp-set before stamping (pre-stamp provenance links).
            let stamp_set = match (decision, decision_id) {
                (Some(_), Some(_)) => {
                    repo::source::fetch_object_sharers_for_receipt(tx, &[object_id])?
                        .remove(&object_id)
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };
            repo::object::set_excluded(tx, object_id, true)?;
            repo::source::set_decision_id_by_object(tx, object_id, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) => Some(object_exclude_receipt(
                    d,
                    did,
                    hash,
                    object_stamp_set_entries(stamp_set),
                    &summary,
                )),
                _ => None,
            })
        },
    )?;

    Ok(ExcludeObjectResult {
        object_id,
        hash_prefix: hash_prefix.to_string(),
        source_count: sources.len(),
        summary,
        warnings,
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
    placement: Option<&ReceiptPlacement>,
    decision: Option<&DecisionParams>,
) -> Result<ClearObjectResult> {
    let summary = format!("Cleared exclusion from object: {hash_prefix}...");

    let warnings = run_exclusion(
        conn,
        decision,
        placement,
        true,
        counts_all(1),
        &summary,
        |tx, decision_id| {
            // Capture the stamp-set before stamping (pre-stamp provenance links).
            let stamp_set = match (decision, decision_id) {
                (Some(_), Some(_)) => {
                    repo::source::fetch_object_sharers_for_receipt(tx, &[object_id])?
                        .remove(&object_id)
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };
            repo::object::set_excluded(tx, object_id, false)?;
            repo::source::set_decision_id_by_object(tx, object_id, decision_id)?;
            Ok(match (decision, decision_id) {
                (Some(d), Some(did)) => Some(object_exclude_receipt(
                    d,
                    did,
                    hash,
                    object_stamp_set_entries(stamp_set),
                    &summary,
                )),
                _ => None,
            })
        },
    )?;

    Ok(ClearObjectResult {
        object_id,
        hash_prefix: hash_prefix.to_string(),
        summary,
        warnings,
    })
}

/// Map a fetched source to object-source display info.
fn object_source_info(s: &Source) -> ObjectSourceInfo {
    ObjectSourceInfo {
        path: s.path(),
        is_archive: s.is_from_role("archive"),
    }
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
    let excluded = repo::object::fetch_excluded(conn)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, insert_source_excluded, is_source_excluded,
        setup_test_db,
    };

    fn make_set_params(scopes: Vec<ScopeMatch>) -> ExcludeSetParams {
        ExcludeSetParams {
            scopes,
            filters: vec![],
        }
    }

    fn make_clear_params(scopes: Vec<ScopeMatch>) -> ExcludeClearParams {
        ExcludeClearParams {
            scopes,
            filters: vec![],
        }
    }

    /// Minimal ExcludeItemData for execute tests (only source_id is consumed).
    fn item(source_id: i64, root: &str, rel_path: &str) -> ExcludeItemData {
        ExcludeItemData {
            source_id,
            root: root.to_string(),
            rel_path: rel_path.to_string(),
            hash: None,
            size: 1000,
            mtime: 1704067200,
            previous_decision_id: None,
        }
    }

    // =========================================================================
    // plan_set() tests
    // =========================================================================

    #[test]
    fn test_plan_set_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.root_count, 0);
        assert_eq!(plan.not_archived_count, 0);
    }

    #[test]
    fn test_plan_set_excludes_already_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let _id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.source_ids(), vec![id1]);
    }

    #[test]
    fn test_plan_set_skips_object_level_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_obj = insert_object(&conn, "abc123", true);
        let _id = insert_source(&conn, root, "a.jpg", Some(excluded_obj));

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        // Object-level excluded sources are filtered out by select_sources()
        assert!(plan.source_ids().is_empty());
    }

    #[test]
    fn test_plan_set_counts_roots() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/root1", "source", false);
        let root2 = insert_root(&conn, "/root2", "source", false);
        insert_source(&conn, root1, "a.jpg", None);
        insert_source(&conn, root2, "b.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.root_count, 2);
    }

    #[test]
    fn test_plan_set_archive_coverage() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // Object that IS archived
        let archived_obj = insert_object(&conn, "archived_hash", false);
        insert_source(&conn, archive_root, "copy.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "file1.jpg", Some(archived_obj));

        // Object that is NOT archived
        let unarchived_obj = insert_object(&conn, "unarchived_hash", false);
        insert_source(&conn, source_root, "file2.jpg", Some(unarchived_obj));

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.source_ids().len(), 2);
        assert_eq!(plan.not_archived_count, 1, "Only the unarchived source");
    }

    #[test]
    fn test_plan_set_unhashed_not_archived() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        insert_source(&conn, root, "unhashed.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(
            plan.not_archived_count, 1,
            "Unhashed counts as not archived"
        );
    }

    #[test]
    fn test_plan_set_includes_paths() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "subdir/a.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.paths(), vec!["/photos/subdir/a.jpg"]);
    }

    #[test]
    fn test_plan_set_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope = insert_source(&conn, root, "2024/a.jpg", None);
        let _out_scope = insert_source(&conn, root, "2023/b.jpg", None);

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_set(&mut conn, &make_set_params(scopes)).unwrap();

        assert_eq!(plan.source_ids(), vec![in_scope]);
    }

    // =========================================================================
    // plan items carry receipt data + accessor regression
    // =========================================================================

    #[test]
    fn test_plan_set_items_carry_receipt_data() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash", false);
        let id = insert_source(&conn, root, "a.jpg", Some(obj));
        // A prior decision on the source becomes previous_decision_id.
        conn.execute(
            "UPDATE sources SET decision_id = 7 WHERE id = ?",
            rusqlite::params![id],
        )
        .unwrap();

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.items.len(), 1);
        let it = &plan.items[0];
        assert_eq!(it.source_id, id);
        assert_eq!(it.hash.as_deref(), Some("sha256:abc123hash"));
        assert_eq!(it.size, 1000);
        assert_eq!(it.mtime, 1704067200);
        assert_eq!(it.previous_decision_id, Some(7));
    }

    #[test]
    fn test_plan_set_item_unhashed_has_no_hash() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "a.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        assert_eq!(plan.items[0].hash, None);
        assert_eq!(plan.items[0].previous_decision_id, None);
    }

    #[test]
    fn test_plan_clear_items_carry_receipt_data() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "clearhashval", false);
        let id = insert_source_excluded(&conn, root, "sub/a.jpg", Some(obj));
        conn.execute(
            "UPDATE sources SET decision_id = 3 WHERE id = ?",
            rusqlite::params![id],
        )
        .unwrap();

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.items.len(), 1);
        let it = &plan.items[0];
        assert_eq!(it.hash.as_deref(), Some("sha256:clearhashval"));
        assert_eq!(it.previous_decision_id, Some(3));
        assert_eq!(it.path(), "/photos/sub/a.jpg");
    }

    #[test]
    fn test_plan_accessors_derive_from_items() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "sub/b.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

        // Accessors derive, in order, from items (the regression guard for the
        // old parallel source_ids/paths vectors).
        let expected_ids: Vec<i64> = plan.items.iter().map(|i| i.source_id).collect();
        let expected_paths: Vec<String> = plan.items.iter().map(|i| i.path()).collect();
        assert_eq!(plan.source_ids(), expected_ids);
        assert_eq!(plan.paths(), expected_paths);
        assert!(plan.source_ids().contains(&id1));
        assert!(plan.source_ids().contains(&id2));
    }

    // =========================================================================
    // plan_clear() tests
    // =========================================================================

    #[test]
    fn test_plan_clear_returns_source_level_only() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_id = insert_source_excluded(&conn, root, "excluded.jpg", None);
        let _normal_id = insert_source(&conn, root, "normal.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.source_ids(), vec![excluded_id]);
    }

    #[test]
    fn test_plan_clear_ignores_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);

        // Source NOT excluded, but object IS excluded
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj));

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(
            plan.source_ids().is_empty(),
            "Object-level excluded sources should NOT appear"
        );
    }

    #[test]
    fn test_plan_clear_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope = insert_source_excluded(&conn, root, "2024/excluded.jpg", None);
        let _out_scope = insert_source_excluded(&conn, root, "2023/excluded.jpg", None);

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_clear(&mut conn, &make_clear_params(scopes)).unwrap();

        assert_eq!(plan.source_ids(), vec![in_scope]);
    }

    #[test]
    fn test_plan_clear_returns_paths() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source_excluded(&conn, root, "subdir/excluded.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.paths(), vec!["/photos/subdir/excluded.jpg"]);
    }

    #[test]
    fn test_plan_clear_counts_roots() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/root1", "source", false);
        let root2 = insert_root(&conn, "/root2", "source", false);
        insert_source_excluded(&conn, root1, "a.jpg", None);
        insert_source_excluded(&conn, root2, "b.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert_eq!(plan.root_count, 2);
    }

    #[test]
    fn test_plan_clear_empty_when_none_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "normal.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.root_count, 0);
    }

    #[test]
    fn test_plan_clear_ignores_suspended_roots() {
        let mut conn = setup_test_db();
        let _suspended = insert_root(&conn, "/suspended", "source", true);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, 'excluded.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 1)",
            rusqlite::params![_suspended],
        )
        .unwrap();

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids().is_empty());
    }

    #[test]
    fn test_plan_clear_ignores_archive_roots() {
        let mut conn = setup_test_db();
        let archive = insert_root(&conn, "/archive", "archive", false);
        insert_source_excluded(&conn, archive, "archived.jpg", None);

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

        assert!(plan.source_ids().is_empty());
    }

    // =========================================================================
    // execute tests
    // =========================================================================

    #[test]
    fn test_execute_set_marks_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "b.jpg", None);

        let plan = ExcludeSetPlan {
            items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
            root_count: 1,
            not_archived_count: 2,
        };

        execute_set(&mut conn, &plan, None, None).unwrap();

        assert!(is_source_excluded(&conn, id1));
        assert!(is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_clear_clears_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
        let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = ExcludeClearPlan {
            items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
            root_count: 1,
        };

        execute_clear(&mut conn, &plan, None, None).unwrap();

        assert!(!is_source_excluded(&conn, id1));
        assert!(!is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_set_returns_count() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);

        let plan = ExcludeSetPlan {
            items: vec![item(id1, "/photos", "a.jpg")],
            root_count: 1,
            not_archived_count: 1,
        };

        let result = execute_set(&mut conn, &plan, None, None).unwrap();
        assert_eq!(result.count, 1);
    }

    #[test]
    fn test_execute_clear_returns_count() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
        let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

        let plan = ExcludeClearPlan {
            items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
            root_count: 1,
        };

        let result = execute_clear(&mut conn, &plan, None, None).unwrap();
        assert_eq!(result.count, 2);
    }

    // =========================================================================
    // plan_duplicates() tests
    // =========================================================================

    fn make_duplicates_params(
        scopes: Vec<ScopeMatch>,
        prefer_prefix: &str,
    ) -> ExcludeDuplicatesParams {
        ExcludeDuplicatesParams {
            scopes,
            filters: vec![],
            prefer_prefix: prefer_prefix.to_string(),
        }
    }

    #[test]
    fn test_plan_duplicates_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/source", "source", false);

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.scope_count, 0);
        assert_eq!(plan.group_count, 0);
    }

    #[test]
    fn test_plan_duplicates_excludes_with_prefer_copy() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "same_hash", false);
        let source_id = insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.source_ids(), vec![source_id]);
        assert_eq!(plan.scope_count, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_no_copy() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let _archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "unique_hash", false);
        insert_source(&conn, source_root, "unique.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.skipped_not_covered, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_multiple_copies() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "multi_hash", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "copy1.jpg", Some(obj));
        insert_source(&conn, archive_root, "copy2.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.skipped_multiple, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_unhashed() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);

        insert_source(&conn, source_root, "unhashed.jpg", None);

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.skipped_no_hash, 1);
        assert_eq!(plan.scope_count, 1);
    }

    #[test]
    fn test_plan_duplicates_skips_in_prefer() {
        let mut conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "source", false);

        let obj = insert_object(&conn, "prefer_hash", false);
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        // Source is in the prefer path itself — should be skipped
        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert!(plan.source_ids().is_empty());
        assert_eq!(plan.skipped_in_prefer, 1);
    }

    #[test]
    fn test_plan_duplicates_computes_group_count() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "group1_hash", false);
        let obj2 = insert_object(&conn, "group2_hash", false);

        // 2 sources for obj1, 2 sources for obj2
        insert_source(&conn, source_root, "a/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "b/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "a/photo2.jpg", Some(obj2));
        insert_source(&conn, source_root, "b/photo2.jpg", Some(obj2));

        // 1 copy each in archive
        insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.source_ids().len(), 4);
        assert_eq!(plan.group_count, 2, "2 distinct object groups");
    }

    #[test]
    fn test_plan_duplicates_includes_paths() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "path_hash", false);
        insert_source(&conn, source_root, "subdir/photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.paths(), vec!["/source/subdir/photo.jpg"]);
    }

    #[test]
    fn test_plan_duplicates_scope_count() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj = insert_object(&conn, "scope_hash", false);
        insert_source(&conn, source_root, "a.jpg", Some(obj));
        insert_source(&conn, source_root, "b.jpg", None); // unhashed
        insert_source(&conn, archive_root, "a.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(
            plan.scope_count, 2,
            "Both sources in scope (before analysis)"
        );
        assert_eq!(
            plan.source_ids().len(),
            1,
            "Only hashed with prefer copy excluded"
        );
    }

    #[test]
    fn test_plan_duplicates_respects_scope() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj1 = insert_object(&conn, "in_scope_hash", false);
        let obj2 = insert_object(&conn, "out_scope_hash", false);

        let in_scope = insert_source(&conn, source_root, "2024/photo.jpg", Some(obj1));
        insert_source(&conn, source_root, "2023/photo.jpg", Some(obj2));
        insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

        let scopes = ScopeMatch::classify_all(&["/source/2024".to_string()]);
        let plan = plan_duplicates(&mut conn, &make_duplicates_params(scopes, "/archive")).unwrap();

        assert_eq!(plan.source_ids(), vec![in_scope]);
        assert_eq!(plan.scope_count, 1);
    }

    // =========================================================================
    // plan_duplicates group reconstruction
    // =========================================================================

    #[test]
    fn test_plan_duplicates_reconstructs_group() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "dup_hash_value", false);
        let src_id = insert_source(&conn, source_root, "photo.jpg", Some(obj));
        let arch_id = insert_source(&conn, archive_root, "kept.jpg", Some(obj));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.group_count, 1);
        let g = &plan.groups[0];
        assert_eq!(g.hash, "sha256:dup_hash_value");
        // excluded = the source-root copy (not under the prefer prefix)
        assert_eq!(
            g.excluded.iter().map(|i| i.source_id).collect::<Vec<_>>(),
            vec![src_id]
        );
        // kept = the archive copy under the prefer prefix (no transition)
        assert_eq!(
            g.kept.iter().map(|i| i.source_id).collect::<Vec<_>>(),
            vec![arch_id]
        );
        assert_eq!(g.kept[0].path(), "/archive/kept.jpg");
        // accessors derive from groups[*].excluded
        assert_eq!(plan.source_ids(), vec![src_id]);
        assert_eq!(plan.paths(), vec!["/source/photo.jpg"]);
    }

    #[test]
    fn test_plan_duplicates_groups_by_object() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "group1_hash", false);
        let obj2 = insert_object(&conn, "group2_hash", false);
        // Two source-root copies per object, one archive copy each.
        insert_source(&conn, source_root, "a/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "b/photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "a/photo2.jpg", Some(obj2));
        insert_source(&conn, source_root, "b/photo2.jpg", Some(obj2));
        insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

        assert_eq!(plan.groups.len(), 2, "one group per object");
        for g in &plan.groups {
            assert_eq!(g.excluded.len(), 2, "both source-root copies excluded");
            assert_eq!(g.kept.len(), 1, "single archive copy kept");
        }
        // 4 excluded across both groups
        assert_eq!(plan.source_ids().len(), 4);
    }

    // =========================================================================
    // execute_duplicates() tests
    // =========================================================================

    #[test]
    fn test_execute_duplicates_marks_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);
        let id2 = insert_source(&conn, root, "b.jpg", None);

        let plan = ExcludeDuplicatesPlan {
            groups: vec![DuplicateGroupData {
                hash: "sha256:dup".to_string(),
                kept: vec![],
                excluded: vec![item(id1, "/source", "a.jpg"), item(id2, "/source", "b.jpg")],
            }],
            group_count: 1,
            prefer_prefix: "/archive".to_string(),
            scope_count: 2,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        };

        execute_duplicates(&mut conn, &plan, None, None).unwrap();

        assert!(is_source_excluded(&conn, id1));
        assert!(is_source_excluded(&conn, id2));
    }

    #[test]
    fn test_execute_duplicates_returns_count() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/source", "source", false);
        let id1 = insert_source(&conn, root, "a.jpg", None);

        let plan = ExcludeDuplicatesPlan {
            groups: vec![DuplicateGroupData {
                hash: "sha256:dup".to_string(),
                kept: vec![],
                excluded: vec![item(id1, "/source", "a.jpg")],
            }],
            group_count: 1,
            prefer_prefix: "/archive".to_string(),
            scope_count: 1,
            skipped_no_hash: 0,
            skipped_in_prefer: 0,
            skipped_not_covered: 0,
            skipped_multiple: 0,
        };

        let result = execute_duplicates(&mut conn, &plan, None, None).unwrap();
        assert_eq!(result.count, 1);
    }

    // =========================================================================
    // plan_set_objects() tests
    // =========================================================================

    use crate::ops::test_helpers::{insert_source_with_size, is_object_excluded};

    fn make_set_objects_params(scopes: Vec<ScopeMatch>) -> ExcludeSetObjectsParams {
        ExcludeSetObjectsParams {
            scopes,
            filters: vec![],
        }
    }

    #[test]
    fn test_plan_set_objects_empty_when_no_sources() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.total_source_count, 0);
        assert_eq!(plan.total_archive_count, 0);
    }

    #[test]
    fn test_plan_set_objects_includes_non_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash_value_x", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.objects[0].object_id, obj);
    }

    #[test]
    fn test_plan_set_objects_skips_already_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "already_excl_hash", true);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_already_excluded, 1);
    }

    #[test]
    fn test_plan_set_objects_skips_unhashed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "unhashed.jpg", None);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_no_hash, 1);
    }

    #[test]
    fn test_plan_set_objects_skips_empty() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "empty_hash_value_x", false);
        insert_source_with_size(&conn, root, "empty.txt", Some(obj), 0);

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert!(plan.objects.is_empty());
        assert_eq!(plan.skipped_empty, 1);
    }

    #[test]
    fn test_plan_set_objects_computes_source_counts() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "counts_hash_value", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.total_source_count, 2);
        assert_eq!(plan.total_archive_count, 1);
    }

    #[test]
    fn test_plan_set_objects_hash_prefix() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abcdef1234567890extra", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects[0].hash_prefix, "abcdef1234567890");
    }

    #[test]
    fn test_plan_set_objects_respects_scope() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "in_scope_obj_hash", false);
        let obj2 = insert_object(&conn, "out_scope_obj_hsh", false);
        insert_source(&conn, root, "2024/photo.jpg", Some(obj1));
        insert_source(&conn, root, "2023/photo.jpg", Some(obj2));

        let scopes = ScopeMatch::classify_all(&["/photos/2024".to_string()]);
        let plan = plan_set_objects(&mut conn, &make_set_objects_params(scopes)).unwrap();

        assert_eq!(plan.objects.len(), 1);
        assert_eq!(plan.objects[0].object_id, obj1);
    }

    #[test]
    fn test_plan_set_objects_deduplicates_objects() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "shared_obj_hash_xx", false);
        // Two sources sharing the same object
        insert_source(&conn, root, "copy1.jpg", Some(obj));
        insert_source(&conn, root, "copy2.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        assert_eq!(plan.objects.len(), 1, "Same object should appear once");
    }

    #[test]
    fn test_plan_set_objects_source_sort_order() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "sort_order_hash_xx", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

        let sources = &plan.objects[0].sources;
        assert_eq!(sources.len(), 2);
        // Source roots come first (role DESC: 'source' > 'archive')
        assert!(!sources[0].is_archive, "Source root should come first");
        assert!(sources[1].is_archive, "Archive root should come second");
    }

    // =========================================================================
    // execute_set_objects() tests
    // =========================================================================

    #[test]
    fn test_execute_set_objects_marks_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "exec_obj_hash1_xx", false);
        let obj2 = insert_object(&conn, "exec_obj_hash2_xx", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, root, "b.jpg", Some(obj2));

        let plan = ExcludeSetObjectsPlan {
            objects: vec![
                ObjectPlanEntry {
                    object_id: obj1,
                    hash_prefix: "exec_obj_hash1_x".to_string(),
                    hash: "sha256:exec_obj_hash1_xx".to_string(),
                    sources: vec![],
                },
                ObjectPlanEntry {
                    object_id: obj2,
                    hash_prefix: "exec_obj_hash2_x".to_string(),
                    hash: "sha256:exec_obj_hash2_xx".to_string(),
                    sources: vec![],
                },
            ],
            total_source_count: 2,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        };

        execute_set_objects(&mut conn, &plan, None, None).unwrap();

        assert!(is_object_excluded(&conn, obj1));
        assert!(is_object_excluded(&conn, obj2));
    }

    #[test]
    fn test_execute_set_objects_returns_count() {
        let mut conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "count_obj_hash_xxx", false);

        let plan = ExcludeSetObjectsPlan {
            objects: vec![ObjectPlanEntry {
                object_id: obj,
                hash_prefix: "count_obj_hash_x".to_string(),
                hash: "sha256:count_obj_hash_xxx".to_string(),
                sources: vec![],
            }],
            total_source_count: 1,
            total_archive_count: 0,
            skipped_no_hash: 0,
            skipped_empty: 0,
            skipped_already_excluded: 0,
        };

        let result = execute_set_objects(&mut conn, &plan, None, None).unwrap();
        assert_eq!(result.count, 1);
    }

    // =========================================================================
    // check_set_source_by_id() tests
    // =========================================================================

    #[test]
    fn test_check_source_by_id_ready() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root, "photo.jpg", None);

        let result = check_set_source_by_id(&conn, id).unwrap();
        match result {
            SourceExclusionCheck::Ready { item } => {
                assert_eq!(item.source_id, id);
                assert_eq!(item.path(), "/photos/photo.jpg");
            }
            SourceExclusionCheck::AlreadyExcluded { .. } => {
                panic!("Expected Ready, got AlreadyExcluded");
            }
        }
    }

    #[test]
    fn test_check_source_by_id_already_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source_excluded(&conn, root, "photo.jpg", None);

        let result = check_set_source_by_id(&conn, id).unwrap();
        match result {
            SourceExclusionCheck::AlreadyExcluded { path } => {
                assert_eq!(path, "/photos/photo.jpg");
            }
            SourceExclusionCheck::Ready { .. } => {
                panic!("Expected AlreadyExcluded, got Ready");
            }
        }
    }

    #[test]
    fn test_check_source_by_id_not_found() {
        let conn = setup_test_db();

        let result = check_set_source_by_id(&conn, 99999);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found"),
            "Error should mention 'not found', got: {err}"
        );
    }

    #[test]
    fn test_check_source_by_id_not_present() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        // Not present (batch_fetch_by_ids filters these out)
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
             VALUES (?, 'deleted.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 0)",
            rusqlite::params![root],
        )
        .unwrap();
        let source_id = conn.last_insert_rowid();

        let result = check_set_source_by_id(&conn, source_id);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found") || err.contains("not present"),
            "Error should mention not found/present, got: {err}"
        );
    }

    // =========================================================================
    // check_set_source_by_path() tests
    // =========================================================================

    #[test]
    fn test_check_source_by_path_ready() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root, "photo.jpg", None);

        let result =
            check_set_source_by_path(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
        match result {
            SourceExclusionCheck::Ready { item } => {
                assert_eq!(item.source_id, id);
                assert_eq!(item.path(), "/photos/photo.jpg");
            }
            SourceExclusionCheck::AlreadyExcluded { .. } => {
                panic!("Expected Ready, got AlreadyExcluded");
            }
        }
    }

    #[test]
    fn test_check_source_by_path_not_found() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);

        let result =
            check_set_source_by_path(&conn, root, "nonexistent.jpg", "/photos/nonexistent.jpg");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No source found"),
            "Error should mention no source found, got: {err}"
        );
    }

    #[test]
    fn test_check_source_by_path_already_excluded() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source_excluded(&conn, root, "photo.jpg", None);

        let result =
            check_set_source_by_path(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
        match result {
            SourceExclusionCheck::AlreadyExcluded { path } => {
                assert_eq!(path, "/photos/photo.jpg");
            }
            SourceExclusionCheck::Ready { .. } => {
                panic!("Expected AlreadyExcluded, got Ready");
            }
        }
    }

    // =========================================================================
    // check_set_object_by_hash() tests
    // =========================================================================

    #[test]
    fn test_check_object_by_hash_ready() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash_value_x", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let result = check_set_object_by_hash(&conn, "abc123hash_value_x").unwrap();
        match result {
            ObjectExclusionCheck::Ready {
                object_id,
                hash_prefix,
                hash: _,
                sources,
            } => {
                assert_eq!(object_id, obj);
                assert_eq!(hash_prefix, "abc123hash_value");
                assert_eq!(sources.len(), 1);
                assert_eq!(sources[0].path, "/photos/photo.jpg");
            }
            ObjectExclusionCheck::AlreadyExcluded { .. } => {
                panic!("Expected Ready, got AlreadyExcluded");
            }
        }
    }

    #[test]
    fn test_check_object_by_hash_not_found() {
        let conn = setup_test_db();

        let result = check_set_object_by_hash(&conn, "nonexistent_hash");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No object found"),
            "Error should mention no object found, got: {err}"
        );
    }

    #[test]
    fn test_check_object_by_hash_already_excluded() {
        let conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);
        let _obj = insert_object(&conn, "excluded_hash_val_", true);

        let result = check_set_object_by_hash(&conn, "excluded_hash_val_").unwrap();
        match result {
            ObjectExclusionCheck::AlreadyExcluded { hash_prefix } => {
                assert_eq!(hash_prefix, "excluded_hash_va");
            }
            ObjectExclusionCheck::Ready { .. } => {
                panic!("Expected AlreadyExcluded, got Ready");
            }
        }
    }

    // =========================================================================
    // check_set_object_by_file() tests
    // =========================================================================

    #[test]
    fn test_check_object_by_file_ready() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "file_obj_hash_val_", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let result =
            check_set_object_by_file(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
        match result {
            ObjectExclusionCheck::Ready {
                object_id,
                hash_prefix,
                hash: _,
                sources,
            } => {
                assert_eq!(object_id, obj);
                assert_eq!(hash_prefix, "file_obj_hash_va");
                assert_eq!(sources.len(), 1);
            }
            ObjectExclusionCheck::AlreadyExcluded { .. } => {
                panic!("Expected Ready, got AlreadyExcluded");
            }
        }
    }

    #[test]
    fn test_check_object_by_file_not_found() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);

        let result = check_set_object_by_file(&conn, root, "missing.jpg", "/photos/missing.jpg");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No hashed source found"),
            "Error should mention no hashed source, got: {err}"
        );
    }

    #[test]
    fn test_check_object_by_file_unhashed() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "unhashed.jpg", None);

        let result = check_set_object_by_file(&conn, root, "unhashed.jpg", "/photos/unhashed.jpg");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No hashed source found"),
            "Error should mention no hashed source, got: {err}"
        );
    }

    #[test]
    fn test_check_object_by_file_empty() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "empty_file_hash_v_", false);
        insert_source_with_size(&conn, root, "empty.txt", Some(obj), 0);

        let result = check_set_object_by_file(&conn, root, "empty.txt", "/photos/empty.txt");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Cannot exclude empty file"),
            "Error should mention empty file, got: {err}"
        );
    }

    // =========================================================================
    // check_clear_object() tests
    // =========================================================================

    #[test]
    fn test_check_clear_object_ready() {
        let conn = setup_test_db();
        let obj = insert_object(&conn, "clear_ready_hash_v", true);

        let result = check_clear_object(&conn, "clear_ready_hash_v").unwrap();
        match result {
            ObjectClearCheck::Ready {
                object_id,
                hash_prefix,
                hash: _,
            } => {
                assert_eq!(object_id, obj);
                assert_eq!(hash_prefix, "clear_ready_hash");
            }
            ObjectClearCheck::NotExcluded { .. } => {
                panic!("Expected Ready, got NotExcluded");
            }
        }
    }

    #[test]
    fn test_check_clear_object_not_found() {
        let conn = setup_test_db();

        let result = check_clear_object(&conn, "nonexistent_hash");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No object found"),
            "Error should mention no object found, got: {err}"
        );
    }

    #[test]
    fn test_check_clear_object_not_excluded() {
        let conn = setup_test_db();
        let _obj = insert_object(&conn, "not_excluded_hash_", false);

        let result = check_clear_object(&conn, "not_excluded_hash_").unwrap();
        match result {
            ObjectClearCheck::NotExcluded { hash_prefix } => {
                assert_eq!(hash_prefix, "not_excluded_has");
            }
            ObjectClearCheck::Ready { .. } => {
                panic!("Expected NotExcluded, got Ready");
            }
        }
    }

    // =========================================================================
    // fetch_object_sources() tests
    // =========================================================================

    #[test]
    fn test_fetch_object_sources_returns_paths() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "fetch_src_hash_val", false);
        insert_source(&conn, root, "2024/photo.jpg", Some(obj));

        let sources = fetch_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].path, "/photos/2024/photo.jpg");
    }

    #[test]
    fn test_fetch_object_sources_includes_role() {
        let conn = setup_test_db();
        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "role_src_hash_val_", false);
        insert_source(&conn, source_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive_root, "photo.jpg", Some(obj));

        let sources = fetch_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 2);
        // Source roots come first (role DESC: 'source' > 'archive')
        assert!(!sources[0].is_archive, "Source root should come first");
        assert!(sources[1].is_archive, "Archive root should come second");
    }

    #[test]
    fn test_fetch_object_sources_empty_rel_path() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/archive/photo.jpg", "archive", false);
        let obj = insert_object(&conn, "empty_rel_hash_val", false);
        insert_source(&conn, root, "", Some(obj));

        let sources = fetch_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].path, "/archive/photo.jpg");
    }

    #[test]
    fn test_fetch_object_sources_excludes_not_present() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "present_hash_val_x", false);
        insert_source(&conn, root, "present.jpg", Some(obj));
        // Not present source
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
             VALUES (?, 'deleted.jpg', ?, 1000, 1704067200, '', 0, 0, 0, 0, 0)",
            rusqlite::params![root, obj],
        )
        .unwrap();

        let sources = fetch_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].path, "/photos/present.jpg");
    }

    // =========================================================================
    // list_excluded_objects() tests
    // =========================================================================

    #[test]
    fn test_list_excluded_objects_returns_entries() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "list_excl_hash_val", true);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let entries = list_excluded_objects(&conn).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object_id, obj);
        assert_eq!(entries[0].hash_prefix, "list_excl_hash_v");
        assert_eq!(entries[0].source_count, 1);
    }

    #[test]
    fn test_list_excluded_objects_source_counts() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "multi_src_hash_val", true);
        insert_source(&conn, root, "photo1.jpg", Some(obj));
        insert_source(&conn, root, "photo2.jpg", Some(obj));
        // Not present — still counted by fetch_sources_by_object_ids
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
             VALUES (?, 'deleted.jpg', ?, 1000, 1704067200, '', 0, 0, 0, 0, 0)",
            rusqlite::params![root, obj],
        )
        .unwrap();

        let entries = list_excluded_objects(&conn).unwrap();

        assert_eq!(entries.len(), 1);
        // fetch_sources_by_object_ids returns present sources only
        // (the repo function filters for present=1)
        assert!(entries[0].source_count >= 2);
    }

    #[test]
    fn test_list_excluded_objects_empty() {
        let conn = setup_test_db();
        let _root = insert_root(&conn, "/photos", "source", false);
        let _obj = insert_object(&conn, "not_excl_hash_val_", false);

        let entries = list_excluded_objects(&conn).unwrap();

        assert!(entries.is_empty());
    }

    // =========================================================================
    // execute_set_object tests
    // =========================================================================

    #[test]
    fn test_execute_set_object_excludes_and_returns_summary() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abcdef1234567890", false);
        let _src = insert_source(&conn, root, "a.jpg", Some(obj_id));

        let sources = fetch_object_sources(&conn, obj_id).unwrap();
        let result = execute_set_object(
            &mut conn,
            obj_id,
            "abcdef1234567890",
            "sha256:abcdef1234567890",
            &sources,
            None,
            None,
        )
        .unwrap();

        assert_eq!(result.object_id, obj_id);
        assert_eq!(result.summary, "Excluded object: abcdef1234567890...");
        assert_eq!(result.source_count, 1);

        // Verify actually excluded in DB
        let objects = crate::repo::object::batch_fetch_by_ids(&conn, &[obj_id]).unwrap();
        assert!(objects.get(&obj_id).unwrap().is_excluded());
    }

    #[test]
    fn test_execute_set_object_summary_includes_hash_prefix() {
        let mut conn = setup_test_db();
        let obj_id = insert_object(&conn, "deadbeef12345678", false);

        let result = execute_set_object(
            &mut conn,
            obj_id,
            "deadbeef12345678",
            "sha256:deadbeef12345678",
            &[],
            None,
            None,
        )
        .unwrap();

        assert!(result.summary.contains("deadbeef12345678"));
    }

    // =========================================================================
    // execute_clear_object tests
    // =========================================================================

    #[test]
    fn test_execute_clear_object_clears_and_returns_summary() {
        let mut conn = setup_test_db();
        let obj_id = insert_object(&conn, "abcdef1234567890", true); // already excluded

        let result = execute_clear_object(
            &mut conn,
            obj_id,
            "abcdef1234567890",
            "sha256:abcdef1234567890",
            None,
            None,
        )
        .unwrap();

        assert_eq!(result.object_id, obj_id);
        assert_eq!(
            result.summary,
            "Cleared exclusion from object: abcdef1234567890..."
        );

        // Verify no longer excluded in DB
        let objects = crate::repo::object::batch_fetch_by_ids(&conn, &[obj_id]).unwrap();
        assert!(!objects.get(&obj_id).unwrap().is_excluded());
    }

    // =========================================================================
    // Phase 3: receipt writing + decision_id linkage at the ledger root
    // =========================================================================

    use crate::domain::config::LedgerConfig;
    use crate::domain::decision::DecisionCommand;
    use tempfile::{tempdir, TempDir};

    /// A recording-enabled decision (DB record + receipt file).
    fn full_decision(command: DecisionCommand) -> DecisionParams {
        DecisionParams {
            command,
            scope: None,
            command_line: "canon test".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: LedgerConfig::default(),
        }
    }

    /// Create an active archive root rooted at a temp dir, returning its id, the
    /// dir handle (kept alive), and a `LedgerRoot` placement pointing at it.
    fn ledger_root(conn: &Connection) -> (i64, TempDir, ReceiptPlacement) {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let root_id = insert_root(conn, &path, "archive", false);
        let placement = ReceiptPlacement::LedgerRoot {
            root_id,
            root_path: path,
        };
        (root_id, dir, placement)
    }

    fn fetch_source_decision_id(conn: &Connection, source_id: i64) -> Option<i64> {
        conn.query_row(
            "SELECT decision_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn run_exclusion_rolls_back_on_error() {
        // The write-path-atomicity guarantee: a failure inside `mutate` rolls
        // back BOTH the source flip and the `started` decision row — no half-state.
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root, "a.jpg", None);

        let decision = DecisionParams {
            command: DecisionCommand::ExcludeSet,
            scope: None,
            command_line: "canon test".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };

        let result = run_exclusion::<ExcludeReceipt, _>(
            &mut conn,
            Some(&decision),
            None,
            true,
            counts_all(1),
            "test",
            |tx, decision_id| {
                // Flip the source inside the transaction, then fail.
                repo::source::set_excluded(tx, id, true, decision_id)?;
                anyhow::bail!("forced failure mid-flip");
            },
        );

        assert!(result.is_err(), "the forced error should propagate");
        assert!(
            !is_source_excluded(&conn, id),
            "the source flip must roll back on error"
        );
        let decisions: i64 = conn
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(decisions, 0, "the started decision row must roll back");
    }

    #[test]
    fn decision_scopes_populated_for_scoped_exclude() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root, "a.jpg", None);
        let plan = ExcludeSetPlan {
            items: vec![item(id, "/photos", "a.jpg")],
            root_count: 1,
            not_archived_count: 1,
        };
        let decision = DecisionParams {
            command: DecisionCommand::ExcludeSet,
            scope: Some(vec!["/photos".to_string()]),
            command_line: "canon exclude set /photos".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };

        execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

        let (rid, prefix): (i64, String) = conn
            .query_row("SELECT root_id, rel_prefix FROM decision_scopes", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(rid, root);
        assert_eq!(prefix, "", "root-level scope has empty rel_prefix");
    }

    #[test]
    fn decision_scopes_empty_for_global_exclude() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root, "a.jpg", None);
        let plan = ExcludeSetPlan {
            items: vec![item(id, "/photos", "a.jpg")],
            root_count: 1,
            not_archived_count: 1,
        };
        // scope: None → global → no scope-index rows.
        let decision = DecisionParams {
            command: DecisionCommand::ExcludeSet,
            scope: None,
            command_line: "canon exclude set --global".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
        };

        execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM decision_scopes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_execute_set_writes_receipt_and_links_decision() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "rcpt_hash_val", false);
        let id = insert_source(&conn, src_root, "junk/a.tmp", Some(obj));

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSet);
        let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        // Decision linked on the excluded source.
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));

        // Receipt landed flat at the ledger root with matching content.
        let receipt = dir.path().join(".canon-ledger/000001-exclude_set.toml");
        let content = std::fs::read_to_string(&receipt).unwrap();
        assert!(content.contains("[[items]]"));
        assert!(content.contains("rel_path = \"junk/a.tmp\""));
        assert!(content.contains("hash = \"sha256:rcpt_hash_val\""));
        assert!(content.contains("command = \"exclude_set\""));
    }

    #[test]
    fn test_execute_set_no_placement_records_decision_without_receipt() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/source", "source", false);
        let id = insert_source(&conn, src_root, "a.jpg", None);

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSet);
        // No archive root → placement None → no receipt, but decision still recorded.
        let result = execute_set(&mut conn, &plan, None, Some(&decision)).unwrap();

        assert!(result.warnings.is_empty());
        assert!(is_source_excluded(&conn, id));
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "decision recorded even without a receipt");
    }

    #[test]
    fn test_execute_set_receipt_failure_surfaces_warning() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/source", "source", false);
        let id = insert_source(&conn, src_root, "a.jpg", None);

        // A file where the ledger directory needs to be → dir creation fails.
        let dir = tempdir().unwrap();
        let blocker = dir.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let placement = ReceiptPlacement::LedgerRoot {
            root_id: src_root,
            root_path: blocker.to_str().unwrap().to_string(),
        };

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSet);
        let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        // Exclusion still happens; the receipt failure is surfaced, not silent.
        assert!(is_source_excluded(&conn, id));
        assert!(!result.warnings.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.to_lowercase().contains("receipt")));
    }

    #[test]
    fn test_execute_duplicates_writes_grouped_receipt() {
        let mut conn = setup_test_db();
        let (archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "dup_rcpt_hash", false);
        let excluded_id = insert_source(&conn, src_root, "photo.jpg", Some(obj));
        insert_source(&conn, archive, "kept.jpg", Some(obj));

        let prefer = dir.path().to_str().unwrap().to_string();
        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], &prefer)).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeDuplicates);
        let result =
            execute_duplicates(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        assert!(is_source_excluded(&conn, excluded_id));

        let receipt = dir
            .path()
            .join(".canon-ledger/000001-exclude_duplicates.toml");
        let content = std::fs::read_to_string(&receipt).unwrap();
        assert!(content.contains("[[groups]]"));
        assert!(content.contains("hash = \"sha256:dup_rcpt_hash\""));
        assert!(content.contains("[[groups.kept]]"));
        assert!(content.contains("[[groups.excluded]]"));
        assert!(content.contains("rel_path = \"photo.jpg\""));
        assert!(content.contains("rel_path = \"kept.jpg\""));
    }

    #[test]
    fn test_execute_set_objects_writes_object_receipt_and_links_all_sources() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "obj_rcpt_hash", false);
        let a = insert_source(&conn, src_root, "copy1.bin", Some(obj));
        let b = insert_source(&conn, src_root, "copy2.bin", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSetObject);
        let result =
            execute_set_objects(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        // decision_id stamped on every source sharing the object.
        assert_eq!(fetch_source_decision_id(&conn, a), Some(1));
        assert_eq!(fetch_source_decision_id(&conn, b), Some(1));

        let receipt = dir
            .path()
            .join(".canon-ledger/000001-exclude_set_object.toml");
        let content = std::fs::read_to_string(&receipt).unwrap();
        assert!(content.contains("[[objects]]"));
        assert!(content.contains("hash = \"sha256:obj_rcpt_hash\""));
        assert!(content.contains("[[objects.sources]]"));
        assert!(content.contains("rel_path = \"copy1.bin\""));
        assert!(content.contains("rel_path = \"copy2.bin\""));
    }

    // =========================================================================
    // Phase 4: decision_id linkage + provenance chain
    // =========================================================================

    #[test]
    fn test_execute_clear_links_decision_and_writes_receipt() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "clear_link_hash", false);
        let id = insert_source_excluded(&conn, src_root, "sub/a.tmp", Some(obj));

        let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeClear);
        let result = execute_clear(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        // Clear is itself a transition: source un-excluded AND decision_id stamped.
        assert!(!is_source_excluded(&conn, id));
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));

        let content =
            std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_clear.toml"))
                .unwrap();
        assert!(content.contains("command = \"exclude_clear\""));
        assert!(content.contains("[[items]]"));
        assert!(content.contains("rel_path = \"sub/a.tmp\""));
    }

    #[test]
    fn test_execute_duplicates_links_excluded_not_kept() {
        let mut conn = setup_test_db();
        let (archive, _dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "dup_link_hash", false);
        let excluded_id = insert_source(&conn, src_root, "photo.jpg", Some(obj));
        let kept_id = insert_source(&conn, archive, "kept.jpg", Some(obj));

        let prefer = _dir.path().to_str().unwrap().to_string();
        let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], &prefer)).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeDuplicates);
        execute_duplicates(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        // Excluded copy carries the decision_id; the kept archive copy does NOT.
        assert_eq!(fetch_source_decision_id(&conn, excluded_id), Some(1));
        assert_eq!(
            fetch_source_decision_id(&conn, kept_id),
            None,
            "kept copy is not a transition — must not be stamped"
        );
    }

    #[test]
    fn test_execute_set_objects_stamps_all_roles_incl_archive() {
        // D1: object exclusion is universal — every source sharing the object,
        // including archive-role copies, gets the decision_id and appears in the receipt.
        let mut conn = setup_test_db();
        let (archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "universal_hash", false);
        let source_copy = insert_source(&conn, src_root, "junk.bin", Some(obj));
        let archive_copy = insert_source(&conn, archive, "kept/junk.bin", Some(obj));

        let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSetObject);
        let result =
            execute_set_objects(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        assert!(is_object_excluded(&conn, obj));
        // Both roles stamped.
        assert_eq!(fetch_source_decision_id(&conn, source_copy), Some(1));
        assert_eq!(
            fetch_source_decision_id(&conn, archive_copy),
            Some(1),
            "archive-role source must be stamped — object exclusion is universal"
        );
        // Both roles listed in the receipt.
        let content = std::fs::read_to_string(
            dir.path()
                .join(".canon-ledger/000001-exclude_set_object.toml"),
        )
        .unwrap();
        assert!(content.contains("rel_path = \"junk.bin\""));
        assert!(content.contains("rel_path = \"kept/junk.bin\""));
    }

    #[test]
    fn test_execute_set_captures_previous_decision_id_chain() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let id = insert_source(&conn, src_root, "a.jpg", None);
        // A prior decision (e.g. scan discovery or an earlier op) on the source.
        conn.execute(
            "UPDATE sources SET decision_id = 99 WHERE id = ?",
            rusqlite::params![id],
        )
        .unwrap();

        let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
        let decision = full_decision(DecisionCommand::ExcludeSet);
        execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        // Live pointer advances to the new decision; the receipt preserves the predecessor.
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
        let content =
            std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_set.toml"))
                .unwrap();
        assert!(
            content.contains("previous_decision_id = 99"),
            "receipt should preserve the predecessor decision\n{content}"
        );
    }

    #[test]
    fn test_execute_set_source_links_and_writes_receipt() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "single_src_hash", false);
        let id = insert_source(&conn, src_root, "one.tmp", Some(obj));

        // Drive the real check -> execute flow.
        let SourceExclusionCheck::Ready { item } = check_set_source_by_id(&conn, id).unwrap()
        else {
            panic!("expected Ready");
        };
        let decision = full_decision(DecisionCommand::ExcludeSet);
        let result =
            execute_set_source(&mut conn, &item, Some(&placement), Some(&decision)).unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        assert!(is_source_excluded(&conn, id));
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
        let content =
            std::fs::read_to_string(dir.path().join(".canon-ledger/000001-exclude_set.toml"))
                .unwrap();
        assert!(content.contains("rel_path = \"one.tmp\""));
        assert!(content.contains("hash = \"sha256:single_src_hash\""));
    }

    #[test]
    fn test_execute_clear_object_unstamps_and_writes_receipt() {
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "clear_obj_hash", true); // already excluded
        let id = insert_source(&conn, src_root, "dup.bin", Some(obj));

        let ObjectClearCheck::Ready {
            object_id,
            hash_prefix,
            hash,
        } = check_clear_object(&conn, "clear_obj_hash").unwrap()
        else {
            panic!("expected Ready");
        };
        let decision = full_decision(DecisionCommand::ExcludeClearObject);
        let result = execute_clear_object(
            &mut conn,
            object_id,
            &hash_prefix,
            &hash,
            Some(&placement),
            Some(&decision),
        )
        .unwrap();

        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        assert!(!is_object_excluded(&conn, obj));
        // clear-object is a transition on every source of the object.
        assert_eq!(fetch_source_decision_id(&conn, id), Some(1));
        let content = std::fs::read_to_string(
            dir.path()
                .join(".canon-ledger/000001-exclude_clear_object.toml"),
        )
        .unwrap();
        assert!(content.contains("[[objects]]"));
        assert!(content.contains("hash = \"sha256:clear_obj_hash\""));
        assert!(content.contains("rel_path = \"dup.bin\""));
    }

    #[test]
    fn test_object_exclude_receipt_lists_stamp_set_including_tombstones() {
        // stamp-set = receipt-set (presence-axis constraint): the object-level
        // stamp touches every sharer, present or not, so the receipt must list
        // the tombstones too — otherwise the stamp isn't reconstructable from
        // disk and the chain walk dead-ends on a receipt item that doesn't exist.
        let mut conn = setup_test_db();
        let (_archive, dir, placement) = ledger_root(&conn);
        let src_root = insert_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "tomb_obj_hash", false);
        let present_id = insert_source(&conn, src_root, "still-here.bin", Some(obj));
        // A tombstone sharer: deleted from disk earlier, provenance link intact.
        let tomb_id = insert_source(&conn, src_root, "deleted.bin", Some(obj));
        conn.execute(
            "UPDATE sources SET present = 0, decision_id = 77 WHERE id = ?",
            rusqlite::params![tomb_id],
        )
        .unwrap();

        let decision = full_decision(DecisionCommand::ExcludeSetObject);
        execute_set_object(
            &mut conn,
            obj,
            "tomb_obj_hash",
            "sha256:tomb_obj_hash",
            &[],
            Some(&placement),
            Some(&decision),
        )
        .unwrap();

        // The stamp touched both sharers...
        assert_eq!(fetch_source_decision_id(&conn, present_id), Some(1));
        assert_eq!(fetch_source_decision_id(&conn, tomb_id), Some(1));

        // ...and the receipt lists the same set, tombstone marked and carrying
        // its pre-stamp predecessor.
        let content = std::fs::read_to_string(
            dir.path()
                .join(".canon-ledger/000001-exclude_set_object.toml"),
        )
        .unwrap();
        assert!(
            content.contains("rel_path = \"still-here.bin\""),
            "{content}"
        );
        assert!(content.contains("rel_path = \"deleted.bin\""), "{content}");
        assert!(content.contains("present = false"), "{content}");
        assert!(content.contains("previous_decision_id = 77"), "{content}");
        // The present sharer is unmarked — the field is serialized only for
        // the exceptional tombstone case.
        assert_eq!(content.matches("present = ").count(), 1, "{content}");
    }

    #[test]
    fn test_execute_set_empty_plan_records_nothing() {
        // F4: a 0-item plan reaching execute must not leave a decision row, a
        // dangling receipt pointer, or a spurious finalize warning.
        let mut conn = setup_test_db();
        let (_archive, _dir, placement) = ledger_root(&conn);
        let plan = ExcludeSetPlan {
            items: vec![],
            root_count: 0,
            not_archived_count: 0,
        };
        let decision = full_decision(DecisionCommand::ExcludeSet);
        let result = execute_set(&mut conn, &plan, Some(&placement), Some(&decision)).unwrap();

        assert_eq!(result.count, 0);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );
        let decisions: i64 = conn
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(decisions, 0, "empty plan must not record a decision");
    }
}
