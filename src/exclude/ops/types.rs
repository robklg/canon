//! Shared plan/result types for the exclude ops stratum: the receipt-capable
//! item, the per-command params/plan pairs, and the object-plan entry types.
//! `object_source_info` lives here (not in `single.rs`, where its check/execute
//! logic sits) because `plan_set_objects` calls it too — placing the shared
//! mapper in `types` keeps the dependency graph one-directional.

use std::collections::HashMap;

use crate::core::domain::object::Object;
use crate::core::domain::scope::ScopeMatch;
use crate::core::domain::source::Source;
use crate::core::ops::receipt::ReceiptPlacement;
use crate::expr::Filter;

/// Where an exclusion's receipt lands — or why it lands nowhere.
///
/// One value rather than two arguments, because the pair is one fact and an
/// incoherent pair is meaningless: a placement and a reason are never both
/// present, and when a receipt was owed and could not be placed, the reason is
/// never absent. `resolve_placement` in the cli stratum is the only constructor.
pub struct ReceiptDestination {
    pub placement: Option<ReceiptPlacement>,
    /// The reason no receipt could be placed — carried into the recorded
    /// summary so the row explains its own empty receipt columns. `None` when
    /// a receipt was placed, or was never owed.
    pub gap: Option<String>,
}

impl ReceiptDestination {
    /// No receipt, no gap: the shape a test that records nothing passes.
    #[cfg(test)]
    pub fn none() -> Self {
        Self {
            placement: None,
            gap: None,
        }
    }
}

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
    pub(super) fn from_source(s: &Source, objects: &HashMap<i64, Object>) -> Self {
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
    /// Empty files skipped (size = 0). Counts sources, not deduped objects —
    /// the interface reports this as a file count.
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

/// Map a fetched source to object-source display info.
pub(super) fn object_source_info(s: &Source) -> ObjectSourceInfo {
    ObjectSourceInfo {
        path: s.path(),
        is_archive: s.is_from_role("archive"),
    }
}
