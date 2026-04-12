//! Facts distribution computation.
//!
//! Computes value distributions and key enumerations for the `facts` command.
//! All functions take source_ids (already selected by the interface) and return
//! typed results for the interface to format.

use std::collections::HashMap;

use anyhow::Result;

use crate::domain::fact::FactEntry;
use crate::expr::value as fact_value;
use crate::expr::{
    self, fact_value_to_display, get_builtin_value, resolve_fact_value, BuiltinKey,
    BuiltinKeyCategory, BuiltinKeyVisibility, FactType, FactValue, ParsedFactKey,
};
use crate::repo::{self, Connection};

// ============================================================================
// Result types
// ============================================================================

/// A single entry in a value distribution.
pub struct DistributionEntry {
    pub value: String,
    pub count: i64,
}

/// Result of computing a value distribution for a fact key.
pub struct DistributionResult {
    pub entries: Vec<DistributionEntry>,
    pub sources_with_value: i64,
    pub total_sources: usize,
    /// Number of values skipped due to type mismatch during transform.
    pub skipped_type_mismatch: i64,
    /// Total distinct values found (before truncation to limit).
    pub total_distinct_values: usize,
}

/// Information about a single fact key (for all-keys enumeration).
pub struct KeyInfo {
    pub key: String,
    pub count: i64,
    pub category: BuiltinKeyCategory,
    pub fact_type: FactType,
}

/// Result of enumerating all available fact keys.
pub struct AllKeysResult {
    pub keys: Vec<KeyInfo>,
    pub total_sources: usize,
}

/// A sub-group within a grouped distribution (e.g., one root's contribution).
pub struct GroupedEntry {
    pub group_values: Vec<String>,
    pub count: i64,
    pub root_id: Option<i64>,
    pub root_path: Option<String>,
}

/// A main value and its grouped breakdown.
pub struct GroupedValueResult {
    pub main_value: String,
    pub total_count: i64,
    pub sub_groups: Vec<GroupedEntry>,
}

/// Result of a grouped distribution computation.
pub struct GroupedDistributionResult {
    pub groups: Vec<GroupedValueResult>,
    pub sources_with_value: i64,
    /// Kept for consistency with DistributionResult/AllKeysResult — the interface
    /// currently passes its own total_sources to display, but this field ensures
    /// the ops result is self-contained if a future caller needs it.
    #[allow(dead_code)]
    pub total_sources: usize,
}

/// A single root's contribution to the selection.
pub struct RootDistributionEntry {
    pub root_id: i64,
    pub root_path: String,
    pub count: usize,
}

/// Result of computing root distribution for a selection.
pub struct RootDistributionResult {
    pub entries: Vec<RootDistributionEntry>,
    pub total_sources: usize,
}

/// Compute source count per root for the given sources.
/// Sources must have root_id populated (from batch_fetch_by_roots).
pub fn compute_root_distribution(
    sources: &[crate::domain::source::Source],
) -> RootDistributionResult {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<i64, (String, usize)> = BTreeMap::new();
    for s in sources {
        let entry = counts
            .entry(s.root_id)
            .or_insert_with(|| (s.root_path.clone(), 0));
        entry.1 += 1;
    }
    let mut entries: Vec<RootDistributionEntry> = counts
        .into_iter()
        .map(|(root_id, (root_path, count))| RootDistributionEntry {
            root_id,
            root_path,
            count,
        })
        .collect();
    // Sort by count descending
    entries.sort_by(|a, b| b.count.cmp(&a.count));
    let total_sources = sources.len();
    RootDistributionResult {
        entries,
        total_sources,
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn is_builtin_or_derived(key: &str) -> bool {
    BuiltinKey::from_str(key)
        .map(|k| k.visibility() != BuiltinKeyVisibility::NotListed)
        .unwrap_or(false)
}

/// Apply default grouping for builtin keys without transforms.
/// Returns a human-readable bucket/group string for the value.
fn default_grouping(base_key: &str, value: FactValue) -> String {
    match base_key {
        "source.size" => {
            if let FactValue::Num(size) = value {
                let size = size as i64;
                if size < 1024 {
                    "< 1 KB"
                } else if size < 1024 * 1024 {
                    "1 KB - 1 MB"
                } else if size < 10 * 1024 * 1024 {
                    "1 MB - 10 MB"
                } else if size < 100 * 1024 * 1024 {
                    "10 MB - 100 MB"
                } else if size < 1024 * 1024 * 1024 {
                    "100 MB - 1 GB"
                } else {
                    "> 1 GB"
                }
                .to_string()
            } else {
                fact_value_to_display(&value)
            }
        }
        "source.mtime" => {
            if let FactValue::Time(ts) = value {
                chrono::DateTime::from_timestamp(ts, 0)
                    .map(|dt| dt.format("%Y").to_string())
                    .unwrap_or_else(|| "(unknown)".to_string())
            } else {
                fact_value_to_display(&value)
            }
        }
        _ => fact_value_to_display(&value),
    }
}

/// Aggregate values into sorted (value, count) entries, truncated to limit.
/// Returns (entries, total_distinct_values_before_truncation).
fn aggregate_and_sort(counts: HashMap<String, i64>, limit: usize) -> (Vec<DistributionEntry>, usize) {
    let mut entries: Vec<DistributionEntry> = counts
        .into_iter()
        .map(|(value, count)| DistributionEntry { value, count })
        .collect();
    entries.sort_by(|a, b| b.count.cmp(&a.count));
    let total_distinct = entries.len();
    if limit > 0 && entries.len() > limit {
        entries.truncate(limit);
    }
    (entries, total_distinct)
}

// ============================================================================
// Compute functions
// ============================================================================

/// Enumerate all fact keys with counts and coverage.
///
/// Combines builtin keys (filtered by visibility + show_hidden) with stored fact
/// keys from the database. Builtins always have count == total_sources.
pub fn compute_all_keys(
    conn: &mut Connection,
    source_ids: &[i64],
    show_hidden: bool,
) -> Result<AllKeysResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(AllKeysResult {
            keys: Vec::new(),
            total_sources,
        });
    }

    let mut keys: Vec<KeyInfo> = Vec::new();

    // Add built-in and derived facts (they always have 100% coverage)
    use strum::IntoEnumIterator;
    for builtin in BuiltinKey::iter() {
        let vis = builtin.visibility();
        if vis == BuiltinKeyVisibility::NotListed {
            continue;
        }
        if vis == BuiltinKeyVisibility::Hidden && !show_hidden {
            continue;
        }
        let name: &'static str = builtin.into();
        keys.push(KeyInfo {
            key: name.to_string(),
            count: total_sources as i64,
            category: builtin.category(),
            fact_type: builtin.fact_type(),
        });
    }

    // Add stored facts from database
    let stored = repo::fact::count_fact_keys(conn, source_ids)?;
    for (key, count, fact_type) in stored {
        keys.push(KeyInfo {
            key,
            count: count as i64,
            category: BuiltinKeyCategory::Stored,
            fact_type,
        });
    }

    Ok(AllKeysResult {
        keys,
        total_sources,
    })
}

/// Compute value distribution for a single fact key.
///
/// Internally dispatches based on key type:
/// - Builtin key WITHOUT transforms → default grouping (size buckets, mtime→year, etc.)
/// - Builtin key WITH transforms → resolve_fact_value()
/// - Stored key WITHOUT transforms → fetch raw values, group by display string
/// - Stored key WITH transforms → fetch raw values, apply transforms, aggregate
///
/// Results are sorted by count descending and truncated to limit (0 = no limit).
pub fn compute_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    limit: usize,
) -> Result<DistributionResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(DistributionResult {
            entries: Vec::new(),
            sources_with_value: 0,
            total_sources,
            skipped_type_mismatch: 0,
            total_distinct_values: 0,
        });
    }

    if is_builtin_or_derived(&key.base_key) {
        compute_builtin_distribution(conn, source_ids, key, total_sources, limit)
    } else if key.has_transforms() {
        compute_transformed_distribution(conn, source_ids, key, total_sources, limit)
    } else {
        compute_stored_distribution(conn, source_ids, key, total_sources, limit)
    }
}

/// Builtin/derived key distribution.
fn compute_builtin_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    total_sources: usize,
    limit: usize,
) -> Result<DistributionResult> {
    let sources = repo::source::batch_fetch_by_ids(conn, source_ids)?;
    let has_transforms = key.has_transforms();
    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;

    for source in sources.values() {
        let val = if has_transforms {
            // Use resolve_fact_value for the full transform pipeline
            match resolve_fact_value(source, key, &HashMap::new())? {
                Some(v) => v,
                None => continue,
            }
        } else {
            // Use default grouping for the no-transform case
            match BuiltinKey::from_str(&key.base_key) {
                Some(builtin) => match get_builtin_value(source, builtin) {
                    Some(raw) => default_grouping(&key.base_key, raw),
                    None => continue,
                },
                None => continue,
            }
        };
        sources_with_value += 1;
        *counts.entry(val).or_insert(0) += 1;
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        skipped_type_mismatch: 0,
        total_distinct_values,
    })
}

/// Stored fact distribution without transforms — group by raw display value.
fn compute_stored_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    total_sources: usize,
    limit: usize,
) -> Result<DistributionResult> {
    let fact_map = repo::fact::batch_fetch_key_for_sources(conn, source_ids, &key.base_key)?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;

    for entry in fact_map.values().flatten() {
        sources_with_value += 1;
        let display_val = fact_value_to_display(&entry.value);
        *counts.entry(display_val).or_insert(0) += 1;
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        skipped_type_mismatch: 0,
        total_distinct_values,
    })
}

/// Stored fact distribution with transforms — fetch raw, apply transforms, aggregate.
fn compute_transformed_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    total_sources: usize,
    limit: usize,
) -> Result<DistributionResult> {
    let fact_map = repo::fact::batch_fetch_key_for_sources(conn, source_ids, &key.base_key)?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;
    let mut skipped_type_mismatch: i64 = 0;

    for entry in fact_map.values().flatten() {
        sources_with_value += 1;
        // Apply accessor + modifiers via the same transform pipeline
        let mut result = entry.value.clone();
        if let Some(ref acc) = key.accessor {
            match expr::apply_accessor(&result, acc, &key.raw) {
                Ok(v) => result = v,
                Err(_) => {
                    skipped_type_mismatch += 1;
                    continue;
                }
            }
        }
        let mut transform_failed = false;
        for modifier_call in &key.modifiers {
            match expr::apply_modifier(&result, modifier_call, &key.raw, true) {
                Ok(v) => result = v,
                Err(_) => {
                    skipped_type_mismatch += 1;
                    transform_failed = true;
                    break;
                }
            }
        }
        if transform_failed {
            continue;
        }
        let display_val = fact_value_to_display(&result);
        *counts.entry(display_val).or_insert(0) += 1;
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        skipped_type_mismatch,
        total_distinct_values,
    })
}

/// Compute grouped distribution (--by-root, --group-by).
///
/// Fetches sources + stored facts, resolves values via resolve_fact_value(),
/// aggregates into nested groups. No default grouping applied (matches existing
/// behavior of show_grouped_distribution).
pub fn compute_grouped_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    main_key: &ParsedFactKey,
    grouping_keys: &[ParsedFactKey],
    limit: usize,
) -> Result<GroupedDistributionResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(GroupedDistributionResult {
            groups: Vec::new(),
            sources_with_value: 0,
            total_sources,
        });
    }

    // Fetch all sources by ID
    let sources = repo::source::batch_fetch_by_ids(conn, source_ids)?;

    // Collect stored fact keys we need to fetch
    let mut stored_keys: Vec<&str> = Vec::new();
    if !main_key.is_builtin() {
        stored_keys.push(&main_key.base_key);
    }
    for gk in grouping_keys {
        if !gk.is_builtin() && !stored_keys.contains(&gk.base_key.as_str()) {
            stored_keys.push(&gk.base_key);
        }
    }

    // Fetch stored facts for all needed keys
    let all_facts: HashMap<i64, HashMap<String, FactEntry>> = if stored_keys.is_empty() {
        HashMap::new()
    } else {
        let mut merged: HashMap<i64, HashMap<String, FactEntry>> = HashMap::new();
        for key in &stored_keys {
            let key_facts = repo::fact::batch_fetch_key_for_sources(conn, source_ids, key)?;
            for (source_id, entry_opt) in key_facts {
                if let Some(entry) = entry_opt {
                    merged
                        .entry(source_id)
                        .or_default()
                        .insert(entry.key.clone(), entry);
                }
            }
        }
        merged
    };

    // Aggregate by (main_value, group_values) composite key
    #[derive(Hash, Eq, PartialEq, Clone)]
    struct CompositeKey {
        main_value: String,
        group_values: Vec<String>,
    }

    struct EntryInfo {
        count: i64,
        root_id: Option<i64>,
        root_path: Option<String>,
    }

    let mut aggregated: HashMap<CompositeKey, EntryInfo> = HashMap::new();
    let mut sources_with_value: i64 = 0;
    let empty_facts: HashMap<String, FactEntry> = HashMap::new();

    for source_id in source_ids {
        let source = match sources.get(source_id) {
            Some(s) => s,
            None => continue,
        };

        let stored_facts = all_facts.get(source_id).unwrap_or(&empty_facts);

        // Resolve main value
        let main_value = match fact_value::resolve_fact_value(source, main_key, stored_facts)? {
            Some(v) => {
                sources_with_value += 1;
                v
            }
            None => continue,
        };

        // Resolve grouping values, tracking root info for display
        let mut group_values: Vec<String> = Vec::new();
        let mut root_id_for_display: Option<i64> = None;
        let mut root_path_for_display: Option<String> = None;

        for gk in grouping_keys {
            let gk_value = match fact_value::resolve_fact_value(source, gk, stored_facts) {
                Ok(Some(v)) => v,
                Ok(None) => "(no value)".to_string(),
                Err(_) => "(transform error)".to_string(),
            };

            // Track root info when grouping by root key
            if is_root_grouping_key(gk) {
                root_id_for_display = Some(source.root_id);
                root_path_for_display = Some(source.root_path.clone());
            }

            group_values.push(gk_value);
        }

        let composite = CompositeKey {
            main_value,
            group_values,
        };
        let entry = aggregated.entry(composite).or_insert(EntryInfo {
            count: 0,
            root_id: root_id_for_display,
            root_path: root_path_for_display,
        });
        entry.count += 1;
    }

    // Build nested result: group by main_value, then sort sub-groups
    let mut by_main_value: HashMap<String, GroupedValueResult> = HashMap::new();

    for (composite, info) in aggregated {
        let entry = by_main_value
            .entry(composite.main_value.clone())
            .or_insert(GroupedValueResult {
                main_value: composite.main_value,
                total_count: 0,
                sub_groups: Vec::new(),
            });
        entry.total_count += info.count;
        entry.sub_groups.push(GroupedEntry {
            group_values: composite.group_values,
            count: info.count,
            root_id: info.root_id,
            root_path: info.root_path,
        });
    }

    // Sort main values by total count descending
    let mut groups: Vec<GroupedValueResult> = by_main_value.into_values().collect();
    groups.sort_by(|a, b| b.total_count.cmp(&a.total_count));

    if limit > 0 && groups.len() > limit {
        groups.truncate(limit);
    }

    // Sort sub-groups within each main value by count descending
    for group in &mut groups {
        group.sub_groups.sort_by(|a, b| b.count.cmp(&a.count));
    }

    Ok(GroupedDistributionResult {
        groups,
        sources_with_value,
        total_sources,
    })
}

/// Check if a parsed key represents source.root (for root ID/path tracking).
fn is_root_grouping_key(key: &ParsedFactKey) -> bool {
    key.raw == "source.root" || key.raw == "root_id"
}

// ============================================================================
// Write operations (plan/execute)
// ============================================================================

use crate::domain::decision::DecisionStatus;
use crate::domain::format_count;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::repo::object::OrphanedStats;
use crate::repo::Db;

/// Plan result for fact deletion.
pub struct DeletePlan {
    /// Kept for plan self-containedness — the interface currently uses its own
    /// copies of key/entity_type for display and execute.
    #[allow(dead_code)]
    pub key: String,
    #[allow(dead_code)]
    pub entity_type: String,
    pub fact_count: i64,
    pub entity_count: i64,
}

/// Plan result for stale fact pruning.
pub struct PruneStalePlan {
    pub stale_count: i64,
}

/// Plan result for excluded fact pruning.
pub struct PruneExcludedPlan {
    #[allow(dead_code)]
    pub scope: String,
    pub source_fact_count: i64,
    pub object_fact_count: i64,
}

impl PruneExcludedPlan {
    pub fn total_count(&self) -> i64 {
        self.source_fact_count + self.object_fact_count
    }
}

/// Validate that a fact key is not protected from deletion.
/// Protected namespaces: `source.*`, `policy.*`.
pub fn validate_delete_key(key: &str) -> Result<()> {
    if key.starts_with("source.") || key.starts_with("policy.") {
        anyhow::bail!(
            "Cannot delete protected fact '{key}'. Facts in source.* and policy.* namespaces cannot be deleted."
        );
    }
    Ok(())
}

/// Plan fact deletion: count matching facts without performing the delete.
pub fn plan_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
) -> Result<DeletePlan> {
    let (fact_count, entity_count) =
        repo::fact::count_by_criteria(conn, source_ids, key, entity_type, value_type)?;

    Ok(DeletePlan {
        key: key.to_string(),
        entity_type: entity_type.to_string(),
        fact_count,
        entity_count,
    })
}

/// Result of fact deletion.
pub struct DeleteResult {
    pub summary: String,
}

/// Execute fact deletion.
pub fn execute_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
    plan: &DeletePlan,
    decision: Option<&DecisionParams>,
) -> Result<DeleteResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    repo::fact::delete_by_criteria(conn, source_ids, key, entity_type, value_type)?;
    let entity_label = if entity_type == "source" {
        "sources"
    } else {
        "objects"
    };
    let summary = format!(
        "Deleted {} fact rows across {} {}",
        format_count(plan.fact_count),
        format_count(plan.entity_count),
        entity_label
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(plan.fact_count),
                completed: Some(plan.fact_count),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(DeleteResult { summary })
}

/// Plan stale fact pruning: count facts with mismatched basis_rev.
pub fn plan_prune_stale(conn: &Connection) -> Result<PruneStalePlan> {
    let stale_count = repo::fact::count_stale(conn)?;
    Ok(PruneStalePlan { stale_count })
}

/// Result of stale fact pruning.
#[allow(dead_code)]
pub struct PruneStaleResult {
    pub deleted: usize,
    pub summary: String,
}

/// Execute stale fact pruning.
pub fn execute_prune_stale(
    conn: &Connection,
    decision: Option<&DecisionParams>,
) -> Result<PruneStaleResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let deleted = repo::fact::delete_stale(conn)?;
    let summary = format!(
        "Deleted {} stale fact rows (observed_basis_rev mismatch)",
        format_count(deleted as i64)
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(deleted as i64),
                completed: Some(deleted as i64),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(PruneStaleResult { deleted, summary })
}

/// Plan orphaned object pruning: count orphaned objects, sources, and facts.
pub fn plan_prune_orphaned(conn: &mut Connection) -> Result<OrphanedStats> {
    repo::object::find_orphaned_stats(conn)
}

/// Result of orphaned object pruning.
#[allow(dead_code)]
pub struct PruneOrphanedResult {
    pub stats: OrphanedStats,
    pub summary: String,
}

/// Execute orphaned object pruning. Owns the transaction for atomicity.
pub fn execute_prune_orphaned(
    db: &mut Db,
    decision: Option<&DecisionParams>,
) -> Result<PruneOrphanedResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(db.conn(), d, None));

    let conn = db.conn_mut();
    let tx = conn.transaction()?;
    let deleted = repo::object::delete_orphaned(&tx)?;
    tx.commit()?;
    let summary = format!(
        "Deleted {} orphaned objects, {} non-present sources, and {} facts",
        format_count(deleted.object_count),
        format_count(deleted.source_count),
        format_count(deleted.total_fact_count())
    );

    if let Some(recorder) = recorder.as_mut() {
        let total_deleted = deleted.object_count + deleted.source_count + deleted.total_fact_count();
        recorder.complete(
            db.conn(),
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(total_deleted),
                completed: Some(total_deleted),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(PruneOrphanedResult {
        stats: deleted,
        summary,
    })
}

/// Validate the scope parameter for excluded fact pruning.
pub fn validate_prune_excluded_scope(scope: &str) -> Result<()> {
    if scope != "all" && scope != "source" && scope != "object" {
        anyhow::bail!("Invalid scope '{scope}'. Use 'source', 'object', or omit for both.");
    }
    Ok(())
}

/// Plan excluded fact pruning: count facts for excluded entities.
pub fn plan_prune_excluded(conn: &Connection, scope: &str) -> Result<PruneExcludedPlan> {
    let (source_fact_count, object_fact_count) = repo::fact::count_excluded(conn, scope)?;
    Ok(PruneExcludedPlan {
        scope: scope.to_string(),
        source_fact_count,
        object_fact_count,
    })
}

/// Result of excluded fact pruning.
#[allow(dead_code)]
pub struct PruneExcludedResult {
    pub source_deleted: usize,
    pub object_deleted: usize,
    pub summary: String,
}

/// Execute excluded fact pruning.
pub fn execute_prune_excluded(
    conn: &Connection,
    scope: &str,
    decision: Option<&DecisionParams>,
) -> Result<PruneExcludedResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let (source_deleted, object_deleted) = repo::fact::delete_excluded(conn, scope)?;
    let total_deleted = source_deleted + object_deleted;
    let mut parts = Vec::new();
    if source_deleted > 0 {
        parts.push(format!(
            "Deleted {} source facts (from excluded sources)",
            format_count(source_deleted as i64)
        ));
    }
    if object_deleted > 0 {
        parts.push(format!(
            "Deleted {} object facts (from excluded objects)",
            format_count(object_deleted as i64)
        ));
    }
    if total_deleted > 0 {
        parts.push(format!(
            "Total: {} facts deleted",
            format_count(total_deleted as i64)
        ));
    }
    let summary = parts.join("\n");

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(total_deleted as i64),
                completed: Some(total_deleted as i64),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(PruneExcludedResult {
        source_deleted,
        object_deleted,
        summary,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, setup_test_db,
    };

    /// Insert a source fact with a time value.
    fn insert_time_fact(conn: &Connection, source_id: i64, key: &str, timestamp: i64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, observed_at, observed_basis_rev)
             VALUES ('source', ?1, ?2, ?3, 0, 0)",
            rusqlite::params![source_id, key, timestamp],
        )
        .unwrap();
    }

    // =========================================================================
    // compute_all_keys tests
    // =========================================================================

    #[test]
    fn test_compute_all_keys_includes_builtins_and_stored() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abc123", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj_id));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj_id));

        // Add a stored fact for one source
        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");

        let result = compute_all_keys(&mut conn, &[s1, s2], false).unwrap();

        // Should include builtins with count == 2 (total_sources)
        let ext_key = result.keys.iter().find(|k| k.key == "source.ext").unwrap();
        assert_eq!(ext_key.count, 2);
        assert_eq!(ext_key.category, BuiltinKeyCategory::BuiltIn);

        // Should include stored fact with count == 1
        let make_key = result.keys.iter().find(|k| k.key == "content.Make").unwrap();
        assert_eq!(make_key.count, 1);
        assert_eq!(make_key.category, BuiltinKeyCategory::Stored);

        assert_eq!(result.total_sources, 2);
    }

    // =========================================================================
    // compute_distribution tests
    // =========================================================================

    #[test]
    fn test_compute_distribution_stored_fact() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let obj3 = insert_object(&conn, "hash3", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::ops::test_helpers::insert_fact(&conn, s2, "content.Make", "Canon");
        crate::ops::test_helpers::insert_fact(&conn, s3, "content.Make", "Nikon");

        let key = ParsedFactKey::parse("content.Make").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0).unwrap();

        assert_eq!(result.entries.len(), 2);
        // Sorted by count desc — Canon (2) first
        assert_eq!(result.entries[0].value, "Canon");
        assert_eq!(result.entries[0].count, 2);
        assert_eq!(result.entries[1].value, "Nikon");
        assert_eq!(result.entries[1].count, 1);
        assert_eq!(result.sources_with_value, 3);
        assert_eq!(result.total_sources, 3);
    }

    #[test]
    fn test_compute_distribution_with_limit() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let obj4 = insert_object(&conn, "h4", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));
        let s4 = insert_source(&conn, root_id, "d.jpg", Some(obj4));

        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::ops::test_helpers::insert_fact(&conn, s2, "content.Make", "Nikon");
        crate::ops::test_helpers::insert_fact(&conn, s3, "content.Make", "Sony");
        crate::ops::test_helpers::insert_fact(&conn, s4, "content.Make", "Canon");

        let key = ParsedFactKey::parse("content.Make").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2, s3, s4], &key, 2).unwrap();

        // Limited to top 2
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].value, "Canon");
        assert_eq!(result.entries[0].count, 2);
    }

    #[test]
    fn test_compute_distribution_builtin_size_buckets() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);

        // Insert sources with different sizes via insert_source_with_size
        // 500 bytes (< 1 KB), 500KB (1 KB - 1 MB), 5MB (1 MB - 10 MB)
        use crate::ops::test_helpers::insert_source_with_size;
        let s1 = insert_source_with_size(&conn, root_id, "tiny.txt", Some(obj1), 500);
        let s2 = insert_source_with_size(&conn, root_id, "medium.jpg", Some(obj2), 512_000);
        let s3 = insert_source_with_size(&conn, root_id, "large.raw", Some(obj3), 5_242_880);

        let key = ParsedFactKey::parse("source.size").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0).unwrap();

        assert_eq!(result.entries.len(), 3);
        let buckets: Vec<&str> = result.entries.iter().map(|e| e.value.as_str()).collect();
        assert!(buckets.contains(&"< 1 KB"));
        assert!(buckets.contains(&"1 KB - 1 MB"));
        assert!(buckets.contains(&"1 MB - 10 MB"));
        assert_eq!(result.sources_with_value, 3);
    }

    #[test]
    fn test_compute_distribution_builtin_mtime_year() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);

        // 2023-06-15, 2024-01-01, 2024-07-01
        use crate::ops::test_helpers::insert_source_with_metadata;
        let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1686830400);
        let s2 = insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 1704067200);
        let s3 = insert_source_with_metadata(&conn, root_id, "c.jpg", Some(obj3), 1000, 1719792000);

        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0).unwrap();

        // 2024 should have 2 entries, 2023 should have 1
        let y2024 = result.entries.iter().find(|e| e.value == "2024").unwrap();
        assert_eq!(y2024.count, 2);
        let y2023 = result.entries.iter().find(|e| e.value == "2023").unwrap();
        assert_eq!(y2023.count, 1);
    }

    #[test]
    fn test_compute_distribution_with_transforms() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

        // Insert time facts for different years
        insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1686830400);
        insert_time_fact(&conn, s2, "content.DateTimeOriginal", 1704067200);
        insert_time_fact(&conn, s3, "content.DateTimeOriginal", 1719792000);

        let key = ParsedFactKey::parse("content.DateTimeOriginal|year").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0).unwrap();

        let y2024 = result.entries.iter().find(|e| e.value == "2024").unwrap();
        assert_eq!(y2024.count, 2);
        let y2023 = result.entries.iter().find(|e| e.value == "2023").unwrap();
        assert_eq!(y2023.count, 1);
    }

    // =========================================================================
    // compute_grouped_distribution tests
    // =========================================================================

    #[test]
    fn test_compute_grouped_basic() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/backup", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let s1 = insert_source(&conn, root1, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root1, "b.png", Some(obj2));
        let s3 = insert_source(&conn, root2, "c.jpg", Some(obj3));

        let main_key = ParsedFactKey::parse("source.ext").unwrap();
        let group_key = ParsedFactKey::parse("source.root").unwrap();
        let result = compute_grouped_distribution(
            &mut conn,
            &[s1, s2, s3],
            &main_key,
            &[group_key],
            0,
        )
        .unwrap();

        // "jpg" should have total_count=2 (from both roots), "png" should have 1
        assert_eq!(result.groups.len(), 2);
        let jpg_group = result.groups.iter().find(|g| g.main_value == "jpg").unwrap();
        assert_eq!(jpg_group.total_count, 2);
        assert_eq!(jpg_group.sub_groups.len(), 2); // one per root

        let png_group = result.groups.iter().find(|g| g.main_value == "png").unwrap();
        assert_eq!(png_group.total_count, 1);

        assert_eq!(result.sources_with_value, 3);
    }

    #[test]
    fn test_compute_grouped_missing_main_value() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));

        // Only s1 has the main key value
        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");

        let main_key = ParsedFactKey::parse("content.Make").unwrap();
        let group_key = ParsedFactKey::parse("source.root").unwrap();
        let result = compute_grouped_distribution(
            &mut conn,
            &[s1, s2],
            &main_key,
            &[group_key],
            0,
        )
        .unwrap();

        // Only 1 source has the main value, s2 is excluded
        assert_eq!(result.sources_with_value, 1);
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.groups[0].main_value, "Canon");
        assert_eq!(result.groups[0].total_count, 1);
    }

    // =========================================================================
    // Write operation tests
    // =========================================================================

    #[test]
    fn test_validate_delete_key_protected() {
        assert!(validate_delete_key("source.policy").is_err());
        assert!(validate_delete_key("source.anything").is_err());
        assert!(validate_delete_key("policy.reviewed").is_err());
        assert!(validate_delete_key("policy.archive").is_err());

        assert!(validate_delete_key("content.Make").is_ok());
        assert!(validate_delete_key("custom.field").is_ok());
        assert!(validate_delete_key("Make").is_ok());
    }

    #[test]
    fn test_plan_delete_counts() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));

        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::ops::test_helpers::insert_fact(&conn, s2, "content.Make", "Nikon");

        let plan = plan_delete(&mut conn, &[s1, s2], "content.Make", "source", None).unwrap();

        assert_eq!(plan.fact_count, 2);
        assert_eq!(plan.entity_count, 2);
        assert_eq!(plan.key, "content.Make");
        assert_eq!(plan.entity_type, "source");
    }

    #[test]
    fn test_plan_prune_stale_counts() {
        let conn = setup_test_db();
        // With no stale facts, count should be 0
        let plan = plan_prune_stale(&conn).unwrap();
        assert_eq!(plan.stale_count, 0);
    }

    #[test]
    fn test_validate_prune_excluded_scope() {
        assert!(validate_prune_excluded_scope("all").is_ok());
        assert!(validate_prune_excluded_scope("source").is_ok());
        assert!(validate_prune_excluded_scope("object").is_ok());
        assert!(validate_prune_excluded_scope("invalid").is_err());
        assert!(validate_prune_excluded_scope("").is_err());
    }
}
