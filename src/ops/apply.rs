//! Apply plan computation.
//!
//! Validates constraints and computes destination paths for an apply operation.
//! No filesystem I/O, no file transfers — the interface handles those.

use std::collections::HashMap;

use anyhow::{bail, Result};

use super::cluster::LockEntry;
use crate::domain::fact::FactEntry;
use crate::domain::path::path_strip_prefix;
use crate::expr::{self, EvalContext, FactValue, Pattern};
use crate::repo::{self, Connection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV
}

/// Parameters for planning an apply operation.
pub struct ApplyPlanParams<'a> {
    /// Filtered sources from the lock file (already filtered by --root).
    pub sources: &'a [&'a LockEntry],
    /// Parsed output pattern.
    pub pattern: &'a Pattern,
    /// Fact keys needed by the pattern (from expr::extract_fact_keys).
    pub needed_keys: &'a [String],
    /// Scope prefix from manifest config (meta.scope).
    pub scope_prefix: Option<&'a str>,
    /// Root ID → root path cache (from repo::root::fetch_all).
    pub root_paths: &'a HashMap<i64, String>,
    /// Destination archive root ID.
    pub archive_root_id: i64,
    /// Relative base directory within archive root (config.output.base_dir).
    pub base_dir_rel: &'a str,
    /// Whether this is a resume operation.
    pub resume: bool,
}

/// A source validated and ready for transfer, with pre-computed destination.
#[derive(Debug)]
pub struct ApplyTransfer {
    /// Source ID from lock entry.
    pub source_id: i64,
    /// Absolute source path.
    pub source_path: String,
    /// Destination path relative to base_dir (for filesystem operations).
    pub dest_rel_path: String,
    /// Destination path relative to archive root (for DB registration).
    pub archive_rel_path: String,
    /// Content object ID (for DB registration).
    pub object_id: Option<i64>,
    /// Partial hash from lock file (for DB registration and staleness).
    pub partial_hash: String,
    /// File size from lock file (for staleness validation).
    pub size: i64,
    /// File mtime from lock file (for staleness validation).
    pub mtime: i64,
}

/// Computed plan for an apply operation. Contains all data the interface
/// needs for violation display, filesystem checks, and transfer execution.
#[derive(Debug)]
pub struct ApplyPlan {
    /// Sources validated and ready for transfer with pre-computed destinations.
    /// In regular mode: all sources that passed pattern expansion.
    /// In resume mode: sources whose destination is NOT already in DB.
    pub transfers: Vec<ApplyTransfer>,
    /// All violations found during planning.
    pub violations: ApplyViolations,
    /// Sources whose DB state has changed since lock file (size/mtime/partial_hash).
    /// Computed via DB check. Interface may also do disk-based validation.
    pub stale_sources: Vec<StaleSource>,
    /// Resume mode: count of sources already registered in archive DB.
    pub already_archived_count: usize,
}

/// Violations found during apply planning. The interface inspects each field
/// and decides whether to bail (hard gate) or proceed (with --allow flags).
#[derive(Debug, Default)]
pub struct ApplyViolations {
    /// Sources that failed pattern expansion: (source_path, error_message).
    pub expansion_failures: Vec<(String, String)>,
    /// Destination paths with multiple sources: (dest_rel_path, source_paths).
    pub collisions: Vec<(String, Vec<String>)>,
    /// Destination paths with stale DB records (present=1, file likely missing).
    pub stale_records: Vec<String>,
    /// Destination paths already occupied in DB (non-resume mode only).
    pub dest_conflicts_in_db: Vec<String>,
    /// Sources whose content already exists in destination archive:
    /// (source_path, archive_path).
    pub archive_conflicts_dest: Vec<(String, String)>,
    /// Sources whose content already exists in other archives:
    /// (source_path, archive_path).
    pub archive_conflicts_other: Vec<(String, String)>,
    /// Sources marked as excluded since manifest generation: (id, path).
    pub excluded_sources: Vec<(i64, String)>,
    /// Sources from suspended roots: (id, path).
    pub suspended_sources: Vec<(i64, String)>,
}

/// A source whose state has changed since the lock file was generated.
#[derive(Debug)]
pub struct StaleSource {
    pub path: String,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Private helpers (moved from apply.rs)
// ---------------------------------------------------------------------------

/// Build an EvalContext for a source using pre-fetched facts and cached root paths.
fn build_eval_context(
    source: &LockEntry,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<EvalContext> {
    let mut ctx = EvalContext::new();

    let root_path = root_paths
        .get(&source.root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found in cache", source.root_id))?;

    let rel_path = if source.path == *root_path {
        String::new()
    } else if let Some(rel) = path_strip_prefix(&source.path, root_path) {
        rel.to_string()
    } else {
        source.path.clone()
    };

    ctx.set_source_root(root_path.clone());
    ctx.set_source_rel_path(rel_path);
    ctx.set_scope_prefix(scope_prefix.map(|s| s.to_string()));

    if let Some(source_facts) = all_facts.get(&source.id) {
        for key in needed_keys {
            if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
                continue;
            }
            if let Some(entry) = source_facts.iter().find(|f| f.key == *key) {
                ctx.set_fact(key, entry.value.clone());
            }
        }
    }

    if let Some(ref hash) = source.hash_value {
        ctx.set_fact("object.hash", FactValue::Text(hash.clone()));
    }

    Ok(ctx)
}

/// Evaluate a pattern for a source, returning the destination relative path.
fn evaluate_pattern(
    pattern: &Pattern,
    source: &LockEntry,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<String> {
    let ctx = build_eval_context(source, needed_keys, scope_prefix, root_paths, all_facts)?;
    expr::evaluate(pattern, &ctx)
}

/// Compute the archive-relative path from base_dir_rel and dest_rel.
fn compute_archive_rel_path(base_dir_rel: &str, dest_rel: &str) -> String {
    if base_dir_rel.is_empty() {
        dest_rel.to_string()
    } else {
        format!("{base_dir_rel}/{dest_rel}")
    }
}

// ---------------------------------------------------------------------------
// Plan function
// ---------------------------------------------------------------------------

/// Compute what `apply` would do — validates constraints and computes
/// destination paths. No filesystem I/O, no file transfers.
///
/// Runs all DB-based preflight checks: unhashed sources, archive hash
/// coverage, pattern expansion, collision detection, stale records,
/// destination conflicts, archive conflicts, excluded/suspended sources,
/// and DB-based source state validation.
///
/// Returns an `ApplyPlan` with validated transfers and any violations.
/// The interface inspects violations to decide whether to proceed.
///
/// Returns `Err` only for precondition failures (unhashed sources, archive
/// hash gap) and unexpected DB errors.
pub fn plan_apply(conn: &mut Connection, params: &ApplyPlanParams) -> Result<ApplyPlan> {
    let mut violations = ApplyViolations::default();

    // --- Preconditions (return Err) ---

    // Check all lock entries have content hashes
    let unhashed: Vec<&str> = params
        .sources
        .iter()
        .filter(|s| s.object_id.is_none())
        .map(|s| s.path.as_str())
        .collect();
    if !unhashed.is_empty() {
        bail!(
            "Manifest contains {} sources without content hash. \
             Cannot apply unhashed sources — deduplication and integrity checks require content hashes.\n\
             Import hashes via worklist pipeline, then run 'canon cluster refresh <manifest>'.",
            unhashed.len()
        );
    }

    // Check archive has complete hash coverage
    let (total, archive_unhashed) =
        repo::source::count_unhashed_for_root(conn, params.archive_root_id)?;
    if archive_unhashed > 0 {
        bail!(
            "Destination archive has {archive_unhashed} files without content hash (out of {total}). \
             Cannot reliably detect duplicates without complete hash coverage.\n\
             Run 'canon scan <archive-path>' to index and hash the archive."
        );
    }

    // --- Batch fetch facts for pattern evaluation ---

    let source_ids: Vec<i64> = params.sources.iter().map(|s| s.id).collect();
    let mut all_facts: HashMap<i64, Vec<FactEntry>> = HashMap::new();
    for key in params.needed_keys {
        if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
            continue;
        }
        let key_facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, key)?;
        for (source_id, entry_opt) in key_facts {
            if let Some(entry) = entry_opt {
                all_facts.entry(source_id).or_default().push(entry);
            }
        }
    }

    // --- Expand patterns and build transfers ---

    let mut transfers: Vec<ApplyTransfer> = Vec::with_capacity(params.sources.len());

    for source in params.sources {
        match evaluate_pattern(
            params.pattern,
            source,
            params.needed_keys,
            params.scope_prefix,
            params.root_paths,
            &all_facts,
        ) {
            Ok(dest_rel) => {
                let archive_rel_path =
                    compute_archive_rel_path(params.base_dir_rel, &dest_rel);
                transfers.push(ApplyTransfer {
                    source_id: source.id,
                    source_path: source.path.clone(),
                    dest_rel_path: dest_rel,
                    archive_rel_path,
                    object_id: source.object_id,
                    partial_hash: source.partial_hash.clone(),
                    size: source.size,
                    mtime: source.mtime,
                });
            }
            Err(e) => {
                violations
                    .expansion_failures
                    .push((source.path.clone(), e.to_string()));
            }
        }
    }

    // --- Detect destination path collisions ---

    let mut dest_to_sources: HashMap<&str, Vec<&str>> = HashMap::new();
    for t in &transfers {
        dest_to_sources
            .entry(&t.dest_rel_path)
            .or_default()
            .push(&t.source_path);
    }
    let mut collisions: Vec<(String, Vec<String>)> = dest_to_sources
        .into_iter()
        .filter(|(_, srcs)| srcs.len() > 1)
        .map(|(dest, srcs)| (dest.to_string(), srcs.into_iter().map(|s| s.to_string()).collect()))
        .collect();
    collisions.sort_by(|a, b| a.0.cmp(&b.0));
    violations.collisions = collisions;

    // --- Check stale records + destination conflicts (DB) ---
    // One batch_check_paths_exist call serves both checks and resume filtering.

    let archive_rel_paths: Vec<&str> = transfers.iter().map(|t| t.archive_rel_path.as_str()).collect();
    let paths_in_db = repo::source::batch_check_paths_exist(
        conn,
        params.archive_root_id,
        &archive_rel_paths,
    )?;

    // Stale records: any dest path already in DB (in regular mode, this is unexpected)
    let mut stale_records: Vec<String> = paths_in_db.iter().cloned().collect();
    stale_records.sort();
    violations.stale_records = stale_records;

    // Destination conflicts (non-resume only): same data, different violation field
    if !params.resume {
        let mut dest_conflicts: Vec<String> = paths_in_db.iter().cloned().collect();
        dest_conflicts.sort();
        violations.dest_conflicts_in_db = dest_conflicts;
    }

    // --- Check archive conflicts ---

    let hash_values: Vec<&str> = transfers
        .iter()
        .filter_map(|t| {
            // Find original lock entry to get hash_value
            params
                .sources
                .iter()
                .find(|s| s.id == t.source_id)
                .and_then(|s| s.hash_value.as_deref())
        })
        .collect();

    if !hash_values.is_empty() {
        let archive_info = repo::object::batch_find_archive_info_by_hash(conn, &hash_values)?;

        for transfer in &transfers {
            let hash = params
                .sources
                .iter()
                .find(|s| s.id == transfer.source_id)
                .and_then(|s| s.hash_value.as_deref());

            if let Some(hash) = hash {
                if let Some(info_list) = archive_info.get(hash) {
                    if let Some(&(archive_id, ref archive_path)) = info_list.first() {
                        if archive_id == params.archive_root_id {
                            violations.archive_conflicts_dest.push((
                                transfer.source_path.clone(),
                                archive_path.clone(),
                            ));
                        } else {
                            violations.archive_conflicts_other.push((
                                transfer.source_path.clone(),
                                archive_path.clone(),
                            ));
                        }
                    }
                }
            }
        }
    }

    // --- Check excluded, suspended, and stale sources (one batch fetch) ---

    let transfer_source_ids: Vec<i64> = transfers.iter().map(|t| t.source_id).collect();
    let sources_map = repo::source::batch_fetch_by_ids(conn, &transfer_source_ids)?;

    let mut stale_sources = Vec::new();

    for transfer in &transfers {
        if let Some(db_source) = sources_map.get(&transfer.source_id) {
            // Excluded check
            if db_source.is_excluded() {
                violations
                    .excluded_sources
                    .push((transfer.source_id, transfer.source_path.clone()));
            }

            // Suspended check
            if !db_source.is_active() {
                violations
                    .suspended_sources
                    .push((transfer.source_id, transfer.source_path.clone()));
            }

            // DB-based staleness check (compare lock entry vs current DB state)
            let mut mismatches = Vec::new();
            if db_source.size != transfer.size {
                mismatches.push(format!("size: {} → {}", transfer.size, db_source.size));
            }
            if db_source.mtime != transfer.mtime {
                mismatches.push(format!("mtime: {} → {}", transfer.mtime, db_source.mtime));
            }
            if db_source.partial_hash.is_empty() {
                mismatches.push("partial hash: missing in DB".to_string());
            } else if db_source.partial_hash != transfer.partial_hash {
                mismatches.push(format!(
                    "partial hash: {}... → {}...",
                    &transfer.partial_hash[..16.min(transfer.partial_hash.len())],
                    &db_source.partial_hash[..16.min(db_source.partial_hash.len())]
                ));
            }

            if !mismatches.is_empty() {
                stale_sources.push(StaleSource {
                    path: transfer.source_path.clone(),
                    reason: mismatches.join(", "),
                });
            }
        }
    }

    // --- Resume mode: filter out sources already in DB ---

    let mut already_archived_count = 0;
    if params.resume {
        let before_len = transfers.len();
        transfers.retain(|t| !paths_in_db.contains(&t.archive_rel_path));
        already_archived_count = before_len - transfers.len();
    }

    Ok(ApplyPlan {
        transfers,
        violations,
        stale_sources,
        already_archived_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_fact, insert_object, insert_root, insert_source_excluded, insert_source_with_metadata,
        setup_test_db,
    };

    fn make_lock_entry(id: i64, root_id: i64, path: &str, object_id: Option<i64>, hash: Option<&str>) -> LockEntry {
        LockEntry {
            id,
            root_id,
            path: path.to_string(),
            device: 0,
            inode: 0,
            size: 1000,
            mtime: 1704067200,
            partial_hash: "testhash".to_string(),
            object_id,
            hash_type: hash.map(|_| "sha256".to_string()),
            hash_value: hash.map(|h| h.to_string()),
        }
    }

    fn default_params<'a>(
        sources: &'a [&'a LockEntry],
        pattern: &'a Pattern,
        needed_keys: &'a [String],
        root_paths: &'a HashMap<i64, String>,
        archive_root_id: i64,
    ) -> ApplyPlanParams<'a> {
        ApplyPlanParams {
            sources,
            pattern,
            needed_keys,
            scope_prefix: None,
            root_paths,
            archive_root_id,
            base_dir_rel: "",
            resume: false,
        }
    }

    // =========================================================================
    // Pattern expansion and destination computation
    // =========================================================================

    #[test]
    fn test_plan_apply_computes_dest_paths() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "vacation/photo.jpg", Some(obj_id), 1000, 1704067200);
        insert_fact(&conn, src_id, "content.Make", "Canon");

        let entry = make_lock_entry(src_id, root_id, "/photos/vacation/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.transfers[0].dest_rel_path, "Canon/photo.jpg");
        assert_eq!(plan.transfers[0].archive_rel_path, "Canon/photo.jpg");
        assert!(plan.violations.expansion_failures.is_empty());
    }

    #[test]
    fn test_plan_apply_dest_paths_with_base_dir() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "2024/vacation";

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.transfers[0].dest_rel_path, "photo.jpg");
        assert_eq!(plan.transfers[0].archive_rel_path, "2024/vacation/photo.jpg");
    }

    #[test]
    fn test_plan_apply_expansion_failure() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // No fact inserted — pattern requires content.Make

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.transfers.is_empty());
        assert_eq!(plan.violations.expansion_failures.len(), 1);
        assert_eq!(plan.violations.expansion_failures[0].0, "/photos/photo.jpg");
    }

    // =========================================================================
    // Collision detection
    // =========================================================================

    #[test]
    fn test_plan_apply_detects_collisions() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        // Two sources with different names but same content.Make → same dest
        let src1 = insert_source_with_metadata(&conn, root_id, "a/photo.jpg", Some(obj1), 1000, 1704067200);
        let src2 = insert_source_with_metadata(&conn, root_id, "b/photo.jpg", Some(obj2), 1000, 1704067200);
        insert_fact(&conn, src1, "content.Make", "Canon");
        insert_fact(&conn, src2, "content.Make", "Canon");

        let e1 = make_lock_entry(src1, root_id, "/photos/a/photo.jpg", Some(obj1), Some("hash1"));
        let e2 = make_lock_entry(src2, root_id, "/photos/b/photo.jpg", Some(obj2), Some("hash2"));
        let sources: Vec<&LockEntry> = vec![&e1, &e2];
        // Pattern uses only Make + filename → both expand to "Canon/photo.jpg"
        let pattern = expr::parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.collisions.len(), 1);
        assert_eq!(plan.violations.collisions[0].0, "Canon/photo.jpg");
        assert_eq!(plan.violations.collisions[0].1.len(), 2);
    }

    #[test]
    fn test_plan_apply_no_collision_different_paths() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let src1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1704067200);
        let src2 = insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 1704067200);

        let e1 = make_lock_entry(src1, root_id, "/photos/a.jpg", Some(obj1), Some("hash1"));
        let e2 = make_lock_entry(src2, root_id, "/photos/b.jpg", Some(obj2), Some("hash2"));
        let sources: Vec<&LockEntry> = vec![&e1, &e2];
        // filename pattern produces different paths
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.violations.collisions.is_empty());
        assert_eq!(plan.transfers.len(), 2);
    }

    // =========================================================================
    // Archive conflict detection
    // =========================================================================

    #[test]
    fn test_plan_apply_archive_conflict_dest() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Same hash already in destination archive
        insert_source_with_metadata(&conn, archive_id, "existing/photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.archive_conflicts_dest.len(), 1);
        assert!(plan.violations.archive_conflicts_other.is_empty());
    }

    #[test]
    fn test_plan_apply_archive_conflict_other() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let other_archive = insert_root(&conn, "/other-archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Same hash in OTHER archive
        insert_source_with_metadata(&conn, other_archive, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.violations.archive_conflicts_dest.is_empty());
        assert_eq!(plan.violations.archive_conflicts_other.len(), 1);
    }

    #[test]
    fn test_plan_apply_no_archive_conflict() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.violations.archive_conflicts_dest.is_empty());
        assert!(plan.violations.archive_conflicts_other.is_empty());
    }

    // =========================================================================
    // Excluded and suspended source detection
    // =========================================================================

    #[test]
    fn test_plan_apply_detects_excluded() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_excluded(&conn, root_id, "photo.jpg", Some(obj_id));

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.excluded_sources.len(), 1);
        assert_eq!(plan.violations.excluded_sources[0].0, src_id);
    }

    #[test]
    fn test_plan_apply_detects_suspended() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true); // suspended
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.suspended_sources.len(), 1);
        assert_eq!(plan.violations.suspended_sources[0].0, src_id);
    }

    // =========================================================================
    // Stale records and destination conflicts
    // =========================================================================

    #[test]
    fn test_plan_apply_detects_stale_records() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Destination path already has a record in archive
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.stale_records.len(), 1);
        assert_eq!(plan.violations.stale_records[0], "photo.jpg");
    }

    #[test]
    fn test_plan_apply_detects_dest_conflicts() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Destination path occupied
        let obj2 = insert_object(&conn, "different_hash", false);
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj2), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = false;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.dest_conflicts_in_db.len(), 1);
    }

    #[test]
    fn test_plan_apply_dest_conflicts_skipped_in_resume() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Same path exists in archive
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        // In resume mode, dest_conflicts_in_db is not populated
        assert!(plan.violations.dest_conflicts_in_db.is_empty());
        // But the source IS filtered out of transfers
        assert!(plan.transfers.is_empty());
        assert_eq!(plan.already_archived_count, 1);
    }

    // =========================================================================
    // DB source state validation
    // =========================================================================

    #[test]
    fn test_plan_apply_detects_stale_sources() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        // DB has size=2000 but lock entry has size=1000
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 2000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.stale_sources.len(), 1);
        assert!(plan.stale_sources[0].reason.contains("size"));
    }

    #[test]
    fn test_plan_apply_fresh_sources_not_stale() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        // DB matches lock entry exactly
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.stale_sources.is_empty());
    }

    // =========================================================================
    // Resume mode
    // =========================================================================

    #[test]
    fn test_plan_apply_resume_filters_archived() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Already in archive at same dest path
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.transfers.is_empty());
        assert_eq!(plan.already_archived_count, 1);
    }

    #[test]
    fn test_plan_apply_resume_keeps_non_archived() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // No existing entry in archive

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.already_archived_count, 0);
    }

    // =========================================================================
    // Preconditions
    // =========================================================================

    #[test]
    fn test_plan_apply_err_unhashed() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        // Source without object_id
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", None, 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", None, None);
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let result = plan_apply(&mut conn, &params);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("without content hash"));
    }

    #[test]
    fn test_plan_apply_err_archive_hash_gap() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Archive has an unhashed file
        insert_source_with_metadata(&conn, archive_id, "unhashed.jpg", None, 500, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let result = plan_apply(&mut conn, &params);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("without content hash"));
    }

    // =========================================================================
    // Empty/edge cases
    // =========================================================================

    #[test]
    fn test_plan_apply_empty_sources() {
        let mut conn = setup_test_db();
        let archive_id = insert_root(&conn, "/archive", "archive", false);

        let sources: Vec<&LockEntry> = vec![];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let root_paths = HashMap::new();

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.transfers.is_empty());
        assert!(plan.violations.expansion_failures.is_empty());
        assert!(plan.violations.collisions.is_empty());
        assert_eq!(plan.already_archived_count, 0);
    }
}
