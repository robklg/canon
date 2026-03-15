//! Apply operations — plan and execute for file transfers.
//!
//! `plan_apply()` validates constraints and computes destination paths.
//! `execute_apply()` performs file transfers, staleness validation, and DB registration.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::cluster::LockEntry;
use super::fs::{compute_partial_hash, copy_file, ensure_parent_dir, move_file, rename_file, MoveOutcome};
use crate::domain::apply::{classify_destination, DestinationState};
use crate::domain::fact::FactEntry;
use crate::domain::path::path_strip_prefix;
use crate::domain::source::NewSource;
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

// ===========================================================================
// Execute types
// ===========================================================================

/// Outcome of a single transfer operation.
#[derive(Debug)]
pub enum TransferOutcome {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String),
    Error(String),
}

/// Progress notification for file transfer operations.
/// The interface implements this to display progress, verbose logging, etc.
/// Fire-and-forget — does not affect the operation's behavior.
pub trait TransferProgress {
    /// Called once before the transfer loop begins.
    fn on_start(&self, total: usize);
    /// Called after each transfer completes.
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome);
    /// Called once after the transfer loop ends.
    fn on_finish(&self);
}

/// No-op implementation for tests.
pub struct NoopProgress;
impl TransferProgress for NoopProgress {
    fn on_start(&self, _total: usize) {}
    fn on_transfer(&self, _index: usize, _total: usize, _source_path: &str, _outcome: &TransferOutcome) {}
    fn on_finish(&self) {}
}

/// Parameters for executing an apply operation.
pub struct ApplyExecuteParams {
    /// Base directory for destination paths (archive root + base_dir from manifest).
    pub base_dir: PathBuf,
    /// Archive root ID for DB registration.
    pub archive_root_id: i64,
    /// How to transfer files.
    pub transfer_mode: TransferMode,
    /// Whether this is a resume operation.
    pub resume: bool,
}

/// Result of executing an apply operation.
pub struct ApplyResult {
    pub copied: u64,
    pub renamed: u64,
    pub moved: u64,
    pub skipped_missing: u64,
    pub skipped_stale: Vec<StaleSource>,
    pub errors: Vec<TransferError>,
    /// Resume mode: count of sources already registered in archive DB.
    pub already_archived: u64,
    /// Resume mode: count of files on disk with correct size (need scan, not transfer).
    pub resumed: u64,
}

/// An error encountered during a file transfer.
pub struct TransferError {
    pub path: String,
    pub error: String,
}

/// Result of disk classification in resume mode.
#[derive(Debug)]
struct ResumeClassification<'a> {
    /// Transfers that need to be executed (not on disk).
    to_transfer: Vec<&'a ApplyTransfer>,
    /// Count of files on disk with correct size (skipped, need scan).
    resumed: usize,
}

struct SizeMismatchError {
    dest_path: String,
    expected: u64,
    actual: u64,
}

// ===========================================================================
// Execute function
// ===========================================================================

/// Execute file transfers from a computed apply plan.
///
/// Performs: source readability checks, resume disk classification (if resume mode),
/// batch staleness validation, and the transfer loop. Each transfer is independent
/// (no transaction wrapping) — the operation is idempotent and resume-safe.
///
/// Does NOT handle: manifest parsing, confirmation prompts, dry-run display,
/// output formatting. These are interface concerns.
pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
) -> Result<ApplyResult> {
    let mut result = ApplyResult {
        copied: 0,
        renamed: 0,
        moved: 0,
        skipped_missing: 0,
        skipped_stale: Vec::new(),
        errors: Vec::new(),
        already_archived: plan.already_archived_count as u64,
        resumed: 0,
    };

    // --- Source readability pre-check ---
    let mut unreadable: Vec<(String, String)> = Vec::new();
    for transfer in &plan.transfers {
        match File::open(&transfer.source_path) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                unreadable.push((transfer.source_path.clone(), "permission denied".to_string()));
            }
            Err(e) => {
                unreadable.push((transfer.source_path.clone(), e.to_string()));
            }
        }
    }
    if !unreadable.is_empty() {
        bail!(
            "{} sources are not readable: {}",
            unreadable.len(),
            unreadable
                .iter()
                .take(10)
                .map(|(p, r)| format!("{p} ({r})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // --- Resume mode: disk classification ---
    let transfers_to_execute: Vec<&ApplyTransfer> = if params.resume {
        let classification = classify_transfers_disk(&plan.transfers, &params.base_dir)?;
        result.resumed = classification.resumed as u64;
        classification.to_transfer
    } else {
        plan.transfers.iter().collect()
    };

    // --- Batch staleness validation ---
    if !transfers_to_execute.is_empty() {
        let stale = validate_transfers_disk(&transfers_to_execute);
        if !stale.is_empty() {
            bail!(
                "{} sources have changed since manifest was generated. \
                 Run `canon scan` then `cluster refresh` to regenerate the lock file. \
                 First: {} ({})",
                stale.len(),
                stale[0].path,
                stale[0].reason
            );
        }
    }

    // --- Transfer loop ---
    let total = transfers_to_execute.len();
    progress.on_start(total);

    for (i, transfer) in transfers_to_execute.iter().enumerate() {
        let outcome = match execute_single_transfer(
            transfer,
            &params.base_dir,
            params.transfer_mode,
            conn,
            params.archive_root_id,
        ) {
            Ok(outcome) => outcome,
            Err(e) => TransferOutcome::Error(e.to_string()),
        };

        match &outcome {
            TransferOutcome::Copied => result.copied += 1,
            TransferOutcome::Renamed => result.renamed += 1,
            TransferOutcome::Moved => result.moved += 1,
            TransferOutcome::SkippedMissing => result.skipped_missing += 1,
            TransferOutcome::SkippedStale(reason) => {
                result.skipped_stale.push(StaleSource {
                    path: transfer.source_path.clone(),
                    reason: reason.clone(),
                });
            }
            TransferOutcome::Error(msg) => {
                result.errors.push(TransferError {
                    path: transfer.source_path.clone(),
                    error: msg.clone(),
                });
            }
        }

        progress.on_transfer(i, total, &transfer.source_path, &outcome);
    }

    progress.on_finish();

    Ok(result)
}

// ===========================================================================
// Execute helpers (private)
// ===========================================================================

/// Execute a single file transfer. Returns the outcome.
fn execute_single_transfer(
    transfer: &ApplyTransfer,
    base_dir: &Path,
    transfer_mode: TransferMode,
    conn: &Connection,
    archive_root_id: i64,
) -> Result<TransferOutcome> {
    let src_path = Path::new(&transfer.source_path);
    let dest_path = base_dir.join(&transfer.dest_rel_path);

    // Check if source exists
    if !src_path.exists() {
        return Ok(TransferOutcome::SkippedMissing);
    }

    // Per-transfer staleness validation (catches race conditions)
    if let Err(reason) = validate_source_state(transfer) {
        return Ok(TransferOutcome::SkippedStale(reason));
    }

    // Create parent directories
    ensure_parent_dir(&dest_path)?;

    match transfer_mode {
        TransferMode::Copy => {
            copy_file(src_path, &dest_path, true)?;
            let new_source = build_new_source(
                &dest_path,
                archive_root_id,
                &transfer.archive_rel_path,
                transfer.object_id,
                &transfer.partial_hash,
            )?;
            repo::source::insert_destination(conn, &new_source)?;
            Ok(TransferOutcome::Copied)
        }
        TransferMode::Rename => {
            rename_file(src_path, &dest_path, true)?;
            relocate_source(conn, transfer.source_id, archive_root_id, &transfer.archive_rel_path)?;
            Ok(TransferOutcome::Renamed)
        }
        TransferMode::Move => {
            match move_file(src_path, &dest_path, true)? {
                MoveOutcome::Renamed => {
                    relocate_source(conn, transfer.source_id, archive_root_id, &transfer.archive_rel_path)?;
                    Ok(TransferOutcome::Renamed)
                }
                MoveOutcome::CopiedAndDeleted => {
                    mark_source_not_present(conn, transfer.source_id)?;
                    let new_source = build_new_source(
                        &dest_path,
                        archive_root_id,
                        &transfer.archive_rel_path,
                        transfer.object_id,
                        &transfer.partial_hash,
                    )?;
                    repo::source::insert_destination(conn, &new_source)?;
                    Ok(TransferOutcome::Moved)
                }
            }
        }
    }
}

/// Validate that a source file on disk matches the state recorded in the transfer.
fn validate_source_state(transfer: &ApplyTransfer) -> std::result::Result<(), String> {
    let meta = match fs::metadata(&transfer.source_path) {
        Ok(m) => m,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err("file not found".to_string());
        }
        Err(e) => {
            return Err(format!("cannot stat: {e}"));
        }
    };

    let mut mismatches = Vec::new();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let current_size = meta.size() as i64;
        let current_mtime = meta.mtime();

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
        }
    }

    #[cfg(not(unix))]
    {
        let current_size = meta.len() as i64;
        let current_mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
        }
    }

    // Partial hash check — recompute from disk and compare to lock
    let current_hash =
        compute_partial_hash(Path::new(&transfer.source_path), transfer.size as u64)
            .map_err(|e| format!("failed to compute partial hash: {e}"))?;
    if current_hash != transfer.partial_hash {
        mismatches.push(format!(
            "partial hash mismatch: {}... → {}...",
            &transfer.partial_hash[..16.min(transfer.partial_hash.len())],
            &current_hash[..16]
        ));
    }

    if !mismatches.is_empty() {
        Err(mismatches.join(", "))
    } else {
        Ok(())
    }
}

/// Batch validate source file states against disk.
fn validate_transfers_disk(transfers: &[&ApplyTransfer]) -> Vec<StaleSource> {
    let mut stale = Vec::new();
    for transfer in transfers {
        if let Err(reason) = validate_source_state(transfer) {
            stale.push(StaleSource {
                path: transfer.source_path.clone(),
                reason,
            });
        }
    }
    stale
}

/// Classify transfers on disk for resume mode. Separates transfers still
/// needed from files already on disk with correct size.
fn classify_transfers_disk<'a>(
    transfers: &'a [ApplyTransfer],
    base_dir: &Path,
) -> Result<ResumeClassification<'a>> {
    let mut to_transfer = Vec::new();
    let mut resumed = 0usize;
    let mut size_mismatches = Vec::new();

    for transfer in transfers {
        let full_path = base_dir.join(&transfer.dest_rel_path);
        let expected_size = transfer.size as u64;

        let on_disk = if full_path.exists() {
            match fs::metadata(&full_path) {
                Ok(meta) => Some(meta.len()),
                Err(_) => None,
            }
        } else {
            None
        };

        let state = classify_destination(false, on_disk, expected_size);

        match state {
            DestinationState::Available => {
                to_transfer.push(transfer);
            }
            DestinationState::Archived => {
                // Should not happen (in_db=false always), but handle gracefully
                to_transfer.push(transfer);
            }
            DestinationState::Resumed => {
                resumed += 1;
            }
            DestinationState::SizeMismatch { expected, actual } => {
                size_mismatches.push(SizeMismatchError {
                    dest_path: full_path.display().to_string(),
                    expected,
                    actual,
                });
            }
        }
    }

    if !size_mismatches.is_empty() {
        let details: Vec<String> = size_mismatches
            .iter()
            .take(10)
            .map(|e| format!("{} (expected {} bytes, found {})", e.dest_path, e.expected, e.actual))
            .collect();
        let suffix = if size_mismatches.len() > 10 {
            format!(" ... and {} more", size_mismatches.len() - 10)
        } else {
            String::new()
        };
        bail!(
            "Found {} partial/mismatched files in destination. \
             Delete the partial files, then re-run with --resume. {}{}",
            size_mismatches.len(),
            details.join("; "),
            suffix
        );
    }

    Ok(ResumeClassification {
        to_transfer,
        resumed,
    })
}

/// Relocate an existing source to a new location (for rename/move on same device).
/// Updates the source row in-place since the inode remains the same.
fn relocate_source(
    conn: &Connection,
    source_id: i64,
    archive_root_id: i64,
    rel_path: &str,
) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
    repo::source::update_location(conn, source_id, archive_root_id, rel_path, now)
}

/// Mark a source as no longer present (for cross-device move after deletion).
fn mark_source_not_present(conn: &Connection, source_id: i64) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
    repo::source::mark_missing(conn, &[source_id], now)?;
    Ok(())
}

/// Build a NewSource from destination file metadata for DB registration.
#[cfg(unix)]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
) -> Result<NewSource> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(dest_path).with_context(|| {
        format!("Failed to read metadata for registration: {}", dest_path.display())
    })?;
    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.size() as i64,
        mtime: meta.mtime(),
        partial_hash: partial_hash.to_string(),
        object_id,
        device: Some(meta.dev() as i64),
        inode: Some(meta.ino() as i64),
    })
}

#[cfg(not(unix))]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
) -> Result<NewSource> {
    let meta = fs::metadata(dest_path).with_context(|| {
        format!("Failed to read metadata for registration: {}", dest_path.display())
    })?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.len() as i64,
        mtime,
        partial_hash: partial_hash.to_string(),
        object_id,
        device: None,
        inode: None,
    })
}

// ===========================================================================
// Tests
// ===========================================================================

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

    // =========================================================================
    // validate_source_state tests
    // =========================================================================

    fn make_transfer_for_file(path: &Path, size: i64, mtime: i64, partial_hash: &str) -> ApplyTransfer {
        ApplyTransfer {
            source_id: 1,
            source_path: path.display().to_string(),
            dest_rel_path: "dest.jpg".to_string(),
            archive_rel_path: "dest.jpg".to_string(),
            object_id: Some(1),
            partial_hash: partial_hash.to_string(),
            size,
            mtime,
        }
    }

    #[test]
    fn validate_unchanged_file() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        f.flush().unwrap();

        let meta = std::fs::metadata(f.path()).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = {
            (meta.len() as i64, 0i64)
        };

        let hash = compute_partial_hash(f.path(), size as u64).unwrap();
        let transfer = make_transfer_for_file(f.path(), size, mtime, &hash);
        assert!(validate_source_state(&transfer).is_ok());
    }

    #[test]
    fn validate_missing_file() {
        let transfer = make_transfer_for_file(
            Path::new("/nonexistent/file.jpg"),
            1000,
            1704067200,
            "testhash",
        );
        let err = validate_source_state(&transfer).unwrap_err();
        assert!(err.contains("not found"), "expected 'not found', got: {err}");
    }

    #[test]
    fn validate_size_changed() {
        use std::io::Write;

        // Create file, record metadata, then change it
        let f = tempfile::NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        std::fs::write(&path, b"short").unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        #[cfg(unix)]
        let (orig_size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (orig_size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&path, orig_size as u64).unwrap();

        // Now write more data so size changes
        std::fs::write(&path, b"much longer content here").unwrap();

        let transfer = make_transfer_for_file(&path, orig_size, mtime, &hash);
        let err = validate_source_state(&transfer).unwrap_err();
        assert!(err.contains("size"), "expected 'size' in error, got: {err}");
    }

    #[test]
    fn validate_hash_changed() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        f.flush().unwrap();

        let meta = std::fs::metadata(f.path()).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = {
            (meta.len() as i64, 0i64)
        };

        // Correct size/mtime but wrong hash
        let transfer = make_transfer_for_file(f.path(), size, mtime, "wrong_hash_value_here");
        let err = validate_source_state(&transfer).unwrap_err();
        assert!(err.contains("partial hash"), "expected 'partial hash' in error, got: {err}");
    }

    // =========================================================================
    // classify_transfers_disk tests
    // =========================================================================

    #[test]
    fn classify_available_when_dest_missing() {
        let dir = tempfile::tempdir().unwrap();
        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: "/src/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_transfers_disk(&transfers, dir.path()).unwrap();
        assert_eq!(result.to_transfer.len(), 1);
        assert_eq!(result.resumed, 0);
    }

    #[test]
    fn classify_resumed_when_size_matches() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: "/src/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_transfers_disk(&transfers, dir.path()).unwrap();
        assert_eq!(result.to_transfer.len(), 0);
        assert_eq!(result.resumed, 1);
    }

    #[test]
    fn classify_size_mismatch_errors() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest).unwrap();
        f.write_all(&vec![0u8; 500]).unwrap(); // Wrong size

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: "/src/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let err = classify_transfers_disk(&transfers, dir.path());
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("partial/mismatched"), "expected size mismatch error, got: {msg}");
    }

    // =========================================================================
    // execute_single_transfer tests (integration, uses tempfiles)
    // =========================================================================

    #[test]
    fn execute_copy_creates_file() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&src_file).unwrap();
        f.write_all(b"photo data").unwrap();
        drop(f);

        let meta = std::fs::metadata(&src_file).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&src_file, size as u64).unwrap();

        let obj_id = insert_object(&conn, "abc123", false);

        let dest_dir = tempfile::tempdir().unwrap();
        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(obj_id),
            partial_hash: hash,
            size,
            mtime,
        };

        let outcome = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::Copied));
        assert!(dest_dir.path().join("photo.jpg").exists());
    }

    #[test]
    fn execute_copy_noclobber() {
        use std::io::Write;
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let src_dir = tempfile::tempdir().unwrap();
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file).unwrap().write_all(b"data").unwrap();

        let meta = std::fs::metadata(&src_file).unwrap();
        #[cfg(unix)]
        let (size, mtime) = {
            use std::os::unix::fs::MetadataExt;
            (meta.size() as i64, meta.mtime())
        };
        #[cfg(not(unix))]
        let (size, mtime) = (meta.len() as i64, 0i64);
        let hash = compute_partial_hash(&src_file, size as u64).unwrap();

        let dest_dir = tempfile::tempdir().unwrap();
        // Pre-create destination
        std::fs::File::create(dest_dir.path().join("photo.jpg")).unwrap();

        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: hash,
            size,
            mtime,
        };

        let result = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn execute_source_missing() {
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let dest_dir = tempfile::tempdir().unwrap();

        let transfer = ApplyTransfer {
            source_id: 1,
            source_path: "/nonexistent/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        };

        let outcome = execute_single_transfer(
            &transfer,
            dest_dir.path(),
            TransferMode::Copy,
            &conn,
            archive_root,
        )
        .unwrap();

        assert!(matches!(outcome, TransferOutcome::SkippedMissing));
    }
}
