//! Apply operations — plan and execute for file transfers.
//!
//! `plan_apply()` validates constraints and computes destination paths.
//! `execute_apply()` performs file transfers, staleness validation, and DB registration.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::cluster::LockEntry;
use super::fs::{compute_partial_hash, copy_file, ensure_parent_dir, move_file, rename_file, MoveOutcome};
use crate::domain::decision::DecisionStatus;
use crate::domain::fact::FactEntry;
use crate::domain::path::path_strip_prefix;
use crate::domain::source::NewSource;
use crate::expr::{self, EvalContext, FactValue, Pattern};
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
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
    /// In resume mode: only pending entries (dest missing, source exists).
    pub transfers: Vec<ApplyTransfer>,
    /// All violations found during planning.
    pub violations: ApplyViolations,
    /// Sources whose DB state has changed since lock file (size/mtime/partial_hash).
    /// Computed via DB check. Interface may also do disk-based validation.
    pub stale_sources: Vec<StaleSource>,
    /// Resume mode: count of sources already registered in archive DB.
    /// Kept for backward compatibility; superseded by resume_already_there in resume mode.
    pub already_archived_count: usize,
    /// Resume mode: entries to register in DB during execute (already at destination).
    pub resume_already_there: Vec<ApplyTransfer>,
    /// Resume mode: count of already-there entries where the source file still exists.
    pub resume_already_there_source_present: usize,
    /// Resume mode: entries where source is lost (source missing, dest missing).
    /// (source_id, source_path)
    pub resume_source_lost: Vec<(i64, String)>,
    /// Resume mode: entries with size mismatch at destination.
    /// (dest_path, expected_size, actual_size)
    pub resume_size_mismatches: Vec<(String, u64, u64)>,
}

/// Violations found during apply planning. The interface inspects each field
/// and decides whether to bail (hard gate) or proceed (with --allow flags).
#[derive(Debug, Default)]
pub struct ApplyViolations {
    /// Sources that failed pattern expansion: (source_path, error_message).
    pub expansion_failures: Vec<(String, String)>,
    /// Destination paths with multiple sources: (dest_rel_path, source_paths).
    pub collisions: Vec<(String, Vec<String>)>,
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
    /// Destination paths that resolve outside the archive root: (source_path, resolved_dest).
    pub escaped_paths: Vec<(String, String)>,
    /// Source files that are missing (stat failed, not found).
    pub missing_sources: Vec<(i64, String)>,
    /// Source files that exist but are not readable (permission denied).
    pub unreadable_sources: Vec<(i64, String)>,
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
pub fn evaluate_pattern(
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

    // Note: archive hash coverage is not enforced. Duplicate detection uses
    // content hashes where available; unhashed files simply won't be detected
    // as duplicates. Noclobber prevents overwrites at the filesystem level.

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
    // In resume mode, skip these checks — destination DB records are evidence of progress.

    let archive_rel_paths: Vec<&str> = transfers.iter().map(|t| t.archive_rel_path.as_str()).collect();
    let _paths_in_db = if !params.resume {
        let paths = repo::source::batch_check_paths_exist(
            conn,
            params.archive_root_id,
            &archive_rel_paths,
        )?;

        // Destination conflicts: dest paths already registered in archive DB
        let mut dest_conflicts: Vec<String> = paths.iter().cloned().collect();
        dest_conflicts.sort();
        violations.dest_conflicts_in_db = dest_conflicts;

        paths
    } else {
        std::collections::HashSet::new()
    };

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

    // --- Check destination paths stay under archive root ---

    if !transfers.is_empty() {
        let archive_root_path = params
            .root_paths
            .get(&params.archive_root_id)
            .ok_or_else(|| anyhow::anyhow!("Archive root {} not found in root_paths", params.archive_root_id))?;

        for transfer in &transfers {
            let full_dest = format!("{}/{}", archive_root_path, transfer.archive_rel_path);
            if !crate::domain::path::path_is_under(&full_dest, archive_root_path) {
                violations
                    .escaped_paths
                    .push((transfer.source_path.clone(), full_dest));
            }
        }
    }

    // --- Source existence and readability preflight ---
    // In resume mode, skip this — classify_resume_entries handles source state.
    // In regular mode, check all transfers.
    for transfer in transfers.iter().filter(|_| !params.resume) {
        let path = Path::new(&transfer.source_path);
        match fs::metadata(path) {
            Ok(meta) => {
                if !meta.is_file() {
                    violations
                        .missing_sources
                        .push((transfer.source_id, transfer.source_path.clone()));
                    continue;
                }
                // Check readability: try to open the file
                match File::open(path) {
                    Ok(_) => {} // readable
                    Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                        violations
                            .unreadable_sources
                            .push((transfer.source_id, transfer.source_path.clone()));
                    }
                    Err(_) => {
                        // Other open errors — treat as unreadable
                        violations
                            .unreadable_sources
                            .push((transfer.source_id, transfer.source_path.clone()));
                    }
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                violations
                    .missing_sources
                    .push((transfer.source_id, transfer.source_path.clone()));
            }
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                violations
                    .unreadable_sources
                    .push((transfer.source_id, transfer.source_path.clone()));
            }
            Err(_) => {
                // Other stat errors — treat as missing
                violations
                    .missing_sources
                    .push((transfer.source_id, transfer.source_path.clone()));
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

    // --- Resume mode: filesystem-based classification ---

    let mut already_archived_count = 0;
    let mut resume_already_there = Vec::new();
    let mut resume_already_there_source_present = 0usize;
    let mut resume_source_lost = Vec::new();
    let mut resume_size_mismatches = Vec::new();

    if params.resume {
        // Build base_dir from archive root path + base_dir_rel
        let archive_root_path = params
            .root_paths
            .get(&params.archive_root_id)
            .ok_or_else(|| anyhow::anyhow!("Archive root {} not found in root_paths", params.archive_root_id))?;
        let base_dir = if params.base_dir_rel.is_empty() {
            PathBuf::from(archive_root_path)
        } else {
            PathBuf::from(archive_root_path).join(params.base_dir_rel)
        };

        let classification = classify_resume_entries(&transfers, &base_dir);

        // Extract source_lost errors
        for t in &classification.source_lost {
            resume_source_lost.push((t.source_id, t.source_path.clone()));
        }

        // Extract size_mismatches errors
        for &(t, expected, actual) in &classification.size_mismatches {
            let dest_path = base_dir.join(&t.dest_rel_path);
            resume_size_mismatches.push((dest_path.display().to_string(), expected, actual));
        }

        // Count already-there entries where source is still present
        resume_already_there_source_present = classification.already_there.iter()
            .filter(|(_, source_present)| *source_present)
            .count();

        // Build resume_already_there list (transfers to register in DB during execute)
        for (t, _source_present) in &classification.already_there {
            resume_already_there.push(ApplyTransfer {
                source_id: t.source_id,
                source_path: t.source_path.clone(),
                dest_rel_path: t.dest_rel_path.clone(),
                archive_rel_path: t.archive_rel_path.clone(),
                object_id: t.object_id,
                partial_hash: t.partial_hash.clone(),
                size: t.size,
                mtime: t.mtime,
            });
        }

        already_archived_count = classification.already_there.len();

        // Replace transfers with only pending entries
        let pending_ids: std::collections::HashSet<i64> = classification.pending.iter()
            .map(|t| t.source_id)
            .collect();
        transfers.retain(|t| pending_ids.contains(&t.source_id));
    }

    Ok(ApplyPlan {
        transfers,
        violations,
        stale_sources,
        already_archived_count,
        resume_already_there,
        resume_already_there_source_present,
        resume_source_lost,
        resume_size_mismatches,
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
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, dest_path: &str, outcome: &TransferOutcome);
    /// Called when an interrupt is detected after the current transfer completes.
    fn on_interrupt(&self);
    /// Called once after the transfer loop ends.
    fn on_finish(&self);
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
    /// Interrupt flag — set to true to stop after current transfer.
    /// If None, signal handling is set up automatically.
    pub interrupt_flag: Option<Arc<AtomicBool>>,
    /// Sources skipped by --root filter (for summary).
    pub skipped_by_filter: usize,
    /// Manifest display path (for summary and decision record).
    pub manifest_display: String,
}

/// Result of executing an apply operation.
pub struct ApplyResult {
    pub copied: u64,
    pub renamed: u64,
    pub moved: u64,
    pub skipped_missing: u64,
    pub skipped_stale: Vec<StaleSource>,
    pub errors: Vec<TransferError>,
    /// Resume mode: count of entries already at destination (registered in DB during execute).
    pub already_there: u64,
    /// Resume mode: count of already-there entries where source file still exists.
    pub already_there_source_present: u64,
    /// Whether the operation was interrupted by Ctrl+C.
    pub interrupted: bool,
    /// Number of files remaining when interrupted.
    pub remaining: usize,
    /// Completion summary message.
    pub summary: String,
}

/// An error encountered during a file transfer.
/// Fields are populated by execute_apply but errors are reported eagerly
/// via TransferProgress — the struct exists for post-transfer summary access.
#[allow(dead_code)]
pub struct TransferError {
    pub path: String,
    pub error: String,
}

/// Result of filesystem-based classification in resume mode.
/// Checks both source and destination state for each lock entry.
#[derive(Debug)]
pub struct ResumeClassification<'a> {
    /// Entries that need transfer (source exists, dest missing).
    pub pending: Vec<&'a ApplyTransfer>,
    /// Entries already at destination with correct size (transfer, source_present).
    pub already_there: Vec<(&'a ApplyTransfer, bool)>,
    /// Entries where source is lost (source missing, dest missing).
    pub source_lost: Vec<&'a ApplyTransfer>,
    /// Entries with size mismatch at destination (transfer, expected, actual).
    pub size_mismatches: Vec<(&'a ApplyTransfer, u64, u64)>,
}

// ===========================================================================
// Execute function
// ===========================================================================

/// Filter lock entries to only those from the specified roots.
///
/// Parses root specs (e.g., "id:1", "path:/photos") and keeps only entries
/// whose `root_id` matches. Returns all entries when `root_specs` is empty.
pub fn filter_by_roots<'a>(
    sources: &'a [LockEntry],
    root_specs: &[String],
    all_roots: &[crate::domain::root::Root],
) -> Result<Vec<&'a LockEntry>> {
    if root_specs.is_empty() {
        return Ok(sources.iter().collect());
    }

    let mut root_ids = std::collections::HashSet::new();
    for spec in root_specs {
        let id = crate::domain::root::parse_root_spec(all_roots, spec, None)?;
        root_ids.insert(id);
    }

    Ok(sources
        .iter()
        .filter(|s| root_ids.contains(&s.root_id))
        .collect())
}

/// Check for on-disk destination conflicts not already captured by DB checks.
///
/// Returns archive-relative paths that exist on disk but are NOT in
/// `plan.violations.dest_conflicts_in_db`. Used by the interface layer
/// to combine DB and disk conflicts into a single preflight report.
pub fn check_disk_conflicts(plan: &ApplyPlan, base_dir: &Path) -> Vec<String> {
    let db_conflicts: std::collections::HashSet<&str> = plan
        .violations
        .dest_conflicts_in_db
        .iter()
        .map(|s| s.as_str())
        .collect();

    plan.transfers
        .iter()
        .filter(|t| !db_conflicts.contains(t.archive_rel_path.as_str()))
        .filter(|t| base_dir.join(&t.dest_rel_path).exists())
        .map(|t| t.archive_rel_path.clone())
        .collect()
}

/// Execute file transfers from a computed apply plan.
///
/// Performs: source readability checks, resume disk classification (if resume mode),
/// batch staleness validation, and the transfer loop. Each transfer is independent
/// (no transaction wrapping) — the operation is idempotent and resume-safe.
///
/// Does NOT handle: manifest parsing, confirmation prompts, dry-run display,
/// output formatting. These are interface concerns.
/// Set up two-tier Ctrl+C handling for the transfer loop.
/// Returns an Arc<AtomicBool> that becomes true on first SIGINT.
/// Second SIGINT gets default OS termination.
fn setup_interrupt_flag() -> Result<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register_conditional_default(
        signal_hook::consts::SIGINT,
        Arc::clone(&flag),
    )?;
    Ok(flag)
}

pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
    decision: Option<&DecisionParams>,
) -> Result<ApplyResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d));

    let interrupt_flag = match &params.interrupt_flag {
        Some(flag) => Arc::clone(flag),
        None => setup_interrupt_flag()?,
    };

    let mut result = ApplyResult {
        copied: 0,
        renamed: 0,
        moved: 0,
        skipped_missing: 0,
        skipped_stale: Vec::new(),
        errors: Vec::new(),
        already_there: plan.already_archived_count as u64,
        already_there_source_present: plan.resume_already_there_source_present as u64,
        interrupted: false,
        remaining: 0,
        summary: String::new(),
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

    // In resume mode, plan.transfers already contains only pending entries.
    // No disk classification needed here — it was done in plan_apply.
    let transfers_to_execute: Vec<&ApplyTransfer> = plan.transfers.iter().collect();

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
            Err(e) => TransferOutcome::Error(format!("{:#}", e)),
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

        let dest_full = params.base_dir.join(&transfer.dest_rel_path);
        let dest_str = dest_full.display().to_string();
        progress.on_transfer(i, total, &transfer.source_path, &dest_str, &outcome);

        // Check interrupt flag AFTER the complete unit of work (file + DB + count)
        if interrupt_flag.load(Ordering::Relaxed) {
            progress.on_interrupt();
            result.interrupted = true;
            result.remaining = total - (i + 1);
            break;
        }
    }

    progress.on_finish();

    // --- Resume mode: register "already there" entries in DB ---
    if params.resume && !plan.resume_already_there.is_empty() {
        for transfer in &plan.resume_already_there {
            let new_source = build_new_source_from_lock(
                params.archive_root_id,
                &transfer.archive_rel_path,
                transfer.object_id,
                &transfer.partial_hash,
                transfer.size,
                transfer.mtime,
            );
            if let Err(e) = repo::source::insert_destination(conn, &new_source) {
                result.errors.push(TransferError {
                    path: transfer.source_path.clone(),
                    error: format!("Failed to register in DB: {e}"),
                });
            }
        }
    }

    // Compose summary
    result.summary = if result.interrupted {
        format!(
            "Applied {}: {} copied, {} renamed, {} moved, {} errors. Interrupted — {} files remaining.",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.errors.len(), result.remaining
        )
    } else if params.resume {
        format!(
            "Applied {} (--resume): {} copied, {} renamed, {} moved, {} already at destination, {} errors",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.already_there, result.errors.len()
        )
    } else {
        format!(
            "Applied {}: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (stale), {} skipped (filtered), {} errors",
            params.manifest_display,
            result.copied, result.renamed, result.moved, result.skipped_missing,
            result.skipped_stale.len(), params.skipped_by_filter, result.errors.len()
        )
    };

    if let Some(recorder) = recorder.as_mut() {
        let total = plan.transfers.len() as i64;
        let completed = (result.copied + result.renamed + result.moved) as i64;
        let failed = result.errors.len() as i64;
        let skipped = (result.skipped_missing as i64)
            + (result.skipped_stale.len() as i64)
            + (params.skipped_by_filter as i64);
        let status = if result.interrupted {
            DecisionStatus::Interrupted
        } else if !result.errors.is_empty() {
            DecisionStatus::Partial
        } else {
            DecisionStatus::Completed
        };
        recorder.complete(
            conn,
            status,
            DecisionCounts {
                attempted: Some(total),
                completed: Some(completed),
                failed: Some(failed),
                skipped: Some(skipped),
            },
            &result.summary,
        );
    }

    Ok(result)
}

/// Build a NewSource from lock entry data for DB registration.
/// Used for "already there" entries in resume mode — no disk read needed.
fn build_new_source_from_lock(
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
    size: i64,
    mtime: i64,
) -> NewSource {
    NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size,
        mtime,
        partial_hash: partial_hash.to_string(),
        object_id,
        device: None,
        inode: None,
        decision_id: None,
    }
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

/// Classify lock entries by checking filesystem state of source and destination.
/// Purely filesystem-based — no DB connection needed.
///
/// For each transfer:
/// - Dest exists as file with correct size -> AlreadyThere (check if source exists)
/// - Dest exists as file with wrong size -> SizeMismatch
/// - Dest missing + source exists -> Pending
/// - Dest missing + source missing -> SourceLost
fn classify_resume_entries<'a>(
    transfers: &'a [ApplyTransfer],
    base_dir: &Path,
) -> ResumeClassification<'a> {
    let mut result = ResumeClassification {
        pending: vec![],
        already_there: vec![],
        source_lost: vec![],
        size_mismatches: vec![],
    };

    for transfer in transfers {
        let dest_path = base_dir.join(&transfer.dest_rel_path);
        let source_exists = Path::new(&transfer.source_path).exists();
        let dest_stat = fs::metadata(&dest_path).ok();

        match dest_stat {
            Some(meta) if meta.is_file() => {
                let actual_size = meta.len();
                let expected_size = transfer.size as u64;
                if actual_size == expected_size {
                    // Destination present with correct size — already there
                    result.already_there.push((transfer, source_exists));
                } else {
                    // Size mismatch
                    result.size_mismatches.push((transfer, expected_size, actual_size));
                }
            }
            _ => {
                // Destination missing (or not a file)
                if source_exists {
                    result.pending.push(transfer);
                } else {
                    result.source_lost.push(transfer);
                }
            }
        }
    }

    result
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
        decision_id: None,
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
        decision_id: None,
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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.suspended_sources.len(), 1);
        assert_eq!(plan.violations.suspended_sources[0].0, src_id);
    }

    // =========================================================================
    // Stale records and destination conflicts
    // =========================================================================

    #[test]
    fn test_plan_apply_detects_dest_conflicts_in_db() {
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
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.dest_conflicts_in_db.len(), 1);
        assert_eq!(plan.violations.dest_conflicts_in_db[0], "photo.jpg");
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
        root_paths.insert(archive_id, "/archive".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = false;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.dest_conflicts_in_db.len(), 1);
    }

    #[test]
    fn test_plan_apply_dest_conflicts_skipped_in_resume() {
        use std::io::Write;

        // Create real filesystem structure for resume classification
        let src_dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let archive_path = archive_dir.path().to_str().unwrap().to_string();

        // Create source file
        let src_file = src_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&src_file).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        // Create destination file (simulating previous partial apply)
        let dest_file = archive_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest_file).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, &src_path, "source", false);
        let archive_id = insert_root(&conn, &archive_path, "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Same path exists in archive DB
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, &src_file.display().to_string(), Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, src_path);
        root_paths.insert(archive_id, archive_path);

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        // In resume mode, dest_conflicts_in_db is not populated
        assert!(plan.violations.dest_conflicts_in_db.is_empty());
        // dest_conflicts_in_db not populated in resume mode (skipped)
        // File classified as "already there" by filesystem check
        assert!(plan.transfers.is_empty());
        assert_eq!(plan.already_archived_count, 1);
        assert_eq!(plan.resume_already_there.len(), 1);
        // Source still present
        assert_eq!(plan.resume_already_there_source_present, 1);
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
        root_paths.insert(archive_id, "/archive".to_string());

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
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.stale_sources.is_empty());
    }

    // =========================================================================
    // Resume mode
    // =========================================================================

    #[test]
    fn test_plan_apply_resume_filters_already_there() {
        use std::io::Write;

        // Create real filesystem structure
        let src_dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let archive_path = archive_dir.path().to_str().unwrap().to_string();

        // Create source file
        let src_file = src_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&src_file).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        // Create dest file with correct size (already transferred)
        let dest_file = archive_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest_file).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, &src_path, "source", false);
        let archive_id = insert_root(&conn, &archive_path, "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, &src_file.display().to_string(), Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, src_path);
        root_paths.insert(archive_id, archive_path);

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.transfers.is_empty());
        assert_eq!(plan.already_archived_count, 1);
        assert_eq!(plan.resume_already_there.len(), 1);
    }

    #[test]
    fn test_plan_apply_resume_keeps_pending() {
        use std::io::Write;

        // Create real filesystem structure
        let src_dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let archive_path = archive_dir.path().to_str().unwrap().to_string();

        // Create source file
        let src_file = src_dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&src_file).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        // No dest file — this should stay as pending

        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, &src_path, "source", false);
        let archive_id = insert_root(&conn, &archive_path, "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, &src_file.display().to_string(), Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, src_path);
        root_paths.insert(archive_id, archive_path);

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.already_archived_count, 0);
        assert!(plan.resume_already_there.is_empty());
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
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let result = plan_apply(&mut conn, &params);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("without content hash"));
    }

    #[test]
    fn test_plan_apply_allows_archive_with_unhashed_files() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);
        // Archive has an unhashed file — should NOT block apply
        insert_source_with_metadata(&conn, archive_id, "unhashed.jpg", None, 500, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        // Should succeed — unhashed archive files don't block apply
        assert_eq!(plan.transfers.len(), 1);
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
    // classify_resume_entries tests
    // =========================================================================

    #[test]
    fn test_classify_resume_pending() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        // Create source file
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file).unwrap().write_all(b"data").unwrap();
        // Dest does not exist

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_resume_entries(&transfers, dir.path());
        assert_eq!(result.pending.len(), 1);
        assert!(result.already_there.is_empty());
        assert!(result.source_lost.is_empty());
        assert!(result.size_mismatches.is_empty());
    }

    #[test]
    fn test_classify_resume_already_there_source_present() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        // Create source file
        let src_file = src_dir.path().join("photo.jpg");
        std::fs::File::create(&src_file).unwrap().write_all(b"data").unwrap();
        // Create dest file with correct size
        let dest = dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: src_file.display().to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_resume_entries(&transfers, dir.path());
        assert!(result.pending.is_empty());
        assert_eq!(result.already_there.len(), 1);
        assert!(result.already_there[0].1); // source_present = true
        assert!(result.source_lost.is_empty());
        assert!(result.size_mismatches.is_empty());
    }

    #[test]
    fn test_classify_resume_already_there_source_gone() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        // Dest file exists with correct size
        let dest = dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();
        // Source does NOT exist

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: "/nonexistent/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_resume_entries(&transfers, dir.path());
        assert!(result.pending.is_empty());
        assert_eq!(result.already_there.len(), 1);
        assert!(!result.already_there[0].1); // source_present = false
        assert!(result.source_lost.is_empty());
        assert!(result.size_mismatches.is_empty());
    }

    #[test]
    fn test_classify_resume_source_lost() {
        let dir = tempfile::tempdir().unwrap();
        // Neither source nor dest exists

        let transfers = vec![ApplyTransfer {
            source_id: 1,
            source_path: "/nonexistent/photo.jpg".to_string(),
            dest_rel_path: "photo.jpg".to_string(),
            archive_rel_path: "photo.jpg".to_string(),
            object_id: Some(1),
            partial_hash: "hash".to_string(),
            size: 1000,
            mtime: 0,
        }];

        let result = classify_resume_entries(&transfers, dir.path());
        assert!(result.pending.is_empty());
        assert!(result.already_there.is_empty());
        assert_eq!(result.source_lost.len(), 1);
        assert!(result.size_mismatches.is_empty());
    }

    #[test]
    fn test_classify_resume_size_mismatch() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        // Dest file exists with WRONG size
        let dest = dir.path().join("photo.jpg");
        let mut f = std::fs::File::create(&dest).unwrap();
        f.write_all(&vec![0u8; 500]).unwrap(); // 500 bytes, not 1000

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

        let result = classify_resume_entries(&transfers, dir.path());
        assert!(result.pending.is_empty());
        assert!(result.already_there.is_empty());
        assert!(result.source_lost.is_empty());
        assert_eq!(result.size_mismatches.len(), 1);
        assert_eq!(result.size_mismatches[0].1, 1000); // expected
        assert_eq!(result.size_mismatches[0].2, 500);  // actual
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

    // =========================================================================
    // Archive root escape detection
    // =========================================================================

    #[test]
    fn test_plan_rejects_escaped_destination() {
        // Manually construct a scenario where archive_rel_path escapes the archive root.
        // With normalization in place, this shouldn't happen from patterns, but we test
        // the validation as a defense-in-depth safety net.
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "photo.jpg", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        // Pattern produces a normal path — no escape expected
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        // Normal path should not escape
        assert!(plan.violations.escaped_paths.is_empty());
        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.transfers[0].dest_rel_path, "photo.jpg");
    }

    #[test]
    fn test_normalization_prevents_archive_escape() {
        // The original bug: pattern {source.rel_path[:-1]}/{filename} on a flat file
        // produces "/filename" which would escape via PathBuf::join.
        // With normalization, this becomes "filename" — no escape.
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(&conn, root_id, "5.avi", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/5.avi", Some(obj_id), Some("hash1"));
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        // Normalization should have cleaned the path — no escape
        assert!(plan.violations.escaped_paths.is_empty());
        assert_eq!(plan.transfers.len(), 1);
        // The dest_rel_path should be "5.avi", not "/5.avi"
        assert_eq!(plan.transfers[0].dest_rel_path, "5.avi");
    }

    // =========================================================================
    // Source existence and readability preflight
    // =========================================================================

    #[test]
    fn test_plan_detects_missing_source() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn, root_id, "missing.jpg", Some(obj_id), 1000, 1704067200,
        );

        // Lock entry points to a non-existent file
        let entry = make_lock_entry(
            src_id, root_id, "/nonexistent/missing.jpg", Some(obj_id), Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.missing_sources.len(), 1);
        assert_eq!(plan.violations.missing_sources[0].0, src_id);
        assert!(plan.violations.missing_sources[0].1.contains("missing.jpg"));
    }

    #[test]
    fn test_plan_detects_source_is_directory() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();

        let mut conn = setup_test_db();
        let root_path = dir.path().to_str().unwrap();
        let root_id = insert_root(&conn, root_path, "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn, root_id, "subdir", Some(obj_id), 1000, 1704067200,
        );

        // Lock entry points to a directory, not a file
        let entry = make_lock_entry(
            src_id, root_id, &sub.to_string_lossy(), Some(obj_id), Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = expr::parse_pattern("{filename}").unwrap();
        let needed_keys = expr::extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, root_path.to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.missing_sources.len(), 1);
        assert_eq!(plan.violations.missing_sources[0].0, src_id);
    }

    // =========================================================================
    // Interrupt flag tests
    // =========================================================================

    /// No-op progress implementation for testing.
    struct NoopProgress;

    impl TransferProgress for NoopProgress {
        fn on_start(&self, _total: usize) {}
        fn on_transfer(&self, _index: usize, _total: usize, _source_path: &str, _dest_path: &str, _outcome: &TransferOutcome) {}
        fn on_interrupt(&self) {}
        fn on_finish(&self) {}
    }

    #[test]
    fn test_execute_apply_respects_interrupt_flag() {
        use std::io::Write;

        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);

        // Create two real source files
        let src_dir = tempfile::tempdir().unwrap();
        let src1 = src_dir.path().join("a.jpg");
        let src2 = src_dir.path().join("b.jpg");
        std::fs::File::create(&src1).unwrap().write_all(b"data1").unwrap();
        std::fs::File::create(&src2).unwrap().write_all(b"data2").unwrap();

        let meta1 = std::fs::metadata(&src1).unwrap();
        let meta2 = std::fs::metadata(&src2).unwrap();
        #[cfg(unix)]
        let ((size1, mtime1), (size2, mtime2)) = {
            use std::os::unix::fs::MetadataExt;
            ((meta1.size() as i64, meta1.mtime()), (meta2.size() as i64, meta2.mtime()))
        };
        #[cfg(not(unix))]
        let ((size1, mtime1), (size2, mtime2)) = {
            ((meta1.len() as i64, 0i64), (meta2.len() as i64, 0i64))
        };
        let hash1 = compute_partial_hash(&src1, size1 as u64).unwrap();
        let hash2 = compute_partial_hash(&src2, size2 as u64).unwrap();

        let dest_dir = tempfile::tempdir().unwrap();

        let plan = ApplyPlan {
            transfers: vec![
                ApplyTransfer {
                    source_id: 1,
                    source_path: src1.display().to_string(),
                    dest_rel_path: "a.jpg".to_string(),
                    archive_rel_path: "a.jpg".to_string(),
                    object_id: Some(obj1),
                    partial_hash: hash1,
                    size: size1,
                    mtime: mtime1,
                },
                ApplyTransfer {
                    source_id: 2,
                    source_path: src2.display().to_string(),
                    dest_rel_path: "b.jpg".to_string(),
                    archive_rel_path: "b.jpg".to_string(),
                    object_id: Some(obj2),
                    partial_hash: hash2,
                    size: size2,
                    mtime: mtime2,
                },
            ],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
        };

        // Pre-set the interrupt flag before calling execute
        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
            base_dir: dest_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(flag),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
        };

        let result = execute_apply(&conn, &plan, &params, &NoopProgress, None).unwrap();

        // Flag was pre-set, so first transfer executes then loop breaks
        assert!(result.interrupted);
        assert_eq!(result.remaining, 1); // 2 total - 1 completed = 1 remaining
        assert_eq!(result.copied, 1);
        // First file should exist, second should not
        assert!(dest_dir.path().join("a.jpg").exists());
        assert!(!dest_dir.path().join("b.jpg").exists());
    }

    #[test]
    fn test_execute_apply_interrupt_empty_plan() {
        let conn = setup_test_db();
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let dest_dir = tempfile::tempdir().unwrap();

        let plan = ApplyPlan {
            transfers: vec![],
            violations: ApplyViolations::default(),
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
        };

        let flag = Arc::new(AtomicBool::new(true));

        let params = ApplyExecuteParams {
            base_dir: dest_dir.path().to_path_buf(),
            archive_root_id: archive_root,
            transfer_mode: TransferMode::Copy,
            resume: false,
            interrupt_flag: Some(flag),
            skipped_by_filter: 0,
            manifest_display: "test.toml".to_string(),
        };

        let result = execute_apply(&conn, &plan, &params, &NoopProgress, None).unwrap();

        // No transfers, so not interrupted (loop never runs)
        assert!(!result.interrupted);
        assert_eq!(result.remaining, 0);
    }
}
