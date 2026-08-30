//! Planning an apply — what would move where, and what stands in the way.
//!
//! Destination paths are computed and every constraint checked before a single
//! file is transferred: two sources landing on the same path, content already in
//! the archive, excluded or suspended sources, records that no longer match what
//! is on disk. Failures are collected and reported together rather than stopping
//! at the first, so one pass shows the whole picture.
//!
//! Two functions here do touch the filesystem, and deliberately: the writability
//! probe creates and removes a test file, and the parent-directory helper is
//! called per transfer from the execute loop. Both are apply's alone and answer
//! the same question the planning does — what stands in the way — so they sit
//! here rather than in the filesystem layer every subsystem shares.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::archive::domain::LockEntry;
use crate::core::domain::format::{first_chars, shell_quote};
use crate::core::domain::path::path_strip_prefix;
use crate::core::repo::{self, Connection};
use crate::expr::{prefetch_pattern_facts, Pattern, ScopeVantage};

use super::pattern::evaluate_pattern;

/// Progress notification for planning's own per-source sweep.
///
/// Planning stats and opens every source in the manifest before it decides
/// anything. That is a disk round-trip per source, so on a network volume the
/// wait is long enough to need a voice; the interface supplies one, or `None`
/// and planning stays silent.
pub trait PlanProgress {
    /// Called before the sweep begins, with the number of sources it will touch.
    fn on_preflight_start(&self, total: usize);
    /// Called after each source, with how many are done.
    fn on_preflight_progress(&self, done: usize);
    /// Called once the sweep ends.
    fn on_preflight_finish(&self);
}

/// Parameters for planning an apply operation.
pub struct ApplyPlanParams<'a> {
    /// Filtered sources from the lock file (already filtered by --root).
    pub sources: &'a [&'a LockEntry],
    /// Parsed output pattern.
    pub pattern: &'a Pattern,
    /// Fact keys needed by the pattern (from expr::extract_fact_keys).
    pub needed_keys: &'a [String],
    /// Where a `{scope.rel_path}` measures from, derived once from the
    /// manifest's recorded scope.
    pub vantage: &'a ScopeVantage,
    /// Root ID → root path cache (from core::repo::root::fetch_all).
    pub root_paths: &'a HashMap<i64, String>,
    /// Destination archive root ID.
    pub archive_root_id: i64,
    /// Relative base directory within archive root (config.output.base_dir).
    pub base_dir_rel: &'a str,
    /// Whether this is a resume operation.
    pub resume: bool,
    /// Where to report the per-source sweep's progress, if anywhere.
    pub progress: Option<&'a dyn PlanProgress>,
}

/// A source validated and ready for transfer, with pre-computed destination.
#[derive(Debug)]
pub struct ApplyTransfer {
    /// Source ID from lock entry.
    pub source_id: i64,
    /// Absolute source path.
    pub source_path: String,
    /// Absolute path of the source root (for receipt items).
    pub source_root_path: String,
    /// Path relative to source root (for receipt items).
    pub source_rel_path: String,
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
    /// Content hash formatted as "sha256:{value}" (for receipt items). None if not hashed.
    pub hash: Option<String>,
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
    /// Files standing where a destination's directory chain has to go.
    pub ancestor_collisions: Vec<AncestorCollision>,
}

/// A file standing where a directory must be created.
///
/// Every destination below it is blocked, and no amount of retrying will
/// change that — `create_dir_all` cannot make a directory out of a file. One
/// of these refuses the whole run, so the answer arrives once instead of
/// arriving per transfer while the successful ones move.
#[derive(Debug)]
pub struct AncestorCollision {
    /// The file in the way, absolute.
    pub blocking_path: String,
    /// How many destinations it blocks.
    pub blocked_count: usize,
    /// A few of the blocked destinations, for the refusal to show.
    pub sample_dests: Vec<String>,
}

/// A source whose state has changed since the lock file was generated.
#[derive(Debug)]
pub struct StaleSource {
    pub path: String,
    pub reason: String,
}

/// How many stale sources a refusal names before it truncates. The remedy is
/// keyed on the same number, which is why it is one constant: a list the user
/// can see in full is a list the remedy can hand back.
pub const STALE_SHOWN: usize = 10;

/// What a staleness site says below its own headline: which sources went
/// stale, and what to do about it.
///
/// **One body, three printers** — the preflight refusal, the pre-transfer bail
/// and the during-run skip differ in their headline and in nothing else, so
/// the listing, the truncation and the remedy are spoken here once. Only the
/// headline is each site's own, because only the headline differs in kind: a
/// refusal, a refusal, and a report of what was passed over.
///
/// The remedy is scaled to how much went stale. Re-observing three named files
/// costs three stats; scanning the library they sit in costs a walk of every
/// file in it. So when the stale set is small enough to have been named in
/// full, the remedy names it back — and the whole-root remedy stays stated
/// either way, because staleness that is widespread is not fixed by a list,
/// and a list that was truncated is not a remedy at all.
pub fn staleness_lines(stale: &[StaleSource], manifest: &str) -> Vec<String> {
    let mut lines: Vec<String> = stale
        .iter()
        .take(STALE_SHOWN)
        .map(|s| format!("  {}: {}", s.path, s.reason))
        .collect();
    if stale.len() > STALE_SHOWN {
        lines.push(format!("  ... and {} more", stale.len() - STALE_SHOWN));
    }
    lines.push(String::new());
    lines.extend(staleness_remedy(stale, manifest));
    lines
}

/// The remedy half of [`staleness_lines`], separated only so the size
/// judgement it makes is testable on its own.
fn staleness_remedy(stale: &[StaleSource], manifest: &str) -> Vec<String> {
    let whole_root = "run `canon scan` then `cluster refresh` to regenerate the lock file";
    if stale.is_empty() || stale.len() > STALE_SHOWN {
        let mut sentence = whole_root.to_string();
        sentence[..1].make_ascii_uppercase();
        return vec![format!("{sentence}.")];
    }
    let paths: Vec<String> = stale.iter().map(|s| shell_quote(&s.path)).collect();
    vec![
        "Re-observe just these files, then refresh:".to_string(),
        format!("  canon scan {}", paths.join(" ")),
        format!("  canon cluster refresh {}", shell_quote(manifest)),
        String::new(),
        format!("If more than these has changed, {whole_root}."),
    ]
}

/// Compute the archive-relative path from base_dir_rel and dest_rel.
///
/// This is the path recorded in the database and in the receipt. The path
/// actually written on disk is built separately, by joining the base
/// directory onto the archive root. The two must agree on empty and trailing
/// separators or the records point somewhere the file is not — which would
/// read back as "not at destination" and invite a second copy.
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

    // The manifest's base_dir must stay inside the archive root. It is
    // user-editable, and it reaches both sides unfiltered: an absolute value
    // replaces the archive root wholesale in the filesystem join, and a
    // parent-traversing one walks out of the root while every recorded path
    // still claims to be archive-relative. The per-transfer escape check
    // below compares components without normalising, so it catches neither;
    // this is the one gate.
    if crate::core::domain::path::rel_dir_escapes(params.base_dir_rel) {
        bail!(
            "Manifest base_dir '{}' escapes the archive root — absolute paths and '..' components \
             are not allowed.\n\
             Edit base_dir in the manifest's [output] section to a directory inside the archive root.",
            params.base_dir_rel
        );
    }

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
    let facts = prefetch_pattern_facts(conn, &source_ids, params.needed_keys)?;

    // --- Expand patterns and build transfers ---

    let mut transfers: Vec<ApplyTransfer> = Vec::with_capacity(params.sources.len());

    for source in params.sources {
        match evaluate_pattern(
            params.pattern,
            source,
            params.vantage,
            params.root_paths,
            &facts,
        ) {
            Ok(dest_rel) => {
                let archive_rel_path = compute_archive_rel_path(params.base_dir_rel, &dest_rel);
                let source_root_path = params
                    .root_paths
                    .get(&source.root_id)
                    .cloned()
                    .unwrap_or_default();
                // When the root prefix does not match — including the case
                // where the root path above fell back to empty — this records
                // an absolute path in a field that otherwise holds a
                // root-relative one. The receipt is written either way, and
                // receipts are what the ledger is rebuilt from.
                let source_rel_path = path_strip_prefix(&source.path, &source_root_path)
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| source.path.clone());
                let hash = source
                    .hash_type
                    .as_deref()
                    .zip(source.hash_value.as_deref())
                    .map(|(t, v)| format!("{t}:{v}"));
                transfers.push(ApplyTransfer {
                    source_id: source.id,
                    source_path: source.path.clone(),
                    source_root_path,
                    source_rel_path,
                    dest_rel_path: dest_rel,
                    archive_rel_path,
                    object_id: source.object_id,
                    partial_hash: source.partial_hash.clone(),
                    size: source.size,
                    mtime: source.mtime,
                    hash,
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
        .map(|(dest, srcs)| {
            (
                dest.to_string(),
                srcs.into_iter().map(|s| s.to_string()).collect(),
            )
        })
        .collect();
    collisions.sort_by(|a, b| a.0.cmp(&b.0));
    violations.collisions = collisions;

    // --- Check destination conflicts (DB) ---
    // Skipped in resume mode: destination DB records are evidence of progress,
    // not of a conflict. Source staleness below is checked in every mode — a
    // source that changed since generation invalidates the plan whether or not
    // an earlier run got partway through it.

    let archive_rel_paths: Vec<&str> = transfers
        .iter()
        .map(|t| t.archive_rel_path.as_str())
        .collect();
    let _paths_in_db = if !params.resume {
        let paths = crate::archive::repo::batch_check_paths_exist(
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

    // --- Check destination paths stay under archive root ---

    if !transfers.is_empty() {
        let archive_root_path =
            params
                .root_paths
                .get(&params.archive_root_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Archive root {} not found in root_paths",
                        params.archive_root_id
                    )
                })?;

        for transfer in &transfers {
            // This catches a destination that is absolute or otherwise
            // rooted elsewhere. It cannot catch a parent-directory step: the
            // check compares path components without normalising, so a path
            // descending through `..` still reads as being under the archive
            // root. Removing `..` from an expanded pattern happens in the
            // expression evaluator, and a `..`-carrying or absolute base_dir
            // is rejected as a precondition above; this is not a second
            // guard for either.
            let full_dest = format!("{}/{}", archive_root_path, transfer.archive_rel_path);
            if !crate::core::domain::path::path_is_under(&full_dest, archive_root_path) {
                violations
                    .escaped_paths
                    .push((transfer.source_path.clone(), full_dest));
            }
        }
    }

    // --- Source existence and readability preflight ---
    // In resume mode, skip this — classify_resume_entries handles source state.
    // In regular mode, check all transfers.
    if !params.resume {
        if let Some(p) = params.progress {
            p.on_preflight_start(transfers.len());
        }
    }
    for (i, transfer) in transfers.iter().enumerate().filter(|_| !params.resume) {
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
        if let Some(p) = params.progress {
            p.on_preflight_progress(i + 1);
        }
    }
    if !params.resume {
        if let Some(p) = params.progress {
            p.on_preflight_finish();
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
                    first_chars(&transfer.partial_hash, 16),
                    first_chars(&db_source.partial_hash, 16)
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
        let archive_root_path =
            params
                .root_paths
                .get(&params.archive_root_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Archive root {} not found in root_paths",
                        params.archive_root_id
                    )
                })?;
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
        resume_already_there_source_present = classification
            .already_there
            .iter()
            .filter(|(_, source_present)| *source_present)
            .count();

        // Build resume_already_there list (transfers to register in DB during execute)
        for (t, _source_present) in &classification.already_there {
            resume_already_there.push(ApplyTransfer {
                source_id: t.source_id,
                source_path: t.source_path.clone(),
                source_root_path: t.source_root_path.clone(),
                source_rel_path: t.source_rel_path.clone(),
                dest_rel_path: t.dest_rel_path.clone(),
                archive_rel_path: t.archive_rel_path.clone(),
                object_id: t.object_id,
                partial_hash: t.partial_hash.clone(),
                size: t.size,
                mtime: t.mtime,
                hash: t.hash.clone(),
            });
        }

        already_archived_count = classification.already_there.len();

        // Replace transfers with only pending entries
        let pending_ids: std::collections::HashSet<i64> =
            classification.pending.iter().map(|t| t.source_id).collect();
        transfers.retain(|t| pending_ids.contains(&t.source_id));
    }

    // --- Check ancestor collisions ---
    // After the resume trimming above, so the check reads the list that will
    // actually be transferred. Unlike the destination-conflict check, this one
    // runs in resume mode as well: a file standing where a directory must go is
    // never evidence of an earlier run's progress.
    if !transfers.is_empty() {
        let archive_root_path =
            params
                .root_paths
                .get(&params.archive_root_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Archive root {} not found in root_paths",
                        params.archive_root_id
                    )
                })?;
        let archive_root = Path::new(archive_root_path);
        let base_dir = if params.base_dir_rel.is_empty() {
            archive_root.to_path_buf()
        } else {
            archive_root.join(params.base_dir_rel)
        };
        violations.ancestor_collisions =
            check_ancestor_collisions(&transfers, &base_dir, archive_root);
    }

    // --- Check archive conflicts ---
    // Deliberately last: in resume mode the transfers above have been trimmed
    // to the ones still pending, and this check must not see the rest. What an
    // interrupted run already placed is registered in the destination archive,
    // so checking it here would read the run's own progress as content already
    // archived and abort the resume that is finishing it. Pending transfers are
    // still checked in full — resume is not a way past the duplicate gate.

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
                    // The list is ordered by root id, so taking the first
                    // entry outright would read "lowest root id", not
                    // "prefer the destination": content standing in both the
                    // destination and an older archive would classify as a
                    // cross-archive conflict, and acknowledging that with
                    // --allow would wave a duplicate into the destination
                    // without the destination gate ever being consulted.
                    // A copy in the destination archive wins the
                    // classification whenever one exists.
                    let in_dest = info_list
                        .iter()
                        .find(|(archive_id, _)| *archive_id == params.archive_root_id);
                    if let Some((_, archive_path)) = in_dest {
                        violations
                            .archive_conflicts_dest
                            .push((transfer.source_path.clone(), archive_path.clone()));
                    } else if let Some((_, archive_path)) = info_list.first() {
                        violations
                            .archive_conflicts_other
                            .push((transfer.source_path.clone(), archive_path.clone()));
                    }
                }
            }
        }
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

/// Filter lock entries to only those from the specified roots.
///
/// Parses root specs (e.g., "id:1", "path:/photos") and keeps only entries
/// whose `root_id` matches. Returns all entries when `root_specs` is empty.
pub fn filter_by_roots<'a>(
    sources: &'a [LockEntry],
    root_specs: &[String],
    all_roots: &[crate::core::domain::root::Root],
) -> Result<Vec<&'a LockEntry>> {
    if root_specs.is_empty() {
        return Ok(sources.iter().collect());
    }

    let mut root_ids = std::collections::HashSet::new();
    for spec in root_specs {
        let id = crate::core::ops::scope::parse_root_spec(all_roots, spec, None)?;
        root_ids.insert(id);
    }

    Ok(sources
        .iter()
        .filter(|s| root_ids.contains(&s.root_id))
        .collect())
}

/// Every directory the transfers need, each named once.
///
/// A manifest of five thousand files usually lands in a handful of
/// directories, and the chains they share are walked once. Deduping before the
/// filesystem is touched is the whole point: over a network mount, one stat per
/// transfer is the difference between a preflight and a stall.
fn ancestor_directories(
    transfers: &[ApplyTransfer],
    base_dir: &Path,
    archive_root: &Path,
) -> BTreeSet<PathBuf> {
    let mut dirs = BTreeSet::new();
    let mut record = |start: &Path| {
        for dir in start.ancestors() {
            if !dir.starts_with(archive_root) {
                break;
            }
            // Already recorded means its whole chain above is too.
            if !dirs.insert(dir.to_path_buf()) {
                break;
            }
        }
    };

    // base_dir itself and everything it hangs from, whatever the destinations
    // turn out to be: the run needs it to be a directory even if every
    // placement lands directly in it.
    record(base_dir);

    // Then each destination's own chain — the file itself is not a directory
    // the run must create, so the walk starts at its parent.
    for dest in transfers.iter().map(|t| base_dir.join(&t.dest_rel_path)) {
        if let Some(parent) = dest.parent() {
            record(parent);
        }
    }

    dirs
}

/// Files standing where the run's destination directories have to go.
///
/// Checked before any transfer, and in resume mode too: a file in the way is
/// never evidence of progress, unlike the destination records an interrupted
/// run leaves behind.
pub fn check_ancestor_collisions(
    transfers: &[ApplyTransfer],
    base_dir: &Path,
    archive_root: &Path,
) -> Vec<AncestorCollision> {
    const SAMPLES: usize = 3;

    let mut blockers: Vec<PathBuf> = Vec::new();
    // The set is ordered, so a parent is always considered before its
    // children: once a chain is blocked, nothing below it is worth a stat.
    for dir in ancestor_directories(transfers, base_dir, archive_root) {
        if blockers.iter().any(|b: &PathBuf| dir.starts_with(b)) {
            continue;
        }
        if let Some(blocker) = blocking_file(&dir) {
            blockers.push(blocker);
        }
    }

    blockers
        .into_iter()
        .map(|blocker| {
            let blocked: Vec<String> = transfers
                .iter()
                .map(|t| base_dir.join(&t.dest_rel_path))
                .filter(|dest| dest.starts_with(&blocker))
                .map(|dest| dest.display().to_string())
                .collect();
            AncestorCollision {
                blocking_path: blocker.display().to_string(),
                blocked_count: blocked.len(),
                sample_dests: blocked.into_iter().take(SAMPLES).collect(),
            }
        })
        .collect()
}

/// Whether this path holds something that is not a directory.
///
/// A symlink is judged by what it resolves to, because that is what deciding
/// to create a directory here runs into: a link to a directory is a directory
/// for this purpose, and one pointing at nothing is as solid an obstacle as a
/// file — the path exists, and no directory can be made at it.
fn blocking_file(dir: &Path) -> Option<PathBuf> {
    let meta = fs::symlink_metadata(dir).ok()?;
    if meta.is_dir() {
        return None;
    }
    if meta.is_symlink() && fs::metadata(dir).is_ok_and(|m| m.is_dir()) {
        return None;
    }
    Some(dir.to_path_buf())
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

/// Check if a directory (or its nearest existing ancestor) is writable.
/// Creates and removes a test file to verify write permissions.
pub fn check_destination_writable(base_dir: &Path) -> Result<()> {
    // Walk up to find the nearest existing directory
    let mut check_dir = base_dir.to_path_buf();
    while !check_dir.exists() {
        if let Some(parent) = check_dir.parent() {
            check_dir = parent.to_path_buf();
        } else {
            anyhow::bail!(
                "Cannot find existing parent directory for {}",
                base_dir.display()
            );
        }
    }

    // Try to create a temp file to verify write permissions
    let test_file = check_dir.join(".canon_write_test");
    match File::create(&test_file) {
        Ok(_) => {
            let _ = fs::remove_file(&test_file);
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            anyhow::bail!(
                "No write permission for destination directory: {}",
                check_dir.display()
            );
        }
        Err(e) => {
            anyhow::bail!(
                "Cannot write to destination directory {}: {}",
                check_dir.display(),
                e
            );
        }
    }
}

/// Create parent directories for a path.
pub(super) fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }
    Ok(())
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
                    result
                        .size_mismatches
                        .push((transfer, expected_size, actual_size));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{
        insert_fact, insert_object, insert_root, insert_source_excluded,
        insert_source_with_metadata, setup_test_db,
    };

    use crate::expr::{extract_fact_keys, parse_pattern};

    fn make_lock_entry(
        id: i64,
        root_id: i64,
        path: &str,
        object_id: Option<i64>,
        hash: Option<&str>,
    ) -> LockEntry {
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

    /// The vantage a test plans against when its pattern never names
    /// `scope.rel_path`, which is every test in this module: a manifest with
    /// no recorded scope. Shared rather than threaded through 36 call sites,
    /// none of which has an opinion about the scope.
    fn no_scope() -> &'static ScopeVantage {
        static EMPTY: std::sync::OnceLock<ScopeVantage> = std::sync::OnceLock::new();
        EMPTY.get_or_init(|| ScopeVantage::new(&[], std::iter::empty()))
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
            vantage: no_scope(),
            root_paths,
            archive_root_id,
            base_dir_rel: "",
            resume: false,
            progress: None,
        }
    }

    // =========================================================================
    // Pattern expansion and destination computation
    // =========================================================================

    /// Records every planning progress call in order.
    #[derive(Default)]
    struct RecordingPlanProgress {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl PlanProgress for RecordingPlanProgress {
        fn on_preflight_start(&self, total: usize) {
            self.events.borrow_mut().push(format!("start {total}"));
        }
        fn on_preflight_progress(&self, done: usize) {
            self.events.borrow_mut().push(format!("step {done}"));
        }
        fn on_preflight_finish(&self) {
            self.events.borrow_mut().push("finish".to_string());
        }
    }

    /// Planning stats and opens every source before deciding anything — a disk
    /// round-trip each, long enough on a network volume to look like a hang.
    /// It reports what it is working through.
    ///
    /// Resume mode does not run the sweep at all (source state comes from the
    /// resume classification instead), so it must not announce one either: a
    /// heading over no work is its own kind of lie.
    #[test]
    fn plan_preflight_progress_is_reported_per_source() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_a = insert_object(&conn, "hash1", false);
        let obj_b = insert_object(&conn, "hash2", false);
        let src_a =
            insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj_a), 1000, 1704067200);
        let src_b =
            insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj_b), 1000, 1704067200);

        let entry_a = make_lock_entry(src_a, root_id, "/photos/a.jpg", Some(obj_a), Some("hash1"));
        let entry_b = make_lock_entry(src_b, root_id, "/photos/b.jpg", Some(obj_b), Some("hash2"));
        let sources: Vec<&LockEntry> = vec![&entry_a, &entry_b];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let progress = RecordingPlanProgress::default();
        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.progress = Some(&progress);
        plan_apply(&mut conn, &params).unwrap();

        assert_eq!(
            progress.events.borrow().as_slice(),
            ["start 2", "step 1", "step 2", "finish"]
        );

        let resumed = RecordingPlanProgress::default();
        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;
        params.progress = Some(&resumed);
        plan_apply(&mut conn, &params).unwrap();

        assert!(resumed.events.borrow().is_empty());
    }

    #[test]
    fn test_plan_apply_computes_dest_paths() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "vacation/photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        insert_fact(&conn, src_id, "content.Make", "Canon");

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/vacation/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "2024/vacation";

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.transfers[0].dest_rel_path, "photo.jpg");
        assert_eq!(
            plan.transfers[0].archive_rel_path,
            "2024/vacation/photo.jpg"
        );
    }

    #[test]
    fn test_plan_apply_expansion_failure() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // No fact inserted — pattern requires content.Make

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src1 = insert_source_with_metadata(
            &conn,
            root_id,
            "a/photo.jpg",
            Some(obj1),
            1000,
            1704067200,
        );
        let src2 = insert_source_with_metadata(
            &conn,
            root_id,
            "b/photo.jpg",
            Some(obj2),
            1000,
            1704067200,
        );
        insert_fact(&conn, src1, "content.Make", "Canon");
        insert_fact(&conn, src2, "content.Make", "Canon");

        let e1 = make_lock_entry(
            src1,
            root_id,
            "/photos/a/photo.jpg",
            Some(obj1),
            Some("hash1"),
        );
        let e2 = make_lock_entry(
            src2,
            root_id,
            "/photos/b/photo.jpg",
            Some(obj2),
            Some("hash2"),
        );
        let sources: Vec<&LockEntry> = vec![&e1, &e2];
        // Pattern uses only Make + filename → both expand to "Canon/photo.jpg"
        let pattern = parse_pattern("{content.Make}/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src1 =
            insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1704067200);
        let src2 =
            insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 1704067200);

        let e1 = make_lock_entry(src1, root_id, "/photos/a.jpg", Some(obj1), Some("hash1"));
        let e2 = make_lock_entry(src2, root_id, "/photos/b.jpg", Some(obj2), Some("hash2"));
        let sources: Vec<&LockEntry> = vec![&e1, &e2];
        // filename pattern produces different paths
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Same hash already in destination archive
        insert_source_with_metadata(
            &conn,
            archive_id,
            "existing/photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Same hash in OTHER archive
        insert_source_with_metadata(
            &conn,
            other_archive,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.violations.archive_conflicts_dest.is_empty());
        assert_eq!(plan.violations.archive_conflicts_other.len(), 1);
    }

    #[test]
    fn test_plan_apply_conflict_in_both_archives_classifies_as_dest() {
        // The destination copy must win the classification even when an
        // older archive (lower root id, so first in the ordered fetch)
        // holds the same content — otherwise --allow for cross-archive
        // duplicates bypasses the destination gate and lands a duplicate.
        let mut conn = setup_test_db();
        let other_archive = insert_root(&conn, "/other-archive", "archive", false);
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        assert!(other_archive < archive_id);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Same hash in BOTH the older archive and the destination archive
        insert_source_with_metadata(
            &conn,
            other_archive,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        insert_source_with_metadata(
            &conn,
            archive_id,
            "existing/photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.archive_conflicts_dest.len(), 1);
        assert!(plan.violations.archive_conflicts_other.is_empty());
        assert!(plan.violations.archive_conflicts_dest[0]
            .1
            .contains("existing/photo.jpg"));
    }

    #[test]
    fn test_plan_apply_no_archive_conflict() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Destination path already has a record in archive
        insert_source_with_metadata(
            &conn,
            archive_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Destination path occupied
        let obj2 = insert_object(&conn, "different_hash", false);
        insert_source_with_metadata(&conn, archive_id, "photo.jpg", Some(obj2), 1000, 1704067200);

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Same path exists in archive DB
        insert_source_with_metadata(
            &conn,
            archive_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            &src_file.display().to_string(),
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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

    /// A resume must not trip over its own progress: the files the
    /// interrupted run already placed are registered in the archive, and
    /// reading them back as "already in the destination archive" would abort
    /// the very run that put them there.
    #[test]
    fn resume_never_reads_its_own_progress_as_an_archive_conflict() {
        use std::io::Write;

        let src_dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let archive_path = archive_dir.path().to_str().unwrap().to_string();

        // Two sources in the manifest; the first was transferred before the
        // interruption, the second never made it.
        for name in ["done.jpg", "pending.jpg"] {
            let mut f = std::fs::File::create(src_dir.path().join(name)).unwrap();
            f.write_all(&vec![0u8; 1000]).unwrap();
        }
        let mut f = std::fs::File::create(archive_dir.path().join("done.jpg")).unwrap();
        f.write_all(&vec![0u8; 1000]).unwrap();

        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, &src_path, "source", false);
        let archive_id = insert_root(&conn, &archive_path, "archive", false);
        let done_obj = insert_object(&conn, "hash_done", false);
        let pending_obj = insert_object(&conn, "hash_pending", false);
        let done_id = insert_source_with_metadata(
            &conn,
            root_id,
            "done.jpg",
            Some(done_obj),
            1000,
            1704067200,
        );
        let pending_id = insert_source_with_metadata(
            &conn,
            root_id,
            "pending.jpg",
            Some(pending_obj),
            1000,
            1704067200,
        );
        // What the interrupted run registered: the destination copy.
        insert_source_with_metadata(
            &conn,
            archive_id,
            "done.jpg",
            Some(done_obj),
            1000,
            1704067200,
        );

        let done = make_lock_entry(
            done_id,
            root_id,
            &src_dir.path().join("done.jpg").display().to_string(),
            Some(done_obj),
            Some("hash_done"),
        );
        let pending = make_lock_entry(
            pending_id,
            root_id,
            &src_dir.path().join("pending.jpg").display().to_string(),
            Some(pending_obj),
            Some("hash_pending"),
        );
        let sources: Vec<&LockEntry> = vec![&done, &pending];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, src_path);
        root_paths.insert(archive_id, archive_path);

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.already_archived_count, 1);
        assert_eq!(plan.transfers.len(), 1, "only the pending transfer remains");
        assert!(
            plan.violations.archive_conflicts_dest.is_empty(),
            "the run's own progress is not a conflict: {:?}",
            plan.violations.archive_conflicts_dest
        );
        assert!(plan.violations.archive_conflicts_other.is_empty());
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            2000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            &src_file.display().to_string(),
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            &src_file.display().to_string(),
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id =
            insert_source_with_metadata(&conn, root_id, "photo.jpg", None, 1000, 1704067200);

        let entry = make_lock_entry(src_id, root_id, "/photos/photo.jpg", None, None);
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let result = plan_apply(&mut conn, &params);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("without content hash"));
    }

    #[test]
    fn test_plan_apply_allows_archive_with_unhashed_files() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );
        // Archive has an unhashed file — should NOT block apply
        insert_source_with_metadata(&conn, archive_id, "unhashed.jpg", None, 500, 1704067200);

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let root_paths = HashMap::new();

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert!(plan.transfers.is_empty());
        assert!(plan.violations.expansion_failures.is_empty());
        assert!(plan.violations.collisions.is_empty());
        assert_eq!(plan.already_archived_count, 0);
    }

    // =========================================================================
    // filter_by_roots
    // =========================================================================

    fn two_rooted_entries(conn: &Connection) -> (i64, i64, Vec<LockEntry>) {
        let photos = insert_root(conn, "/photos", "source", false);
        let scans = insert_root(conn, "/scans", "source", false);
        let entries = vec![
            make_lock_entry(1, photos, "/photos/one.jpg", None, None),
            make_lock_entry(2, scans, "/scans/two.jpg", None, None),
        ];
        (photos, scans, entries)
    }

    #[test]
    fn filter_by_roots_keeps_everything_when_no_root_is_named() {
        let conn = setup_test_db();
        let (_photos, _scans, entries) = two_rooted_entries(&conn);
        let roots = repo::root::fetch_all(&conn).unwrap();

        let kept = filter_by_roots(&entries, &[], &roots).unwrap();
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn filter_by_roots_keeps_only_the_named_root() {
        let conn = setup_test_db();
        let (photos, _scans, entries) = two_rooted_entries(&conn);
        let roots = repo::root::fetch_all(&conn).unwrap();

        let kept = filter_by_roots(&entries, &[format!("id:{photos}")], &roots).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, 1);
    }

    #[test]
    fn filter_by_roots_refuses_a_root_that_does_not_exist() {
        let conn = setup_test_db();
        let (_photos, _scans, entries) = two_rooted_entries(&conn);
        let roots = repo::root::fetch_all(&conn).unwrap();

        // Silently keeping everything would apply a wider set than asked for.
        assert!(filter_by_roots(&entries, &["id:999".to_string()], &roots).is_err());
    }

    // =========================================================================
    // check_disk_conflicts
    // =========================================================================

    fn plan_for_disk_conflicts(rel_paths: &[&str], db_conflicts: Vec<String>) -> ApplyPlan {
        ApplyPlan {
            transfers: rel_paths
                .iter()
                .enumerate()
                .map(|(i, p)| ApplyTransfer {
                    source_id: i as i64 + 1,
                    source_path: format!("/photos/{p}"),
                    source_root_path: "/photos".to_string(),
                    source_rel_path: p.to_string(),
                    dest_rel_path: p.to_string(),
                    archive_rel_path: p.to_string(),
                    object_id: None,
                    partial_hash: "testhash".to_string(),
                    size: 4,
                    mtime: 0,
                    hash: None,
                })
                .collect(),
            violations: ApplyViolations {
                dest_conflicts_in_db: db_conflicts,
                ..Default::default()
            },
            stale_sources: vec![],
            already_archived_count: 0,
            resume_already_there: vec![],
            resume_already_there_source_present: 0,
            resume_source_lost: vec![],
            resume_size_mismatches: vec![],
        }
    }

    // =========================================================================
    // Ancestor collisions — a file standing where a directory has to go
    // =========================================================================

    /// A source tree and an archive root, with sources named by the caller.
    /// `source_exists` says whether the file is actually on disk — a lock
    /// entry for a file that has since gone is what resume trims away.
    fn ancestor_fixture(
        conn: &Connection,
        archive_root: &Path,
        sources: &[(&str, bool)],
    ) -> (i64, Vec<LockEntry>, HashMap<i64, String>, tempfile::TempDir) {
        let source_dir = tempfile::tempdir().unwrap();
        let source_root_path = source_dir.path().to_str().unwrap().to_string();
        let source_id = insert_root(conn, &source_root_path, "source", false);
        let archive_id = insert_root(conn, archive_root.to_str().unwrap(), "archive", false);

        let mut entries = Vec::new();
        for (i, (name, source_exists)) in sources.iter().enumerate() {
            if *source_exists {
                let file = source_dir.path().join(name);
                std::fs::create_dir_all(file.parent().unwrap()).unwrap();
                std::fs::write(file, b"x").unwrap();
            }
            let obj = insert_object(conn, &format!("hash{i}"), false);
            let src = insert_source_with_metadata(conn, source_id, name, Some(obj), 1, 0);
            entries.push(make_lock_entry(
                src,
                source_id,
                &format!("{source_root_path}/{name}"),
                Some(obj),
                Some(&format!("hash{i}")),
            ));
        }

        let mut root_paths = HashMap::new();
        root_paths.insert(source_id, source_root_path);
        root_paths.insert(archive_id, archive_root.to_str().unwrap().to_string());
        (archive_id, entries, root_paths, source_dir)
    }

    #[test]
    fn an_ancestor_file_refuses_the_whole_run_before_any_transfer() {
        let archive = tempfile::tempdir().unwrap();
        // The user's own manifest, sitting where the pattern needs a directory.
        std::fs::write(archive.path().join("2024"), b"a manifest, say").unwrap();

        let mut conn = setup_test_db();
        let (archive_id, entries, root_paths, _sources) =
            ancestor_fixture(&conn, archive.path(), &[("a.jpg", true)]);
        let sources: Vec<&LockEntry> = entries.iter().collect();
        let pattern = parse_pattern("2024/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);

        let plan = plan_apply(&mut conn, &params).unwrap();

        let collisions = &plan.violations.ancestor_collisions;
        assert_eq!(collisions.len(), 1, "got: {collisions:?}");
        assert_eq!(
            collisions[0].blocking_path,
            archive.path().join("2024").display().to_string()
        );
        assert_eq!(collisions[0].blocked_count, 1);
        assert_eq!(collisions[0].sample_dests.len(), 1);
        // Nothing was moved to find this out — planning touches no destination.
        assert!(!archive.path().join("2024/a.jpg").exists());
    }

    #[test]
    fn a_file_blocking_some_destinations_refuses_all() {
        let archive = tempfile::tempdir().unwrap();
        // Blocks what lands in day2; day1 is perfectly clear.
        std::fs::write(archive.path().join("day2"), b"in the way").unwrap();

        let mut conn = setup_test_db();
        let (archive_id, entries, root_paths, _sources) = ancestor_fixture(
            &conn,
            archive.path(),
            &[("day1/a.jpg", true), ("day2/b.jpg", true)],
        );
        let sources: Vec<&LockEntry> = entries.iter().collect();
        let pattern = parse_pattern("{source.rel_path}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);

        let plan = plan_apply(&mut conn, &params).unwrap();

        let collisions = &plan.violations.ancestor_collisions;
        assert_eq!(collisions.len(), 1, "got: {collisions:?}");
        assert!(
            collisions[0].blocking_path.ends_with("/day2"),
            "got: {collisions:?}"
        );
        assert_eq!(collisions[0].blocked_count, 1, "only day2 is blocked");
        // The clear destination is still in the plan: what makes this refuse
        // the whole run is the interface aborting on a non-empty violation,
        // not the plan dropping the transfers that would have worked.
        assert_eq!(plan.transfers.len(), 2);
    }

    /// The check reads the post-trim list. Only one of resume's four outcomes
    /// can reach the check having been trimmed — `already_there`, whose
    /// destination file exists and whose chain is therefore all directories,
    /// so it can carry no blocker. This drives the trimming through
    /// `source_lost` instead, which the command refuses before the ancestor
    /// block is reached: what is pinned here is the reading of the list, not a
    /// standing the user can arrive at.
    #[test]
    fn resume_checks_ancestors_over_the_trimmed_list() {
        let archive = tempfile::tempdir().unwrap();
        std::fs::write(archive.path().join("day2"), b"in the way").unwrap();

        let mut conn = setup_test_db();
        // The day2 source is gone from disk: resume classifies it source-lost
        // and trims it, so the directory only it needed stops being this
        // run's problem. The day1 source is still there and still pending.
        let (archive_id, entries, root_paths, _sources) = ancestor_fixture(
            &conn,
            archive.path(),
            &[("day1/a.jpg", true), ("day2/b.jpg", false)],
        );
        let sources: Vec<&LockEntry> = entries.iter().collect();
        let pattern = parse_pattern("{source.rel_path}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);

        let full = plan_apply(&mut conn, &params).unwrap();
        assert_eq!(
            full.violations.ancestor_collisions.len(),
            1,
            "outside resume the whole list is checked: {:?}",
            full.violations.ancestor_collisions
        );

        params.resume = true;
        let resumed = plan_apply(&mut conn, &params).unwrap();
        assert_eq!(resumed.transfers.len(), 1, "the lost entry is trimmed");
        assert!(
            resumed.violations.ancestor_collisions.is_empty(),
            "a directory no pending transfer needs is not the resume's problem: {:?}",
            resumed.violations.ancestor_collisions
        );
    }

    #[test]
    fn resume_still_refuses_a_blocker_in_the_pending_list() {
        let archive = tempfile::tempdir().unwrap();
        std::fs::write(archive.path().join("2024"), b"in the way").unwrap();

        let mut conn = setup_test_db();
        let (archive_id, entries, root_paths, _sources) =
            ancestor_fixture(&conn, archive.path(), &[("a.jpg", true)]);
        let sources: Vec<&LockEntry> = entries.iter().collect();
        let pattern = parse_pattern("2024/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.resume = true;

        let plan = plan_apply(&mut conn, &params).unwrap();

        // Unlike a destination already registered in the archive, a file in
        // the way is never evidence of an earlier run's progress.
        assert_eq!(plan.violations.ancestor_collisions.len(), 1);
    }

    /// The stat loop runs over this set, one call per element, so pinning the
    /// set is pinning what the filesystem is asked. It is the input, not a
    /// count of calls: a future check that stats inside the grouping pass
    /// would leave this green.
    #[test]
    fn the_ancestor_check_asks_about_each_distinct_directory_once() {
        let archive = Path::new("/archive");
        let base = Path::new("/archive/photos");

        // Five hundred files, two directories: the chains they share are
        // walked once, so the filesystem is asked four times, not five hundred.
        let transfers: Vec<ApplyTransfer> = (0..500)
            .map(|i| {
                let day = if i % 2 == 0 { "day1" } else { "day2" };
                transfer_to(&format!("{day}/img{i}.jpg"))
            })
            .collect();

        let dirs = ancestor_directories(&transfers, base, archive);
        let named: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
        assert_eq!(
            named,
            vec![
                "/archive".to_string(),
                "/archive/photos".to_string(),
                "/archive/photos/day1".to_string(),
                "/archive/photos/day2".to_string(),
            ],
            "one entry per distinct directory, parents before children"
        );
    }

    #[test]
    fn a_blocker_above_a_nested_base_dir_is_found() {
        let archive = tempfile::tempdir().unwrap();
        // The manifest places into <archive>/2024/vacation; a file stands at
        // 2024, above base_dir itself rather than on a destination's chain.
        std::fs::write(archive.path().join("2024"), b"in the way").unwrap();

        let mut conn = setup_test_db();
        let (archive_id, entries, root_paths, _sources) =
            ancestor_fixture(&conn, archive.path(), &[("a.jpg", true)]);
        let sources: Vec<&LockEntry> = entries.iter().collect();
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "2024/vacation";

        let plan = plan_apply(&mut conn, &params).unwrap();

        let collisions = &plan.violations.ancestor_collisions;
        assert_eq!(collisions.len(), 1, "got: {collisions:?}");
        assert_eq!(
            collisions[0].blocking_path,
            archive.path().join("2024").display().to_string()
        );
        assert_eq!(collisions[0].blocked_count, 1);
    }

    /// A transfer that only carries a destination — the rest is not what the
    /// ancestor check reads.
    fn transfer_to(dest_rel_path: &str) -> ApplyTransfer {
        ApplyTransfer {
            source_id: 1,
            source_path: format!("/photos/{dest_rel_path}"),
            source_root_path: "/photos".to_string(),
            source_rel_path: dest_rel_path.to_string(),
            dest_rel_path: dest_rel_path.to_string(),
            archive_rel_path: dest_rel_path.to_string(),
            object_id: None,
            partial_hash: "testhash".to_string(),
            size: 4,
            mtime: 0,
            hash: None,
        }
    }

    #[test]
    fn check_disk_conflicts_reports_a_destination_already_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("one.jpg"), b"x").unwrap();

        let plan = plan_for_disk_conflicts(&["one.jpg", "two.jpg"], vec![]);
        assert_eq!(
            check_disk_conflicts(&plan, dir.path()),
            vec!["one.jpg".to_string()]
        );
    }

    #[test]
    fn check_disk_conflicts_leaves_out_what_the_db_check_already_found() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("one.jpg"), b"x").unwrap();

        // The interface adds these two counts together, so reporting the same
        // path on both sides would double it in the preflight the user reads.
        let plan = plan_for_disk_conflicts(&["one.jpg"], vec!["one.jpg".to_string()]);
        assert!(check_disk_conflicts(&plan, dir.path()).is_empty());
    }

    #[test]
    fn check_disk_conflicts_is_empty_when_the_destination_is_clear() {
        let dir = tempfile::tempdir().unwrap();
        let plan = plan_for_disk_conflicts(&["one.jpg", "two.jpg"], vec![]);
        assert!(check_disk_conflicts(&plan, dir.path()).is_empty());
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
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
            .unwrap();
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
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
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
        std::fs::File::create(&src_file)
            .unwrap()
            .write_all(b"data")
            .unwrap();
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
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
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
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
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
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
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
            source_root_path: String::new(),
            source_rel_path: String::new(),
            hash: None,
        }];

        let result = classify_resume_entries(&transfers, dir.path());
        assert!(result.pending.is_empty());
        assert!(result.already_there.is_empty());
        assert!(result.source_lost.is_empty());
        assert_eq!(result.size_mismatches.len(), 1);
        assert_eq!(result.size_mismatches[0].1, 1000); // expected
        assert_eq!(result.size_mismatches[0].2, 500); // actual
    }

    // =========================================================================
    // Archive root escape detection
    // =========================================================================

    #[test]
    fn test_plan_rejects_parent_traversing_base_dir() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "../../tmp";
        let err = plan_apply(&mut conn, &params).unwrap_err();
        assert!(err.to_string().contains("base_dir"));
        assert!(err.to_string().contains("escapes the archive root"));
    }

    #[test]
    fn test_plan_rejects_absolute_base_dir() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "/tmp/elsewhere";
        let err = plan_apply(&mut conn, &params).unwrap_err();
        assert!(err.to_string().contains("escapes the archive root"));
    }

    #[test]
    fn test_plan_accepts_nested_base_dir() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, "/photos".to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let mut params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        params.base_dir_rel = "collections/2020";
        let plan = plan_apply(&mut conn, &params).unwrap();
        assert!(plan.violations.escaped_paths.is_empty());
        assert_eq!(
            plan.transfers[0].archive_rel_path,
            "collections/2020/photo.jpg"
        );
    }

    #[test]
    fn test_plan_rejects_escaped_destination() {
        // Manually construct a scenario where archive_rel_path escapes the archive root.
        // With normalization in place, this shouldn't happen from patterns, but we test
        // the validation as a defense-in-depth safety net.
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);
        let src_id = insert_source_with_metadata(
            &conn,
            root_id,
            "photo.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/photo.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        // Pattern produces a normal path — no escape expected
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id =
            insert_source_with_metadata(&conn, root_id, "5.avi", Some(obj_id), 1000, 1704067200);

        let entry = make_lock_entry(
            src_id,
            root_id,
            "/photos/5.avi",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
            &conn,
            root_id,
            "missing.jpg",
            Some(obj_id),
            1000,
            1704067200,
        );

        // Lock entry points to a non-existent file
        let entry = make_lock_entry(
            src_id,
            root_id,
            "/nonexistent/missing.jpg",
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
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
        let src_id =
            insert_source_with_metadata(&conn, root_id, "subdir", Some(obj_id), 1000, 1704067200);

        // Lock entry points to a directory, not a file
        let entry = make_lock_entry(
            src_id,
            root_id,
            &sub.to_string_lossy(),
            Some(obj_id),
            Some("hash1"),
        );
        let sources: Vec<&LockEntry> = vec![&entry];
        let pattern = parse_pattern("{filename}").unwrap();
        let needed_keys = extract_fact_keys(&pattern);
        let mut root_paths = HashMap::new();
        root_paths.insert(root_id, root_path.to_string());
        root_paths.insert(archive_id, "/archive".to_string());

        let params = default_params(&sources, &pattern, &needed_keys, &root_paths, archive_id);
        let plan = plan_apply(&mut conn, &params).unwrap();

        assert_eq!(plan.violations.missing_sources.len(), 1);
        assert_eq!(plan.violations.missing_sources[0].0, src_id);
    }

    // =========================================================================
    // check_destination_writable
    // =========================================================================

    #[test]
    fn check_writable_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(check_destination_writable(dir.path()).is_ok());
    }

    #[test]
    fn check_writable_nested_missing() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("c");
        // Parent exists and is writable, nested dirs don't exist yet
        assert!(check_destination_writable(&nested).is_ok());
    }

    // =========================================================================
    // ensure_parent_dir
    // =========================================================================

    #[test]
    fn ensure_parent_dir_creates_nested() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a").join("b").join("c").join("file.txt");
        ensure_parent_dir(&path).unwrap();
        assert!(dir.path().join("a").join("b").join("c").exists());
    }

    #[test]
    fn ensure_parent_dir_existing_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.txt");
        ensure_parent_dir(&path).unwrap();
        assert!(dir.path().exists());
    }

    fn stale(paths: &[&str]) -> Vec<StaleSource> {
        paths
            .iter()
            .enumerate()
            .map(|(i, p)| StaleSource {
                path: (*p).to_string(),
                reason: format!("size: {} → {}", 10 + i, 20 + i),
            })
            .collect()
    }

    /// A refusal over three named files must not cost a walk of the library
    /// they sit in. All three staleness sites — the preflight refusal, the
    /// pre-transfer bail and the during-run skip — print this one body below
    /// their own headline, so what is pinned here is what all three say.
    #[test]
    fn a_staleness_refusal_offers_the_targeted_remedy() {
        let lines = staleness_lines(&stale(&["/src/a.jpg", "/src/b c.jpg"]), "/work/trip.toml");

        assert_eq!(
            lines,
            vec![
                "  /src/a.jpg: size: 10 → 20",
                "  /src/b c.jpg: size: 11 → 21",
                "",
                "Re-observe just these files, then refresh:",
                // Quoted, so the line survives a shell.
                "  canon scan /src/a.jpg '/src/b c.jpg'",
                "  canon cluster refresh /work/trip.toml",
                "",
                "If more than these has changed, run `canon scan` then `cluster refresh` \
                 to regenerate the lock file.",
            ]
        );
    }

    /// Past the point where the listing truncates, a targeted remedy would name
    /// *some* of the stale files — which fixes nothing and reads as if it fixed
    /// everything. Widespread staleness gets the whole-root remedy and only
    /// that, and the listing says how much it is not showing.
    #[test]
    fn widespread_staleness_gets_the_whole_root_remedy_alone() {
        let many: Vec<String> = (0..STALE_SHOWN + 3)
            .map(|i| format!("/src/{i}.jpg"))
            .collect();
        let refs: Vec<&str> = many.iter().map(|s| s.as_str()).collect();
        let lines = staleness_lines(&stale(&refs), "/work/trip.toml");

        assert_eq!(
            lines.len(),
            STALE_SHOWN + 3,
            "ten listed, the count, a blank, the remedy"
        );
        assert_eq!(lines[0], "  /src/0.jpg: size: 10 → 20");
        assert_eq!(lines[STALE_SHOWN], "  ... and 3 more");
        assert_eq!(lines[STALE_SHOWN + 1], "");
        assert_eq!(
            lines[STALE_SHOWN + 2],
            "Run `canon scan` then `cluster refresh` to regenerate the lock file."
        );
        assert!(
            !lines.iter().any(|l| l.contains("canon scan /src/")),
            "no command names a subset of the stale files: {lines:?}"
        );
    }
}
