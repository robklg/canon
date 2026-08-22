//! The walk pipeline: observe→reconcile→persist.
//!
//! The interface creates the directory walker and passes entries here.
//! This module processes each entry through the pipeline, detects missing
//! sources, and returns typed results. A `ScanProgress` trait provides
//! per-file observability without writing to stderr.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rusqlite::{Transaction, TransactionBehavior};

use crate::core::domain::source::Source;
use crate::core::ops::fs::compute_partial_hash;
use crate::core::repo::{self, Connection};
use crate::scan::domain::{
    find_missing, reconcile_at_path, reconcile_pathless, resolve_moves, DeferredMove,
    FileObservation, MoveCandidate, MoveResolution, OldPathCheck, PathlessOutcome, Reconciliation,
};
use crate::scan::repo as scan_repo;

use super::types::{
    DeletionReceiptItem, FileAction, FileToHash, HashNeed, MarkMissingPathResult, ScanOptions,
    ScanProgress, ScanRootResult, ScanStats, SourceOutcome,
};

/// Scan a root directory, processing each entry through the
/// observe→reconcile→persist pipeline.
///
/// The interface creates the directory walker and passes entries here.
/// This function:
/// 1. Fetches expected source IDs (for missing detection)
/// 2. Processes each entry via process_file()
/// 3. Detects missing sources via scan::domain::find_missing()
/// 4. Marks missing/disconnected via mark_missing_sources()
///
/// Returns accumulated stats, files needing hashing, and warnings.
#[allow(clippy::too_many_arguments)]
pub fn scan_root(
    conn: &Connection,
    root_id: i64,
    root_path: &str,
    scan_prefix: Option<&str>,
    entries: impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>>,
    options: &ScanOptions,
    progress: &dyn ScanProgress,
    now: i64,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<ScanRootResult> {
    let root_path = Path::new(root_path);
    let mut stats = ScanStats::default();
    let mut seen_source_ids: HashSet<i64> = HashSet::new();
    let mut files_to_hash: Vec<FileToHash> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    // Track outcomes for sources (for mount protection)
    let mut outcomes: Vec<(i64, SourceOutcome)> = Vec::new();
    let mut handled_ids: HashSet<i64> = HashSet::new();

    // Batch buffer for unchanged file updates (source_id, device, inode)
    let mut unchanged_batch: Vec<(i64, i64, i64)> = Vec::new();

    // Observations held back for end-of-walk move resolution, with the rows
    // they corroborated against. The rows are kept as walked so resolution can
    // compare them against a fresh read and notice anything that moved since.
    let mut deferred: Vec<DeferredMove> = Vec::new();
    let mut donors: HashMap<i64, Source> = HashMap::new();
    // Roots whose own paths could not be checked, and how many nominations each
    // cost. Ordered so the warnings read the same on every run.
    let mut unverified_by_root: BTreeMap<String, u64> = BTreeMap::new();

    // Snapshot the walk path's device ID before the walk starts.
    // If the mount disappears mid-scan (NAS disconnect, volume ejected),
    // the OS silently tears down the mount — files just vanish rather than
    // returning errors. We detect this by comparing device IDs before and
    // after the walk; a mismatch means the mount changed and missing
    // detection would be unreliable.
    let walk_root = match scan_prefix {
        Some(prefix) => root_path.join(prefix),
        None => root_path.to_path_buf(),
    };
    let pre_walk_device = get_dir_device(&walk_root);

    // Fetch expected source IDs at start (for missing detection via pure function)
    let expected_ids: HashSet<i64> =
        scan_repo::source::fetch_source_ids_for_root(conn, root_id, scan_prefix)?
            .into_iter()
            .collect();

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                // Counted, not just warned: an unreadable entry means part of
                // the tree went unseen, which gates missing detection below —
                // unseen must never read as deleted.
                stats.walk_errors += 1;
                progress.on_walk_error(&e.to_string());
                continue;
            }
        };

        // Handle empty directories — may contain sources on disconnected mounts
        if entry.file_type().is_dir() {
            if is_empty_dir(entry.path()) {
                if let Some(current_dev) = get_dir_device(entry.path()) {
                    let rel = entry
                        .path()
                        .strip_prefix(root_path)
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    let (dir_outcomes, dir_warnings) =
                        classify_sources_in_empty_dir(conn, root_id, &rel, current_dev)?;
                    warnings.extend(dir_warnings);
                    for (id, outcome) in dir_outcomes {
                        handled_ids.insert(id);
                        outcomes.push((id, outcome));
                    }
                }
            }
            continue;
        }

        if !entry.file_type().is_file() {
            continue;
        }

        let full_path = entry.path();
        let rel_path = full_path
            .strip_prefix(root_path)
            .context("Failed to strip root prefix")?;

        let rel_path_str = rel_path.to_str().context("Path is not valid UTF-8")?;

        let metadata = match fs::metadata(full_path) {
            Ok(m) => m,
            Err(e) => {
                progress.on_process_error(
                    &full_path.display().to_string(),
                    &format!("Failed to stat: {e}"),
                );
                continue;
            }
        };

        let device = metadata.dev() as i64;
        let inode = metadata.ino() as i64;
        let size = metadata.size() as i64;
        let mtime = metadata.mtime();

        stats.scanned += 1;

        // Reconcile: read DB state, determine outcome, compute partial hash
        let reconciled = match reconcile_file(
            conn,
            root_id,
            rel_path_str,
            full_path,
            device,
            inode,
            size,
            mtime,
        ) {
            Ok(r) => r,
            Err(e) => {
                progress.on_process_error(&full_path.display().to_string(), &e.to_string());
                stats.skipped += 1;
                continue;
            }
        };

        let ReconcileResult {
            observation,
            outcome,
            source_at_path,
            companions,
            unverified,
            unverified_roots,
        } = reconciled;

        // Persist: unchanged files are batched, others get individual transactions
        let (action, source_id, old_object_id) = match outcome {
            ReconcileOutcome::Deferred(corroborated) => {
                for donor in &corroborated {
                    donors.entry(donor.id).or_insert_with(|| donor.clone());
                }
                deferred.push(DeferredMove {
                    observation,
                    candidate_ids: corroborated.iter().map(|d| d.id).collect(),
                });
                // Held back deliberately: nothing about this file is persisted,
                // counted, or reported until the walk has seen every path, so
                // that which observation takes which row is decided globally
                // rather than by whichever the walker reached first.
                continue;
            }
            ReconcileOutcome::Settled(Reconciliation::Unchanged { source_id }) => {
                unchanged_batch.push((source_id, device, inode));
                if unchanged_batch.len() >= UNCHANGED_BATCH_SIZE {
                    flush_unchanged(conn, &unchanged_batch, now)?;
                    unchanged_batch.clear();
                }
                (
                    FileAction::Unchanged,
                    source_id,
                    source_at_path.and_then(|s| s.object_id),
                )
            }
            ReconcileOutcome::Settled(reconciliation) => {
                // Only New reconciliations receive decision_id (conservative scan semantics).
                // Modified, Moved preserve the existing value via omission in SQL.
                let file_decision_id = match &reconciliation {
                    Reconciliation::New => decision_id,
                    _ => None,
                };
                let source = match persist_file(
                    conn,
                    &observation,
                    &reconciliation,
                    now,
                    file_decision_id,
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        progress.on_process_error(&full_path.display().to_string(), &e.to_string());
                        stats.skipped += 1;
                        continue;
                    }
                };
                (
                    action_for(&reconciliation),
                    source.id,
                    old_object_link(&reconciliation),
                )
            }
        };

        // Both counters are per *file*, not per nomination, and both are
        // incremented only now — after the file is persisted. The arm reports
        // how many rows were still standing or unreachable, which is several
        // for a group of twins; but these two numbers describe files. The
        // companion count qualifies the `new` count ("N new, of which M are
        // companions") and would describe nothing if it could exceed it, which
        // it can if a file that failed to persist still counted. A file can
        // make at most one move, so an unverifiable check costs one, however
        // many rows it could not ask about.
        if companions > 0 {
            stats.hardlink_companions += 1;
        }
        if unverified > 0 {
            stats.moves_unverified += 1;
        }
        for root in unverified_roots {
            *unverified_by_root.entry(root).or_default() += 1;
        }

        // Notify progress
        progress.on_file(rel_path_str, &action);

        // Track seen sources
        seen_source_ids.insert(source_id);
        outcomes.push((source_id, SourceOutcome::Seen));

        match action {
            FileAction::New => stats.new += 1,
            FileAction::Modified => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }

        // Collect files for hashing. The gate is asked here and at end-of-walk
        // resolution below, and nowhere else.
        if let Some(need) = needs_hash(&action, options, old_object_id) {
            queue_for_hash(
                &mut files_to_hash,
                need,
                source_id,
                full_path.to_path_buf(),
                old_object_id,
            );
        }
    }

    // Flush remaining unchanged files
    flush_unchanged(conn, &unchanged_batch, now)?;

    // Decide the held-back moves. This sits here, after the walk and *before*
    // the missing-detection block below, for one reason: a row claimed by a
    // move must already be in the seen set when `find_missing` computes its
    // difference. Resolve after it and every move would be reported as a
    // deletion and an arrival of the same file.
    for resolution in resolve_moves(deferred, &donors) {
        let (rel_path, claimed) = match &resolution {
            MoveResolution::Moved {
                observation,
                source_id,
            } => (observation.rel_path.clone(), Some(*source_id)),
            MoveResolution::New { observation } => (observation.rel_path.clone(), None),
        };
        let (action, source_id, old_object_id) =
            match persist_resolution(conn, resolution, &donors, now, decision_id) {
                Ok(resolved) => resolved,
                Err(e) => {
                    progress.on_process_error(
                        &root_path.join(&rel_path).display().to_string(),
                        &e.to_string(),
                    );
                    stats.skipped += 1;
                    // A write that failed is not evidence that the donor's file
                    // is gone. Left out of the seen set it would fall to
                    // missing detection, which would flip a present row to
                    // absent and write a deletion receipt for content this very
                    // walk observed on disk.
                    if let Some(donor) = claimed {
                        seen_source_ids.insert(donor);
                        outcomes.push((donor, SourceOutcome::Seen));
                    }
                    continue;
                }
            };

        progress.on_file(&rel_path, &action);
        seen_source_ids.insert(source_id);
        outcomes.push((source_id, SourceOutcome::Seen));

        match action {
            FileAction::New => stats.new += 1,
            FileAction::Modified => stats.updated += 1,
            FileAction::Moved => stats.moved += 1,
            FileAction::Unchanged => stats.unchanged += 1,
        }

        if let Some(need) = needs_hash(&action, options, old_object_id) {
            queue_for_hash(
                &mut files_to_hash,
                need,
                source_id,
                root_path.join(&rel_path),
                old_object_id,
            );
        }
    }

    // A nomination Canon could not check is stated with the root that could not
    // answer, so "new" on an unreachable-root universe never passes silently
    // for "not moved".
    for (root, count) in &unverified_by_root {
        warnings.push(format!(
            "{count} possible moves could not be verified — root {root} unreachable"
        ));
    }

    // Check if the mount is still the same device after the walk.
    // If the device changed (or disappeared), the mount was disrupted during
    // the scan — skip missing detection to avoid falsely marking files as gone.
    let post_walk_device = get_dir_device(&walk_root);
    let mount_stable = pre_walk_device.is_some()
        && post_walk_device.is_some()
        && pre_walk_device == post_walk_device;

    if mount_stable && stats.walk_errors == 0 {
        // Identify sources that are truly missing using pure domain function
        // Sources not seen during walk AND not handled by empty-dir logic are missing
        let all_accounted: HashSet<i64> = seen_source_ids.union(&handled_ids).copied().collect();
        let missing_ids = find_missing(&expected_ids, &all_accounted);
        for id in missing_ids {
            outcomes.push((id, SourceOutcome::Missing));
        }
    } else {
        // Inferred absence cannot be trusted on an unstable or incomplete
        // walk: unseen is not evidence of gone. Every inferred Missing is
        // discarded — the post-walk difference is never computed, and Missing
        // classifications from empty directories are dropped too (their
        // Disconnected siblings stand; those never mark deletion by
        // themselves). The skip is counted in the stats — not just warned
        // about — so the decision summary durably records that this scan
        // could not verify absence (a scan that couldn't observe deletions
        // must be distinguishable in the trail from one that observed none).
        outcomes.retain(|(_, outcome)| !matches!(outcome, SourceOutcome::Missing));
        if !mount_stable {
            stats.missing_detection_skipped = 1;
            let detail = if pre_walk_device != post_walk_device {
                "Mount changed during scan"
            } else {
                "Mount device could not be verified"
            };
            warnings.push(format!(
                "{detail} — skipping missing detection to avoid data loss"
            ));
        } else {
            warnings.push(format!(
                "{} walk errors — skipping missing detection to avoid data loss",
                stats.walk_errors
            ));
        }
    }

    // Mark missing/disconnected files based on outcomes. Deletion receipt items
    // are captured before the flip (when capturing), so they carry each source's
    // pre-flip provenance link.
    let (missing_count, disconnected_count, deleted_items, missing_warnings) =
        mark_missing_sources(
            conn,
            &outcomes,
            now,
            options.ignore_device_id,
            decision_id,
            capture_deletions,
        )?;
    warnings.extend(missing_warnings);
    stats.missing = missing_count;
    stats.disconnected = disconnected_count;

    Ok(ScanRootResult {
        stats,
        files_to_hash,
        deleted_items,
        warnings,
    })
}

/// Intermediate result from reconciling a file against DB state (before persistence).
struct ReconcileResult {
    observation: FileObservation,
    outcome: ReconcileOutcome,
    /// The source at this path before reconciliation (for old_object_id on Unchanged).
    source_at_path: Option<Source>,
    /// Nominated rows still standing at their own paths — this file is their
    /// sibling, not their former self. Counted because during convergence there
    /// are tens of thousands of them, and an unexplained flood of "new" reads
    /// like data loss.
    companions: u32,
    /// Nominations whose own paths could not be checked — the arm's own count,
    /// which is what the summary reports.
    unverified: u32,
    /// Which roots could not answer, one entry per unverifiable nomination. A
    /// declared projection of `unverified` for the warning's sake: the arm
    /// decides, this only says where. Pinned to agree by
    /// `an_unverifiable_nomination_is_counted_and_its_root_named`.
    unverified_roots: Vec<String>,
}

/// Whether a file's outcome is decided, or waits for the end of the walk.
enum ReconcileOutcome {
    Settled(Reconciliation),
    /// Corroborated rows, gone from their own paths, one of which this
    /// observation may claim once every path has been seen.
    Deferred(Vec<Source>),
}

/// The report's word for a persisted reconciliation. One mapping, consumed by
/// the walk and by end-of-walk resolution alike.
fn action_for(reconciliation: &Reconciliation) -> FileAction {
    match reconciliation {
        Reconciliation::New => FileAction::New,
        Reconciliation::Modified { .. } => FileAction::Modified,
        Reconciliation::Moved { .. } => FileAction::Moved,
        Reconciliation::Unchanged { .. } => FileAction::Unchanged,
    }
}

/// The object link a reconciliation carried in, for the hash pass's
/// unexpected-change check. `New` has no predecessor, and `Unchanged` reads the
/// still-standing row rather than this.
fn old_object_link(reconciliation: &Reconciliation) -> Option<i64> {
    match reconciliation {
        Reconciliation::New | Reconciliation::Unchanged { .. } => None,
        Reconciliation::Modified { old_object_id, .. }
        | Reconciliation::Moved { old_object_id, .. } => *old_object_id,
    }
}

/// The hash gate, spoken once: does this file go to the hash pass, and what for.
///
/// The question is **need-driven, not action-driven**. What a file's
/// reconciliation was called says whether its content is known to have
/// *changed*; it says nothing about whether Canon holds its content identity at
/// all. A row that was never hashed carries the same debt whether this walk
/// found it new, moved, or sitting exactly where it always sat — and a gate
/// keyed on the outcome leaves that debt standing for as long as the file stays
/// still, which on the quiet parts of a library is forever. Debt indexed in
/// January survived a clean full scan in August that way, and surfaced only when
/// an archive operation could not see the content at all.
///
/// The row's object link is already in hand at both call sites, so asking costs
/// no query. Empty files are asked about like any other: identity claims about
/// empty content are vacuous, but the hash is still computed, so an unhashed
/// empty source is debt exactly as a photograph is.
fn needs_hash(
    action: &FileAction,
    options: &ScanOptions,
    object_id: Option<i64>,
) -> Option<HashNeed> {
    if !options.hash {
        return None;
    }
    match action {
        FileAction::New | FileAction::Modified => Some(HashNeed::Basis),
        FileAction::Moved | FileAction::Unchanged => {
            if options.hash_all {
                Some(HashNeed::Reverify)
            } else if object_id.is_none() {
                Some(HashNeed::Backlog)
            } else {
                None
            }
        }
    }
}

/// Queue a file for the hash pass, carrying the gate's own answer with it.
///
/// The need travels rather than being re-derived downstream: the pass reads it
/// twice, once to know whether a changed hash is expected and once to know
/// whether reading this file paid off debt, and both are the same question the
/// gate already answered. Neither is counted here — a file is queued long
/// before it is read, and what a scan reports about debt has to follow what the
/// pass actually managed to read.
fn queue_for_hash(
    files_to_hash: &mut Vec<FileToHash>,
    need: HashNeed,
    source_id: i64,
    full_path: std::path::PathBuf,
    old_object_id: Option<i64>,
) {
    files_to_hash.push(FileToHash {
        source_id,
        full_path,
        old_object_id,
        need,
    });
}

/// What the disk says about a nominated row's own path.
///
/// The distinction this draws is between absence and ignorance, and it is the
/// whole reason the check exists. A file gone from the storage that recorded it
/// is evidence: something happened there, and a move is one explanation.
/// Anything else — a permission error, an I/O failure, an unreachable root —
/// says only that Canon cannot see, and a claim built on not seeing is a guess.
///
/// **Absence has to be absence from the right storage.** A root's path is a
/// directory, and a directory whose volume is not mounted is still a directory:
/// it answers, it is empty, and every file ever recorded under it reads as gone.
/// That is not absence, it is a mountpoint with nothing behind it — so the
/// check confirms it is looking at the same storage the row was recorded on, by
/// the stored device the row carries. This is device serving as *mount-presence*
/// evidence, which is the one job it keeps here and the same job it does in the
/// empty-directory classifier; it is not identity, and nothing about the file is
/// decided by it.
///
/// The cost is deliberate and one-sided: for the first scan after a genuine
/// remount the stored devices are stale, so moves out of that root degrade to
/// new paths until it is scanned again and its rows refresh. Refusing to answer
/// costs a conservative verdict; answering from a mountpoint shell costs a row
/// relocated away from content that is sitting untouched on an unplugged disk.
fn check_old_path(candidate: &Source) -> OldPathCheck {
    let root = Path::new(&candidate.root_path);
    match fs::metadata(root.join(&candidate.rel_path)) {
        Ok(_) => OldPathCheck::Present,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match fs::metadata(root).map(|meta| meta.dev() as i64) {
                Ok(device) if device == candidate.device => OldPathCheck::Vacated,
                _ => OldPathCheck::Unverifiable,
            }
        }
        Err(_) => OldPathCheck::Unverifiable,
    }
}

/// Reconcile a single file: read DB state, determine outcome, compute partial hash if needed.
/// Does NOT persist — caller decides how to write (batch, individual
/// transaction, or end-of-walk resolution).
#[allow(clippy::too_many_arguments)]
fn reconcile_file(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    full_path: &Path,
    device: i64,
    inode: i64,
    size: i64,
    mtime: i64,
) -> Result<ReconcileResult> {
    let mut observation = FileObservation {
        root_id,
        rel_path: rel_path.to_string(),
        device: device as u64,
        inode: inode as u64,
        size,
        mtime,
        partial_hash: None,
    };

    let source_at_path = repo::source::fetch_by_path(conn, root_id, rel_path)?;

    // Every partial hash below is computed outside the transaction — filesystem
    // I/O can be slow on NAS/network storage and must not hold the write lock.
    let Some(existing) = source_at_path.as_ref() else {
        return reconcile_pathless_file(conn, observation, full_path, size, inode);
    };

    // The path holds a row, so the row is this file's; the only question is
    // whether the content under it moved. The head read is demanded exactly
    // when the inode changed beneath a standing path — that is the one case
    // where a bit-identical recreation and a real edit look alike from metadata
    // alone. A stored inode of 0 was never tracked (a platform that records
    // none), so nothing says it moved and the fingerprint decides, as it always
    // has.
    let inode_changed = existing.inode != 0 && existing.inode as u64 != observation.inode;
    if inode_changed {
        observation.partial_hash = Some(compute_partial_hash(full_path, size as u64)?);
    }
    let reconciliation =
        reconcile_at_path(&observation, existing, observation.partial_hash.as_deref())
            .into_reconciliation();

    // A head read taken for the decision is the one the write needs — a file is
    // never opened twice for the same observation.
    if reconciliation.needs_partial_hash() && observation.partial_hash.is_none() {
        observation.partial_hash = Some(compute_partial_hash(full_path, size as u64)?);
    }

    Ok(ReconcileResult {
        observation,
        outcome: ReconcileOutcome::Settled(reconciliation),
        source_at_path,
        companions: 0,
        unverified: 0,
        unverified_roots: Vec::new(),
    })
}

/// The pathless half: nominate by inode, ask the disk about each nomination,
/// then let the domain arm decide.
///
/// The head read is unconditional here, and free: a new path's INSERT stores a
/// partial hash whatever the outcome, so corroborating with it costs nothing
/// that was not already owed.
fn reconcile_pathless_file(
    conn: &Connection,
    mut observation: FileObservation,
    full_path: &Path,
    size: i64,
    inode: i64,
) -> Result<ReconcileResult> {
    let partial_hash = compute_partial_hash(full_path, size as u64)?;
    observation.partial_hash = Some(partial_hash.clone());

    let candidates: Vec<MoveCandidate> = scan_repo::source::fetch_by_inode(conn, inode as u64)?
        .into_iter()
        // A suspended root is one the user has closed the door on: its content
        // keeps exactly the standing it had, and a scan of some other root may
        // not reach in and relocate a row out of it. Nominations from there are
        // dropped rather than counted — the parked root's story is unchanged,
        // and this root's report is accurate without mentioning it.
        .filter(|source| !source.root_suspended)
        .map(|source| {
            let old_path = check_old_path(&source);
            MoveCandidate { source, old_path }
        })
        .collect();

    match reconcile_pathless(&observation, &candidates, &partial_hash) {
        PathlessOutcome::New {
            companions,
            unverified,
        } => {
            let mut unverified_roots: Vec<String> = candidates
                .iter()
                .filter(|c| c.old_path == OldPathCheck::Unverifiable)
                .map(|c| c.source.root_path.clone())
                .collect();
            // One entry per root, not per row: the warning counts files whose
            // move that root could not settle, and a file nominating five rows
            // on one unreachable root is still one file.
            unverified_roots.sort();
            unverified_roots.dedup();
            Ok(ReconcileResult {
                observation,
                outcome: ReconcileOutcome::Settled(Reconciliation::New),
                source_at_path: None,
                companions,
                unverified,
                unverified_roots,
            })
        }
        PathlessOutcome::Deferred {
            corroborated_candidate_ids,
        } => {
            let corroborated = candidates
                .into_iter()
                .filter(|c| corroborated_candidate_ids.contains(&c.source.id))
                .map(|c| c.source)
                .collect();
            Ok(ReconcileResult {
                observation,
                outcome: ReconcileOutcome::Deferred(corroborated),
                source_at_path: None,
                companions: 0,
                unverified: 0,
                unverified_roots: Vec::new(),
            })
        }
    }
}

/// Persist one resolved move, re-checking the claim inside the transaction.
///
/// Between the walk and this moment another process may have moved, deleted or
/// replaced the row about to be claimed — canon is explicitly a
/// several-processes-at-once tool. The check is deliberately *inside* the write
/// transaction, because a check outside it answers a question about the past.
/// If the row is not exactly as it was nominated — same root, same path, same
/// inode, still present — the observation degrades to a new path, which costs
/// one row and claims nothing.
fn persist_resolution(
    conn: &Connection,
    resolution: MoveResolution,
    nominated_as: &HashMap<i64, Source>,
    now: i64,
    decision_id: Option<i64>,
) -> Result<(FileAction, i64, Option<i64>)> {
    let (observation, claim) = match resolution {
        MoveResolution::Moved {
            source_id,
            observation,
        } => (observation, Some(source_id)),
        MoveResolution::New { observation } => (observation, None),
    };

    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

    let reconciliation = match claim.and_then(|id| nominated_as.get(&id).map(|row| (id, row))) {
        Some((source_id, nominated)) => {
            let unchanged_since_the_walk =
                repo::source::fetch_by_id(&tx, source_id)?.is_some_and(|current| {
                    current.root_id == nominated.root_id
                        && current.rel_path == nominated.rel_path
                        && current.inode == nominated.inode
                });
            if unchanged_since_the_walk {
                Reconciliation::Moved {
                    source_id,
                    from_root_id: nominated.root_id,
                    from_path: nominated.rel_path.clone(),
                    old_object_id: nominated.object_id,
                }
            } else {
                Reconciliation::New
            }
        }
        None => Reconciliation::New,
    };

    let file_decision_id = match reconciliation {
        Reconciliation::New => decision_id,
        _ => None,
    };
    let source = scan_repo::source::apply_reconciliation(
        &tx,
        &observation,
        &reconciliation,
        now,
        file_decision_id,
    )?;
    tx.commit()?;

    Ok((
        action_for(&reconciliation),
        source.id,
        old_object_link(&reconciliation),
    ))
}

/// Persist a non-unchanged reconciliation in its own transaction.
fn persist_file(
    conn: &Connection,
    observation: &FileObservation,
    reconciliation: &Reconciliation,
    now: i64,
    decision_id: Option<i64>,
) -> Result<crate::core::domain::source::Source> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let source = scan_repo::source::apply_reconciliation(
        &tx,
        observation,
        reconciliation,
        now,
        decision_id,
    )?;
    tx.commit()?;
    Ok(source)
}

/// Flush accumulated unchanged file updates in a single transaction.
fn flush_unchanged(conn: &Connection, batch: &[(i64, i64, i64)], now: i64) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    scan_repo::source::batch_update_unchanged(&tx, batch, now)?;
    tx.commit()?;
    Ok(())
}

const UNCHANGED_BATCH_SIZE: usize = 500;

/// Translate source outcomes to DB mutations.
///
/// When `capture_deletions` is set, the sources about to be marked missing are
/// snapshotted **before** the flip (so their captured `previous_decision_id` is the
/// pre-flip value) and returned as receipt items; otherwise the returned Vec is empty.
///
/// Returns (missing_count, disconnected_count, deleted_items, warnings).
fn mark_missing_sources(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<(u64, u64, Vec<DeletionReceiptItem>, Vec<String>)> {
    let mut missing_ids: Vec<i64> = Vec::new();
    let mut disconnected_count = 0u64;

    // Observation outranks inference. A source can collect two outcomes in one
    // scan: the empty-directory classifier reads a directory that a move has
    // just emptied as evidence its sources are gone — true of the directory,
    // false of the file, which the same walk saw at its new path. Which of the
    // two lands last depends only on the order the walker happened to reach
    // them in, and the wrong order marks a file deleted that is sitting right
    // there. Having been seen is the stronger claim, so the seen set is
    // subtracted before anything is flipped.
    let seen: HashSet<i64> = outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, SourceOutcome::Seen))
        .map(|(id, _)| *id)
        .collect();

    for (id, outcome) in outcomes {
        if seen.contains(id) {
            continue;
        }
        match outcome {
            SourceOutcome::Seen => {}
            SourceOutcome::Missing => {
                missing_ids.push(*id);
            }
            SourceOutcome::Disconnected => {
                if ignore_device_id {
                    missing_ids.push(*id);
                } else {
                    disconnected_count += 1;
                }
            }
        }
    }

    // Capture receipt items before the flip; mark_missing then stamps decision_id
    // on exactly these sources (stamp-set = receipt-set).
    let deleted_items = if capture_deletions {
        capture_deletion_items(conn, &missing_ids)?
    } else {
        Vec::new()
    };

    let missing_count = repo::source::mark_missing(conn, &missing_ids, now, decision_id)?;

    let mut warnings = Vec::new();
    if disconnected_count > 0 {
        warnings.push(format!(
            "Skipped {disconnected_count} files (device ID mismatch - possibly disconnected storage)"
        ));
        warnings.push(
            "  If device IDs changed (e.g., NAS remount), re-run with --ignore-device-id"
                .to_string(),
        );
    }

    Ok((missing_count, disconnected_count, deleted_items, warnings))
}

/// Snapshot sources about to be marked missing into deletion-receipt items.
/// Must be called before `mark_missing` flips them so each item's
/// `previous_decision_id` is the pre-flip provenance link. Items are sorted by
/// rel_path for a stable, readable receipt.
fn capture_deletion_items(
    conn: &Connection,
    missing_ids: &[i64],
) -> Result<Vec<DeletionReceiptItem>> {
    if missing_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut items: Vec<DeletionReceiptItem> =
        scan_repo::source::fetch_for_receipt(conn, missing_ids)?
            .into_iter()
            .map(|s| DeletionReceiptItem {
                rel_path: s.rel_path,
                hash: s.hash,
                size: s.size,
                mtime: s.mtime,
                previous_decision_id: s.previous_decision_id,
            })
            .collect();
    items.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(items)
}

/// Mark every present source under `path` as deleted, for `--missing` on a
/// folder that no longer exists on disk (so it can't be walked).
///
/// This is the second deletion-detection path; it receives the same treatment as
/// the sweep: the flip is stamped with `decision_id`, and — when `capture_deletions`
/// is set — the sources are snapshotted **before** the flip into receipt items
/// carrying their pre-flip provenance link. The caller writes the source-local
/// receipt via [`write_deletion_receipts`].
///
/// `path` must resolve to a known root (relative paths are cleaned against `cwd`);
/// otherwise this errors. A path that matches a root but has no present sources
/// returns `missing_count = 0` and no items — the caller decides how to report it.
///
/// [`write_deletion_receipts`]: super::receipt::write_deletion_receipts
pub fn mark_missing_path(
    conn: &Connection,
    path: &Path,
    roots: &[crate::core::domain::root::Root],
    cwd: &Path,
    now: i64,
    decision_id: Option<i64>,
    capture_deletions: bool,
) -> Result<MarkMissingPathResult> {
    let cleaned = crate::core::domain::path::clean_path(path, cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let (root_id, root_path, rel_prefix) =
        match crate::core::domain::root::find_containing_root(&cleaned_str, roots) {
            Some((id, root_path, _role, rel)) => (id, root_path, rel),
            None => bail!(
                "Cannot mark missing: {} is not under any known root",
                path.display()
            ),
        };

    // The same suspended-root refusal the walk path makes. It matters most
    // here: a suspended root is precisely one whose path fails to
    // canonicalize, so --missing is the arm that would otherwise reach it
    // and mark a disconnected drive's sources deleted.
    if let Some(root) = roots.iter().find(|r| r.id == root_id) {
        if root.is_suspended() {
            bail!("Root '{root_path}' is suspended. Use 'canon roots unsuspend' to reactivate.");
        }
    }

    let prefix_arg = if rel_prefix.is_empty() {
        None
    } else {
        Some(rel_prefix.as_str())
    };
    let source_ids = scan_repo::source::fetch_source_ids_for_root(conn, root_id, prefix_arg)?;

    // Capture receipt items before the flip so each item's previous_decision_id
    // is the pre-flip value (stamp-set = receipt-set).
    let deleted_items = if capture_deletions {
        capture_deletion_items(conn, &source_ids)?
    } else {
        Vec::new()
    };

    let missing_count = repo::source::mark_missing(conn, &source_ids, now, decision_id)?;

    Ok(MarkMissingPathResult {
        root_id,
        root_path,
        missing_count,
        deleted_items,
    })
}

/// A source's id paired with its classified outcome.
type SourceOutcomes = Vec<(i64, SourceOutcome)>;

/// Classify sources under an empty directory by comparing stored device to current device.
/// Returns outcomes and any warnings about disconnected storage.
fn classify_sources_in_empty_dir(
    conn: &Connection,
    root_id: i64,
    rel_prefix: &str,
    current_device: i64,
) -> Result<(SourceOutcomes, Vec<String>)> {
    let sources = scan_repo::source::fetch_device_info_by_prefix(conn, root_id, rel_prefix)?;

    let mut disconnected_count = 0usize;
    let results: Vec<_> = sources
        .into_iter()
        .map(|(id, stored_device)| {
            let outcome = match stored_device {
                Some(dev) if dev != current_device => {
                    disconnected_count += 1;
                    SourceOutcome::Disconnected
                }
                _ => SourceOutcome::Missing,
            };
            (id, outcome)
        })
        .collect();

    let mut warnings = Vec::new();
    if disconnected_count > 0 {
        let path_desc = if rel_prefix.is_empty() {
            "(root)"
        } else {
            rel_prefix
        };
        warnings.push(format!(
            "{path_desc} contains {disconnected_count} files on different device (possibly disconnected storage)"
        ));
    }

    Ok((results, warnings))
}

/// Get device ID of a directory (Unix only).
fn get_dir_device(path: &Path) -> Option<i64> {
    fs::metadata(path).ok().map(|m| m.dev() as i64)
}

/// Check if a directory is empty (no entries).
fn is_empty_dir(path: &Path) -> bool {
    fs::read_dir(path)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::TempDir;

    use super::super::types::current_timestamp;

    /// No-op progress implementation for tests.
    struct NoopProgress;
    impl ScanProgress for NoopProgress {
        fn on_file(&self, _path: &str, _action: &FileAction) {}
        fn on_walk_error(&self, _error: &str) {}
        fn on_process_error(&self, _path: &str, _error: &str) {}
    }

    /// Test result from process_file helper.
    struct ProcessResult {
        // Mirrors persist_file's real return data; only `action` is currently
        // asserted on, kept available for tests that need to check identity.
        #[allow(dead_code)]
        source_id: i64,
        action: FileAction,
        #[allow(dead_code)]
        old_object_id: Option<i64>,
    }

    /// Test helper: reconcile + persist a single file (replicates old process_file behavior).
    #[allow(clippy::too_many_arguments)]
    fn process_file(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        full_path: &Path,
        device: i64,
        inode: i64,
        size: i64,
        mtime: i64,
        now: i64,
    ) -> Result<ProcessResult> {
        let reconciled = reconcile_file(
            conn, root_id, rel_path, full_path, device, inode, size, mtime,
        )?;

        match reconciled.outcome {
            // This helper drives one file in isolation, and a deferred move is
            // by definition undecidable in isolation — it waits for the walk to
            // see every path. Tests about moves go through `scan_pass`.
            ReconcileOutcome::Deferred(_) => {
                panic!("a deferred move is resolved at end of walk — drive it through scan_pass")
            }
            ReconcileOutcome::Settled(Reconciliation::Unchanged { source_id }) => {
                // Persist unchanged inline (no batching in tests)
                flush_unchanged(conn, &[(source_id, device, inode)], now)?;
                Ok(ProcessResult {
                    source_id,
                    action: FileAction::Unchanged,
                    old_object_id: reconciled.source_at_path.and_then(|s| s.object_id),
                })
            }
            ReconcileOutcome::Settled(reconciliation) => {
                let source =
                    persist_file(conn, &reconciled.observation, &reconciliation, now, None)?;
                Ok(ProcessResult {
                    source_id: source.id,
                    action: action_for(&reconciliation),
                    old_object_id: old_object_link(&reconciliation),
                })
            }
        }
    }

    /// Create a temp file with content and return (path, device, inode, size, mtime).
    fn create_temp_file(dir: &TempDir, name: &str, content: &str) -> (PathBuf, u64, u64, i64, i64) {
        let path = dir.path().join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        drop(file);

        let meta = fs::metadata(&path).unwrap();
        (
            path,
            meta.dev(),
            meta.ino(),
            meta.len() as i64,
            meta.modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        )
    }

    #[test]
    fn process_file_new() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) = create_temp_file(&temp_dir, "new.txt", "content");
        let now = current_timestamp();

        let result = process_file(
            &conn,
            root_id,
            "new.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::New));

        let source = repo::source::fetch_by_path(&conn, root_id, "new.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_unchanged() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "unchanged.txt", "content");

        repo::insert_test_source(
            &conn,
            root_id,
            "unchanged.txt",
            device as i64,
            inode as i64,
            size,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "unchanged.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Unchanged));
    }

    #[test]
    fn process_file_modified_size() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "modified.txt", "new content");

        repo::insert_test_source(
            &conn,
            root_id,
            "modified.txt",
            device as i64,
            inode as i64,
            5,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "modified.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Modified));
    }

    #[test]
    fn process_file_device_changed() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) = create_temp_file(&temp_dir, "file.txt", "content");

        // Pre-insert with DIFFERENT device but same inode
        repo::insert_test_source(&conn, root_id, "file.txt", 99999, inode as i64, size, mtime);

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "file.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::Unchanged));

        let source = repo::source::fetch_by_path(&conn, root_id, "file.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.device, device as i64);
    }

    #[test]
    fn process_file_replaced() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "replaced.txt", "new content");

        let old_inode = inode + 99999;
        repo::insert_test_source(
            &conn,
            root_id,
            "replaced.txt",
            device as i64,
            old_inode as i64,
            50,
            mtime,
        );

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "replaced.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        // The path stands and its content changed: the file the user has is an
        // edited one, whatever inode the editor left behind.
        assert!(matches!(result.action, FileAction::Modified));

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "replaced.txt"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let source = repo::source::fetch_by_path(&conn, root_id, "replaced.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.inode, inode as i64);
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_revives_stale_record() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path, device, inode, size, mtime) =
            create_temp_file(&temp_dir, "revived.txt", "new content");

        let old_source_id = repo::insert_test_source(&conn, root_id, "revived.txt", 1, 1, 50, 1000);
        conn.execute(
            "UPDATE sources SET present = 0 WHERE id = ?",
            [old_source_id],
        )
        .unwrap();

        let now = current_timestamp();
        let result = process_file(
            &conn,
            root_id,
            "revived.txt",
            &path,
            device as i64,
            inode as i64,
            size,
            mtime,
            now,
        )
        .unwrap();

        assert!(matches!(result.action, FileAction::New));

        let source = repo::source::fetch_by_path(&conn, root_id, "revived.txt")
            .unwrap()
            .unwrap();
        assert_eq!(source.id, old_source_id);
        assert_eq!(source.inode, inode as i64);
        assert_eq!(source.size, size);
    }

    #[test]
    fn process_file_independent_operations() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let (path1, dev1, ino1, size1, mtime1) =
            create_temp_file(&temp_dir, "new.txt", "new content");
        let (path2, dev2, ino2, size2, mtime2) =
            create_temp_file(&temp_dir, "existing.txt", "existing");
        let (path3, dev3, ino3, size3, mtime3) =
            create_temp_file(&temp_dir, "modified.txt", "modified content");

        repo::insert_test_source(
            &conn,
            root_id,
            "existing.txt",
            dev2 as i64,
            ino2 as i64,
            size2,
            mtime2,
        );
        repo::insert_test_source(
            &conn,
            root_id,
            "modified.txt",
            dev3 as i64,
            ino3 as i64,
            5,
            mtime3,
        );

        let now = current_timestamp();

        let r1 = process_file(
            &conn,
            root_id,
            "new.txt",
            &path1,
            dev1 as i64,
            ino1 as i64,
            size1,
            mtime1,
            now,
        )
        .unwrap();
        let r2 = process_file(
            &conn,
            root_id,
            "existing.txt",
            &path2,
            dev2 as i64,
            ino2 as i64,
            size2,
            mtime2,
            now,
        )
        .unwrap();
        let r3 = process_file(
            &conn,
            root_id,
            "modified.txt",
            &path3,
            dev3 as i64,
            ino3 as i64,
            size3,
            mtime3,
            now,
        )
        .unwrap();

        assert!(matches!(r1.action, FileAction::New));
        assert!(matches!(r2.action, FileAction::Unchanged));
        assert!(matches!(r3.action, FileAction::Modified));

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn mark_missing_sources_counts_correctly() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "file1.txt", 1, 1, 100, 1000);
        let id2 = repo::insert_test_source(&conn, root_id, "file2.txt", 1, 2, 100, 1000);
        let id3 = repo::insert_test_source(&conn, root_id, "file3.txt", 1, 3, 100, 1000);

        let outcomes = vec![
            (id1, SourceOutcome::Seen),
            (id2, SourceOutcome::Missing),
            (id3, SourceOutcome::Disconnected),
        ];

        let now = current_timestamp();
        let (missing_count, disconnected_count, _items, warnings) =
            mark_missing_sources(&conn, &outcomes, now, false, None, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 1);
        assert!(!warnings.is_empty()); // Should have disconnected warning

        let s1 = repo::source::fetch_by_path(&conn, root_id, "file1.txt").unwrap();
        assert!(s1.is_some());

        let s2: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id2], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s2, 0);

        let s3: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [id3], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s3, 1);
    }

    #[test]
    fn mark_missing_sources_disconnected_with_ignore_flag() {
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "file1.txt", 1, 1, 100, 1000);

        let outcomes = vec![(id1, SourceOutcome::Disconnected)];

        let now = current_timestamp();
        let (missing_count, disconnected_count, _items, warnings) =
            mark_missing_sources(&conn, &outcomes, now, true, None, false).unwrap();

        assert_eq!(missing_count, 1);
        assert_eq!(disconnected_count, 0);
        assert!(warnings.is_empty()); // No warnings when ignore_device_id=true
    }

    #[test]
    fn mark_missing_sources_stamps_decision_id() {
        // The sweep threads the scan decision_id into the deletion transition.
        let conn = repo::open_in_memory_for_test();
        let temp_dir = TempDir::new().unwrap();
        let root_id =
            repo::insert_test_root(&conn, temp_dir.path().to_str().unwrap(), "source", false);

        let id1 = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        let outcomes = vec![(id1, SourceOutcome::Missing)];

        let now = current_timestamp();
        let (missing_count, _, _items, _) =
            mark_missing_sources(&conn, &outcomes, now, false, Some(123), false).unwrap();
        assert_eq!(missing_count, 1);

        let decision_id: Option<i64> = conn
            .query_row("SELECT decision_id FROM sources WHERE id = ?", [id1], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(decision_id, Some(123));
    }

    // =========================================================================
    // Deletion capture before the flip (scan_root)
    // =========================================================================

    #[test]
    fn a_source_seen_this_scan_is_never_marked_missing() {
        // A move empties the directory the file came from, and the
        // empty-directory classifier reads that as evidence its sources are
        // gone. True of the directory; false of the file, which this same scan
        // saw at its new path. Whether the two collide depends on which the
        // walker reaches first, so the order is fixed here rather than left to
        // the filesystem: the emptied directory, then the new path.
        //
        // Without the seen-set subtraction this marks a present file deleted
        // and writes a deletion receipt for it — the most damaging thing a scan
        // can get wrong, since a scan only ever observes.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        std::fs::create_dir(temp.path().join("from")).unwrap();
        std::fs::write(temp.path().join("from/x.txt"), "content").unwrap();
        scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(temp.path()),
            &no_hash_options(),
            &NoopProgress,
            current_timestamp(),
            Some(1),
            false,
        )
        .unwrap();
        let source_id = repo::source::fetch_by_path(&conn, root_id, "from/x.txt")
            .unwrap()
            .unwrap()
            .id;

        std::fs::create_dir(temp.path().join("to")).unwrap();
        std::fs::rename(temp.path().join("from/x.txt"), temp.path().join("to/x.txt")).unwrap();

        let emptied = WalkDir::new(temp.path().join("from"))
            .into_iter()
            .next()
            .unwrap();
        let entries = std::iter::once(emptied).chain(walk(&temp.path().join("to")));

        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            entries,
            &no_hash_options(),
            &NoopProgress,
            current_timestamp(),
            Some(2),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing, 0, "a file seen this scan is present");
        assert!(
            result.deleted_items.is_empty(),
            "and no deletion receipt claims otherwise"
        );
        let present: i64 = conn
            .query_row(
                "SELECT present FROM sources WHERE id = ?",
                [source_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(present, 1);
    }

    use walkdir::WalkDir;

    /// Build a `.canon-ledger`-filtered walker over `root`, like the interface does.
    fn walk(root: &Path) -> impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>> {
        WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !(e.file_type().is_dir() && e.file_name() == ".canon-ledger"))
    }

    fn no_hash_options() -> ScanOptions {
        ScanOptions {
            hash: false,
            hash_all: false,
            ignore_device_id: false,
        }
    }

    // =========================================================================
    // Identity at a standing path (scan_root, end to end)
    // =========================================================================

    /// One full pass of the pipeline over `dir`, as the interface drives it.
    fn scan_pass(
        conn: &Connection,
        root_id: i64,
        dir: &Path,
        decision_id: Option<i64>,
    ) -> ScanRootResult {
        scan_pass_with(conn, root_id, dir, walk(dir), decision_id)
    }

    /// The same pass, over an entry stream the test controls — for the cases
    /// where *when* the walker reaches a path is the thing under test.
    fn scan_pass_with(
        conn: &Connection,
        root_id: i64,
        dir: &Path,
        entries: impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>>,
        decision_id: Option<i64>,
    ) -> ScanRootResult {
        scan_root(
            conn,
            root_id,
            dir.to_str().unwrap(),
            None,
            entries,
            &no_hash_options(),
            &NoopProgress,
            current_timestamp(),
            decision_id,
            false,
        )
        .unwrap()
    }

    /// The five report counters, as one comparable tuple.
    fn counts(stats: &ScanStats) -> (u64, u64, u64, u64, u64) {
        (
            stats.new,
            stats.updated,
            stats.moved,
            stats.missing,
            stats.skipped,
        )
    }

    /// Every row's provenance link, for the whole-table assertion that an
    /// observation re-stamped nothing.
    fn all_decision_ids(conn: &Connection) -> Vec<(i64, Option<i64>)> {
        conn.prepare("SELECT id, decision_id FROM sources ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// Renumber a column across every row — how a remount or a filesystem that
    /// synthesizes inodes per session looks from Canon's side.
    fn renumber(conn: &Connection, column: &str, by: i64) {
        conn.execute(&format!("UPDATE sources SET {column} = {column} + ?"), [by])
            .unwrap();
    }

    fn row(conn: &Connection, root_id: i64, rel_path: &str) -> Option<Source> {
        repo::source::fetch_by_path(conn, root_id, rel_path).unwrap()
    }

    /// A root directory under `base`, registered and ready to scan.
    fn root_at(conn: &Connection, base: &Path, name: &str) -> (i64, PathBuf) {
        let dir = base.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let id = repo::insert_test_root(conn, dir.to_str().unwrap(), "source", false);
        (id, dir)
    }

    fn write_at(dir: &Path, rel_path: &str, content: &str) -> PathBuf {
        let path = dir.join(rel_path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
        path
    }

    /// The headline invariant, asserted wherever a test has just changed state:
    /// scanning an unchanged disk reports nothing. Applied ambiently rather
    /// than in one dedicated test, because it must hold after every kind of
    /// mutation, not merely once.
    fn assert_second_scan_quiet(conn: &Connection, root_id: i64, dir: &Path) {
        let stats = scan_pass(conn, root_id, dir, Some(9_999)).stats;
        assert_eq!(
            (
                stats.new,
                stats.updated,
                stats.moved,
                stats.missing,
                stats.skipped
            ),
            (0, 0, 0, 0, 0),
            "a second scan of an unchanged disk must report nothing"
        );
    }

    /// Replace `path` the way an atomic-save editor does: write a sibling temp
    /// file and rename it over the path. The result is a new inode at a
    /// standing path — and, unlike delete-then-create, the old inode cannot be
    /// handed straight back, so the test's premise is guaranteed.
    fn atomic_save(path: &Path, content: &str) {
        let tmp = path.with_extension("tmp-save");
        std::fs::write(&tmp, content).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    fn inode_of(path: &Path) -> u64 {
        fs::metadata(path).unwrap().ino()
    }

    fn row_decision_id(conn: &Connection, source_id: i64) -> Option<i64> {
        conn.query_row(
            "SELECT decision_id FROM sources WHERE id = ?",
            [source_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn an_atomic_save_replacement_reads_as_updated() {
        // The report speaks in the user's terms: they edited one file, so the
        // scan says one file was updated — never "new", which would claim the
        // path had never been seen.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let notes = temp.path().join("notes.md");
        std::fs::write(&notes, "first draft").unwrap();

        let first = scan_pass(&conn, root_id, temp.path(), Some(1));
        assert_eq!(first.stats.new, 1);
        let source_id = repo::source::fetch_by_path(&conn, root_id, "notes.md")
            .unwrap()
            .unwrap()
            .id;
        let first_inode = inode_of(&notes);

        atomic_save(&notes, "second draft, rewritten");
        assert_ne!(inode_of(&notes), first_inode, "the editor left a new inode");

        let second = scan_pass(&conn, root_id, temp.path(), Some(2));
        assert_eq!(second.stats.updated, 1);
        assert_eq!(second.stats.new, 0);
        assert_eq!(second.stats.missing, 0);

        // The same row, carried through — not a new one beside a lost one.
        let after = repo::source::fetch_by_path(&conn, root_id, "notes.md")
            .unwrap()
            .unwrap();
        assert_eq!(after.id, source_id);
        assert_eq!(after.inode, inode_of(&notes) as i64);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_replacement_preserves_the_standing_decision_id() {
        // A scan observes; it does not act. The row keeps pointing at the
        // decision that last performed something on it, so story and trail keep
        // narrating the judgment rather than the observation that noticed it.
        // Asserted end to end, because what changed is which arm the pipeline
        // reaches: a path still routed to New would pass every arm-level guard
        // and stamp anyway.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let doc = temp.path().join("report.txt");
        std::fs::write(&doc, "original").unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(11));
        let source_id = repo::source::fetch_by_path(&conn, root_id, "report.txt")
            .unwrap()
            .unwrap()
            .id;
        // First indexing is a state transition of its own, and takes the stamp.
        assert_eq!(row_decision_id(&conn, source_id), Some(11));

        atomic_save(&doc, "rewritten entirely");
        let second = scan_pass(&conn, root_id, temp.path(), Some(22));
        assert_eq!(second.stats.updated, 1);
        assert_eq!(
            row_decision_id(&conn, source_id),
            Some(11),
            "the replacement observation must not overwrite the standing link"
        );

        assert_second_scan_quiet(&conn, root_id, temp.path());
        assert_eq!(
            row_decision_id(&conn, source_id),
            Some(11),
            "nor may a quiet scan"
        );
    }

    #[test]
    fn an_identical_content_recreation_reports_nothing() {
        // A restore or a dedup pass can hand back a byte-identical file under a
        // fresh inode. Nothing happened to the user's data, so nothing is
        // reported — the location metadata is refreshed and the row stands.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let restored = temp.path().join("restored.bin");
        std::fs::write(&restored, "identical bytes").unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let before = repo::source::fetch_by_path(&conn, root_id, "restored.bin")
            .unwrap()
            .unwrap();
        let original_mtime =
            filetime::FileTime::from_last_modification_time(&fs::metadata(&restored).unwrap());

        atomic_save(&restored, "identical bytes");
        filetime::set_file_mtime(&restored, original_mtime).unwrap();
        assert_ne!(inode_of(&restored), before.inode as u64);

        let second = scan_pass(&conn, root_id, temp.path(), Some(2));
        assert_eq!(
            (
                second.stats.new,
                second.stats.updated,
                second.stats.moved,
                second.stats.missing
            ),
            (0, 0, 0, 0),
            "recreating a file byte for byte is not an event"
        );
        assert_eq!(second.stats.unchanged, 1);

        // Refreshed silently: the row now carries where the file actually is.
        let after = repo::source::fetch_by_path(&conn, root_id, "restored.bin")
            .unwrap()
            .unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.inode, inode_of(&restored) as i64);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_head_read_is_taken_only_when_the_inode_moved() {
        // Reading every file's head on every scan would cost a whole-library
        // read; reading none would let a replacement pass as unchanged. The
        // line is the inode: it decides *whether to look*, never what the
        // answer is.
        //
        // Each row below carries a stored partial hash that disagrees with its
        // file, so the head read is observable by its verdict alone: where one
        // is taken the file reads as updated, and where none is taken the
        // fingerprint carries the row.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);

        let mut rows = Vec::new();
        for (name, stored_inode) in [
            ("steady.txt", None),         // the file's own inode: nothing moved
            ("untracked.txt", Some(0)),   // never tracked: nothing says it moved
            ("swapped.txt", Some(4_242)), // a different inode: look
        ] {
            let path = temp.path().join(name);
            std::fs::write(&path, format!("contents of {name}")).unwrap();
            let meta = fs::metadata(&path).unwrap();
            let inode = stored_inode.unwrap_or_else(|| meta.ino() as i64);
            rows.push(repo::insert_test_source(
                &conn,
                root_id,
                name,
                meta.dev() as i64,
                inode,
                meta.size() as i64,
                meta.mtime(),
            ));
        }

        let stats = scan_pass(&conn, root_id, temp.path(), Some(1)).stats;
        assert_eq!(stats.unchanged, 2, "no head read where the inode stood");
        assert_eq!(stats.updated, 1, "a head read where the inode moved");
        assert_eq!(stats.new, 0);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    // =========================================================================
    // Moves and per-path grain (scan_root, end to end)
    // =========================================================================

    #[test]
    fn a_remount_rescan_reports_nothing_and_restamps_nothing() {
        // The measured failure this whole story exists to kill: a NAS remount
        // renumbered the device and a scan reported tens of thousands of files
        // new and moved on a disk where nothing had happened — stamping
        // decision_id across the library, which story, trail and book then
        // narrate as judgment. The report must speak about the user's disk.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        for name in ["a.txt", "sub/b.txt", "sub/c.txt"] {
            write_at(temp.path(), name, &format!("contents of {name}"));
        }

        let first = scan_pass(&conn, root_id, temp.path(), Some(1)).stats;
        assert_eq!(first.new, 3);
        let provenance_before = all_decision_ids(&conn);

        renumber(&conn, "device", 7);

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(
            counts(&stats),
            (0, 0, 0, 0, 0),
            "a remount is an event about mounts, not about files"
        );
        assert_eq!(stats.unchanged, 3);
        assert_eq!(
            all_decision_ids(&conn),
            provenance_before,
            "no row's provenance link may move on a scan that observed nothing"
        );
    }

    #[test]
    fn an_inode_renumbering_rescan_reports_nothing() {
        // The harder half of the same failure: a filesystem that synthesizes
        // inodes afresh per session. Every stored inode is wrong at once, so
        // nomination is useless — the path holds the row, and the head read
        // confirms the content. Nothing defers, because no path is pathless.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        for name in ["a.txt", "sub/b.txt"] {
            write_at(temp.path(), name, &format!("contents of {name}"));
        }

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let provenance_before = all_decision_ids(&conn);

        renumber(&conn, "inode", 1_000_000);
        renumber(&conn, "device", 3);

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(counts(&stats), (0, 0, 0, 0, 0));
        assert_eq!(stats.unchanged, 2);
        assert_eq!(all_decision_ids(&conn), provenance_before);
    }

    #[test]
    fn a_new_hardlink_twin_is_new_never_moved() {
        // Disk truth, end to end. A second path onto the same inode is an
        // ordinary source sharing content — never a move, because the file the
        // row names is still exactly where it was. Getting this wrong is what
        // made ~27.9K twin paths churn as "moved" on every scan, forever.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "albums/trip.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let before = row(&conn, root_id, "albums/trip.jpg").unwrap();

        std::fs::create_dir_all(temp.path().join("by-year")).unwrap();
        std::fs::hard_link(&original, temp.path().join("by-year/trip.jpg")).unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(stats.new, 1, "the twin path is a source of its own");
        assert_eq!(stats.moved, 0, "and nothing moved");
        assert_eq!(stats.missing, 0);
        assert_eq!(stats.hardlink_companions, 1, "counted, never silent");

        let after = row(&conn, root_id, "albums/trip.jpg").unwrap();
        assert_eq!(
            (after.id, after.inode, after.basis_rev, after.decision_id),
            (
                before.id,
                before.inode,
                before.basis_rev,
                before.decision_id
            ),
            "the twin's arrival left the original's row untouched"
        );

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_genuine_move_is_reported_once() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "inbox/photo.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let before = row(&conn, root_id, "inbox/photo.jpg").unwrap();

        std::fs::create_dir_all(temp.path().join("sorted")).unwrap();
        std::fs::rename(&original, temp.path().join("sorted/photo.jpg")).unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(counts(&stats), (0, 0, 1, 0, 0), "one move, nothing else");

        let after = row(&conn, root_id, "sorted/photo.jpg").unwrap();
        assert_eq!(after.id, before.id, "the same row, at its new path");
        assert!(row(&conn, root_id, "inbox/photo.jpg").is_none());

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_moved_and_modified_file_degrades_to_new_and_missing() {
        // Both the path and the content changed, so no trusted evidence ties
        // the two together — inode agreement alone is exactly the guess this
        // story refuses. Conservative degradation: two rows, both truthful, and
        // the deletion receipt keeps the old row's provenance reachable.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "drafts/doc.txt", "first draft");

        scan_pass(&conn, root_id, temp.path(), Some(1));

        let moved_to = temp.path().join("final/doc.txt");
        std::fs::create_dir_all(moved_to.parent().unwrap()).unwrap();
        std::fs::rename(&original, &moved_to).unwrap();
        std::fs::write(&moved_to, "a thoroughly rewritten document").unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(stats.new, 1);
        assert_eq!(stats.missing, 1);
        assert_eq!(stats.moved, 0, "a guess is not a move");

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn twin_sequencing_is_order_independent() {
        // Which twin the walker reaches first must not change what the scan
        // decides. Forward and reversed entry orders over identical fixtures
        // must land in identical end states.
        /// Every row's shape, plus the report — the whole observable end state.
        type EndState = (Vec<(String, i64, i64)>, (u64, u64, u64, u64, u64));

        let end_state = |reverse: bool| -> EndState {
            let conn = repo::open_in_memory_for_test();
            let temp = TempDir::new().unwrap();
            let root_id =
                repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
            let original = write_at(temp.path(), "albums/trip.jpg", "photo bytes");
            scan_pass(&conn, root_id, temp.path(), Some(1));

            std::fs::create_dir_all(temp.path().join("by-year")).unwrap();
            std::fs::hard_link(&original, temp.path().join("by-year/trip.jpg")).unwrap();

            let mut entries: Vec<_> = walk(temp.path()).collect();
            if reverse {
                entries.reverse();
            }
            let stats =
                scan_pass_with(&conn, root_id, temp.path(), entries.into_iter(), Some(2)).stats;

            let rows = conn
                .prepare("SELECT rel_path, present, size FROM sources ORDER BY rel_path")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            (rows, counts(&stats))
        };

        assert_eq!(end_state(false), end_state(true));
    }

    #[test]
    fn a_scoped_scan_sees_a_move_into_its_scope() {
        // The old-path check is a point stat, not a walk requirement, so a scan
        // narrowed to the destination still recognises the move — it asks the
        // disk about one path rather than needing to have walked it.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let original = write_at(temp.path(), "a/x.jpg", "content");
        scan_pass(&conn, root_id, temp.path(), Some(1));
        let before = row(&conn, root_id, "a/x.jpg").unwrap();

        let destination = temp.path().join("b");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::rename(&original, destination.join("x.jpg")).unwrap();

        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("b"),
            walk(&destination),
            &no_hash_options(),
            &NoopProgress,
            current_timestamp(),
            Some(2),
            false,
        )
        .unwrap();

        assert_eq!(result.stats.moved, 1);
        assert_eq!(result.stats.new, 0);
        assert_eq!(row(&conn, root_id, "b/x.jpg").unwrap().id, before.id);
    }

    #[test]
    fn an_unverifiable_nomination_is_counted_and_its_root_named() {
        // The root holding the nominated row is gone, so Canon cannot tell
        // whether the old file left or is merely out of view. Ignorance never
        // claims — and it is stated, with the root that could not answer, so
        // "new" never passes silently for "not moved".
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (detached_id, detached) = root_at(&conn, temp.path(), "detached");
        let (live_id, live) = root_at(&conn, temp.path(), "live");

        let parked = write_at(&detached, "parked.jpg", "photo bytes");
        scan_pass(&conn, detached_id, &detached, Some(1));

        // The file reappears in the live root, and the root it came from is
        // no longer there to be asked.
        std::fs::rename(&parked, live.join("arrived.jpg")).unwrap();
        std::fs::remove_dir_all(&detached).unwrap();

        let result = scan_pass(&conn, live_id, &live, Some(2));
        assert_eq!(result.stats.new, 1);
        assert_eq!(result.stats.moved, 0);
        assert_eq!(result.stats.moves_unverified, 1);
        assert!(
            result.warnings.iter().any(|w| {
                w.contains("could not be verified") && w.contains(detached.to_str().unwrap())
            }),
            "the warning names the root that could not answer: {:?}",
            result.warnings
        );

        // The unreachable root's row is untouched — nothing was claimed from it.
        assert!(row(&conn, detached_id, "parked.jpg").is_some());
    }

    #[test]
    fn deleting_one_companion_touches_only_its_own_row() {
        // Per-path grain, in the direction that matters most: twins are
        // ordinary sources, so removing one path is one deletion and the
        // sibling that still exists is left entirely alone.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "albums/trip.jpg", "photo bytes");
        std::fs::create_dir_all(temp.path().join("by-year")).unwrap();
        let twin = temp.path().join("by-year/trip.jpg");
        std::fs::hard_link(&original, &twin).unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let kept_before = row(&conn, root_id, "albums/trip.jpg").unwrap();

        std::fs::remove_file(&twin).unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(stats.missing, 1);
        assert_eq!(stats.new, 0);
        assert_eq!(stats.moved, 0);

        assert!(row(&conn, root_id, "by-year/trip.jpg").is_none());
        let kept_after = row(&conn, root_id, "albums/trip.jpg").unwrap();
        assert_eq!(
            (kept_after.id, kept_after.decision_id, kept_after.basis_rev),
            (
                kept_before.id,
                kept_before.decision_id,
                kept_before.basis_rev
            )
        );

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn inode_reuse_never_relocates_a_row() {
        // An inode number returns after a delete and names different content.
        // The nomination is real and worthless: corroboration refuses, the new
        // path is new, the old row goes missing at its own path — and that path
        // is never rewritten, which is the damage a false move would do.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);

        let arrived = write_at(temp.path(), "arrived.bin", "brand new content here");
        let inode = inode_of(&arrived) as i64;
        // A row that once held this inode, at a path nothing stands at now. It
        // carries the root's real device, so the old-path check reaches a
        // genuine Vacated and the refusal below is corroboration's, not the
        // storage check's — the two arms reach the same verdict by different
        // routes, and this test is about the second one.
        let device = get_dir_device(temp.path()).unwrap();
        let recycled =
            repo::insert_test_source(&conn, root_id, "gone.bin", device, inode, 999_999, 1);

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(stats.new, 1);
        assert_eq!(stats.moved, 0);
        assert_eq!(stats.missing, 1);

        let (rel_path, present): (String, i64) = conn
            .query_row(
                "SELECT rel_path, present FROM sources WHERE id = ?",
                [recycled],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            rel_path, "gone.bin",
            "the old row's path is never rewritten"
        );
        assert_eq!(present, 0);
    }

    #[test]
    fn a_candidate_mutated_before_resolution_degrades_to_new() {
        // The third gate. Canon is a several-processes-at-once tool: between
        // the walk nominating a row and resolution claiming it, another scan
        // may have taken it. The re-check runs inside the write transaction,
        // and a row that is no longer as nominated is not claimed at all.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "inbox/photo.jpg", "photo bytes");
        scan_pass(&conn, root_id, temp.path(), Some(1));
        let claimed_id = row(&conn, root_id, "inbox/photo.jpg").unwrap().id;

        std::fs::create_dir_all(temp.path().join("sorted")).unwrap();
        std::fs::rename(&original, temp.path().join("sorted/photo.jpg")).unwrap();

        // A harmless trailing entry (a non-empty directory, which the walk
        // skips) whose *yielding* mutates the nominated row — after the walk
        // has deferred the move, before resolution decides it.
        let trailing = walk(&temp.path().join("sorted")).next().unwrap();
        let entries = walk(temp.path()).chain(std::iter::once_with(|| {
            conn.execute(
                "UPDATE sources SET inode = inode + 1 WHERE id = ?",
                [claimed_id],
            )
            .unwrap();
            trailing
        }));

        let stats = scan_pass_with(&conn, root_id, temp.path(), entries, Some(2)).stats;
        assert_eq!(stats.moved, 0, "a row that changed under us is not claimed");
        assert_eq!(stats.new, 1);
        assert_eq!(stats.missing, 1);
    }

    #[test]
    fn a_claimed_row_is_seen_and_the_unclaimed_one_goes_missing() {
        // Two rows corroborate one observation, and a row can be claimed only
        // once. The winner is seen — never reported deleted and re-found — and
        // the loser falls through to ordinary missing detection.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let first = write_at(temp.path(), "a.jpg", "photo bytes");
        let second = temp.path().join("b.jpg");
        std::fs::hard_link(&first, &second).unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(1));
        // Insertion order is the walker's, not alphabetical — the rule is
        // "lowest id", so the test asks which that is rather than assuming.
        let a_id = row(&conn, root_id, "a.jpg").unwrap().id;
        let b_id = row(&conn, root_id, "b.jpg").unwrap().id;

        // One twin renamed, the other removed: both rows are now vacated, and
        // both corroborate the single file that remains.
        std::fs::rename(&first, temp.path().join("c.jpg")).unwrap();
        std::fs::remove_file(&second).unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(stats.moved, 1);
        assert_eq!(stats.missing, 1);
        assert_eq!(stats.new, 0);
        assert_eq!(
            row(&conn, root_id, "c.jpg").unwrap().id,
            a_id.min(b_id),
            "the lowest-id candidate wins a tie, deterministically"
        );

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_whole_group_move_pairs_each_row_to_its_own_path() {
        // Two hardlinked files renamed together: every observation corroborates
        // every candidate, because they are literally the same content. Only
        // the pairing rule can decide, and it must give each row back its own
        // filename rather than crossing them.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let a = write_at(temp.path(), "old/a.jpg", "shared bytes");
        std::fs::hard_link(&a, temp.path().join("old/b.jpg")).unwrap();

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let a_id = row(&conn, root_id, "old/a.jpg").unwrap().id;
        let b_id = row(&conn, root_id, "old/b.jpg").unwrap().id;

        std::fs::rename(temp.path().join("old"), temp.path().join("new")).unwrap();

        let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
        assert_eq!(counts(&stats), (0, 0, 2, 0, 0), "two moves, nothing else");
        assert_eq!(row(&conn, root_id, "new/a.jpg").unwrap().id, a_id);
        assert_eq!(row(&conn, root_id, "new/b.jpg").unwrap().id, b_id);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_root_whose_storage_is_not_mounted_never_claims_a_move() {
        // A mountpoint with nothing behind it is still a directory: it answers,
        // it is empty, and every file ever recorded under it reads as gone. That
        // is not absence — it is Canon looking at the wrong storage. Without the
        // device check the whole root's content would be permanently claimable
        // by any file that happened to collide on an inode number and agree on
        // content, which an rsync copy of that very root supplies wholesale.
        //
        // The shell is simulated by its signature rather than by unmounting: a
        // root directory that exists but whose device is not the one the row was
        // recorded on. The control below is the identical fixture with the row's
        // device matching the directory's, and it moves — so the refusal is the
        // storage mismatch alone, not the fixture.
        let outcome = |device_matches: bool| -> (u64, u64, u64) {
            let conn = repo::open_in_memory_for_test();
            let temp = TempDir::new().unwrap();
            let (parked_id, parked_dir) = root_at(&conn, temp.path(), "parked");
            let (live_id, live_dir) = root_at(&conn, temp.path(), "live");

            let file = write_at(&parked_dir, "photo.jpg", "photo bytes");
            scan_pass(&conn, parked_id, &parked_dir, Some(1));
            std::fs::rename(&file, live_dir.join("photo.jpg")).unwrap();

            if !device_matches {
                // The volume went away; the mountpoint directory did not.
                conn.execute(
                    "UPDATE sources SET device = device + 1 WHERE root_id = ?",
                    [parked_id],
                )
                .unwrap();
            }

            let stats = scan_pass(&conn, live_id, &live_dir, Some(2)).stats;
            (stats.new, stats.moved, stats.moves_unverified)
        };

        assert_eq!(
            outcome(false),
            (1, 0, 1),
            "no claim against storage Canon cannot confirm it is looking at, and the doubt is stated"
        );
        assert_eq!(
            outcome(true),
            (0, 1, 0),
            "the same storage, and the move is followed"
        );
    }

    #[test]
    fn a_suspended_roots_row_is_never_relocated() {
        // Suspension is the user closing the door on a root: everything inside
        // keeps exactly the standing it had, and a scan of some *other* root may
        // not reach in and relocate a row out of it. The control below runs the
        // identical fixture unsuspended and gets the move, so what refuses here
        // is the closed door and nothing else.
        let outcome = |suspended: bool| -> (u64, u64, u64, bool) {
            let conn = repo::open_in_memory_for_test();
            let temp = TempDir::new().unwrap();
            let (parked_id, parked_dir) = root_at(&conn, temp.path(), "parked");
            let (live_id, live_dir) = root_at(&conn, temp.path(), "live");

            let file = write_at(&parked_dir, "photo.jpg", "photo bytes");
            scan_pass(&conn, parked_id, &parked_dir, Some(1));
            let parked_row = row(&conn, parked_id, "photo.jpg").unwrap().id;

            std::fs::rename(&file, live_dir.join("photo.jpg")).unwrap();
            if suspended {
                conn.execute("UPDATE roots SET suspended = 1 WHERE id = ?", [parked_id])
                    .unwrap();
            }

            let stats = scan_pass(&conn, live_id, &live_dir, Some(2)).stats;
            // Followed by id: in the control the row legitimately changes root,
            // which is the difference the test is about.
            let still_parked: bool = conn
                .query_row(
                    "SELECT root_id = ? AND rel_path = 'photo.jpg' FROM sources WHERE id = ?",
                    [parked_id, parked_row],
                    |r| r.get(0),
                )
                .unwrap();
            (stats.new, stats.moved, stats.moves_unverified, still_parked)
        };

        assert_eq!(
            outcome(true),
            (1, 0, 0, true),
            "the parked root's row stands where it stood, and the arrival is simply new"
        );
        assert_eq!(
            outcome(false),
            (0, 1, 0, false),
            "unsuspended, the identical fixture is a move — so suspension is what refused"
        );
    }

    #[test]
    fn an_empty_file_never_takes_another_rows_path() {
        // The contentless law on the live move path, not merely in the
        // predicate. Every empty file's content agrees with every other's, so a
        // vacated empty row would corroborate any empty file appearing
        // anywhere — and a relocation is a claim about where content *was*.
        // Refused, so the arriving path is new and the vacated row goes missing
        // at its own path, which is what actually happened.
        //
        // The control below is the same fixture with one byte in each file, and
        // it moves — so the refusal is the emptiness and nothing else.
        let outcome = |bytes: &str| -> (u64, u64, u64, String) {
            let conn = repo::open_in_memory_for_test();
            let temp = TempDir::new().unwrap();
            let root_id =
                repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
            let original = write_at(temp.path(), "was/here.log", bytes);
            scan_pass(&conn, root_id, temp.path(), Some(1));
            let source_id = row(&conn, root_id, "was/here.log").unwrap().id;

            std::fs::create_dir_all(temp.path().join("now")).unwrap();
            std::fs::rename(&original, temp.path().join("now/here.log")).unwrap();

            let stats = scan_pass(&conn, root_id, temp.path(), Some(2)).stats;
            let rel_path: String = conn
                .query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |r| r.get(0),
                )
                .unwrap();
            (stats.new, stats.moved, stats.missing, rel_path)
        };

        assert_eq!(
            outcome(""),
            (1, 0, 1, "was/here.log".to_string()),
            "no empty file earns a relocation, and the old row keeps its path"
        );
        assert_eq!(
            outcome("x"),
            (0, 1, 0, "now/here.log".to_string()),
            "one byte of content is enough to say which file this is"
        );
    }

    #[test]
    fn a_move_carries_the_previous_object_link_to_the_hash_pass() {
        // Moved is the arm that *keeps* its object link: the content did not
        // change, only where it sits. The hash pass reads the link it came in
        // with to spot content changing under an unchanged fingerprint, so the
        // link has to survive the trip through end-of-walk resolution — which
        // is where a move is now decided, well after the walk that saw it.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "inbox/photo.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let source_id = row(&conn, root_id, "inbox/photo.jpg").unwrap().id;
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', 'known')",
            [],
        )
        .unwrap();
        let object_id = conn.last_insert_rowid();
        conn.execute(
            "UPDATE sources SET object_id = ? WHERE id = ?",
            [object_id, source_id],
        )
        .unwrap();

        std::fs::create_dir_all(temp.path().join("sorted")).unwrap();
        std::fs::rename(&original, temp.path().join("sorted/photo.jpg")).unwrap();

        let result = scan_root(
            &conn,
            root_id,
            temp.path().to_str().unwrap(),
            None,
            walk(temp.path()),
            &ScanOptions {
                hash: true,
                hash_all: true,
                ignore_device_id: false,
            },
            &NoopProgress,
            current_timestamp(),
            Some(2),
            false,
        )
        .unwrap();

        assert_eq!(result.stats.moved, 1);
        let queued: Vec<_> = result
            .files_to_hash
            .iter()
            .filter(|f| f.source_id == source_id)
            .collect();
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].old_object_id,
            Some(object_id),
            "the link the row came in with reaches the hash pass"
        );
        assert_eq!(
            queued[0].need,
            HashNeed::Reverify,
            "a move is not a content change, so an altered hash here is unexpected"
        );
    }

    #[test]
    fn a_move_still_resolves_when_the_walk_could_not_finish() {
        // Missing detection is gated on a complete, stable walk because absence
        // there is *inferred* — unseen must never read as deleted. A move is
        // not inferred at any step: the file was observed and read, its old
        // path was checked on disk, the content corroborated, and the row
        // re-checked at write time. So resolution deliberately runs above that
        // gate, and a row follows its file even on a walk that could not see
        // everything — while the same walk still refuses to call anything gone.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "inbox/photo.jpg", "photo bytes");
        write_at(temp.path(), "other/keep.txt", "kept");
        scan_pass(&conn, root_id, temp.path(), Some(1));
        let source_id = row(&conn, root_id, "inbox/photo.jpg").unwrap().id;

        std::fs::create_dir_all(temp.path().join("sorted")).unwrap();
        std::fs::rename(&original, temp.path().join("sorted/photo.jpg")).unwrap();
        // The file this walk never reaches, and must not conclude anything about.
        std::fs::remove_file(temp.path().join("other/keep.txt")).unwrap();

        let broken = WalkDir::new(temp.path().join("no-such-dir"))
            .into_iter()
            .next()
            .unwrap()
            .unwrap_err();
        let entries = walk(temp.path()).chain(std::iter::once(Err(broken)));

        let result = scan_pass_with(&conn, root_id, temp.path(), entries, Some(2));

        assert_eq!(result.stats.walk_errors, 1);
        assert_eq!(result.stats.moved, 1, "positive evidence still decides");
        assert_eq!(
            result.stats.missing, 0,
            "and inferred absence still does not"
        );
        assert_eq!(
            row(&conn, root_id, "sorted/photo.jpg").unwrap().id,
            source_id
        );
    }

    // =========================================================================
    // Hash debt — content Canon has never identified
    // =========================================================================

    use crate::scan::ops::hash::{hash_files, HashProgress, HashStats};
    use crate::scan::repo::source::count_unhashed;

    struct NoopHashProgress;
    impl HashProgress for NoopHashProgress {
        fn on_start(&self, _: usize) {}
        fn on_hash(&self, _: usize, _: &Path) {}
        fn on_hash_error(&self, _: &Path, _: &str) {}
        fn on_unexpected_change(&self, _: &Path) {}
        fn on_finish(&self) {}
    }

    fn hash_options() -> ScanOptions {
        ScanOptions {
            hash: true,
            hash_all: false,
            ignore_device_id: false,
        }
    }

    /// A pass with hashing on, as a plain `canon scan` runs it.
    fn plain_scan(
        conn: &Connection,
        root_id: i64,
        dir: &Path,
        decision_id: Option<i64>,
    ) -> ScanRootResult {
        scan_root(
            conn,
            root_id,
            dir.to_str().unwrap(),
            None,
            walk(dir),
            &hash_options(),
            &NoopProgress,
            current_timestamp(),
            decision_id,
            false,
        )
        .unwrap()
    }

    /// Run the hash pass over what a walk queued, the way the interface does
    /// once every root has been walked.
    fn pay(conn: &Connection, result: &ScanRootResult) -> HashStats {
        hash_files(conn, &result.files_to_hash, &NoopHashProgress).unwrap()
    }

    fn object_of(conn: &Connection, root_id: i64, rel_path: &str) -> Option<i64> {
        row(conn, root_id, rel_path).unwrap().object_id
    }

    #[test]
    fn the_walk_has_exactly_one_place_that_queues_a_file_for_hashing() {
        // The behavioral guards below prove that both *existing* gate sites ask
        // the one owner. They cannot prove that a third site is never added:
        // a third direct push onto the queue vector, anywhere in this walk,
        // would compile, pass every test in this file, and quietly reintroduce
        // a second spelling of both the gate and the basis question. The queue is
        // claim-bearing — it decides what content Canon ever identifies — so
        // the one-definition-site property is pinned here rather than left to
        // the next reader's memory.
        let source = include_str!("pipeline.rs");
        // Assembled rather than written out, so this test's own text is not one
        // of the occurrences it counts.
        let queue_site = ["files_to_hash", ".push("].concat();
        assert_eq!(
            source.matches(&queue_site).count(),
            1,
            "queue a file through queue_for_hash, which asks needs_hash — never \
             by pushing onto files_to_hash directly"
        );
    }

    #[test]
    fn the_hash_gate_asks_what_is_needed_not_what_happened() {
        // The gate's whole table, in one place. The row that matters is
        // Unchanged with no object under a plain scan: nothing happened to the
        // file, and it still must be hashed, because Canon has never read it.
        let plain = hash_options();
        let verify = ScanOptions {
            hash_all: true,
            ..hash_options()
        };
        let off = no_hash_options();
        let known = Some(7);

        // Content evidence changed (or the path is newly indexed): always.
        assert_eq!(
            needs_hash(&FileAction::New, &plain, None),
            Some(HashNeed::Basis)
        );
        assert_eq!(
            needs_hash(&FileAction::Modified, &plain, known),
            Some(HashNeed::Basis)
        );

        // Nothing changed and the content is known: nothing to do...
        assert_eq!(needs_hash(&FileAction::Unchanged, &plain, known), None);
        assert_eq!(needs_hash(&FileAction::Moved, &plain, known), None);

        // ...but nothing changed and the content was never read is debt, on
        // either arm — a standing row and a relocated one carry it alike.
        assert_eq!(
            needs_hash(&FileAction::Unchanged, &plain, None),
            Some(HashNeed::Backlog)
        );
        assert_eq!(
            needs_hash(&FileAction::Moved, &plain, None),
            Some(HashNeed::Backlog)
        );

        // --verify re-reads everything, and says so: a row it re-reads is not
        // reported as a backlog pay-down, because the user asked for the pass.
        assert_eq!(
            needs_hash(&FileAction::Unchanged, &verify, known),
            Some(HashNeed::Reverify)
        );
        assert_eq!(
            needs_hash(&FileAction::Unchanged, &verify, None),
            Some(HashNeed::Reverify)
        );

        // --no-hash queues nothing at all, debt included: it opts out of paying,
        // never out of being told (the summary states what stands).
        assert_eq!(needs_hash(&FileAction::New, &off, None), None);
        assert_eq!(needs_hash(&FileAction::Unchanged, &off, None), None);
    }

    #[test]
    fn a_source_indexed_without_hashing_is_hashed_by_the_next_plain_scan() {
        // The hole this closes, in its own shape: a file indexed in January
        // under --no-hash sat through a clean full scan in August still
        // unhashed, because every scan after the first found it unchanged and
        // an action-driven gate had nothing to say about it. It surfaced months
        // later, when an archive operation could not see the content at all.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        write_at(temp.path(), "photos/holiday.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        assert_eq!(object_of(&conn, root_id, "photos/holiday.jpg"), None);
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 1);

        // The next plain scan finds nothing about the file changed — and reads
        // it anyway, counting it as debt rather than as work the walk caused.
        let result = plain_scan(&conn, root_id, temp.path(), Some(2));
        assert_eq!(result.stats.unchanged, 1);
        assert_eq!(result.stats.new, 0);
        assert_eq!(result.files_to_hash.len(), 1, "the debt is queued");
        assert_eq!(
            result.files_to_hash[0].need,
            HashNeed::Backlog,
            "and queued as debt, not as work the walk caused"
        );
        let paid = pay(&conn, &result);
        assert_eq!((paid.hashed, paid.backlog_hashed), (1, 1));

        assert!(object_of(&conn, root_id, "photos/holiday.jpg").is_some());
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 0);

        // And the debt does not come back: the scan after the pay-down queues
        // nothing.
        let settled = plain_scan(&conn, root_id, temp.path(), Some(3));
        assert!(settled.files_to_hash.is_empty());
        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_debt_carrying_row_that_moved_is_queued_at_the_resolution_gate() {
        // The gate is asked twice — once in the walk, once after it, where a
        // move is decided — and an edit that changes only the first leaves debt
        // standing for every file that happened to move. Nothing about this
        // file's content changed, so only the debt rule can queue it.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        let original = write_at(temp.path(), "inbox/photo.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let source_id = row(&conn, root_id, "inbox/photo.jpg").unwrap().id;
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 1);

        std::fs::create_dir_all(temp.path().join("sorted")).unwrap();
        std::fs::rename(&original, temp.path().join("sorted/photo.jpg")).unwrap();

        let result = plain_scan(&conn, root_id, temp.path(), Some(2));

        assert_eq!(result.stats.moved, 1);
        let queued: Vec<_> = result
            .files_to_hash
            .iter()
            .filter(|f| f.source_id == source_id)
            .collect();
        assert_eq!(
            queued.len(),
            1,
            "the second gate queues debt too, or a moved file's debt is permanent"
        );
        assert_eq!(queued[0].old_object_id, None);
        assert_eq!(
            queued[0].need,
            HashNeed::Backlog,
            "a move is not a content change — the file was merely never read"
        );

        let paid = pay(&conn, &result);
        assert_eq!((paid.hashed, paid.backlog_hashed), (1, 1));
        assert!(object_of(&conn, root_id, "sorted/photo.jpg").is_some());
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 0);
        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_file_that_could_not_be_hashed_is_queued_again_by_the_next_scan() {
        // A hash error leaves the row exactly as it was: present, unlinked, in
        // debt. Nothing marks it as tried-and-failed, and nothing needs to —
        // the next plain scan asks the same question and gets the same answer.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        write_at(temp.path(), "photo.jpg", "photo bytes");

        let first = plain_scan(&conn, root_id, temp.path(), Some(1));
        assert_eq!(first.files_to_hash.len(), 1);

        // The hash pass reads the file after the walk has moved on, so it can
        // find it gone, locked, or unreadable. Point the pass at a path that
        // isn't there, leaving the disk itself untouched: the file is still
        // where the walk saw it when the next scan runs.
        let vanished = vec![FileToHash {
            source_id: first.files_to_hash[0].source_id,
            full_path: temp.path().join("no-such-file.jpg"),
            old_object_id: None,
            need: HashNeed::Basis,
        }];
        let failed = hash_files(&conn, &vanished, &NoopHashProgress).unwrap();
        assert_eq!((failed.errors, failed.hashed), (1, 0));
        assert_eq!(object_of(&conn, root_id, "photo.jpg"), None);
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 1);

        // The retry needs no repair command and no flag: it is the ordinary
        // next scan, which finds the file unchanged and the debt standing.
        let retry = plain_scan(&conn, root_id, temp.path(), Some(2));
        assert_eq!(retry.stats.unchanged, 1);
        assert_eq!(retry.files_to_hash.len(), 1, "the debt is queued again");
        assert_eq!(retry.files_to_hash[0].need, HashNeed::Backlog);
        let paid = pay(&conn, &retry);
        assert_eq!((paid.hashed, paid.backlog_hashed), (1, 1));
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 0);
    }

    #[test]
    fn paying_debt_never_reads_as_content_changing_under_canon() {
        // The unexpected-change detector fires when a file's hash differs from
        // the object it was linked to while nothing about its basis explains
        // it — possible corruption, and the scan exits non-zero for it. A debt
        // row was never linked to anything, so there is nothing to differ from:
        // the pay-down must be silent, or every backlog scan cries corruption.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        write_at(temp.path(), "photo.jpg", "photo bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        let result = plain_scan(&conn, root_id, temp.path(), Some(2));

        assert_eq!(result.files_to_hash.len(), 1, "the debt is queued");
        let queued = &result.files_to_hash[0];
        assert_eq!(
            (&queued.need, queued.old_object_id),
            (&HashNeed::Backlog, None),
            "debt carries no predecessor link, which is what keeps the detector quiet"
        );
        // Documentation, not a guard: with no predecessor link the detector
        // cannot fire whatever else breaks, so this line must never be counted
        // toward a rung's expected failure set. The tuple above is the
        // falsifiable half.
        assert_eq!(pay(&conn, &result).unexpected_hash_changes, 0);
    }

    #[test]
    fn verify_still_rereads_a_file_whose_content_is_already_known() {
        // --verify keeps its own meaning: re-read everything, debt or not. Its
        // rows are not counted as backlog — the user asked for the pass, so
        // there is no surprising pay-down to explain.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        write_at(temp.path(), "photo.jpg", "photo bytes");

        let first = plain_scan(&conn, root_id, temp.path(), Some(1));
        pay(&conn, &first);
        let object_id = object_of(&conn, root_id, "photo.jpg");
        assert!(object_id.is_some());

        let verified = scan_root(
            &conn,
            root_id,
            temp.path().to_str().unwrap(),
            None,
            walk(temp.path()),
            &ScanOptions {
                hash: true,
                hash_all: true,
                ignore_device_id: false,
            },
            &NoopProgress,
            current_timestamp(),
            Some(2),
            false,
        )
        .unwrap();

        assert_eq!(verified.stats.unchanged, 1);
        assert_eq!(verified.files_to_hash.len(), 1, "--verify re-reads it");
        assert_eq!(verified.files_to_hash[0].old_object_id, object_id);
        assert_eq!(
            verified.files_to_hash[0].need,
            HashNeed::Reverify,
            "asked for, not owed — a re-read is never reported as a backlog pay-down"
        );
        assert_eq!(pay(&conn, &verified).backlog_hashed, 0);
    }

    #[test]
    fn a_scoped_scan_pays_only_the_debt_inside_its_scope() {
        // A scan speaks about the scope it walked. Debt outside it is neither
        // paid nor claimed to be paid — it simply wasn't this scan's subject.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        write_at(temp.path(), "vacation/a.jpg", "a bytes");
        write_at(temp.path(), "other/b.jpg", "b bytes");

        scan_pass(&conn, root_id, temp.path(), Some(1));
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 2);

        let scoped = scan_root(
            &conn,
            root_id,
            temp.path().to_str().unwrap(),
            Some("vacation"),
            walk(&temp.path().join("vacation")),
            &hash_options(),
            &NoopProgress,
            current_timestamp(),
            Some(2),
            false,
        )
        .unwrap();

        assert_eq!(
            scoped.files_to_hash.len(),
            1,
            "only the debt inside the walked scope is queued"
        );
        assert_eq!(scoped.files_to_hash[0].need, HashNeed::Backlog);
        let paid = pay(&conn, &scoped);
        assert_eq!((paid.hashed, paid.backlog_hashed), (1, 1));
        assert!(object_of(&conn, root_id, "vacation/a.jpg").is_some());
        assert_eq!(
            object_of(&conn, root_id, "other/b.jpg"),
            None,
            "content outside the walked scope is untouched"
        );
        assert_eq!(count_unhashed(&conn, root_id, Some("vacation")).unwrap(), 0);
        assert_eq!(count_unhashed(&conn, root_id, None).unwrap(), 1);
    }

    // =========================================================================
    // Convergence — the first scan after per-path grain arrives
    // =========================================================================

    /// Seed one row for a path, the way the old grain left the database: one
    /// row per hardlink group, every other path in the group unrepresented.
    fn seed_old_grain(conn: &Connection, root_id: i64, dir: &Path, rel_path: &str) -> i64 {
        let full = dir.join(rel_path);
        let meta = fs::metadata(&full).unwrap();
        let partial_hash = compute_partial_hash(&full, meta.size()).unwrap();
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, decision_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 1, 77)",
            rusqlite::params![
                root_id,
                rel_path,
                meta.dev() as i64,
                meta.ino() as i64,
                meta.size() as i64,
                meta.mtime(),
                partial_hash,
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// A hardlink group: one real file and `twins` further paths onto it.
    fn hardlink_group(dir: &Path, stem: &str, twins: usize) -> PathBuf {
        let original = write_at(
            dir,
            &format!("originals/{stem}.jpg"),
            &format!("bytes {stem}"),
        );
        for i in 0..twins {
            let twin = dir.join(format!("links/{i}/{stem}.jpg"));
            std::fs::create_dir_all(twin.parent().unwrap()).unwrap();
            std::fs::hard_link(&original, &twin).unwrap();
        }
        original
    }

    #[test]
    fn convergence_indexes_every_twin_path_and_then_goes_quiet() {
        // The migration story, which is deliberately not a migration: the first
        // scan after per-path grain arrives finds every twin path unrepresented
        // and indexes it, annotating why. The second scan reports nothing. Done
        // when quiet — a property the user can check for themselves.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);

        hardlink_group(temp.path(), "one", 2);
        hardlink_group(temp.path(), "two", 1);
        let grain_one = seed_old_grain(&conn, root_id, temp.path(), "originals/one.jpg");
        let grain_two = seed_old_grain(&conn, root_id, temp.path(), "originals/two.jpg");

        let result = scan_pass(&conn, root_id, temp.path(), Some(5));
        assert_eq!(result.stats.new, 3, "the three unrepresented twin paths");
        assert_eq!(
            result.stats.hardlink_companions, 3,
            "and each is explained — counted per file, so it can never exceed the new count"
        );
        assert_eq!(result.stats.moved, 0, "no row was relocated to reach them");
        assert_eq!(result.stats.missing, 0);
        assert_eq!(result.stats.unchanged, 2);
        assert!(result
            .stats
            .compose_summary()
            .contains("hardlink companions of already-indexed files"));

        // Provenance-preserving: the rows that already existed still point at
        // the decisions that made them, not at the convergence scan.
        for grain in [grain_one, grain_two] {
            assert_eq!(row_decision_id(&conn, grain), Some(77));
        }

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn an_interrupted_convergence_finishes_on_the_next_scan() {
        // Convergence is ordinary scanning, so an interruption needs no repair
        // command: whatever was indexed stays indexed, and the next scan picks
        // up the rest. Fix-forward, and the end state is the same either way.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);
        hardlink_group(temp.path(), "one", 3);
        seed_old_grain(&conn, root_id, temp.path(), "originals/one.jpg");

        // A scan that stopped early: some entries processed, then the walk
        // could not continue. Missing detection is gated off by the error, which
        // is exactly right — an incomplete walk has no evidence of absence.
        let broken = WalkDir::new(temp.path().join("no-such-dir"))
            .into_iter()
            .next()
            .unwrap()
            .unwrap_err();
        let partial = walk(temp.path())
            .take(4)
            .chain(std::iter::once(Err(broken)));
        let interrupted = scan_pass_with(&conn, root_id, temp.path(), partial, Some(5));
        assert_eq!(interrupted.stats.missing, 0);
        assert_eq!(interrupted.stats.walk_errors, 1);

        // The next ordinary scan finishes the job.
        scan_pass(&conn, root_id, temp.path(), Some(6));

        let indexed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexed, 4, "one original and its three twin paths");

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn a_large_convergence_never_defers_a_single_file() {
        // The scale claim, and the reason convergence costs nothing beyond the
        // walk: during a flood every nominated row is still on disk, so the
        // companion fast path takes all of it and the deferred set stays empty.
        // Deferral is memory held across a whole walk; a flood that deferred
        // would hold the library.
        //
        // The deferred set is internal, so it is observed by its consequences:
        // deferral is the only route to `moved`, and every one of these paths is
        // reported new and counted as a companion.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_id = repo::insert_test_root(&conn, temp.path().to_str().unwrap(), "source", false);

        const GROUPS: usize = 250;
        const TWINS: usize = 3;
        for i in 0..GROUPS {
            hardlink_group(temp.path(), &format!("f{i}"), TWINS);
            seed_old_grain(&conn, root_id, temp.path(), &format!("originals/f{i}.jpg"));
        }

        let stats = scan_pass(&conn, root_id, temp.path(), Some(5)).stats;
        assert_eq!(stats.new as usize, GROUPS * TWINS);
        assert_eq!(stats.hardlink_companions as usize, GROUPS * TWINS);
        assert_eq!(stats.moved, 0, "nothing deferred, so nothing moved");
        assert_eq!(stats.unchanged as usize, GROUPS);
        assert_eq!(stats.missing, 0);

        assert_second_scan_quiet(&conn, root_id, temp.path());
    }

    #[test]
    fn scan_root_captures_deletion_before_flip() {
        // A source whose file is gone is captured for the receipt with its pre-scan
        // decision_id, then stamped with the scan's — stamp-set = receipt-set.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        // "gone.txt" is expected (present in DB) but absent on disk → missing.
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        conn.execute("UPDATE sources SET decision_id = 42 WHERE id = ?", [gone])
            .unwrap();
        // A real file keeps the walk non-empty (New, seen).
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(temp.path()),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(99),
            true,
        )
        .unwrap();

        // Receipt-set: exactly the deleted source, with its pre-flip provenance link.
        assert_eq!(result.deleted_items.len(), 1);
        let item = &result.deleted_items[0];
        assert_eq!(item.rel_path, "gone.txt");
        assert_eq!(item.previous_decision_id, Some(42));
        assert!(item.hash.is_none());

        // Stamp-set: the same source is now absent and stamped with the scan decision.
        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [gone],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(99));
    }

    #[test]
    fn scan_root_no_capture_when_disabled() {
        // Records mode (receipts off): the source is still marked missing and
        // stamped, but no items are captured for a receipt.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);
        // A real file keeps the root non-empty so "gone.txt" is inferred missing
        // (not routed through empty-dir device classification).
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(temp.path()),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(7),
            false,
        )
        .unwrap();

        assert!(result.deleted_items.is_empty());
        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [gone],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(7));
    }

    #[test]
    fn scan_root_scoped_prefix_does_not_sweep_string_prefix_siblings() {
        // A scan scoped to "alpha" must not treat sources under "alpha-beta" —
        // a sibling directory sharing "alpha" as a string prefix — as expected
        // and then missing. The sibling's file stands on disk untouched the
        // whole time; only the path boundary separates it from a false deletion.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        std::fs::create_dir(temp.path().join("alpha")).unwrap();
        std::fs::create_dir(temp.path().join("alpha-beta")).unwrap();
        std::fs::write(temp.path().join("alpha").join("a.txt"), "a").unwrap();
        std::fs::write(temp.path().join("alpha-beta").join("b.txt"), "b").unwrap();
        let sibling = repo::insert_test_source(&conn, root_id, "alpha-beta/b.txt", 1, 1, 1, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("alpha"),
            walk(&temp.path().join("alpha")),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(9),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn scan_root_scoped_prefix_treats_wildcard_bytes_literally() {
        // '_' in a scope path is a path byte, not a pattern: a scan scoped to
        // "alpha_beta" must not expect sources under "alphaXbeta" — a sibling
        // one wildcard-match away — and then sweep them into false deletion.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        std::fs::create_dir(temp.path().join("alpha_beta")).unwrap();
        std::fs::create_dir(temp.path().join("alphaXbeta")).unwrap();
        std::fs::write(temp.path().join("alpha_beta").join("a.txt"), "a").unwrap();
        std::fs::write(temp.path().join("alphaXbeta").join("b.txt"), "b").unwrap();
        let sibling = repo::insert_test_source(&conn, root_id, "alphaXbeta/b.txt", 1, 1, 1, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("alpha_beta"),
            walk(&temp.path().join("alpha_beta")),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(9),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn scan_root_unstable_mount_records_no_deletion() {
        // When the walk root's device is unavailable (an unstable mount), missing
        // detection is skipped, so a pre-inserted source is neither marked missing
        // nor captured — the guard prevents false deletion records.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let missing_root = temp.path().join("not-there");
        let root_path = missing_root.to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let gone = repo::insert_test_source(&conn, root_id, "gone.txt", 1, 1, 100, 1000);

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            walk(&missing_root),
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert!(result.deleted_items.is_empty());
        assert_eq!(result.stats.missing, 0);
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [gone], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            present, 1,
            "source must not be marked missing on unstable mount"
        );
    }

    // =========================================================================
    // mark_missing_path (the --missing deleted-folder path)
    // =========================================================================

    /// Fetch roots as domain objects, the way the interface passes them in.
    /// Test paths are absolute, so the cwd handed to `mark_missing_path` is a
    /// placeholder (`/`).
    fn all_roots(conn: &Connection) -> Vec<crate::core::domain::root::Root> {
        repo::root::fetch_all(conn).unwrap()
    }

    #[test]
    fn scan_root_walk_error_skips_missing_detection() {
        // An unreadable directory means part of the tree went unseen; a source
        // that merely wasn't seen must not be marked deleted on such a walk.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);
        let unseen = repo::insert_test_source(&conn, root_id, "sub/gone.txt", 1, 1, 100, 1000);
        std::fs::write(temp.path().join("here.txt"), "data").unwrap();

        // A real walk error, produced by walking a path that does not exist,
        // chained after the real entries.
        let err = WalkDir::new(temp.path().join("no-such-dir"))
            .into_iter()
            .next()
            .unwrap()
            .unwrap_err();
        let entries = walk(temp.path()).chain(std::iter::once(Err(err)));

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            None,
            entries,
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.walk_errors, 1);
        assert_eq!(result.stats.missing, 0);
        assert_eq!(result.stats.missing_detection_skipped, 0);
        assert!(result.deleted_items.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.contains("walk errors — skipping missing detection")));
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [unseen], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn scan_root_unstable_mount_discards_empty_dir_missing() {
        // Missing classifications from empty directories obey the mount guard
        // too: a walk whose device story cannot be verified must not delete —
        // otherwise one scan could both say "missing detection skipped" and
        // still write a deletion record.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(&conn, root_path, "source", false);

        // The scan is scoped to "vanishing", which holds an empty dir whose
        // DB source sits on the same device — the empty-dir classifier will
        // mark it Missing mid-walk.
        let scoped = temp.path().join("vanishing");
        std::fs::create_dir(&scoped).unwrap();
        std::fs::create_dir(scoped.join("emptied")).unwrap();
        let device = get_dir_device(temp.path()).unwrap();
        let gone = repo::insert_test_source(
            &conn,
            root_id,
            "vanishing/emptied/gone.txt",
            device,
            7,
            100,
            1000,
        );

        // An extra entry outside the scoped dir, captured up front; yielding
        // it removes the scoped dir so the post-walk device check fails.
        std::fs::create_dir(temp.path().join("other")).unwrap();
        std::fs::write(temp.path().join("other").join("x.txt"), "x").unwrap();
        let extra = WalkDir::new(temp.path().join("other"))
            .into_iter()
            .nth(1)
            .unwrap();
        let scoped_clone = scoped.clone();
        let entries = walk(&scoped).chain(std::iter::once_with(move || {
            std::fs::remove_dir_all(&scoped_clone).unwrap();
            extra
        }));

        let now = current_timestamp();
        let result = scan_root(
            &conn,
            root_id,
            root_path,
            Some("vanishing"),
            entries,
            &no_hash_options(),
            &NoopProgress,
            now,
            Some(5),
            true,
        )
        .unwrap();

        assert_eq!(result.stats.missing_detection_skipped, 1);
        assert_eq!(result.stats.missing, 0);
        assert!(result.deleted_items.is_empty());
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [gone], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn mark_missing_path_marks_sources() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        for i in 0..5 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("vacation/img{i}.jpg"),
                1,
                100 + i,
                1000,
                1000,
            );
        }

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 5);
        assert_eq!(result.root_id, root_id);
        let present: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(present, 0);
    }

    #[test]
    fn mark_missing_path_refuses_a_suspended_root() {
        // A suspended root's path is exactly one that fails to canonicalize,
        // so --missing lands here — without the refusal it would mark a
        // disconnected drive's sources deleted.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", true);
        let src = repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 100, 1000, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("suspended"));
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [src], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            present, 1,
            "no source on a suspended root is marked deleted"
        );
    }

    #[test]
    fn mark_missing_path_not_under_any_root() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", false);

        let result = mark_missing_path(
            &conn,
            Path::new("/nonexistent/path"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not under any known root"));
    }

    #[test]
    fn mark_missing_path_prefix_matches_subset() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        for i in 0..3 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("vacation/img{i}.jpg"),
                1,
                200 + i,
                1000,
                1000,
            );
        }
        for i in 0..2 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("work/doc{i}.pdf"),
                1,
                300 + i,
                1000,
                1000,
            );
        }

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 3);
        let work_present: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path LIKE 'work/%' AND present = 1",
                [root_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(work_present, 2);
    }

    #[test]
    fn mark_missing_path_stops_at_the_path_boundary() {
        // Marking "vacation" missing must not touch "vacation-2023" — a sibling
        // directory sharing the name as a string prefix is outside the scope.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 200, 1000, 1000);
        let sibling =
            repo::insert_test_source(&conn, root_id, "vacation-2023/img.jpg", 1, 201, 1000, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        let present: i64 = conn
            .query_row("SELECT present FROM sources WHERE id = ?", [sibling], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(present, 1);
    }

    #[test]
    fn mark_missing_path_already_not_present() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "vacation/img.jpg", 1, 400, 1000, 1000);
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [sid])
            .unwrap();

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        // mark_missing only flips present=1 rows, so nothing is marked here.
        assert_eq!(result.missing_count, 0);
    }

    #[test]
    fn mark_missing_path_empty_prefix_marks_all() {
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        for i in 0..4 {
            repo::insert_test_source(
                &conn,
                root_id,
                &format!("img{i}.jpg"),
                1,
                500 + i,
                1000,
                1000,
            );
        }

        let result = mark_missing_path(
            &conn,
            Path::new("/photos"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 4);
    }

    #[test]
    fn mark_missing_path_no_sources_found() {
        let conn = repo::open_in_memory_for_test();
        repo::insert_test_root(&conn, "/photos", "source", false);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/empty"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 0);
        assert!(result.deleted_items.is_empty());
    }

    #[test]
    fn mark_missing_path_stamps_decision_id() {
        // --missing threads the scan decision_id into the deletion transition,
        // exactly as the sweep does.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "gone.jpg", 1, 1, 100, 1000);

        let result = mark_missing_path(
            &conn,
            Path::new("/photos"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            Some(555),
            false,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        let did: Option<i64> = conn
            .query_row("SELECT decision_id FROM sources WHERE id = ?", [sid], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(did, Some(555));
    }

    #[test]
    fn mark_missing_path_captures_items_before_flip() {
        // With capture on, --missing snapshots sources before the flip — the same
        // treatment the sweep gives — so each item carries its pre-flip
        // decision_id, and the source ends up absent and stamped with the scan.
        let conn = repo::open_in_memory_for_test();
        let root_id = repo::insert_test_root(&conn, "/photos", "source", false);
        let sid = repo::insert_test_source(&conn, root_id, "vacation/gone.jpg", 1, 1, 100, 1000);
        conn.execute("UPDATE sources SET decision_id = 42 WHERE id = ?", [sid])
            .unwrap();

        let result = mark_missing_path(
            &conn,
            Path::new("/photos/vacation"),
            &all_roots(&conn),
            Path::new("/"),
            9999,
            Some(99),
            true,
        )
        .unwrap();

        assert_eq!(result.missing_count, 1);
        assert_eq!(result.deleted_items.len(), 1);
        let captured = &result.deleted_items[0];
        assert_eq!(captured.rel_path, "vacation/gone.jpg");
        assert_eq!(captured.previous_decision_id, Some(42));

        let (present, did): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                [sid],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(did, Some(99));
    }
}
