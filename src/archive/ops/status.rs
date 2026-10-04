//! Reading a manifest's status — what has landed, and what is still waiting.
//!
//! A diagnostic read: each lock entry is compared against its source and its
//! destination on disk, and what is found is reported. Nothing is moved and
//! nothing is recorded.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::archive::domain::{
    LockEntry, LockStanding, ManifestStanding, NextStep, OwedWayBack, RefreshRefusal,
};
use crate::core::domain::root::{ParkedRoot, Root};
use crate::core::domain::time::zone::UserZone;
use crate::core::ops::scope::resolve_recorded_scope;
use crate::core::repo::{self, Connection};
use crate::expr::{prefetch_pattern_facts, Unmeasured};

use super::manifest::{read_lock_file, read_manifest_config};
use super::pattern::PatternGaps;

/// What a destination path reads when the pattern could not be expanded for
/// that entry. Spoken here because the interface recovers the same fact by
/// matching on it: two spellings would let the count and the listing beneath
/// it drift apart silently, the count staying right while the listing
/// quietly empties.
pub const EXPANSION_FAILED: &str = "<pattern expansion failed";

/// What one lock entry's two endpoints add up to — derived once, here, and
/// read by the counts and by every line the interface prints. A header that
/// counts one way while the list beneath it filters another is how a status
/// report ends up disagreeing with itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// The destination holds the file, at the expected size.
    AtDestination,
    /// The destination holds a file of a different size.
    SizeMismatch,
    /// Not at the destination yet; the source is still there to bring.
    Pending,
    /// Not at the destination, and the source is gone.
    SourceLost,
}

/// The closed doors a lock reaches behind: how many of its entries stand on
/// each root the user has closed.
///
/// **One computation, two readers.** `apply` refuses on it before its own
/// summary, and `cluster status` states it in apply's conditional — what a
/// lock reaches behind a door must be one fact at both, or a report invites an
/// act that the act itself turns away. What each *does* about it is its permit
/// class, not this function's.
///
/// **`cluster refresh` is deliberately not a reader.** A record is not a
/// naming: the lock is a snapshot of where sources stood when the selection
/// was last made, and a refresh is asking again, so it re-selects from the
/// open world and simply does not gather what stands behind a door. The lock
/// is refused where it would be *carried across* — at apply.
///
/// Asked of the **index**, never of the lock's own recorded `root_id`: the
/// door is a question about where a source stands *now*, and a lock is a
/// snapshot of where it stood when the selection was made. An entry with no
/// *present* source behind it contributes nothing — gone from the index, or
/// standing as a tombstone, there is no standing left to read either way.
///
/// Counts are ordered by root id, so the same lock states itself the same way
/// on every run.
pub fn parked_lock_entries(
    conn: &Connection,
    entries: &[LockEntry],
    roots: &[Root],
) -> Result<Vec<(ParkedRoot, usize)>> {
    let ids: Vec<i64> = entries.iter().map(|e| e.id).collect();
    let sources = repo::source::batch_fetch_by_ids(conn, &ids)?;

    let mut counts: Vec<(ParkedRoot, usize)> = Vec::new();
    for source in sources.values() {
        match counts.iter_mut().find(|(r, _)| r.root_id == source.root_id) {
            Some((_, n)) => *n += 1,
            None => {
                if let Some(parked) = roots
                    .iter()
                    .find(|r| r.id == source.root_id)
                    .and_then(Root::parked)
                {
                    counts.push((parked, 1));
                }
            }
        }
    }
    counts.sort_by_key(|(r, _)| r.root_id);
    Ok(counts)
}

/// The one derivation of an entry's status from what was found on disk.
fn classify_entry(source_exists: bool, dest_exists: bool, dest_size_match: bool) -> EntryStatus {
    if dest_exists && dest_size_match {
        EntryStatus::AtDestination
    } else if dest_exists {
        EntryStatus::SizeMismatch
    } else if source_exists {
        EntryStatus::Pending
    } else {
        EntryStatus::SourceLost
    }
}

/// State of a single lock entry in status assessment.
pub struct StatusEntry {
    /// Absolute source path.
    pub source_path: String,
    /// Last component of source path, for display.
    pub source_filename: String,
    /// Whether the source file exists on disk.
    pub source_exists: bool,
    /// Full destination path.
    pub dest_path: String,
    /// Whether the destination is registered in the DB (present=1).
    pub db_registered: bool,
    /// What the three facts above add up to.
    pub status: EntryStatus,
}

/// Result of manifest status assessment.
pub struct ManifestStatus {
    /// Path to the manifest file.
    pub manifest_path: String,
    /// Display string for destination (archive root + base_dir).
    pub dest_display: String,
    /// Output pattern from the manifest.
    pub pattern: String,
    /// Number of entries in the lock file. Zero where there is no lock.
    pub lock_entry_count: usize,
    /// The lock beside the manifest, as the read found it — absent, or read
    /// with its two claims about itself. One field rather than three loose
    /// booleans, so a claim about a file that was never read is
    /// unrepresentable.
    pub lock: LockStanding,
    /// Per-entry status assessment.
    pub entries: Vec<StatusEntry>,
    /// Count of entries where dest exists with correct size.
    pub at_destination: usize,
    /// Count of entries where source exists but dest does not.
    pub pending: usize,
    /// Count of entries where source is missing and dest is missing.
    pub source_lost: usize,
    /// Count of entries where dest exists but size doesn't match.
    pub size_mismatch: usize,
    /// Count of entries at dest where source file still exists.
    pub source_still_present: usize,
    /// Scope prefixes the manifest records that resolve to no known root.
    /// Carried rather than acted on: a report reports. The interface states
    /// them; nothing here narrows a count on their account.
    pub unrooted_scope: Vec<String>,
    /// Scope prefixes that name a known root but no byte-form of which the
    /// index knows sources under. The other half of "this line contributes
    /// nothing", and a different answer from the one above — a place Canon
    /// cannot confirm, rather than one it has never heard of. Reported for the
    /// same reason and on the same terms.
    pub set_aside_scope: Vec<String>,
    /// Scope prefixes standing on a root the user closed — the manifest
    /// door's third partition. Carried, never acted on: a report reports, and
    /// `cluster status` is a **view**, which states the pause and refuses
    /// nothing.
    pub parked_scope: Vec<crate::core::domain::root::ParkedPath>,
    /// Whether the recorded scope kept nothing, so `cluster refresh` refuses
    /// by the source-existence policy's terminal rule rather than by a door
    /// (`ScopeResolution::kept_nothing`). Carried because a report must name
    /// no act that would be turned away, and this is the half of that which
    /// is not a door at all.
    pub scope_kept_nothing: bool,
    /// Whether no recorded prefix confirmed, so a rebuild would supply no
    /// measurement (`ScopeResolution::scopes` empty). A different question
    /// from the one above — that asks whether a refresh may run, this asks
    /// what it would give — and the one that decides whether a rebuild helps a
    /// pattern that cannot expand.
    pub scope_confirms_nothing: bool,
    /// How many lock entries stand behind each closed door.
    pub parked_entries: Vec<(ParkedRoot, usize)>,
    /// The destination's own door, when the archive root this manifest writes
    /// into is one the user closed. Nothing would be written there until it is
    /// opened, so the report says so rather than inviting an apply.
    pub parked_destination: Option<ParkedRoot>,
    /// Whether the pattern failed to expand for any entry, and so would fail
    /// for the whole run: `apply` collects expansion failures and aborts on
    /// them before it transfers anything.
    ///
    /// One input of the decision table, never the whole answer to *can
    /// `apply` run at all* — that question has its own owner
    /// (`archive::domain::ManifestStanding`), reached through
    /// [`standing`](ManifestStatus::standing).
    pub pattern_unexpandable: bool,
    /// What the pattern said when it could not expand, in the order the
    /// entries were read and deduplicated: usually one reason for every
    /// entry, since a pattern fails the same way for all of them.
    ///
    /// Carried because a report that declines to name a next step owes the
    /// user the reason it declined — the per-entry table shows a fixed "not at
    /// dest" and never this, so without it the run would report nothing at all
    /// about a manifest that cannot be applied.
    pub expansion_failures: Vec<String>,
    /// Which causes the expansion failures met, and whether the pattern places
    /// files relative to a scope the lock carries no measurement for.
    ///
    /// What tells the expansion failures apart, and they take different
    /// advice: a scope the manifest does not have is repaired by scoping it or
    /// by measuring from each file's own root, a fact no source carries by
    /// supplying the fact, a save time Canon cannot read by leaving the file
    /// out or correcting it. Asked of the language facility's typed cause
    /// rather than of the refusal's wording, so the remedy cannot drift from
    /// what actually failed.
    pub pattern_gaps: PatternGaps,
    /// How many entries failed, which is not the length of the list above —
    /// that deduplicates by message, and a pattern usually fails the same way
    /// for every source. Carried rather than recovered by the interface,
    /// which would otherwise have to match a display string to count.
    pub expansion_failure_count: usize,
}

impl ManifestStatus {
    /// Are all source files accounted for (either at source or at destination)?
    pub fn all_accounted_for(&self) -> bool {
        self.source_lost == 0 && self.size_mismatch == 0
    }

    /// This manifest as the decision table reads it.
    ///
    /// The one projection, and the reason every question below is a call
    /// rather than a derivation: *what stops which act* is written out once,
    /// in the domain, and a report that re-tested the causes beside it would
    /// be exactly the drift the table was minted to close.
    ///
    /// *Can `apply` run* has no shorthand here beside the refresh's, and that
    /// is demand rather than oversight: nothing in the report asks it on its
    /// own — the next step asks a wider question, and the door lines read the
    /// doors themselves. A caller that needs it says `standing().can_run(
    /// Act::Apply)`, which is the same words the table uses.
    pub fn standing(&self) -> ManifestStanding {
        ManifestStanding {
            lock: self.lock,
            pattern_expands: !self.pattern_unexpandable,
            pattern_needs_a_scope_it_lacks: self.pattern_gaps.needs_a_scope_it_lacks,
            parked_origins: !self.parked_entries.is_empty(),
            parked_destination: self.parked_destination.is_some(),
            parked_scope: !self.parked_scope.is_empty(),
            scope_kept_nothing: self.scope_kept_nothing,
            scope_confirms_nothing: self.scope_confirms_nothing,
        }
    }

    /// The step that moves this manifest forward, as far as the two acts go.
    pub fn next_step(&self) -> NextStep {
        self.standing().next_step()
    }

    /// Whether `cluster refresh` could run at all — and where it could not,
    /// which of its two refusals turns it away.
    ///
    /// **Its two, which are neither of apply's.** A refresh re-queries from
    /// the recorded scope and rewrites the manifest and its lock — files that
    /// live where the user put them, never inside the archive root — so a
    /// parked **scope line** closes it, and neither a parked lock entry nor a
    /// parked destination does. A lock entry is a record, not a naming: the
    /// rebuild re-selects from the open world and leaves parked content out of
    /// the new lock, which is what an act ignoring parked content looks like
    /// here. The second is the source-existence policy's terminal rule — a
    /// recorded scope that kept nothing — which is not a door at all, and
    /// which a report naming the rebuild once did not consult.
    ///
    /// The two predicates share no cause, which is the whole point of having
    /// two: a suggestion consults **its own act's** precondition, and each
    /// half of that has been the defect once. Its converse does not hold: a
    /// command that would run is named only where it brings the withheld act
    /// within reach, which is the table's `next_step` to state.
    ///
    /// **It answers with the cause rather than a bare yes**, because the two
    /// refusals leave the reader differently placed: a door is announced with
    /// the act that opens it, a scope that kept nothing with a skipped line
    /// naming no act at all. A caller that only wants the boolean reads
    /// `.is_none()`; a projection spelling that separately would be the same
    /// question asked twice.
    pub fn refresh_refused_by(&self) -> Option<RefreshRefusal> {
        self.standing().refresh_refused_by()
    }

    /// What the reader is owed where the table names no step — the table's
    /// answer, carried through unchanged.
    ///
    /// A report that has named a cause and no act has stopped mid-answer, and
    /// which causes speak for themselves is a property of the rows, not of the
    /// report. See [`ManifestStanding::owed_way_back`].
    pub fn owed_way_back(&self) -> OwedWayBack {
        self.standing().owed_way_back()
    }
}

/// Compute the status of a manifest by checking filesystem and DB state.
///
/// This is a read-only diagnostic: no DB writes, no file operations.
/// Needs `&mut Connection` because fact fetching uses temp tables.
pub fn compute_manifest_status(
    conn: &mut Connection,
    manifest_path: &Path,
) -> Result<ManifestStatus> {
    use super::pattern::evaluate_pattern;
    use crate::expr::{extract_fact_keys, parse_pattern};

    // 1. Read the manifest
    let config = read_manifest_config(manifest_path)?;
    let lock_path = manifest_path.with_extension("lock");
    // The pattern is the manifest's own, so it is parsed before the lock is
    // read: the lock is asked one question about it below, while it is still
    // whole.
    let pattern = parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = extract_fact_keys(&pattern);
    // 2. Read the lock beside it, or record that there is none, and settle
    //    the two claims a lock that *was* read makes about itself.
    //
    // An absent lock is a state this report states, not one it fails on: a
    // refresh whose query matched nothing takes the lock away and records the
    // empty hash, so the commonest shape of an empty manifest is exactly this
    // — and a diagnostic that stopped reading here would answer the one
    // question it exists for with an error, on a file Canon itself wrote.
    // Every other failure is still an error: absence is not unreadability.
    //
    // The hash is asked only of a lock that was read. Asking it of an absent
    // one and calling the failure a mismatch would report a claim about bytes
    // nobody has, which is the second cause dressed as the first.
    let (lock, unmeasured, pattern_needs_a_scope_it_lacks, lock_entries) =
        match read_lock_file(&lock_path)? {
            None => (
                LockStanding::absent(&config.meta.lock_hash),
                // Never read: the expansion loop below runs once per entry and
                // there are none. Named rather than left to a default so the
                // fact that nothing consumes it is visible here.
                Unmeasured::NoScopeRecorded,
                false,
                Vec::new(),
            ),
            Some(lock) => {
                let hash_matches = match crate::core::ops::fs::compute_full_hash(&lock_path) {
                    Ok(actual_hash) => actual_hash == config.meta.lock_hash,
                    Err(_) => false,
                };
                let standing = LockStanding::Read {
                    hash_matches,
                    carries_measurement: lock.header.is_some(),
                };
                (
                    standing,
                    lock.unmeasured_reason(),
                    lock.pattern_needs_a_scope_it_lacks(&pattern),
                    lock.entries,
                )
            }
        };
    let lock_entry_count = lock_entries.len();

    // 3. Fetch roots, build root_paths map
    let roots = repo::root::fetch_all(conn)?;
    let root_paths: HashMap<i64, String> = roots.iter().map(|r| (r.id, r.path.clone())).collect();

    // 4. Find archive root and compute base_dir
    let archive_root = roots
        .iter()
        .find(|r| r.id == config.output.archive_root_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Archive root id {} not found",
                config.output.archive_root_id
            )
        })?;
    let base_dir = if config.output.base_dir.is_empty() {
        PathBuf::from(&archive_root.path)
    } else {
        PathBuf::from(&archive_root.path).join(&config.output.base_dir)
    };
    let dest_display = base_dir.to_string_lossy().to_string();

    // 5. The manifest's own scope, through the same resolution `cluster generate`
    // and `cluster refresh` use. Status takes no measurement from it — that
    // comes off each lock entry, settled when the selection was — but this is
    // the diagnostic that tells a user what state their manifest is in, and a
    // scope line contributing nothing is part of that state.
    let scope = resolve_recorded_scope(conn, &config.meta.scope, &roots)?;

    // Batch fetch facts for all lock entries if pattern uses content facts
    let source_ids: Vec<i64> = lock_entries.iter().map(|s| s.id).collect();
    let facts = prefetch_pattern_facts(conn, &source_ids, &needed_keys)?;

    // 6. Evaluate patterns to get dest paths, then check filesystem + DB
    let mut pattern_unexpandable = false;
    let mut pattern_gaps = PatternGaps::for_lock(pattern_needs_a_scope_it_lacks);
    let mut expansion_failures: Vec<String> = Vec::new();
    let mut expansion_failure_count = 0usize;
    let mut entries = Vec::with_capacity(lock_entry_count);
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(lock_entry_count);

    // AUDIT: placement reads UTC until a manifest records the zone it was made in; it must never
    // read the machine's zone.
    let zone = UserZone::utc();
    for lock_entry in &lock_entries {
        let dest_rel =
            match evaluate_pattern(&pattern, lock_entry, unmeasured, &root_paths, &facts, &zone) {
                Ok(rel) => rel,
                Err(e) => {
                    // If pattern expansion fails, we can't determine dest path.
                    // Use a placeholder and mark as not-at-dest. It is also the
                    // whole run's answer: apply aborts on expansion failures
                    // before it transfers anything, so a report that has seen one
                    // must not go on to name apply.
                    pattern_unexpandable = true;
                    pattern_gaps.met(&e);
                    expansion_failure_count += 1;
                    let reason = e.to_string();
                    if !expansion_failures.contains(&reason) {
                        expansion_failures.push(reason);
                    }
                    let filename = Path::new(&lock_entry.path)
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| lock_entry.path.clone());
                    let source_exists = Path::new(&lock_entry.path).exists();
                    entries.push(StatusEntry {
                        source_path: lock_entry.path.clone(),
                        source_filename: filename,
                        source_exists,
                        dest_path: format!("{EXPANSION_FAILED}: {e}>"),
                        db_registered: false,
                        status: classify_entry(source_exists, false, false),
                    });
                    dest_rel_paths.push(String::new());
                    continue;
                }
            };

        let archive_rel_path = if config.output.base_dir.is_empty() {
            dest_rel.clone()
        } else {
            format!("{}/{}", config.output.base_dir, &dest_rel)
        };

        let dest_full = base_dir.join(&dest_rel);
        let source_exists = Path::new(&lock_entry.path).exists();
        let dest_stat = fs::metadata(&dest_full).ok();

        let (dest_exists, dest_size_match) = match &dest_stat {
            Some(meta) if meta.is_file() => {
                let actual_size = meta.len();
                let expected_size = lock_entry.size as u64;
                (true, actual_size == expected_size)
            }
            _ => (false, false),
        };

        let filename = Path::new(&lock_entry.path)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| lock_entry.path.clone());

        entries.push(StatusEntry {
            source_path: lock_entry.path.clone(),
            source_filename: filename,
            source_exists,
            dest_path: dest_full.to_string_lossy().to_string(),
            db_registered: false, // filled in below
            status: classify_entry(source_exists, dest_exists, dest_size_match),
        });
        dest_rel_paths.push(archive_rel_path);
    }

    // 7. Batch check DB registration
    let rel_refs: Vec<&str> = dest_rel_paths.iter().map(|s| s.as_str()).collect();
    let registered = crate::archive::repo::batch_check_paths_exist(
        conn,
        config.output.archive_root_id,
        &rel_refs,
    )?;
    for (entry, rel_path) in entries.iter_mut().zip(dest_rel_paths.iter()) {
        if !rel_path.is_empty() && registered.contains(rel_path.as_str()) {
            entry.db_registered = true;
        }
    }

    // 8. Compute counts
    let mut at_destination = 0usize;
    let mut pending = 0usize;
    let mut source_lost = 0usize;
    let mut size_mismatch = 0usize;
    let mut source_still_present = 0usize;

    for entry in &entries {
        match entry.status {
            EntryStatus::AtDestination => {
                at_destination += 1;
                if entry.source_exists {
                    source_still_present += 1;
                }
            }
            EntryStatus::SizeMismatch => size_mismatch += 1,
            EntryStatus::Pending => pending += 1,
            EntryStatus::SourceLost => source_lost += 1,
        }
    }

    Ok(ManifestStatus {
        manifest_path: manifest_path.to_string_lossy().to_string(),
        dest_display,
        pattern: config.output.pattern.clone(),
        lock_entry_count,
        lock,
        entries,
        at_destination,
        pending,
        source_lost,
        size_mismatch,
        source_still_present,
        unrooted_scope: scope.unrooted().to_vec(),
        set_aside_scope: scope.set_aside().to_vec(),
        parked_scope: scope.parked().to_vec(),
        scope_kept_nothing: scope.kept_nothing(),
        scope_confirms_nothing: scope.scopes().is_empty(),
        parked_entries: parked_lock_entries(conn, &lock_entries, &roots)?,
        parked_destination: archive_root.parked(),
        pattern_unexpandable,
        expansion_failures,
        pattern_gaps,
        expansion_failure_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{
        insert_object, insert_root, insert_source_with_size, setup_test_db,
    };

    use super::super::generate::{
        execute_generate, plan_generate, ClusterGenerateParams, ExecuteGenerateParams,
    };

    // Duplicated from the generate module's tests rather than shared: those
    // tests use it too, and a cross-module test helper would tie the two
    // modules together for no gain beyond saving a few lines.
    fn default_params() -> ClusterGenerateParams {
        ClusterGenerateParams {
            scopes: vec![],
            filters: vec![],
            allow_archived: false,
            allow_duplicates: false,
        }
    }

    // =========================================================================
    // compute_manifest_status
    // =========================================================================

    /// Generate a real manifest and lock over `names`, one four-byte file per
    /// name in a source root, beside an empty archive root. Returns the
    /// connection, the temp dir (the caller holds it so the files outlive the
    /// call), the manifest path and the archive directory.
    fn status_fixture(names: &[&str]) -> (Connection, tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("photos");
        let archive_dir = dir.path().join("archive");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&archive_dir).unwrap();

        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, src_dir.to_str().unwrap(), "source", false);
        let archive_root = insert_root(&conn, archive_dir.to_str().unwrap(), "archive", false);

        for (i, name) in names.iter().enumerate() {
            std::fs::write(src_dir.join(name), b"aaaa").unwrap();
            let obj = insert_object(&conn, &format!("hash{i}"), false);
            insert_source_with_size(&conn, src_root, name, Some(obj), 4);
        }

        let mut plan = plan_generate(&mut conn, &default_params()).unwrap();
        let manifest_path = dir.path().join("cluster.toml");
        let params = ExecuteGenerateParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            expanded_filters: vec![],
            original_filters: vec![],
            scope: crate::core::domain::scope::ScopeResolution::from_outcomes(vec![]),
            roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
            archive_root_id: archive_root,
            base_dir: String::new(),
            allow: vec![],
        };
        execute_generate(&mut plan, &params).unwrap();

        (conn, dir, manifest_path, archive_dir)
    }

    /// **A manifest whose lock is gone is a state this read states, not one
    /// it fails on.** Canon writes that state itself: a refresh whose query
    /// matches nothing takes the lock away and records the empty hash. The
    /// read used to stop at the missing file and the whole report went with
    /// it, on a manifest Canon had just written and by following the report's
    /// own suggestion.
    ///
    /// The manifest's own record tells the two absences apart, so the
    /// standing carries which: nothing recorded is the zero-match refresh, a
    /// recorded hash is a lock that has since gone.
    #[test]
    fn an_absent_lock_is_a_state_the_read_states() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        // A pattern that **cannot** expand, over a manifest recording no
        // scope — so the derivation is the only thing keeping
        // `pattern_unexpandable` false, and this assertion fails the day it
        // is taken anywhere but the entry loop. With the fixture's own
        // expandable pattern the row below could not fail against the defect
        // it is cited for, which is the whole reason this line is here.
        rewrite_manifest_pattern(&manifest, "{scope.rel_path}");
        std::fs::remove_file(dir.path().join("cluster.lock")).unwrap();

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(
            status.lock,
            LockStanding::Absent {
                manifest_names_a_hash: true
            },
            "this manifest still names the lock it lost"
        );
        assert_eq!(status.lock_entry_count, 0);
        assert_eq!(status.pending, 0);
        assert_eq!(
            status.standing().obstacles(),
            vec![crate::archive::domain::Obstacle::NoLock],
            "one obstacle, and no claim about bytes nobody has"
        );
    }

    /// A lock that is *unreadable* is still an error, and that distinction is
    /// the point: absence is a state of a manifest, an empty or malformed file
    /// is a file that never finished being written, and a reader that folded
    /// them together would state a cause it cannot know.
    #[test]
    fn an_unreadable_lock_is_still_an_error() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        std::fs::write(dir.path().join("cluster.lock"), "").unwrap();

        let err = match compute_manifest_status(&mut conn, &manifest) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("an empty lock is not a state; it is a file that never finished"),
        };
        assert!(err.contains("empty"), "{err}");
    }

    /// The two halves of the recorded scope the decision table reads are
    /// carried out of the resolution, not left for a reader to re-derive from
    /// the registers beside them: whether a refresh may run at all, and
    /// whether a rebuild would have anything to measure from.
    ///
    /// An unscoped manifest is the plain shape of the second: it confirms
    /// nothing and kept nothing, because there was nothing recorded to keep.
    #[test]
    fn an_unscoped_manifest_confirms_nothing_and_keeps_nothing() {
        let (mut conn, _dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(status.scope_confirms_nothing, "no prefix was recorded");
        assert!(
            !status.scope_kept_nothing,
            "nothing was set aside, so the terminal rule does not fire"
        );
        assert!(
            status.refresh_refused_by().is_none(),
            "an unscoped manifest refreshes perfectly well"
        );
    }

    /// An absent measurement has two causes and a report must not blame the
    /// wrong one. A lock built while the manifest declared no scope measures
    /// nothing — correctly, there was nothing to measure from — and it is a
    /// perfectly **current** lock. So the message must name the lock as built,
    /// not the lock's age and not the manifest (which may declare a scope
    /// since), and must not prescribe a refresh on its own — whether one
    /// helps is the table's question.
    ///
    /// The `apply` side of this is `an_unmeasured_lock_names_its_cause_not_its_age`;
    /// it refuses and moves nothing, but prints the cause on stderr. Here the
    /// same string is data.
    #[test]
    fn an_unmeasured_lock_blames_the_lock_not_its_age() {
        let (mut conn, _dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        rewrite_manifest_pattern(&manifest, "{scope.rel_path}");

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();

        assert!(
            matches!(
                status.lock,
                LockStanding::Read {
                    carries_measurement: true,
                    ..
                }
            ),
            "the lock is a current one; its age explains nothing here"
        );
        assert!(
            status.pattern_unexpandable,
            "the run cannot expand, which is what a next-step hint must ask"
        );
        let dest = &status.entries[0].dest_path;
        assert!(dest.contains("were not scoped"), "{dest}");
        assert!(
            !dest.contains("cluster refresh"),
            "a refresh cannot give an unscoped manifest a scope: {dest}"
        );
    }

    /// Give every entry of the fixture's lock a measurement, by hand. The
    /// fixture generates unscoped, so its entries carry none; this is the
    /// other lock a pin needs in order to tell the lock's answer from a
    /// pattern-only one. The manifest's hash is left stale on purpose — the
    /// read still parses the entries, and the question asked here is not the
    /// hash's.
    fn measure_lock_by_hand(dir: &Path) {
        let lock_path = dir.join("cluster.lock");
        let text = std::fs::read_to_string(&lock_path).unwrap();
        let measured: Vec<String> = text
            .lines()
            .map(|line| {
                let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
                if value.get("id").is_some() {
                    value["scope_rel_path"] = serde_json::Value::String("a.jpg".to_string());
                }
                value.to_string()
            })
            .collect();
        std::fs::write(&lock_path, measured.join("\n") + "\n").unwrap();
    }

    /// **The report carries the lock's own answer on which remedy the pattern
    /// takes**, read through the wiring production uses. The composition pin
    /// in `cli/cluster.rs` sets the field by hand and can only see what it is
    /// handed; this one asks the real fixture over both locks — one unmeasured,
    /// one measured — so a report that stopped asking the lock fails here by
    /// name whether it substituted a constant or the pattern's half alone.
    #[test]
    fn the_report_asks_the_lock_which_remedy_the_pattern_takes() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);

        rewrite_manifest_pattern(&manifest, "{scope.rel_path}");
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(
            status.pattern_gaps.needs_a_scope_it_lacks,
            "an unmeasured lock under a scope-relative pattern needs the scope it lacks"
        );

        rewrite_manifest_pattern(&manifest, "{source.rel_path}");
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(
            !status.pattern_gaps.needs_a_scope_it_lacks,
            "a pattern that never names the scope key needs no scope"
        );

        measure_lock_by_hand(dir.path());
        rewrite_manifest_pattern(&manifest, "{scope.rel_path}");
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(
            !status.pattern_gaps.needs_a_scope_it_lacks,
            "a measured lock lacks nothing the pattern needs — the pattern alone cannot say this"
        );
    }

    /// Set one lock entry's recorded save time, by hand. The manifest's hash goes stale;
    /// the read still parses the entries, and the hash is not the question here.
    fn set_lock_mtime(dir: &Path, name: &str, mtime: i64) {
        let lock_path = dir.join("cluster.lock");
        let text = std::fs::read_to_string(&lock_path).unwrap();
        let edited: Vec<String> = text
            .lines()
            .map(|line| {
                let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
                let named = value
                    .get("path")
                    .and_then(|p| p.as_str())
                    .is_some_and(|p| p.ends_with(name));
                if named {
                    value["mtime"] = serde_json::Value::from(mtime);
                }
                value.to_string()
            })
            .collect();
        std::fs::write(&lock_path, edited.join("\n") + "\n").unwrap();
    }

    /// **The report records which cause each failure met**, read through the wiring
    /// production uses: a save time Canon cannot read (the sentinel 9999-12-31 23:59:59
    /// UTC) is its own cause and never a fact not carried, its entry's reason states that
    /// cause, and a run meeting both causes records both.
    #[test]
    fn the_report_records_each_cause_a_pattern_failure_met() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg", "b.jpg"]);
        set_lock_mtime(dir.path(), "a.jpg", 253402300799);

        rewrite_manifest_pattern(&manifest, "{source.mtime|year}/{filename}");
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(status.pattern_gaps.unreadable_save_time);
        assert!(!status.pattern_gaps.fact_not_carried);
        assert!(status.pattern_gaps.too_coarse.is_empty());
        let reasons: Vec<&str> = status
            .entries
            .iter()
            .map(|e| e.dest_path.as_str())
            .collect();
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("Canon cannot read this file's save time")),
            "{reasons:?}"
        );

        // a.jpg carries the fact and fails on its save time; b.jpg fails on the fact.
        let a: i64 = conn
            .query_row("SELECT id FROM sources WHERE rel_path = 'a.jpg'", [], |r| {
                r.get(0)
            })
            .unwrap();
        crate::core::testing::insert_fact(&conn, a, "content.Make", "Canon");
        rewrite_manifest_pattern(&manifest, "{content.Make}/{source.mtime|year}");
        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(status.pattern_gaps.unreadable_save_time);
        assert!(status.pattern_gaps.fact_not_carried);
    }

    /// The report places by UTC too, until a manifest records the zone it was made in —
    /// the same destination `apply` would compute, whatever the machine's zone.
    #[test]
    fn the_report_places_by_utc_whatever_the_machines_zone() {
        use crate::core::domain::time::zone::{with_test_zone, UserZone};
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        set_lock_mtime(dir.path(), "a.jpg", 1704065400);
        rewrite_manifest_pattern(&manifest, "{source.mtime|year}/{filename}");

        let status = with_test_zone(UserZone::for_test("Europe/Amsterdam"), || {
            compute_manifest_status(&mut conn, &manifest).unwrap()
        });
        let dest = &status.entries[0].dest_path;
        assert!(dest.ends_with("/2023/a.jpg"), "{dest}");
    }

    /// Write `output.pattern` into a manifest, the way a user editing the file
    /// does. The lock is untouched, so the pair still agrees — which is the
    /// point: this is an edit that takes effect without a refresh.
    fn rewrite_manifest_pattern(manifest: &Path, pattern: &str) {
        let text = std::fs::read_to_string(manifest).unwrap();
        let rewritten: String = text
            .lines()
            .map(|line| {
                if line.starts_with("pattern = ") {
                    format!("pattern = \"{pattern}\"\n")
                } else {
                    format!("{line}\n")
                }
            })
            .collect();
        std::fs::write(manifest, rewritten).unwrap();
    }

    /// D1 — a recorded prefix under no known root leaves the computation as
    /// data. Status is a report, so it neither refuses nor narrows: the
    /// prefix is carried out for the interface to state, and the accounting
    /// claim — which is about source files — is untouched by it.
    #[test]
    fn an_unrooted_scope_is_carried_out_of_the_computation() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["settled.jpg"]);
        std::fs::write(dir.path().join("archive").join("settled.jpg"), b"aaaa").unwrap();

        // A manifest that records two places, one of which no root answers
        // for. The other names the source root itself, so the resolution has
        // something to keep as well as something to carry.
        let root: String = conn
            .query_row("SELECT path FROM roots WHERE role = 'source'", [], |r| {
                r.get(0)
            })
            .unwrap();
        rewrite_manifest_scope(&manifest, &[root, "/canon-test/no-such-root".to_string()]);

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();

        assert_eq!(status.unrooted_scope, ["/canon-test/no-such-root"]);
        assert!(
            status.all_accounted_for(),
            "the accounting claim is about source files and must not move"
        );
        assert_eq!(status.at_destination, 1);
    }

    /// Write `meta.scope` into a manifest, the way a user editing the file
    /// does. The lock is untouched, so the pair still agrees. An empty scope
    /// is not serialized at all, so the key is inserted above `generated_at`
    /// when it is absent and its whole `[ ... ]` span replaced when it is
    /// present — the serializer writes arrays across several lines.
    fn rewrite_manifest_scope(manifest: &Path, prefixes: &[String]) {
        let text = std::fs::read_to_string(manifest).unwrap();
        let quoted: Vec<String> = prefixes.iter().map(|p| format!("{p:?}")).collect();
        let line = format!("scope = [{}]", quoted.join(", "));
        let rewritten = match text.find("\nscope = [") {
            Some(at) => {
                let start = at + 1;
                #[expect(clippy::string_slice, reason = "just past a found ASCII newline")]
                let end = start + text[start..].find(']').expect("unterminated scope array") + 1;
                #[expect(clippy::string_slice, reason = "at and past found ASCII text")]
                let rewritten = format!("{}{line}{}", &text[..start], &text[end..]);
                rewritten
            }
            None => {
                let at = text
                    .find("\ngenerated_at = ")
                    .expect("no [meta] table to write a scope into")
                    + 1;
                #[expect(clippy::string_slice, reason = "just past a found ASCII newline")]
                let rewritten = format!("{}{line}\n{}", &text[..at], &text[at..]);
                rewritten
            }
        };
        std::fs::write(manifest, rewritten).unwrap();
    }

    #[test]
    fn compute_manifest_status_counts_a_file_at_its_destination() {
        let (mut conn, _dir, manifest, archive) = status_fixture(&["settled.jpg"]);
        std::fs::write(archive.join("settled.jpg"), b"aaaa").unwrap();

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(status.at_destination, 1);
        assert_eq!(status.source_still_present, 1);
        assert_eq!(status.pending, 0);
        assert_eq!(status.size_mismatch, 0);
        assert_eq!(status.source_lost, 0);
    }

    #[test]
    fn compute_manifest_status_counts_a_file_still_waiting() {
        let (mut conn, _dir, manifest, _archive) = status_fixture(&["waiting.jpg"]);

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(status.pending, 1);
        assert_eq!(status.at_destination, 0);
        assert_eq!(status.size_mismatch, 0);
        assert_eq!(status.source_lost, 0);
    }

    #[test]
    fn compute_manifest_status_counts_a_destination_of_the_wrong_size() {
        let (mut conn, _dir, manifest, archive) = status_fixture(&["resized.jpg"]);
        std::fs::write(archive.join("resized.jpg"), b"aa").unwrap();

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(status.size_mismatch, 1);
        assert_eq!(status.at_destination, 0);
        assert_eq!(status.pending, 0);
        assert_eq!(status.source_lost, 0);
    }

    #[test]
    fn compute_manifest_status_counts_a_source_that_is_gone() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["gone.jpg"]);
        std::fs::remove_file(dir.path().join("photos").join("gone.jpg")).unwrap();

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(status.source_lost, 1);
        assert_eq!(status.at_destination, 0);
        assert_eq!(status.pending, 0);
        assert_eq!(status.size_mismatch, 0);
    }

    #[test]
    fn manifest_status_is_accounted_for_unless_content_is_lost_or_wrong() {
        let (mut conn, _dir, manifest, archive) = status_fixture(&["settled.jpg"]);
        std::fs::write(archive.join("settled.jpg"), b"aaaa").unwrap();
        let settled = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(settled.all_accounted_for());

        // Still waiting counts as accounted for — the file is where it began.
        std::fs::remove_file(archive.join("settled.jpg")).unwrap();
        let waiting = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(waiting.pending, 1);
        assert!(waiting.all_accounted_for());

        // A destination of the wrong size is not, even with the source intact.
        std::fs::write(archive.join("settled.jpg"), b"aa").unwrap();
        let mismatched = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(mismatched.size_mismatch, 1);
        assert!(!mismatched.all_accounted_for());
    }

    /// The header count and the list under it read one classification. The
    /// case that split them: a destination file of the wrong size whose source
    /// is also gone — counted as a size mismatch, but admitted by a
    /// separately-spelled "lost" filter that only asked whether the source was
    /// missing and the destination not good.
    #[test]
    fn every_entry_lands_in_exactly_one_class() {
        let (mut conn, dir, manifest, archive) = status_fixture(&["settled.jpg", "broken.jpg"]);
        std::fs::write(archive.join("settled.jpg"), b"aaaa").unwrap();
        std::fs::write(archive.join("broken.jpg"), b"aa").unwrap();
        std::fs::remove_file(dir.path().join("photos").join("broken.jpg")).unwrap();

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert_eq!(status.at_destination, 1);
        assert_eq!(status.size_mismatch, 1);
        assert_eq!(
            status.source_lost, 0,
            "a wrong-size destination is not lost"
        );

        let counted = |class| status.entries.iter().filter(|e| e.status == class).count();
        assert_eq!(counted(EntryStatus::AtDestination), status.at_destination);
        assert_eq!(counted(EntryStatus::SizeMismatch), status.size_mismatch);
        assert_eq!(counted(EntryStatus::Pending), status.pending);
        assert_eq!(counted(EntryStatus::SourceLost), status.source_lost);
        assert_eq!(
            status.at_destination + status.size_mismatch + status.pending + status.source_lost,
            status.entries.len(),
            "the classes partition the entries"
        );
    }

    #[test]
    fn compute_manifest_status_flags_a_lock_file_that_no_longer_matches() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        let fresh = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(matches!(
            fresh.lock,
            LockStanding::Read {
                hash_matches: true,
                ..
            }
        ));

        // Change the lock without touching the manifest's recorded hash. The
        // entries stay parseable so the read still reaches the comparison —
        // which means duplicating the *entries*, not the whole file: a second
        // header line partway down is a malformed lock, and the read would
        // then fail on that instead of reaching the hash comparison.
        let lock = dir.path().join("cluster.lock");
        let content = std::fs::read_to_string(&lock).unwrap();
        let entries: String = content.lines().skip(1).map(|l| format!("{l}\n")).collect();
        std::fs::write(&lock, format!("{content}{entries}")).unwrap();

        let tampered = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(matches!(
            tampered.lock,
            LockStanding::Read {
                hash_matches: false,
                ..
            }
        ));
    }

    // =========================================================================
    // The doors a lock reaches behind
    // =========================================================================

    /// A lock entry standing on a closed root is found, counted per root, and
    /// the root is named — which is what both surfaces reading this need:
    /// status to state it in apply's conditional, apply to refuse before it
    /// asks the user to proceed. A refresh reads none of it.
    fn entry(id: i64, root_id: i64) -> LockEntry {
        LockEntry {
            id,
            root_id,
            path: format!("/photos/{id}.jpg"),
            device: 1,
            inode: id,
            size: 10,
            mtime: 0,
            partial_hash: "ph".to_string(),
            object_id: None,
            hash_type: None,
            hash_value: None,
            scope_rel_path: None,
        }
    }

    #[test]
    fn parked_lock_entries_counts_one_line_per_door() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/photos", "source", true);
        let live = insert_root(&conn, "/live", "source", false);
        let a = crate::core::testing::insert_source(&conn, parked, "a.jpg", None);
        let b = crate::core::testing::insert_source(&conn, parked, "b.jpg", None);
        let c = crate::core::testing::insert_source(&conn, live, "c.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();

        let doors = parked_lock_entries(
            &conn,
            &[entry(a, parked), entry(b, parked), entry(c, live)],
            &roots,
        )
        .unwrap();

        assert_eq!(doors.len(), 1, "one door: {doors:?}");
        assert_eq!(doors[0].0.root_path, "/photos");
        assert_eq!(doors[0].1, 2);
    }

    /// Asked of the **index**, not of the lock's own recorded root: a lock is
    /// a snapshot of where a source stood when the selection was made, and the
    /// door is a question about where it stands now. An entry whose source has
    /// left the index has no standing left to read and contributes nothing.
    #[test]
    fn parked_lock_entries_reads_the_index_rather_than_the_lock() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/photos", "source", true);
        let live = insert_root(&conn, "/live", "source", false);
        let moved = crate::core::testing::insert_source(&conn, parked, "a.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();

        // The lock says this entry stood on the live root; the index says it
        // stands behind a door now.
        let doors = parked_lock_entries(&conn, &[entry(moved, live)], &roots).unwrap();
        assert_eq!(doors.len(), 1);
        assert_eq!(doors[0].0.root_path, "/photos");

        // And an entry the index no longer knows says nothing either way.
        assert!(parked_lock_entries(&conn, &[entry(9999, parked)], &roots)
            .unwrap()
            .is_empty());
    }
}
