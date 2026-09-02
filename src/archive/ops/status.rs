//! Reading a manifest's status — what has landed, and what is still waiting.
//!
//! A diagnostic read: each lock entry is compared against its source and its
//! destination on disk, and what is found is reported. Nothing is moved and
//! nothing is recorded.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::core::ops::scope::resolve_recorded_scope;
use crate::core::repo::{self, Connection};
use crate::expr::prefetch_pattern_facts;

use super::manifest::{read_lock_file, read_manifest_config};

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
    /// Number of entries in the lock file.
    pub lock_entry_count: usize,
    /// Whether the lock file hash matches the manifest.
    pub lock_hash_valid: bool,
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
    /// Whether the lock file predates the recorded measurement, and so is
    /// refused by `apply` outright, whatever the pattern says.
    pub lock_predates_measurement: bool,
    /// Whether the pattern failed to expand for any entry, and so would fail
    /// for the whole run: `apply` collects expansion failures and aborts on
    /// them before it transfers anything.
    ///
    /// Together with the flag above this is the whole answer to *can `apply`
    /// run at all* — which is the question a report must ask before naming it,
    /// and which the lock's own age does not answer on its own.
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

    // 1. Read manifest and lock
    let config = read_manifest_config(manifest_path)?;
    let lock_path = manifest_path.with_extension("lock");
    let lock = read_lock_file(&lock_path)?;
    let lock_predates_measurement = lock.header.is_none();
    let unmeasured = lock.unmeasured_reason();
    let lock_entries = lock.entries;
    let lock_entry_count = lock_entries.len();

    // 2. Validate lock hash (non-fatal)
    let lock_hash_valid = match crate::core::ops::fs::compute_full_hash(&lock_path) {
        Ok(actual_hash) => actual_hash == config.meta.lock_hash,
        Err(_) => false,
    };

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

    // 5. Parse pattern and fetch needed facts
    let pattern = parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = extract_fact_keys(&pattern);
    // The manifest's own scope, through the same resolution `cluster generate`
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
    let mut expansion_failures: Vec<String> = Vec::new();
    let mut expansion_failure_count = 0usize;
    let mut entries = Vec::with_capacity(lock_entry_count);
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(lock_entry_count);

    for lock_entry in &lock_entries {
        let dest_rel = match evaluate_pattern(&pattern, lock_entry, unmeasured, &root_paths, &facts)
        {
            Ok(rel) => rel,
            Err(e) => {
                // If pattern expansion fails, we can't determine dest path.
                // Use a placeholder and mark as not-at-dest. It is also the
                // whole run's answer: apply aborts on expansion failures
                // before it transfers anything, so a report that has seen one
                // must not go on to name apply.
                pattern_unexpandable = true;
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
        lock_hash_valid,
        entries,
        at_destination,
        pending,
        source_lost,
        size_mismatch,
        source_still_present,
        unrooted_scope: scope.unrooted().to_vec(),
        set_aside_scope: scope.set_aside().to_vec(),
        lock_predates_measurement,
        pattern_unexpandable,
        expansion_failures,
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

    /// An absent measurement has two causes and a report must not blame the
    /// wrong one. A manifest recording no scope measures nothing — correctly,
    /// there is nothing to measure from — in a perfectly **current** lock. So
    /// the message must name the manifest, not the lock's age, and must not
    /// prescribe a refresh that would rebuild the identical lock.
    ///
    /// The `apply` side of this is `an_unscoped_manifest_says_so_rather_than_blaming_the_lock`;
    /// it refuses and moves nothing, but prints the cause on stderr. Here the
    /// same string is data.
    #[test]
    fn an_unscoped_manifest_blames_the_manifest_not_the_lock() {
        let (mut conn, _dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        rewrite_manifest_pattern(&manifest, "{scope.rel_path}");

        let status = compute_manifest_status(&mut conn, &manifest).unwrap();

        assert!(
            !status.lock_predates_measurement,
            "the lock is a current one; its age explains nothing here"
        );
        assert!(
            status.pattern_unexpandable,
            "the run cannot expand, which is what a next-step hint must ask"
        );
        let dest = &status.entries[0].dest_path;
        assert!(dest.contains("records no scope"), "{dest}");
        assert!(
            !dest.contains("cluster refresh"),
            "a refresh cannot give an unscoped manifest a scope: {dest}"
        );
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
                let end = start + text[start..].find(']').expect("unterminated scope array") + 1;
                format!("{}{line}{}", &text[..start], &text[end..])
            }
            None => {
                let at = text
                    .find("\ngenerated_at = ")
                    .expect("no [meta] table to write a scope into")
                    + 1;
                format!("{}{line}\n{}", &text[..at], &text[at..])
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
        assert!(fresh.lock_hash_valid);

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
        assert!(!tampered.lock_hash_valid);
    }
}
