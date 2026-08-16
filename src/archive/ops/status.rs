//! Reading a manifest's status — what has landed, and what is still waiting.
//!
//! A diagnostic read: each lock entry is compared against its source and its
//! destination on disk, and what is found is reported. Nothing is moved and
//! nothing is recorded.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::core::repo::{self, Connection};

use super::manifest::{read_lock_entries, read_manifest_config};

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
    /// Whether the destination file exists on disk.
    pub dest_exists: bool,
    /// Whether dest size matches expected size (only meaningful when dest_exists).
    pub dest_size_match: bool,
    /// Whether the destination is registered in the DB (present=1).
    pub db_registered: bool,
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
    use crate::expr;

    // 1. Read manifest and lock
    let config = read_manifest_config(manifest_path)?;
    let lock_path = manifest_path.with_extension("lock");
    let lock_entries = read_lock_entries(&lock_path)?;
    let lock_entry_count = lock_entries.len();

    // 2. Validate lock hash (non-fatal)
    let lock_hash_valid = match crate::ops::fs::compute_full_hash(&lock_path) {
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
    let pattern = expr::parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = expr::extract_fact_keys(&pattern);
    let scope_prefix = config.meta.scope.as_deref();

    // Batch fetch facts for all lock entries if pattern uses content facts
    let source_ids: Vec<i64> = lock_entries.iter().map(|s| s.id).collect();
    let mut all_facts: HashMap<i64, Vec<crate::core::domain::fact::FactEntry>> = HashMap::new();
    for key in &needed_keys {
        // Must list the same namespaces as the fetch and evaluation sites in
        // the apply operation — a namespace listed in one place and not the
        // others changes which facts reach pattern evaluation.
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

    // 6. Evaluate patterns to get dest paths, then check filesystem + DB
    let mut entries = Vec::with_capacity(lock_entry_count);
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(lock_entry_count);

    for lock_entry in &lock_entries {
        let dest_rel = match evaluate_pattern(
            &pattern,
            lock_entry,
            &needed_keys,
            scope_prefix,
            &root_paths,
            &all_facts,
        ) {
            Ok(rel) => rel,
            Err(e) => {
                // If pattern expansion fails, we can't determine dest path.
                // Use a placeholder and mark as not-at-dest.
                let filename = Path::new(&lock_entry.path)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| lock_entry.path.clone());
                entries.push(StatusEntry {
                    source_path: lock_entry.path.clone(),
                    source_filename: filename,
                    source_exists: Path::new(&lock_entry.path).exists(),
                    dest_path: format!("<pattern expansion failed: {}>", e),
                    dest_exists: false,
                    dest_size_match: false,
                    db_registered: false,
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
            dest_exists,
            dest_size_match,
            db_registered: false, // filled in below
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
        if entry.dest_exists && entry.dest_size_match {
            at_destination += 1;
            if entry.source_exists {
                source_still_present += 1;
            }
        } else if entry.dest_exists && !entry.dest_size_match {
            size_mismatch += 1;
        } else if entry.source_exists {
            pending += 1;
        } else {
            source_lost += 1;
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
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

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        let manifest_path = dir.path().join("cluster.toml");
        let params = ExecuteGenerateParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            expanded_filters: vec![],
            original_filters: vec![],
            scope_prefixes: vec![],
            archive_root_id: archive_root,
            base_dir: String::new(),
            allow: vec![],
        };
        execute_generate(&plan, &params).unwrap();

        (conn, dir, manifest_path, archive_dir)
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

    #[test]
    fn compute_manifest_status_flags_a_lock_file_that_no_longer_matches() {
        let (mut conn, dir, manifest, _archive) = status_fixture(&["a.jpg"]);
        let fresh = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(fresh.lock_hash_valid);

        // Change the lock without touching the manifest's recorded hash. The
        // entries stay parseable so the read still reaches the comparison.
        let lock = dir.path().join("cluster.lock");
        let content = std::fs::read_to_string(&lock).unwrap();
        std::fs::write(&lock, format!("{content}{content}")).unwrap();

        let tampered = compute_manifest_status(&mut conn, &manifest).unwrap();
        assert!(!tampered.lock_hash_valid);
    }
}
