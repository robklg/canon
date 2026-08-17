//! The hash pipeline: computing full content hashes for files collected
//! during the walk, linking each to its content object.

use std::path::Path;

use anyhow::Result;
use rusqlite::{Transaction, TransactionBehavior};

use crate::core::repo::{self, Connection};
use crate::scan::repo as scan_repo;

use super::types::{current_timestamp, FileToHash};

/// Observability for the hash pipeline. The interface implements this
/// to display progress bars, emit warnings, etc.
pub trait HashProgress {
    fn on_start(&self, total: usize);
    fn on_hash(&self, index: usize, path: &Path);
    fn on_hash_error(&self, path: &Path, error: &str);
    fn on_unexpected_change(&self, path: &Path);
    fn on_finish(&self);
}

/// Result of the hash pipeline.
#[derive(Default)]
pub struct HashStats {
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
    pub errors: u64,
}

/// Hash files collected during scan, linking each to its content object.
///
/// For each file: computes full SHA256, creates/looks up the object,
/// links the source, stores the hash fact. Each file is wrapped in its
/// own Immediate transaction for atomicity without blocking concurrent
/// processes for long periods.
///
/// Individual hash I/O errors are reported via `progress` and skipped
/// (not fatal). DB/transaction errors propagate as `Err`.
pub fn hash_files(
    conn: &Connection,
    files: &[FileToHash],
    progress: &dyn HashProgress,
) -> Result<HashStats> {
    if files.is_empty() {
        return Ok(HashStats::default());
    }

    progress.on_start(files.len());

    let mut stats = HashStats::default();

    for (i, file) in files.iter().enumerate() {
        progress.on_hash(i, &file.full_path);

        // Compute full SHA256 hash
        let hash_value = match crate::core::ops::fs::compute_full_hash(&file.full_path) {
            Ok(h) => h,
            Err(e) => {
                progress.on_hash_error(&file.full_path, &format!("{:#}", e));
                stats.errors += 1;
                continue;
            }
        };

        // Wrap object creation + source linking + fact storage in a single
        // transaction for atomicity. Uses Immediate for reliable busy-handler
        // support under concurrency.
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

        let new_object = get_or_create_object(&tx, "sha256", &hash_value)?;

        // Check for unexpected hash change (only if basis didn't change and file had existing hash)
        if !file.basis_changed {
            if let Some(old_oid) = file.old_object_id {
                if old_oid != new_object.id {
                    progress.on_unexpected_change(&file.full_path);
                    stats.unexpected_hash_changes += 1;
                }
            }
        }

        repo::source::set_object_id(&tx, file.source_id, new_object.id)?;

        scan_repo::fact::store_object_fact(
            &tx,
            new_object.id,
            "content.hash.sha256",
            &hash_value,
            current_timestamp(),
        )?;

        tx.commit()?;

        stats.hashed += 1;
    }

    progress.on_finish();

    Ok(stats)
}

/// Get or create an object by hash, returning the Object.
fn get_or_create_object(
    conn: &Connection,
    hash_type: &str,
    hash_value: &str,
) -> Result<crate::core::domain::object::Object> {
    repo::object::get_or_create(conn, hash_type, hash_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Records every `HashProgress` callback so tests can assert the pipeline
    /// reported what it did.
    #[derive(Default)]
    struct RecordingHashProgress {
        started: std::cell::Cell<usize>,
        hashed: std::cell::Cell<usize>,
        errors: std::cell::Cell<usize>,
        unexpected: std::cell::Cell<usize>,
        finished: std::cell::Cell<bool>,
    }

    impl HashProgress for RecordingHashProgress {
        fn on_start(&self, total: usize) {
            self.started.set(total);
        }
        fn on_hash(&self, _index: usize, _path: &Path) {
            self.hashed.set(self.hashed.get() + 1);
        }
        fn on_hash_error(&self, _path: &Path, _error: &str) {
            self.errors.set(self.errors.get() + 1);
        }
        fn on_unexpected_change(&self, _path: &Path) {
            self.unexpected.set(self.unexpected.get() + 1);
        }
        fn on_finish(&self) {
            self.finished.set(true);
        }
    }

    /// Root + one indexed source backed by a real file, ready to hash.
    fn hashable_source(
        conn: &Connection,
        temp: &TempDir,
        name: &str,
        content: &str,
    ) -> (i64, PathBuf) {
        let root_path = temp.path().to_str().unwrap();
        let root_id = repo::insert_test_root(conn, root_path, "source", false);
        let path = temp.path().join(name);
        std::fs::write(&path, content).unwrap();
        // The hash pass reads only the source id and the path; the location
        // metadata is placeholder, so these tests stay independent of the
        // walk's own fixtures.
        let source_id = repo::insert_test_source(conn, root_id, name, 1, 1, 0, 0);
        (source_id, path)
    }

    #[test]
    fn hash_files_empty_input_returns_default() {
        // Nothing to hash means no work and no progress chatter — the caller
        // must not see a 0-of-0 progress bar start and finish.
        let conn = repo::open_in_memory_for_test();
        let progress = RecordingHashProgress::default();

        let stats = hash_files(&conn, &[], &progress).unwrap();

        assert_eq!(stats.hashed, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.unexpected_hash_changes, 0);
        assert_eq!(progress.started.get(), 0);
        assert!(!progress.finished.get());
    }

    #[test]
    fn hash_files_links_object_and_stores_fact() {
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let expected = crate::core::ops::fs::compute_full_hash(&full_path).unwrap();
        let progress = RecordingHashProgress::default();

        let stats = hash_files(
            &conn,
            &[FileToHash {
                source_id,
                full_path,
                old_object_id: None,
                basis_changed: true,
            }],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.hashed, 1);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.unexpected_hash_changes, 0);
        assert_eq!(progress.started.get(), 1);
        assert_eq!(progress.hashed.get(), 1);
        assert!(progress.finished.get());

        // The source is linked to an object carrying the computed hash...
        let (object_id, hash_type, hash_value): (i64, String, String) = conn
            .query_row(
                "SELECT o.id, o.hash_type, o.hash_value
                 FROM sources s JOIN objects o ON s.object_id = o.id
                 WHERE s.id = ?",
                [source_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(hash_type, "sha256");
        assert_eq!(hash_value, expected);

        // ...and the same hash is stored as an object fact.
        let fact: String = conn
            .query_row(
                "SELECT value_text FROM facts
                 WHERE entity_type = 'object' AND entity_id = ? AND key = 'content.hash.sha256'",
                [object_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fact, expected);
    }

    #[test]
    fn hash_files_detects_unexpected_change() {
        // The file's content hash differs from the object it was linked to, and
        // nothing about its basis (size/mtime) changed to explain it — silent
        // corruption or an out-of-band edit, which the user must hear about.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let stale = repo::object::get_or_create(&conn, "sha256", "stalehash").unwrap();
        let progress = RecordingHashProgress::default();

        let stats = hash_files(
            &conn,
            &[FileToHash {
                source_id,
                full_path,
                old_object_id: Some(stale.id),
                basis_changed: false,
            }],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.hashed, 1);
        assert_eq!(stats.unexpected_hash_changes, 1);
        assert_eq!(progress.unexpected.get(), 1);
    }

    #[test]
    fn hash_files_no_unexpected_change_when_basis_changed() {
        // Same mismatch, but the file's size/mtime moved — a new hash is exactly
        // what a modified file should produce, so it is not reported.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let stale = repo::object::get_or_create(&conn, "sha256", "stalehash").unwrap();
        let progress = RecordingHashProgress::default();

        let stats = hash_files(
            &conn,
            &[FileToHash {
                source_id,
                full_path,
                old_object_id: Some(stale.id),
                basis_changed: true,
            }],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.hashed, 1);
        assert_eq!(stats.unexpected_hash_changes, 0);
        assert_eq!(progress.unexpected.get(), 0);
    }

    #[test]
    fn hash_files_io_error_is_counted_not_fatal() {
        // A file that vanished between the walk and the hash pass is reported and
        // skipped; the rest of the batch still gets hashed.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let progress = RecordingHashProgress::default();

        let stats = hash_files(
            &conn,
            &[
                FileToHash {
                    source_id,
                    full_path: temp.path().join("vanished.jpg"),
                    old_object_id: None,
                    basis_changed: true,
                },
                FileToHash {
                    source_id,
                    full_path,
                    old_object_id: None,
                    basis_changed: true,
                },
            ],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.errors, 1);
        assert_eq!(stats.hashed, 1);
        assert_eq!(progress.errors.get(), 1);
        assert!(progress.finished.get());
    }
}
