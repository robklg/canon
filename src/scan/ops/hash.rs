//! The hash pipeline: computing full content hashes for files collected
//! during the walk, linking each to its content object.

use std::path::Path;

use anyhow::Result;
use rusqlite::{Transaction, TransactionBehavior};

use crate::core::domain::scope::DecisionScope;
use crate::core::repo::{self, Connection};
use crate::scan::domain::{outermost_scopes, StandingDebt};
use crate::scan::repo as scan_repo;

use super::types::{current_timestamp, FileToHash, HashNeed, HashPassResult, HashStats};

/// Observability for the hash pipeline. The interface implements this
/// to display progress bars, emit warnings, etc.
pub trait HashProgress {
    fn on_start(&self, total: usize);
    fn on_hash(&self, index: usize, path: &Path);
    fn on_hash_error(&self, path: &Path, error: &str);
    fn on_unexpected_change(&self, path: &Path);
    fn on_finish(&self);
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
                // The id, not just a tally: what the summary states is how much
                // of the debt still standing at the end of the scan is content
                // this pass could not read, and only the debt query can say
                // that.
                stats.errored_ids.push(file.source_id);
                continue;
            }
        };

        // Wrap object creation + source linking + fact storage in a single
        // transaction for atomicity. Uses Immediate for reliable busy-handler
        // support under concurrency.
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

        let new_object = get_or_create_object(&tx, "sha256", &hash_value)?;

        // Both readings of the walk's own verdict, taken here rather than
        // re-derived: a changed hash is expected exactly when the file was
        // admitted for a changed basis, and debt is paid off exactly when a
        // file admitted for backlog is successfully read. The matches are
        // exhaustive, so a future reason for hashing cannot be added without
        // answering both questions.
        let basis_changed = match file.need {
            HashNeed::Basis => true,
            HashNeed::Backlog | HashNeed::Reverify => false,
        };
        if !basis_changed {
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
        match file.need {
            HashNeed::Backlog => stats.backlog_hashed += 1,
            HashNeed::Basis | HashNeed::Reverify => {}
        }
    }

    progress.on_finish();

    Ok(stats)
}

/// Run the hash pass and then ask what debt remains — one call, because the
/// order is the whole meaning of the number. Asked first, the count would say
/// what was owed before the scan paid; asked after, it says what a declined,
/// failed, or interrupted pay-down actually left behind.
///
/// Safe to call with nothing queued: `--no-hash` runs no pass and still gets
/// its answer.
pub fn run_hash_pass(
    conn: &Connection,
    files: &[FileToHash],
    walked: &[DecisionScope],
    progress: &dyn HashProgress,
) -> Result<HashPassResult> {
    let stats = hash_files(conn, files, progress)?;
    let standing_debt = count_standing_debt(conn, walked, &stats.errored_ids)?;

    Ok(HashPassResult {
        stats,
        standing_debt,
    })
}

/// How much content this scan spoke about still holds no identity.
///
/// Asked once, after the hash pass, so the number is what stands when the scan
/// is over rather than what stood before it paid: under `--no-hash` it is the
/// debt the user declined to pay, and after a hashing scan it is the residue a
/// failed or interrupted pay-down left behind. Either way the scan states it
/// instead of leaving the omission to be discovered months later by an archive
/// operation that cannot see the content.
///
/// Bounded to the scopes the scan walked — a scan speaks about its own scope —
/// and narrowed to the outermost of them, so overlapping paths on one command
/// line cannot report the same standing source twice.
///
/// `unreadable` names the sources this scan's pass could not read. Each scope
/// answers for its own share of them in the same statement that counts its
/// debt, so the qualifier the summary prints is always a part of the number it
/// qualifies — a file that failed and was hashed a moment later by a concurrent
/// scan leaves both counts together, and one that failed outside the walked
/// scopes enters neither.
pub fn count_standing_debt(
    conn: &Connection,
    walked: &[DecisionScope],
    unreadable: &[i64],
) -> Result<StandingDebt> {
    let mut debt = StandingDebt::default();

    for scope in outermost_scopes(walked) {
        let prefix = Some(scope.rel_prefix.as_str()).filter(|p| !p.is_empty());
        debt.add(scan_repo::source::count_unhashed(
            conn,
            scope.root_id,
            prefix,
            unreadable,
        )?);
    }

    Ok(debt)
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
        assert_eq!(stats.errored_ids.len(), 0);
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
                need: HashNeed::Basis,
            }],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.hashed, 1);
        assert_eq!(stats.errored_ids.len(), 0);
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
                need: HashNeed::Reverify,
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
                need: HashNeed::Basis,
            }],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.hashed, 1);
        assert_eq!(stats.unexpected_hash_changes, 0);
        assert_eq!(progress.unexpected.get(), 0);
    }

    /// A root with a source at `rel_path`, hashed or not, ready to be counted.
    fn indexed_source(conn: &Connection, root_path: &str, rel_path: &str, hashed: bool) -> i64 {
        let root_id = repo::root::fetch_all(conn)
            .unwrap()
            .into_iter()
            .find(|r| r.path == root_path)
            .map(|r| r.id)
            .unwrap_or_else(|| repo::insert_test_root(conn, root_path, "source", false));
        let source_id = repo::insert_test_source(conn, root_id, rel_path, 1, 1, 0, 0);
        if hashed {
            let object = repo::object::get_or_create(conn, "sha256", rel_path).unwrap();
            repo::source::set_object_id(conn, source_id, object.id).unwrap();
        }
        root_id
    }

    fn object_of(conn: &Connection, source_id: i64) -> Option<i64> {
        conn.query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn scope(root_id: i64, root_path: &str, rel_prefix: &str) -> DecisionScope {
        DecisionScope::new(root_id, root_path.to_string(), rel_prefix.to_string())
    }

    #[test]
    fn a_backlog_file_that_could_not_be_read_was_not_paid_off() {
        // The backlog count qualifies the hashed count — the summary prints it
        // as "Hashed N files (M from backlog)" — so it must never exceed it.
        // Counting at queue time would: a pay-down of two files where one is
        // unreadable would report one file hashed, two of them from the
        // backlog, and that impossible sentence is what the durable decision
        // record would carry.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (readable, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let root_id = repo::root::fetch_all(&conn).unwrap()[0].id;
        let unreadable = repo::insert_test_source(&conn, root_id, "gone.jpg", 1, 1, 0, 0);

        let stats = hash_files(
            &conn,
            &[
                FileToHash {
                    source_id: unreadable,
                    full_path: temp.path().join("gone.jpg"),
                    old_object_id: None,
                    need: HashNeed::Backlog,
                },
                FileToHash {
                    source_id: readable,
                    full_path,
                    old_object_id: None,
                    need: HashNeed::Backlog,
                },
            ],
            &RecordingHashProgress::default(),
        )
        .unwrap();

        assert_eq!((stats.hashed, stats.errored_ids.len()), (1, 1));
        assert_eq!(stats.backlog_hashed, 1, "one was paid off, not two");
        assert!(stats.backlog_hashed <= stats.hashed);
    }

    #[test]
    fn only_debt_counts_as_debt_paid() {
        // The other two reasons for hashing are not pay-downs: a new file is
        // work the walk caused, and a --verify re-read is work the user asked
        // for. Neither is a surprise needing explanation.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let (basis, basis_path) = hashable_source(&conn, &temp, "new.jpg", "fresh");
        let root_id = repo::root::fetch_all(&conn).unwrap()[0].id;
        let reverify_path = temp.path().join("known.jpg");
        std::fs::write(&reverify_path, "known").unwrap();
        let reverify = repo::insert_test_source(&conn, root_id, "known.jpg", 1, 1, 0, 0);

        let stats = hash_files(
            &conn,
            &[
                FileToHash {
                    source_id: basis,
                    full_path: basis_path,
                    old_object_id: None,
                    need: HashNeed::Basis,
                },
                FileToHash {
                    source_id: reverify,
                    full_path: reverify_path,
                    old_object_id: None,
                    need: HashNeed::Reverify,
                },
            ],
            &RecordingHashProgress::default(),
        )
        .unwrap();

        assert_eq!(stats.hashed, 2);
        assert_eq!(stats.backlog_hashed, 0);
    }

    #[test]
    fn count_standing_debt_sums_what_every_walked_root_still_owes() {
        let conn = repo::open_in_memory_for_test();
        let a = indexed_source(&conn, "/photos", "one.jpg", false);
        indexed_source(&conn, "/photos", "two.jpg", false);
        indexed_source(&conn, "/photos", "known.jpg", true);
        let b = indexed_source(&conn, "/backup", "three.jpg", false);

        let walked = [scope(a, "/photos", ""), scope(b, "/backup", "")];
        assert_eq!(count_standing_debt(&conn, &walked, &[]).unwrap().total, 3);

        // A root the scan never walked is not this scan's subject: its debt is
        // real, but claiming it here would make the number say something the
        // scan did not observe.
        assert_eq!(
            count_standing_debt(&conn, &walked[..1], &[]).unwrap().total,
            2,
            "a scan speaks about the scope it walked"
        );
    }

    #[test]
    fn count_standing_debt_counts_an_overlapping_scan_once() {
        // `canon scan photos photos/2024` walks the inner path twice. The event
        // counters live with that; a count of what *stands* must not, or the
        // scan reports debt the universe does not carry.
        let conn = repo::open_in_memory_for_test();
        let root_id = indexed_source(&conn, "/photos", "2024/a.jpg", false);
        indexed_source(&conn, "/photos", "2023/b.jpg", false);

        let walked = [
            scope(root_id, "/photos", "2024"),
            scope(root_id, "/photos", ""),
        ];
        assert_eq!(count_standing_debt(&conn, &walked, &[]).unwrap().total, 2);

        let scoped = [scope(root_id, "/photos", "2024")];
        assert_eq!(count_standing_debt(&conn, &scoped, &[]).unwrap().total, 1);
    }

    #[test]
    fn the_debt_a_scan_reports_is_what_survived_its_pay_down() {
        // Order is the number's whole meaning. Asked before the pass, this
        // would report the debt the scan was about to clear — a scan that fixed
        // the problem announcing the problem. The pass and the count are one
        // call so the question cannot be asked early.
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let root_id = repo::root::fetch_all(&conn).unwrap()[0].id;

        let pass = run_hash_pass(
            &conn,
            &[FileToHash {
                source_id,
                full_path,
                old_object_id: None,
                need: HashNeed::Backlog,
            }],
            &[scope(root_id, &root_path, "")],
            &RecordingHashProgress::default(),
        )
        .unwrap();

        assert_eq!(pass.stats.hashed, 1);
        assert_eq!(
            pass.standing_debt.total, 0,
            "the debt it just paid is not debt"
        );
    }

    #[test]
    fn a_scan_that_hashes_nothing_still_reports_what_stands() {
        // --no-hash queues nothing, and must still answer the question: opting
        // out of paying is not opting out of being told.
        let conn = repo::open_in_memory_for_test();
        let root_id = indexed_source(&conn, "/photos", "one.jpg", false);
        indexed_source(&conn, "/photos", "two.jpg", false);

        let pass = run_hash_pass(
            &conn,
            &[],
            &[scope(root_id, "/photos", "")],
            &RecordingHashProgress::default(),
        )
        .unwrap();

        assert_eq!(pass.stats.hashed, 0);
        assert_eq!(pass.standing_debt.total, 2);
    }

    #[test]
    fn count_standing_debt_is_what_the_hash_pass_left_behind() {
        // Asked after the pay-down, so a failed hash is visible as debt still
        // standing rather than hidden behind a scan that "ran with hashing on".
        let conn = repo::open_in_memory_for_test();
        let temp = TempDir::new().unwrap();
        let root_path = temp.path().to_str().unwrap().to_string();
        let (source_id, full_path) = hashable_source(&conn, &temp, "photo.jpg", "content");
        let root_id = repo::root::fetch_all(&conn).unwrap()[0].id;
        let walked = [scope(root_id, &root_path, "")];
        assert_eq!(
            count_standing_debt(&conn, &walked, &[]).unwrap().total,
            1,
            "photo.jpg is the only row yet, and it owes"
        );

        // A second row joins, for a file that is not on disk at all: one hashes,
        // one cannot be read. The debt count lands on 1 both times, but never
        // the same 1 — photo.jpg's debt is paid and gone.jpg's takes its place.
        let stats = hash_files(
            &conn,
            &[
                FileToHash {
                    source_id: repo::insert_test_source(&conn, root_id, "gone.jpg", 1, 1, 0, 0),
                    full_path: temp.path().join("gone.jpg"),
                    old_object_id: None,
                    need: HashNeed::Basis,
                },
                FileToHash {
                    source_id,
                    full_path,
                    old_object_id: None,
                    need: HashNeed::Basis,
                },
            ],
            &RecordingHashProgress::default(),
        )
        .unwrap();

        assert_eq!((stats.hashed, stats.errored_ids.len()), (1, 1));
        assert_eq!(
            count_standing_debt(&conn, &walked, &[]).unwrap().total,
            1,
            "now it is gone.jpg that owes: the file that could not be hashed is \
             still content Canon cannot see"
        );
        assert!(
            object_of(&conn, source_id).is_some(),
            "and photo.jpg's debt really was cleared, not merely displaced in the count"
        );
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
                    need: HashNeed::Basis,
                },
                FileToHash {
                    source_id,
                    full_path,
                    old_object_id: None,
                    need: HashNeed::Basis,
                },
            ],
            &progress,
        )
        .unwrap();

        assert_eq!(stats.errored_ids.len(), 1);
        assert_eq!(stats.hashed, 1);
        assert_eq!(progress.errors.get(), 1);
        assert!(progress.finished.get());
    }
}
