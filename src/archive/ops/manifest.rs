//! Reading and writing the two files a cluster produces.
//!
//! The manifest is TOML the user edits; the lock file is JSONL recording what
//! the run settled — a header line carrying the resolved scope, then the
//! sources as they stood, each with the scope-relative path it was measured
//! to. Generating writes the pair; the status read and apply consume it. The
//! accessors sit together because the two files are one on-disk format — a
//! change to either is almost always a change to both. Apply reads the
//! *manifest* text itself, because it reads the notes at the bottom of it a
//! second time, but it parses through the same domain door this does; the lock
//! it reads through here.

use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::archive::domain::{parse_manifest_config, LockEntry, LockHeader, ManifestConfig};
use crate::expr::{Pattern, Unmeasured};

/// Write content to a file and fsync to ensure it's flushed to disk.
/// This prevents race conditions when opening the file in an editor
/// immediately after writing, especially on network volumes (NAS/SMB).
pub(super) fn write_and_sync(path: &Path, content: &str) -> Result<()> {
    use std::io::Write as _;
    let mut file = fs::File::create(path)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Write a JSONL lock file: the header first, then one line per entry.
///
/// Synced to disk like the manifest beside it, and for the same reason twice
/// over: the manifest is written second and carries a hash of these bytes, so
/// a manifest that survives a power loss must not outlive the lock it names.
pub(super) fn write_lock_file(
    lock_path: &Path,
    header: &LockHeader,
    entries: &[LockEntry],
) -> Result<()> {
    let lock_file = std::fs::File::create(lock_path)
        .with_context(|| format!("Failed to create lock file: {}", lock_path.display()))?;
    let mut writer = BufWriter::new(lock_file);

    serde_json::to_writer(&mut writer, header).context("Failed to write lock file header")?;
    writeln!(writer)?;

    for entry in entries {
        serde_json::to_writer(&mut writer, entry)
            .with_context(|| format!("Failed to write lock entry for {}", entry.path))?;
        writeln!(writer)?;
    }

    writer.flush()?;
    writer
        .into_inner()
        .map_err(|e| anyhow::anyhow!("Failed to flush lock file: {e}"))?
        .sync_all()
        .with_context(|| format!("Failed to sync lock file: {}", lock_path.display()))?;
    Ok(())
}

/// Read and parse a manifest TOML config file.
///
/// The gate-before-shape ordering belongs to the domain parser this calls; all
/// that is added here is the file read.
pub fn read_manifest_config(manifest_path: &Path) -> Result<ManifestConfig> {
    let config_content = fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "Failed to read manifest config: {}",
            manifest_path.display()
        )
    })?;
    parse_manifest_config(&config_content, manifest_path)
}

/// A lock file as read back: the header it declares, and its entries.
#[derive(Debug)]
pub struct LockFile {
    /// `None` for a lock written before the header existed. Nothing guesses
    /// past that — what an old lock costs is the reader's own disposition.
    pub header: Option<LockHeader>,
    pub entries: Vec<LockEntry>,
}

impl LockFile {
    /// Why an entry that carries no measurement carries none.
    ///
    /// The two causes are indistinguishable from an entry — both are an
    /// absent field — and they take different answers, so the question is
    /// asked of the **file**, which can tell them apart. No header at all
    /// means the lock predates the field. A header whose scope is empty means
    /// the lock was built while the manifest declared no scope that confirmed,
    /// so there was nothing to measure from then — a refresh changes that
    /// exactly when the manifest has since come to declare one that does,
    /// which is the decision table's question, not this file's.
    ///
    /// A header whose scope is *not* empty leaves no unmeasured entries at
    /// all — and that rests on one fact, which is why it is worth naming:
    /// a run selects from `ScopeResolution::selection`, the **same** confirmed
    /// register the header is built from and the vantage measures from. Select from the recorded
    /// list instead and the claim breaks, because an unrooted prefix can be an
    /// *ancestor* of a known root: `path_is_under` matches it where
    /// `find_containing_root` does not, so it gathers sources no vantage can
    /// measure, in a lock whose header is not empty. This arm needs no third
    /// answer only while those two registers stay the same one.
    pub fn unmeasured_reason(&self) -> Unmeasured {
        match &self.header {
            None => Unmeasured::LockPredatesMeasurement,
            Some(_) => Unmeasured::NoScopeRecorded,
        }
    }

    /// Whether the pattern places files relative to a scope this lock cannot
    /// place them from.
    ///
    /// The one fact that tells the two expansion failures apart, and they take
    /// opposite advice: a scope the manifest lacks is repaired by scoping it
    /// or by `{source.rel_path}`, a fact no source carries only by the fact.
    /// Both commands that meet the state read it, so it is asked once, here,
    /// and of the **whole file** — like the reason above it. Two things follow.
    /// The pattern half is the language facility's own reading
    /// (`Pattern::names_scope_rel_path`), never the refusal's wording. And the
    /// lock half is the lock's, never a run's: `apply --root` carries a subset
    /// of the entries, and a predicate computed over a subset could answer
    /// differently from the file it was cut from, leaving the two commands
    /// naming opposite remedies for one manifest
    /// (`a_pattern_needing_a_scope_is_asked_of_the_whole_lock`).
    pub fn pattern_needs_a_scope_it_lacks(&self, pattern: &Pattern) -> bool {
        pattern.names_scope_rel_path() && self.entries.iter().any(|e| e.scope_rel_path.is_none())
    }
}

/// Read and parse a lock file (JSONL: a header line, then one entry per line),
/// or `None` where there is no lock file beside the manifest at all.
///
/// The first line is tried as a header and, failing that, read as an entry —
/// which is what recognises a lock written before headers existed. The two
/// shapes cannot be confused: a header line has no `id`, and an entry line has
/// no `lock_version`, so each fails the other's required fields.
///
/// **Absence is not unreadability, and only absence is `None`.** A lock that
/// is not there is an ordinary state of a manifest — a zero-match refresh
/// takes the lock away and records the empty hash — so a reader that could
/// only fail would make the commonest state of an empty manifest look like a
/// broken one. Every other failure stays an error: a file that is empty, one
/// whose lines will not parse, one that cannot be opened for any reason but
/// its absence. What each caller *does* about `None` is its own disposition,
/// the same way an absent header is.
pub fn read_lock_file(lock_path: &Path) -> Result<Option<LockFile>> {
    use std::io::{BufRead, BufReader};
    let lock_file = match std::fs::File::open(lock_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow::Error::from(e))
                .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))
        }
    };
    let mut lines = BufReader::new(lock_file).lines().enumerate();

    let mut header = None;
    let mut entries = Vec::new();

    let Some((i, first)) = lines.next() else {
        // Zero bytes is not "written before the header existed" — it is a
        // file that never finished being written, and a reader states what it
        // observes rather than a cause it cannot know.
        bail!("Lock file is empty: {}", lock_path.display());
    };
    {
        let first = first.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
        match serde_json::from_str::<LockHeader>(&first) {
            Ok(h) => header = Some(h),
            Err(_) => entries.push(
                serde_json::from_str(&first)
                    .with_context(|| format!("Failed to parse line {} of lock file", i + 1))?,
            ),
        }
    }

    for (i, line) in lines {
        let line = line.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
        entries.push(
            serde_json::from_str(&line)
                .with_context(|| format!("Failed to parse line {} of lock file", i + 1))?,
        );
    }

    Ok(Some(LockFile { header, entries }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::domain::{LockScope, CURRENT_LOCK_VERSION};

    // =========================================================================
    // write_lock_file
    // =========================================================================

    #[test]
    fn test_write_lock_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");
        let entries = vec![
            LockEntry {
                id: 1,
                root_id: 10,
                path: "/photos/a.jpg".to_string(),
                device: 100,
                inode: 200,
                size: 5000,
                mtime: 1700000000,
                partial_hash: "abc123".to_string(),
                object_id: Some(42),
                hash_type: Some("sha256".to_string()),
                hash_value: Some("deadbeef".to_string()),
                scope_rel_path: Some("a.jpg".to_string()),
            },
            LockEntry {
                id: 2,
                root_id: 10,
                path: "/photos/b.jpg".to_string(),
                device: 100,
                inode: 201,
                size: 3000,
                mtime: 1700000001,
                partial_hash: "def456".to_string(),
                object_id: None,
                hash_type: None,
                hash_value: None,
                scope_rel_path: None,
            },
        ];

        let header = LockHeader {
            lock_version: CURRENT_LOCK_VERSION,
            scope: vec![LockScope {
                root_id: 10,
                root_path: "/photos".to_string(),
                rel_prefix: "trip".to_string(),
            }],
        };
        write_lock_file(&lock_path, &header, &entries).unwrap();

        // Read back through the reader production uses, so the header split
        // and the entry parse are exercised as a pair.
        let read = read_lock_file(&lock_path)
            .unwrap()
            .expect("the lock was just written");
        let header = read
            .header
            .expect("a freshly written lock carries a header");
        assert_eq!(header.lock_version, CURRENT_LOCK_VERSION);
        assert_eq!(header.scope.len(), 1);
        assert_eq!(header.scope[0].rel_prefix, "trip");
        assert_eq!(read.entries.len(), 2);
        assert_eq!(read.entries[0].id, 1);
        assert_eq!(read.entries[0].path, "/photos/a.jpg");
        assert_eq!(read.entries[0].hash_value.as_deref(), Some("deadbeef"));
        assert_eq!(read.entries[0].scope_rel_path.as_deref(), Some("a.jpg"));
        assert_eq!(read.entries[1].id, 2);
        assert!(read.entries[1].object_id.is_none());
        assert!(read.entries[1].scope_rel_path.is_none());
    }

    /// A zero-byte lock is a file that never finished being written, not one
    /// "built before lock files recorded the scopes they were built from". A
    /// reader states
    /// what it observes rather than a cause it cannot know — the same rule the
    /// provenance spine applies to empty receipt columns.
    #[test]
    fn an_empty_lock_is_malformed_not_old() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("empty.lock");
        std::fs::write(&lock_path, "").unwrap();

        let err = read_lock_file(&lock_path).unwrap_err().to_string();
        assert!(err.contains("empty"), "{err}");
        assert!(
            !err.contains("before"),
            "an empty file supports no claim about when it was written: {err}"
        );
    }

    /// A lock written before the header existed is recognised, not
    /// misparsed: its first line is an entry, so every entry survives and the
    /// header comes back absent. The two shapes cannot be confused — a header
    /// carries no `id` and an entry no `lock_version` — and what an absent
    /// header costs is each reader's own disposition, not this function's.
    #[test]
    fn a_lock_written_before_the_header_reads_as_headerless() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("old.lock");
        std::fs::write(
            &lock_path,
            "{\"id\":1,\"root_id\":10,\"path\":\"/photos/a.jpg\",\"device\":1,\"inode\":2,\
             \"size\":5,\"mtime\":6,\"partial_hash\":\"h\",\"object_id\":null,\
             \"hash_type\":null,\"hash_value\":null}\n\
             {\"id\":2,\"root_id\":10,\"path\":\"/photos/b.jpg\",\"device\":1,\"inode\":3,\
             \"size\":5,\"mtime\":6,\"partial_hash\":\"h\",\"object_id\":null,\
             \"hash_type\":null,\"hash_value\":null}\n",
        )
        .unwrap();

        let read = read_lock_file(&lock_path)
            .unwrap()
            .expect("the file is there; only its header is not");
        assert!(read.header.is_none(), "an old lock declares no header");
        assert_eq!(read.entries.len(), 2, "no entry is eaten by the header try");
        assert_eq!(read.entries[0].id, 1);
        assert!(read.entries[0].scope_rel_path.is_none());
    }

    fn measured(scope_rel_path: Option<&str>) -> LockEntry {
        LockEntry {
            id: 1,
            root_id: 10,
            path: "/photos/a.jpg".to_string(),
            device: 100,
            inode: 200,
            size: 5000,
            mtime: 1700000000,
            partial_hash: "abc123".to_string(),
            object_id: Some(42),
            hash_type: Some("sha256".to_string()),
            hash_value: Some("deadbeef".to_string()),
            scope_rel_path: scope_rel_path.map(str::to_string),
        }
    }

    /// **Which remedy a failed expansion takes is the lock's answer, asked of
    /// the whole file.** Both halves are exercised: a pattern that does not
    /// name the scope key never needs a scope, however the lock stands; one
    /// that does needs it exactly where some entry carries no measurement.
    /// The last case is the one a per-run subset would get wrong — the
    /// measured entry alone says no, the file says yes.
    #[test]
    fn a_pattern_needing_a_scope_is_asked_of_the_whole_lock() {
        use crate::expr::parse_pattern;
        let scoped = parse_pattern("{scope.rel_path}").unwrap();
        let unscoped = parse_pattern("{source.rel_path}").unwrap();
        let lock = |entries: Vec<LockEntry>| LockFile {
            header: None,
            entries,
        };

        assert!(!lock(vec![measured(None)]).pattern_needs_a_scope_it_lacks(&unscoped));
        assert!(!lock(vec![measured(Some("a.jpg"))]).pattern_needs_a_scope_it_lacks(&scoped));
        assert!(!lock(Vec::new()).pattern_needs_a_scope_it_lacks(&scoped));
        assert!(lock(vec![measured(None)]).pattern_needs_a_scope_it_lacks(&scoped));
        assert!(
            lock(vec![measured(Some("a.jpg")), measured(None)])
                .pattern_needs_a_scope_it_lacks(&scoped),
            "one unmeasured entry anywhere in the file is the file's answer"
        );
    }
}
