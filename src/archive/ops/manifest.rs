//! Reading and writing the two files a cluster produces.
//!
//! The manifest is TOML the user edits; the lock file is JSONL recording the
//! sources as they stood when the manifest was generated. Generating writes the
//! pair and the status read consumes it. The four accessors sit together
//! because the two files are one on-disk format — a change to either is almost
//! always a change to both. Apply reads the pair inline rather than through
//! these, which is a known wart, not a second format.

use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::archive::domain::{validate_manifest_version, LockEntry, ManifestConfig};

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

/// Write a JSONL lock file from lock entries.
///
/// Synced to disk like the manifest beside it, and for the same reason twice
/// over: the manifest is written second and carries a hash of these bytes, so
/// a manifest that survives a power loss must not outlive the lock it names.
pub(super) fn write_lock_file(lock_path: &Path, entries: &[LockEntry]) -> Result<()> {
    let lock_file = std::fs::File::create(lock_path)
        .with_context(|| format!("Failed to create lock file: {}", lock_path.display()))?;
    let mut writer = BufWriter::new(lock_file);

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
pub fn read_manifest_config(manifest_path: &Path) -> Result<ManifestConfig> {
    let config_content = fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "Failed to read manifest config: {}",
            manifest_path.display()
        )
    })?;
    let config: ManifestConfig = toml::from_str(&config_content).with_context(|| {
        format!(
            "Failed to parse manifest config: {}",
            manifest_path.display()
        )
    })?;
    validate_manifest_version(config.meta.version)?;
    Ok(config)
}

/// Read and parse a lock file (JSONL format, one LockEntry per line).
pub fn read_lock_entries(lock_path: &Path) -> Result<Vec<LockEntry>> {
    use std::io::{BufRead, BufReader};
    let lock_file = std::fs::File::open(lock_path)
        .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;
    BufReader::new(lock_file)
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let line =
                line.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
            serde_json::from_str(&line)
                .with_context(|| format!("Failed to parse line {} of lock file", i + 1))
        })
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            },
        ];

        write_lock_file(&lock_path, &entries).unwrap();

        // Read back and verify
        let content = std::fs::read_to_string(&lock_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let entry1: LockEntry = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry1.id, 1);
        assert_eq!(entry1.path, "/photos/a.jpg");
        assert_eq!(entry1.hash_value.as_deref(), Some("deadbeef"));
        let entry2: LockEntry = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(entry2.id, 2);
        assert!(entry2.object_id.is_none());
    }
}
