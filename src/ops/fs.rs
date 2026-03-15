//! Filesystem access layer for canon.
//!
//! Structured access to files on disk — parallel to how `repo` provides
//! structured access to the database. Operations layer orchestrates both.
//!
//! Functions here perform filesystem operations but do not make business
//! decisions. The ops layer decides what to do; this module does it.
//!
//! No database access, no terminal I/O.

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const PARTIAL_HASH_CHUNK_SIZE: usize = 8192; // 8KB

/// Outcome of a move operation — caller needs to know which DB path to take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOutcome {
    /// Same-device rename succeeded (source relocated)
    Renamed,
    /// Cross-device: copied to dest, deleted source
    CopiedAndDeleted,
}

/// Compute SHA256 hash of first 8KB + last 8KB of a file.
/// For files <= 16KB, hash the entire file.
pub fn compute_partial_hash(path: &Path, size: u64) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for partial hash: {}", path.display()))?;
    let mut hasher = Sha256::new();

    if size <= (PARTIAL_HASH_CHUNK_SIZE * 2) as u64 {
        // Small file - hash entire content
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        hasher.update(&buf);
    } else {
        // Large file - hash first 8KB + last 8KB
        let mut buf = [0u8; PARTIAL_HASH_CHUNK_SIZE];

        // Read first 8KB
        file.read_exact(&mut buf)?;
        hasher.update(buf);

        // Seek to last 8KB and read
        file.seek(SeekFrom::End(-(PARTIAL_HASH_CHUNK_SIZE as i64)))?;
        file.read_exact(&mut buf)?;
        hasher.update(buf);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Compute full SHA256 hash of a file.
pub fn compute_full_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536]; // 64KB buffer

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Preserve source file metadata (mtime, permissions) on a destination file.
#[cfg(unix)]
pub fn preserve_metadata(dest: &Path, src_meta: &Metadata) -> Result<()> {
    use filetime::FileTime;

    let mtime = FileTime::from_last_modification_time(src_meta);
    filetime::set_file_mtime(dest, mtime)
        .with_context(|| format!("Failed to set mtime on {}", dest.display()))?;
    fs::set_permissions(dest, src_meta.permissions())
        .with_context(|| format!("Failed to set permissions on {}", dest.display()))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn preserve_metadata(_dest: &Path, _src_meta: &Metadata) -> Result<()> {
    Ok(())
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
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }
    Ok(())
}

/// Copy a file and preserve its metadata (mtime, permissions).
/// If noclobber is true, errors when dest already exists.
pub fn copy_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    if noclobber && dest.exists() {
        bail!("Destination already exists: {}", dest.display());
    }
    let src_meta = fs::metadata(src)
        .with_context(|| format!("Failed to read metadata: {}", src.display()))?;
    fs::copy(src, dest).with_context(|| {
        format!(
            "Failed to copy {} to {}",
            src.display(),
            dest.display()
        )
    })?;
    preserve_metadata(dest, &src_meta)?;
    Ok(())
}

/// Rename a file (same filesystem only).
/// If noclobber is true, errors when dest already exists.
pub fn rename_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    if noclobber && dest.exists() {
        bail!("Destination already exists: {}", dest.display());
    }
    fs::rename(src, dest).with_context(|| {
        format!(
            "Failed to rename {} to {}",
            src.display(),
            dest.display()
        )
    })?;
    Ok(())
}

/// Move a file: try rename, fall back to copy+delete on cross-device (EXDEV).
/// If noclobber is true, errors when dest already exists.
/// Returns which strategy was used so the caller can take the appropriate DB path.
pub fn move_file(src: &Path, dest: &Path, noclobber: bool) -> Result<MoveOutcome> {
    if noclobber && dest.exists() {
        bail!("Destination already exists: {}", dest.display());
    }
    match fs::rename(src, dest) {
        Ok(()) => Ok(MoveOutcome::Renamed),
        #[cfg(unix)]
        Err(e) if e.raw_os_error() == Some(libc::EXDEV) => {
            // Cross-device: fallback to copy + delete
            copy_file(src, dest, false)?;
            fs::remove_file(src)
                .with_context(|| format!("Failed to delete source: {}", src.display()))?;
            Ok(MoveOutcome::CopiedAndDeleted)
        }
        Err(e) => Err(e).with_context(|| {
            format!(
                "Failed to rename {} to {}",
                src.display(),
                dest.display()
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn partial_hash_small_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();
        let hash = compute_partial_hash(f.path(), 11).unwrap();
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn partial_hash_large_file() {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0u8; 32768]; // 32KB of zeros
        f.write_all(&data).unwrap();
        let hash = compute_partial_hash(f.path(), 32768).unwrap();
        assert!(!hash.is_empty());
        // Verify deterministic
        let hash2 = compute_partial_hash(f.path(), 32768).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn partial_hash_boundary_16kb() {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![42u8; 16384];
        f.write_all(&data).unwrap();
        let hash = compute_partial_hash(f.path(), 16384).unwrap();
        assert!(!hash.is_empty());
    }

    #[test]
    fn full_hash_known_content() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();
        let hash = compute_full_hash(f.path()).unwrap();
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn full_hash_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let hash = compute_full_hash(f.path()).unwrap();
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn partial_and_full_hash_match_for_small_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        let partial = compute_partial_hash(f.path(), 12).unwrap();
        let full = compute_full_hash(f.path()).unwrap();
        assert_eq!(partial, full);
    }

    #[cfg(unix)]
    #[test]
    fn preserve_metadata_mtime() {
        use filetime::FileTime;

        let src = NamedTempFile::new().unwrap();
        let dest = NamedTempFile::new().unwrap();

        let known_mtime = FileTime::from_unix_time(1704067200, 0);
        filetime::set_file_mtime(src.path(), known_mtime).unwrap();

        let src_meta = fs::metadata(src.path()).unwrap();
        preserve_metadata(dest.path(), &src_meta).unwrap();

        let dest_meta = fs::metadata(dest.path()).unwrap();
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);
        assert_eq!(dest_mtime, known_mtime);
    }

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

    // =========================================================================
    // copy_file
    // =========================================================================

    #[test]
    fn copy_file_success() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();

        let dest = dir.path().join("dest.txt");
        copy_file(&src, &dest, true).unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
        // Source still exists (it's a copy)
        assert!(src.exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_file_preserves_mtime() {
        use filetime::FileTime;

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        let known_mtime = FileTime::from_unix_time(1704067200, 0);
        filetime::set_file_mtime(&src, known_mtime).unwrap();

        let dest = dir.path().join("dest.txt");
        copy_file(&src, &dest, true).unwrap();

        let dest_meta = fs::metadata(&dest).unwrap();
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);
        assert_eq!(dest_mtime, known_mtime);
    }

    #[test]
    fn copy_file_noclobber_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        let dest = dir.path().join("dest.txt");
        std::fs::write(&dest, b"existing").unwrap();

        let err = copy_file(&src, &dest, true).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn copy_file_overwrites_without_noclobber() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"new content").unwrap();
        let dest = dir.path().join("dest.txt");
        std::fs::write(&dest, b"old content").unwrap();

        copy_file(&src, &dest, false).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"new content");
    }

    #[test]
    fn copy_file_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("nonexistent.txt");
        let dest = dir.path().join("dest.txt");

        let err = copy_file(&src, &dest, true).unwrap_err();
        assert!(err.to_string().contains("metadata"));
    }

    // =========================================================================
    // rename_file
    // =========================================================================

    #[test]
    fn rename_file_success() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();

        let dest = dir.path().join("dest.txt");
        rename_file(&src, &dest, true).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
    }

    #[test]
    fn rename_file_noclobber_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        let dest = dir.path().join("dest.txt");
        std::fs::write(&dest, b"existing").unwrap();

        let err = rename_file(&src, &dest, true).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // =========================================================================
    // move_file
    // =========================================================================

    #[test]
    fn move_file_same_device() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();

        let dest = dir.path().join("dest.txt");
        let outcome = move_file(&src, &dest, true).unwrap();

        assert_eq!(outcome, MoveOutcome::Renamed);
        assert!(!src.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
    }

    #[test]
    fn move_file_noclobber_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        let dest = dir.path().join("dest.txt");
        std::fs::write(&dest, b"existing").unwrap();

        let err = move_file(&src, &dest, true).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
