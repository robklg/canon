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

/// Whether the path exists and is a directory — the reachability probe for
/// review surfaces (a disconnected drive reads as unreachable, not an error).
pub fn dir_exists(path: &Path) -> bool {
    path.is_dir()
}

/// Canonicalize a path that may not exist yet by finding the nearest existing
/// ancestor and appending the remaining components.
pub fn canonicalize_maybe_missing(path: &Path) -> Result<String> {
    // Try canonicalizing the full path first
    if let Ok(canon) = fs::canonicalize(path) {
        return Ok(canon.to_string_lossy().to_string());
    }

    // Walk up to find existing ancestor
    let mut existing = path.to_path_buf();
    let mut missing_parts = Vec::new();

    while !existing.exists() {
        if let Some(name) = existing.file_name() {
            missing_parts.push(name.to_os_string());
        }
        if !existing.pop() {
            bail!("Cannot resolve path: {}", path.display());
        }
    }

    // Canonicalize the existing part
    let canon_existing = fs::canonicalize(&existing)
        .with_context(|| format!("Failed to resolve path: {}", existing.display()))?;

    // Append missing parts
    let mut result = canon_existing;
    for part in missing_parts.into_iter().rev() {
        result.push(part);
    }

    Ok(result.to_string_lossy().to_string())
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
/// If noclobber is true, uses atomic O_EXCL create to prevent overwriting.
pub fn copy_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    let src_meta =
        fs::metadata(src).with_context(|| format!("Failed to read metadata: {}", src.display()))?;

    if noclobber {
        // Atomic noclobber: create dest with O_EXCL, then copy content manually
        let mut src_file =
            File::open(src).with_context(|| format!("Failed to open source: {}", src.display()))?;
        let dest_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true) // O_CREAT | O_EXCL — atomic, fails if exists
            .open(dest)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    anyhow::anyhow!("Destination already exists: {}", dest.display())
                } else {
                    anyhow::anyhow!("Failed to create {}: {}", dest.display(), e)
                }
            })?;
        let mut writer = std::io::BufWriter::new(dest_file);
        std::io::copy(&mut src_file, &mut writer)
            .with_context(|| format!("Failed to copy {} to {}", src.display(), dest.display()))?;
        use std::io::Write;
        writer
            .flush()
            .with_context(|| format!("Failed to flush {}", dest.display()))?;
    } else {
        // Allow overwrite
        fs::copy(src, dest)
            .with_context(|| format!("Failed to copy {} to {}", src.display(), dest.display()))?;
    }

    preserve_metadata(dest, &src_meta)?;
    Ok(())
}

/// Recursively copy a directory tree, preserving each file's metadata
/// (mtime, permissions) via `copy_file` with noclobber. Returns the number
/// of files copied. Fails on the first error; non-file, non-directory
/// entries (sockets, symlinks) are skipped.
pub fn copy_tree(src: &Path, dest: &Path) -> Result<usize> {
    fs::create_dir_all(dest)
        .with_context(|| format!("Failed to create directory: {}", dest.display()))?;
    let mut count = 0;
    let entries = fs::read_dir(src)
        .with_context(|| format!("Failed to read directory: {}", src.display()))?;
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dest.join(entry.file_name());
        if file_type.is_dir() {
            count += copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            copy_file(&entry.path(), &target, true)?;
            count += 1;
        }
    }
    Ok(count)
}

/// Rename with atomic noclobber where the platform supports it.
/// Falls back to stat-then-rename on unsupported platforms.
fn noclobber_rename(src: &Path, dest: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let src_c = CString::new(src.as_os_str().as_bytes())
            .with_context(|| format!("Invalid source path: {}", src.display()))?;
        let dest_c = CString::new(dest.as_os_str().as_bytes())
            .with_context(|| format!("Invalid dest path: {}", dest.display()))?;

        // renamex_np with RENAME_EXCL — atomic noclobber on macOS
        const RENAME_EXCL: u32 = 0x00000004;
        let ret = unsafe { libc::renamex_np(src_c.as_ptr(), dest_c.as_ptr(), RENAME_EXCL) };
        if ret == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EEXIST) {
            bail!("Destination already exists: {}", dest.display());
        }
        // ENOTSUP: filesystem doesn't support renamex_np (e.g., SMB/NFS).
        // Fall back to stat-then-rename.
        if err.raw_os_error() == Some(libc::ENOTSUP) {
            if dest.exists() {
                bail!("Destination already exists: {}", dest.display());
            }
            return fs::rename(src, dest).with_context(|| {
                format!("Failed to rename {} to {}", src.display(), dest.display())
            });
        }
        Err(err)
            .with_context(|| format!("Failed to rename {} to {}", src.display(), dest.display()))
    }

    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let src_c = CString::new(src.as_os_str().as_bytes())
            .with_context(|| format!("Invalid source path: {}", src.display()))?;
        let dest_c = CString::new(dest.as_os_str().as_bytes())
            .with_context(|| format!("Invalid dest path: {}", dest.display()))?;

        // renameat2 with RENAME_NOREPLACE — atomic noclobber on Linux
        let ret = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                src_c.as_ptr(),
                libc::AT_FDCWD,
                dest_c.as_ptr(),
                1u32, // RENAME_NOREPLACE
            )
        };
        if ret == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EEXIST) || err.raw_os_error() == Some(libc::ENOTEMPTY) {
            bail!("Destination already exists: {}", dest.display());
        }
        Err(err)
            .with_context(|| format!("Failed to rename {} to {}", src.display(), dest.display()))
    }

    // Fallback: stat-then-rename (TOCTOU gap, but negligible on unsupported platforms)
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        if dest.exists() {
            bail!("Destination already exists: {}", dest.display());
        }
        fs::rename(src, dest)
            .with_context(|| format!("Failed to rename {} to {}", src.display(), dest.display()))?;
        Ok(())
    }
}

/// Rename a file (same filesystem only).
/// If noclobber is true, uses platform-native atomic noclobber.
pub fn rename_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    if noclobber {
        noclobber_rename(src, dest)
    } else {
        fs::rename(src, dest)
            .with_context(|| format!("Failed to rename {} to {}", src.display(), dest.display()))
    }
}

/// Move a file: try rename, fall back to copy+delete on cross-device (EXDEV).
/// If noclobber is true, uses platform-native atomic noclobber for the rename
/// and passes noclobber through to the copy fallback.
/// Returns which strategy was used so the caller can take the appropriate DB path.
pub fn move_file(src: &Path, dest: &Path, noclobber: bool) -> Result<MoveOutcome> {
    let rename_result = if noclobber {
        noclobber_rename(src, dest)
    } else {
        fs::rename(src, dest)
            .with_context(|| format!("Failed to rename {} to {}", src.display(), dest.display()))
    };

    match rename_result {
        Ok(()) => Ok(MoveOutcome::Renamed),
        Err(e) => {
            // Check for cross-device error in the anyhow chain.
            // noclobber_rename wraps io::Error in context, so we traverse the chain.
            let is_exdev = e.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .and_then(|io_err| io_err.raw_os_error())
                    == Some(libc::EXDEV)
            });
            if is_exdev {
                copy_file(src, dest, noclobber)?;
                fs::remove_file(src)
                    .with_context(|| format!("Failed to delete source: {}", src.display()))?;
                Ok(MoveOutcome::CopiedAndDeleted)
            } else {
                Err(e)
            }
        }
    }
}

/// Write content to a `.incomplete` file at the same location as `path`.
///
/// The `.incomplete` file survives crashes as recoverable evidence.
/// The final path is not touched — call `finalize_file()` to rename on success.
pub fn write_file_incomplete(path: &Path, content: &[u8]) -> Result<()> {
    let incomplete = path.with_extension("incomplete");
    if let Some(parent) = incomplete.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&incomplete, content)?;
    Ok(())
}

/// Rename the `.incomplete` file to the final path (e.g., `.toml`).
///
/// Called after all work is done to atomically publish the receipt.
/// If the `.incomplete` file does not exist, returns an error.
pub fn finalize_file(path: &Path) -> Result<()> {
    let incomplete = path.with_extension("incomplete");
    std::fs::rename(&incomplete, path)?;
    Ok(())
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

    // =========================================================================
    // canonicalize_maybe_missing
    // =========================================================================

    #[test]
    fn canonicalize_maybe_missing_full_path_exists() {
        let dir = tempfile::tempdir().unwrap();
        let result = canonicalize_maybe_missing(dir.path()).unwrap();
        let expected = fs::canonicalize(dir.path())
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn canonicalize_maybe_missing_tolerates_missing_subdir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist-yet");
        let result = canonicalize_maybe_missing(&missing).unwrap();
        let expected = fs::canonicalize(dir.path())
            .unwrap()
            .join("does-not-exist-yet")
            .to_string_lossy()
            .to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn canonicalize_maybe_missing_tolerates_multiple_missing_levels() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("a").join("b").join("c");
        let result = canonicalize_maybe_missing(&missing).unwrap();
        let expected = fs::canonicalize(dir.path())
            .unwrap()
            .join("a")
            .join("b")
            .join("c")
            .to_string_lossy()
            .to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn canonicalize_maybe_missing_no_existing_ancestor_errors() {
        // A relative path with no component that exists under the process
        // cwd runs out of ancestors to pop before finding one that exists.
        let result =
            canonicalize_maybe_missing(Path::new("canon-test-no-such-relative-root-3f8a1c/sub"));
        assert!(result.is_err());
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

    #[cfg(unix)]
    #[test]
    fn copy_tree_copies_nested_files_preserving_mtime() {
        use filetime::FileTime;

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("ledger");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("000001-scan.toml"), b"a").unwrap();
        std::fs::write(src.join("sub/000002-apply.toml"), b"b").unwrap();
        let known_mtime = FileTime::from_unix_time(1704067200, 0);
        filetime::set_file_mtime(src.join("000001-scan.toml"), known_mtime).unwrap();

        let dest = dir.path().join("copy");
        let count = copy_tree(&src, &dest).unwrap();

        assert_eq!(count, 2);
        assert_eq!(
            std::fs::read(dest.join("000001-scan.toml")).unwrap(),
            b"a".to_vec()
        );
        assert_eq!(
            std::fs::read(dest.join("sub/000002-apply.toml")).unwrap(),
            b"b".to_vec()
        );
        let meta = fs::metadata(dest.join("000001-scan.toml")).unwrap();
        assert_eq!(FileTime::from_last_modification_time(&meta), known_mtime);
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

    #[test]
    fn test_write_file_incomplete_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("receipt.toml");
        write_file_incomplete(&final_path, b"content here").unwrap();
        let incomplete = dir.path().join("receipt.incomplete");
        assert!(incomplete.exists());
        assert!(!final_path.exists());
        assert_eq!(std::fs::read(&incomplete).unwrap(), b"content here");
    }

    #[test]
    fn test_finalize_file_renames() {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("receipt.toml");
        write_file_incomplete(&final_path, b"content").unwrap();
        finalize_file(&final_path).unwrap();
        assert!(final_path.exists());
        assert!(!dir.path().join("receipt.incomplete").exists());
        assert_eq!(std::fs::read(&final_path).unwrap(), b"content");
    }

    #[test]
    fn test_finalize_file_missing_incomplete_errors() {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("receipt.toml");
        let err = finalize_file(&final_path).unwrap_err();
        assert!(
            err.to_string().contains("receipt.incomplete")
                || err.to_string().contains("No such file")
        );
    }
}
