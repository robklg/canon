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
///
/// Success means the storage accepted the time, not that it will keep it: a pending update
/// from the destination's write can overwrite it later (`copy_file`).
#[cfg(unix)]
fn preserve_metadata(dest: &Path, src_meta: &Metadata) -> Result<()> {
    use filetime::FileTime;

    let mtime = FileTime::from_last_modification_time(src_meta);
    filetime::set_file_mtime(dest, mtime)
        .with_context(|| format!("Failed to set mtime on {}", dest.display()))?;
    fs::set_permissions(dest, src_meta.permissions())
        .with_context(|| format!("Failed to set permissions on {}", dest.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn preserve_metadata(_dest: &Path, _src_meta: &Metadata) -> Result<()> {
    Ok(())
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

/// Write `src`'s bytes to `dest` and hand back the open destination file.
///
/// Under `noclobber` the destination is created with `O_EXCL` (fails if it exists);
/// otherwise it is created or truncated. The file is returned still open because a
/// late write error is reported on the descriptor that wrote and is dropped at close:
/// a proof that the data reached storage has to be made on this descriptor.
fn write_copy(src: &Path, dest: &Path, noclobber: bool) -> Result<File> {
    let mut src_file =
        File::open(src).with_context(|| format!("Failed to open source: {}", src.display()))?;
    let mut options = fs::OpenOptions::new();
    options.write(true);
    if noclobber {
        options.create_new(true); // O_CREAT | O_EXCL — atomic, fails if exists
    } else {
        options.create(true).truncate(true);
    }
    let dest_file = options.open(dest).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            anyhow::anyhow!("Destination already exists: {}", dest.display())
        } else {
            anyhow::anyhow!("Failed to create {}: {}", dest.display(), e)
        }
    })?;
    let mut writer = std::io::BufWriter::new(dest_file);
    std::io::copy(&mut src_file, &mut writer)
        .with_context(|| format!("Failed to copy {} to {}", src.display(), dest.display()))?;
    writer
        .into_inner()
        .map_err(|e| e.into_error())
        .with_context(|| format!("Failed to flush {}", dest.display()))
}

/// Copy a file and preserve its metadata (mtime, permissions).
/// If noclobber is true, uses atomic O_EXCL create to prevent overwriting.
///
/// A plain `fsync` on the writing descriptor reports a late write error that `close` would
/// drop, except on a filesystem that offers no sync (`offers_no_sync`). It is not a full
/// sync, so nothing here shows the data reached the disk. A move that removes its origin
/// proves the copy first (`move_file`). A failed sync fails the copy and leaves the file,
/// which a resume cannot tell from a good copy.
///
/// The destination is opened once between its close and setting its modification time.
/// An SMB client can keep a written file open on the server after `close`, and the server
/// then updates the file's write time about two seconds after the write, over a time set
/// sooner. A new open makes the macOS client send that close, which clears the pending
/// update. No data is read, and a failed open is ignored: the copy is whole, and a
/// destination that can be written but not read is a legitimate one.
pub fn copy_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    copy_file_with(src, dest, noclobber, plain_fsync)
}

/// `copy_file` with its sync passed in, so a test can fail it.
fn copy_file_with(
    src: &Path,
    dest: &Path,
    noclobber: bool,
    sync: impl FnOnce(&File) -> std::io::Result<()>,
) -> Result<()> {
    let src_meta =
        fs::metadata(src).with_context(|| format!("Failed to read metadata: {}", src.display()))?;
    let written = write_copy(src, dest, noclobber)?;
    match sync(&written) {
        Err(e) if !offers_no_sync(&e) => {
            return Err(e).with_context(|| format!("Failed to write out {}", dest.display()));
        }
        _ => {}
    }
    drop(written);
    drop(File::open(dest));
    preserve_metadata(dest, &src_meta)
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
                move_across_devices(src, dest, noclobber, prove_copy)
            } else {
                Err(e)
            }
        }
    }
}

/// Copy `src` to `dest` on another device, then remove `src` — but only past `prove`.
///
/// The origin-removal law: an origin is removed only after its destination is proven.
/// `prove` receives the descriptor that wrote the copy, so a late write error is seen
/// where it is reported. The removal is reached only through a successful proof.
///
/// When the proof or the removal fails and the origin is left standing, a destination
/// this call created is removed too (`noclobber` means it did): nothing is lost, and
/// the entry is plainly pending again. When the origin is already gone, the copy may
/// be all there is, so it stays. A write that fails before the proof (a full disk)
/// returns early and leaves its partial file with the origin standing, as a plain copy
/// does; a resume reports that file by its size.
fn move_across_devices(
    src: &Path,
    dest: &Path,
    noclobber: bool,
    prove: impl FnOnce(&File, &Path, &Path, &Metadata) -> Result<()>,
) -> Result<MoveOutcome> {
    let before =
        fs::metadata(src).with_context(|| format!("Failed to read metadata: {}", src.display()))?;
    let written = write_copy(src, dest, noclobber)?;
    let proof = prove(&written, src, dest, &before).map_err(|cause| {
        anyhow::anyhow!(
            "{} was kept: could not confirm {} before removing it: {:#}",
            src.display(),
            dest.display(),
            cause
        )
    });
    let moved = proof.and_then(|()| {
        fs::remove_file(src).map_err(|cause| {
            anyhow::anyhow!(
                "{} could not be removed: {}. Nothing was moved.",
                src.display(),
                cause
            )
        })
    });
    if let Err(e) = moved {
        if noclobber && fs::metadata(src).is_ok() {
            let _ = fs::remove_file(dest);
        }
        return Err(e);
    }
    Ok(MoveOutcome::CopiedAndDeleted)
}

/// The proof a cross-device move makes before its origin goes: the copy is written out
/// to storage, its modification time and permissions are set, its directory entry is synced, its
/// length equals the origin's, and the origin's length and modification time are what they were
/// before the copy began. The bytes are not read back.
fn prove_copy(written: &File, src: &Path, dest: &Path, before: &Metadata) -> Result<()> {
    sync_to_storage(written)
        .with_context(|| format!("could not write {} out to storage", dest.display()))?;
    // The modification time is set only after the data is on its way, never with data unsent.
    preserve_metadata(dest, before)?;
    sync_parent_dir(dest)?;
    let copied = fs::metadata(dest)
        .with_context(|| format!("Failed to read metadata: {}", dest.display()))?;
    if copied.len() != before.len() {
        bail!(
            "{} is {} bytes, {} is {} bytes",
            dest.display(),
            copied.len(),
            src.display(),
            before.len()
        );
    }
    let after =
        fs::metadata(src).with_context(|| format!("Failed to read metadata: {}", src.display()))?;
    if after.len() != before.len() || after.modified().ok() != before.modified().ok() {
        bail!("{} changed during the copy", src.display());
    }
    Ok(())
}

/// Write a file's data out to storage: a full sync, and plain `fsync` where the
/// filesystem refuses the full one (`ENOTSUP`, as an SMB share does for a directory).
fn sync_to_storage(file: &File) -> std::io::Result<()> {
    let full = file.sync_all();
    #[cfg(unix)]
    if let Err(e) = &full {
        if e.raw_os_error() == Some(libc::ENOTSUP) {
            return plain_fsync(file);
        }
    }
    full
}

/// `fsync(2)` itself, which `File::sync_all` is not on macOS. A signal that interrupts it
/// retries it, so a Ctrl-C does not read as a failed write.
#[cfg(unix)]
fn plain_fsync(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    loop {
        // SAFETY: the descriptor is open for as long as `file` is borrowed.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

#[cfg(not(unix))]
fn plain_fsync(file: &File) -> std::io::Result<()> {
    file.sync_all()
}

/// Whether a sync's error says the filesystem offers no such sync, which says nothing
/// about the write.
#[cfg(unix)]
fn offers_no_sync(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(code) if code == libc::ENOTSUP || code == libc::EINVAL
    )
}

#[cfg(not(unix))]
fn offers_no_sync(_e: &std::io::Error) -> bool {
    false
}

/// Sync the directory that holds `dest`, so the new name survives a power cut.
/// A filesystem that offers no directory sync (`ENOTSUP` or `EINVAL` from both calls)
/// is not a failed proof: the file's own sync stands.
fn sync_parent_dir(dest: &Path) -> Result<()> {
    let parent = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let dir = File::open(parent)
        .with_context(|| format!("Failed to open directory: {}", parent.display()))?;
    match sync_to_storage(&dir) {
        Ok(()) => Ok(()),
        Err(e) if offers_no_sync(&e) => Ok(()),
        Err(e) => Err(e).with_context(|| format!("could not sync {}", parent.display())),
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

    // =========================================================================
    // The origin-removal law: a cross-device move proves its copy first
    // =========================================================================

    /// A source file and a destination path in one scratch directory.
    fn move_fixture(content: &[u8]) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.bin");
        std::fs::write(&src, content).unwrap();
        let dest = dir.path().join("dest.bin");
        (dir, src, dest)
    }

    /// The origin-removal law holds at the cross-device arm, not only at the helper
    /// the other tests drive. No test reaches that arm (it needs two filesystems),
    /// so its links are read from this file's production text: the arm hands its copy
    /// to `prove_copy`; `prove_copy` writes the data out to storage before it sets the
    /// modification time, and syncs the directory; and the file removes an origin in exactly
    /// one place, inside `move_across_devices`. An arm that went back to
    /// copy-then-remove, or passed a proof that always succeeds, or a proof without
    /// its sync, fails here.
    ///
    /// **Blind spots, named:** it matches spellings. An origin removal written without
    /// `remove_file(` and `src` on one line (a rename into a trash, a removal through
    /// another name for the path) passes unseen, and so does a call to the proof that
    /// sits in a comment while the code does something else. The behavioural tests
    /// drive the helper; this pins the wiring around it.
    #[test]
    fn the_cross_device_arm_proves_before_the_one_origin_removal() {
        let text = include_str!("fs.rs");
        let production = text.split("\n#[cfg(test)]").next().unwrap();

        let proof = production
            .split("fn prove_copy(")
            .nth(1)
            .and_then(|rest| rest.split("\nfn ").next())
            .expect("prove_copy is in production");
        let synced = proof.find("sync_to_storage(written)");
        let dated = proof.find("preserve_metadata(dest, before)");
        assert!(
            synced.is_some() && synced < dated,
            "the data is written out to storage before the modification time is set"
        );
        assert!(
            proof.contains("sync_parent_dir(dest)"),
            "the directory is synced too"
        );

        assert_eq!(
            production
                .matches("move_across_devices(src, dest, noclobber, prove_copy)")
                .count(),
            1,
            "the cross-device arm must hand its copy to prove_copy"
        );
        let removals: Vec<&str> = production
            .lines()
            .filter(|l| l.contains("remove_file(") && l.contains("src"))
            .collect();
        assert_eq!(removals.len(), 1, "one origin removal, found {removals:?}");

        let helper = production
            .split("fn move_across_devices(")
            .nth(1)
            .and_then(|rest| rest.split("\nfn ").next())
            .expect("move_across_devices is in production");
        assert!(
            helper.contains("remove_file(") && helper.contains("src"),
            "the one origin removal sits inside move_across_devices"
        );
    }

    /// A plain copy syncs on the writing descriptor, so a late write error is seen, then
    /// closes it and opens the destination once before setting the modification time, so an
    /// SMB server's pending update cannot overwrite that time. The behaviour needs a share,
    /// so the order is read from this file's production text.
    ///
    /// **Blind spots, named:** it matches spellings, and it says nothing about what a
    /// client or server does with the open. A client that does not send its deferred close
    /// on an open leaves the copy as it was without this step.
    #[test]
    fn a_plain_copy_syncs_and_opens_before_setting_the_time() {
        let text = include_str!("fs.rs");
        let production = text.split("\n#[cfg(test)]").next().unwrap();
        let copy = production
            .split("fn copy_file_with(")
            .nth(1)
            .and_then(|rest| rest.split("\n}\n").next())
            .expect("copy_file_with is in production");
        assert!(
            production.contains("copy_file_with(src, dest, noclobber, plain_fsync)"),
            "copy_file hands over the real sync"
        );

        let synced = copy.find("sync(&written)");
        let closed = copy.find("drop(written);");
        let opened = copy.find("drop(File::open(dest));");
        let dated = copy.find("preserve_metadata(dest");
        assert!(
            synced.is_some() && synced < closed && closed < opened && opened < dated,
            "sync the written file, close it, open it once, then set the time"
        );
    }

    #[cfg(unix)]
    #[test]
    fn only_a_filesystem_with_no_sync_excuses_a_failed_sync() {
        use std::io::Error;
        assert!(offers_no_sync(&Error::from_raw_os_error(libc::ENOTSUP)));
        assert!(offers_no_sync(&Error::from_raw_os_error(libc::EINVAL)));
        assert!(!offers_no_sync(&Error::from_raw_os_error(libc::EIO)));
        assert!(!offers_no_sync(&Error::from_raw_os_error(libc::ENOSPC)));
    }

    /// A failed sync fails the copy; a filesystem that offers no sync does not.
    #[cfg(unix)]
    #[test]
    fn a_failed_sync_fails_the_copy() {
        use std::io::Error;
        let (_dir, src, dest) = move_fixture(b"content");
        let err = copy_file_with(&src, &dest, true, |_| {
            Err(Error::from_raw_os_error(libc::EIO))
        })
        .unwrap_err();
        assert!(err.to_string().contains("Failed to write out"));

        let (_dir, src, dest) = move_fixture(b"content");
        copy_file_with(&src, &dest, true, |_| {
            Err(Error::from_raw_os_error(libc::ENOTSUP))
        })
        .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"content");
    }

    #[test]
    fn plain_fsync_accepts_a_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        plain_fsync(&file).unwrap();
    }

    #[test]
    fn a_failed_proof_keeps_the_origin() {
        let (_dir, src, dest) = move_fixture(b"the only copy");

        let err =
            move_across_devices(&src, &dest, true, |_, _, _, _| bail!("sync refused")).unwrap_err();

        assert!(src.exists(), "a failed proof must keep the origin");
        assert_eq!(std::fs::read(&src).unwrap(), b"the only copy");
        assert!(
            !dest.exists(),
            "the origin stands, so the copy this call created is removed"
        );
        let said = err.to_string();
        assert!(said.contains("was kept"), "{said}");
        assert!(said.contains("could not confirm"), "{said}");
        assert!(said.contains("sync refused"), "{said}");
    }

    #[test]
    fn a_failed_proof_keeps_the_copy_when_the_origin_is_gone() {
        let (_dir, src, dest) = move_fixture(b"the only copy");

        let origin = src.clone();
        let result = move_across_devices(&src, &dest, true, move |_, _, _, _| {
            // The origin vanishes while the proof runs: the copy may be all there is.
            std::fs::remove_file(&origin).unwrap();
            bail!("proof failed after the origin went")
        });

        assert!(result.is_err());
        assert!(!src.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"the only copy");
    }

    #[cfg(unix)]
    #[test]
    fn an_origin_that_cannot_be_removed_leaves_nothing_at_the_destination() {
        use std::os::unix::fs::PermissionsExt;

        if unsafe { libc::geteuid() } == 0 {
            return; // root removes from a read-only directory
        }
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        std::fs::create_dir(&from).unwrap();
        let src = from.join("src.bin");
        std::fs::write(&src, b"kept").unwrap();
        let dest = dir.path().join("dest.bin");

        std::fs::set_permissions(&from, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = move_across_devices(&src, &dest, true, |_, _, _, _| Ok(()));
        std::fs::set_permissions(&from, std::fs::Permissions::from_mode(0o755)).unwrap();

        let err = result.unwrap_err();
        assert!(src.exists());
        assert!(
            !dest.exists(),
            "the origin stands, so no copy is left behind"
        );
        let said = err.to_string();
        assert!(said.contains("could not be removed"), "{said}");
        assert!(said.contains("Nothing was moved"), "{said}");
    }

    #[test]
    fn a_proven_copy_removes_the_origin() {
        use filetime::FileTime;

        let (_dir, src, dest) = move_fixture(b"moves whole");
        let known = FileTime::from_unix_time(1704067200, 0);
        filetime::set_file_mtime(&src, known).unwrap();

        let outcome = move_across_devices(&src, &dest, true, prove_copy).unwrap();

        assert_eq!(outcome, MoveOutcome::CopiedAndDeleted);
        assert!(!src.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"moves whole");
        let saved = FileTime::from_last_modification_time(&fs::metadata(&dest).unwrap());
        assert_eq!(
            saved, known,
            "the destination holds the origin's modification time"
        );
    }

    #[test]
    fn prove_copy_refuses_a_shorter_destination() {
        let (_dir, src, dest) = move_fixture(b"0123456789");
        std::fs::write(&dest, b"01234").unwrap();
        let before = fs::metadata(&src).unwrap();
        let written = fs::OpenOptions::new().write(true).open(&dest).unwrap();

        let err = prove_copy(&written, &src, &dest, &before).unwrap_err();

        let said = err.to_string();
        assert!(said.contains("is 5 bytes"), "{said}");
        assert!(said.contains("is 10 bytes"), "{said}");
    }

    #[test]
    fn prove_copy_refuses_an_origin_whose_save_time_moved() {
        use filetime::FileTime;

        let (_dir, src, dest) = move_fixture(b"0123456789");
        std::fs::write(&dest, b"0123456789").unwrap();
        let before = fs::metadata(&src).unwrap();
        filetime::set_file_mtime(&src, FileTime::from_unix_time(1704067200, 0)).unwrap();
        let written = fs::OpenOptions::new().write(true).open(&dest).unwrap();

        let err = prove_copy(&written, &src, &dest, &before).unwrap_err();

        assert!(err.to_string().contains("changed during the copy"));
    }

    #[test]
    fn prove_copy_accepts_a_whole_copy_of_an_unchanged_origin() {
        let (_dir, src, dest) = move_fixture(b"0123456789");
        std::fs::write(&dest, b"0123456789").unwrap();
        let before = fs::metadata(&src).unwrap();
        let written = fs::OpenOptions::new().write(true).open(&dest).unwrap();

        prove_copy(&written, &src, &dest, &before).unwrap();
    }

    #[test]
    fn sync_to_storage_accepts_a_file_and_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"x").unwrap();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        sync_to_storage(&file).unwrap();
        sync_parent_dir(&path).unwrap();
    }
}
