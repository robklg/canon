//! Filesystem access layer for canon.
//!
//! Structured access to files on disk — parallel to how `repo` provides
//! structured access to the database. Operations layer orchestrates both.
//!
//! Functions here perform filesystem operations but do not make business
//! decisions. The ops layer decides what to do; this module does it.
//!
//! No database access, no terminal I/O.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const PARTIAL_HASH_CHUNK_SIZE: usize = 8192; // 8KB

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
}
