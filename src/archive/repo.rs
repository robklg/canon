//! Archive's own SQL — the four writes and reads that only apply performs.
//!
//! Registering a file that has landed in an archive, re-pointing a source
//! that was moved there rather than copied, reading the decision that last
//! stood at a destination path, and the preflight that asks which
//! destinations are already occupied. Nothing else in Canon does these
//! things, so they live here rather than in the shared source repository.

use anyhow::Result;
use rusqlite::OptionalExtension;

use crate::core::domain::source::{NewSource, Source};
use crate::core::repo::source::{fetch_by_path, BATCH_SIZE};
use crate::core::repo::Connection;

/// Fetch the current decision_id for a source at the given path.
///
/// Returns None if no present source exists at this path, or if its decision_id is NULL.
/// Used by apply before overwriting a destination to capture the provenance chain.
pub fn fetch_decision_id_at_path(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<Option<i64>> {
    conn.prepare_cached(
        "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = ? AND present = 1",
    )?
    .query_row(rusqlite::params![root_id, rel_path], |row| {
        row.get::<_, Option<i64>>(0)
    })
    .optional()
    .map(|opt| opt.flatten())
    .map_err(Into::into)
}

/// Check which destination paths are already registered in an archive.
///
/// This is used by apply's preflight check to detect destination conflicts
/// before any file operations begin. In regular mode, any existing paths
/// are an error. In --resume mode, existing paths are classified for skip/transfer.
///
/// # Arguments
/// * `conn` - Database connection
/// * `archive_root_id` - The archive root to check within
/// * `rel_paths` - Relative paths to check (within the archive)
///
/// # Returns
/// Set of rel_paths that exist in the archive with present=1.
/// Paths not in the result set are available for writing.
///
/// # Example
/// ```ignore
/// let existing = batch_check_paths_exist(conn, archive_id, &["2024/a.jpg", "2024/b.jpg"])?;
/// if existing.contains("2024/a.jpg") {
///     // This path is already occupied
/// }
/// ```
pub fn batch_check_paths_exist(
    conn: &Connection,
    archive_root_id: i64,
    rel_paths: &[&str],
) -> Result<std::collections::HashSet<String>> {
    use std::collections::HashSet;

    if rel_paths.is_empty() {
        return Ok(HashSet::new());
    }

    let mut result = HashSet::new();

    // Process rel_paths in batches to avoid SQLite variable limit
    for chunk in rel_paths.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT rel_path FROM sources WHERE root_id = ? AND present = 1 AND rel_path IN ({})",
            placeholders.join(", ")
        );

        // Build params: archive_root_id first, then all rel_paths
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() + 1);
        params.push(&archive_root_id);
        for path in chunk {
            params.push(path);
        }

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), |row| row.get::<_, String>(0))?;

        for row in rows {
            result.insert(row?);
        }
    }

    Ok(result)
}

/// Insert a new source record for a destination file in an archive.
///
/// This function registers a file that has been copied or moved to an archive root.
/// It handles both fresh inserts and updates to stale records.
///
/// # Behavior
///
/// - **Fresh insert**: If no record exists for (root_id, rel_path), creates a new
///   record with basis_rev=0.
/// - **Stale record revival**: If a stale record exists (present=0), updates it
///   with the new metadata, increments basis_rev, and sets present=1. This preserves
///   the row history and correctly reflects that new content now exists at this path.
/// - **Active record conflict**: If an active record exists (present=1), returns an
///   error. The caller's pre-flight check should have prevented this.
///
/// # Returns
///
/// The complete Source record as it exists in the database after the operation,
/// including joined fields (root_path, root_role, root_suspended, object_excluded).
/// This is fetched via SELECT after the write to ensure accuracy.
///
/// # Caller Responsibilities
///
/// - Ensure the file has been successfully written to disk before calling
/// - Manage transaction boundaries (this function does not BEGIN/COMMIT)
/// - Run pre-flight checks to detect active record conflicts before file operations
///
/// # Example
///
/// ```ignore
/// let new_source = NewSource {
///     root_id: archive_root_id,
///     rel_path: "2024/photo.jpg".to_string(),
///     size: 1024,
///     mtime: 1704067200,
///     partial_hash: "abc123".to_string(),
///     object_id: Some(42),
///     device: Some(65024),
///     inode: Some(12345),
/// };
///
/// let created = insert_destination(conn, &new_source)?;
/// println!("Created source {} at {}", created.id, created.path());
/// ```
pub fn insert_destination(conn: &Connection, new: &NewSource) -> Result<Source> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    // First try to update any existing record at this path (present=0 or present=1).
    // This preserves the row and increments basis_rev to reflect new content at this path.
    // Handles both stale records (present=0) and active records from a scan (present=1).
    let updated = conn.execute(
        "UPDATE sources SET
            device = COALESCE(?, device),
            inode = COALESCE(?, inode),
            size = ?,
            mtime = ?,
            partial_hash = ?,
            object_id = ?,
            basis_rev = basis_rev + 1,
            scanned_at = ?,
            last_seen_at = ?,
            present = 1,
            excluded = 0,
            decision_id = ?
         WHERE root_id = ? AND rel_path = ?",
        rusqlite::params![
            new.device,
            new.inode,
            new.size,
            new.mtime,
            new.partial_hash,
            new.object_id,
            now,
            now,
            new.decision_id,
            new.root_id,
            new.rel_path,
        ],
    )?;

    if updated == 0 {
        // No stale record exists. Insert new record.
        // Use COALESCE for device/inode to handle platforms without these values.
        conn.execute(
            "INSERT INTO sources (
                root_id, rel_path, device, inode, size, mtime, partial_hash,
                object_id, basis_rev, scanned_at, last_seen_at, present, excluded,
                decision_id
             ) VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?, ?, ?, 0, ?, ?, 1, 0, ?)",
            rusqlite::params![
                new.root_id,
                new.rel_path,
                new.device,
                new.inode,
                new.size,
                new.mtime,
                new.partial_hash,
                new.object_id,
                now,
                now,
                new.decision_id,
            ],
        )?;
    }

    // Fetch the complete Source record with all joined fields.
    // This ensures the returned Source accurately reflects database state.
    fetch_by_path(conn, new.root_id, &new.rel_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to fetch source after insert: root_id={}, rel_path={}",
            new.root_id,
            new.rel_path
        )
    })
}

/// Update a source's location (root and path) after a rename/move operation.
///
/// Used when a source file is relocated to an archive. Updates the root_id,
/// rel_path, and timestamps to reflect the new location.
///
/// # Arguments
/// * `conn` - Database connection
/// * `source_id` - ID of the source to update
/// * `new_root_id` - The new root (typically the archive root)
/// * `new_rel_path` - The new relative path within the root
/// * `now` - Timestamp to record
pub fn update_location(
    conn: &Connection,
    source_id: i64,
    new_root_id: i64,
    new_rel_path: &str,
    now: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    conn.execute(
        "UPDATE sources SET root_id = ?, rel_path = ?, scanned_at = ?, last_seen_at = ?, decision_id = ?
         WHERE id = ?",
        rusqlite::params![new_root_id, new_rel_path, now, now, decision_id, source_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    // These three fixtures are duplicated from the shared source repository's
    // test module rather than shared: the tests that stayed there use them too,
    // and a cross-module test helper would tie the two modules together for no
    // gain beyond saving a few lines.

    /// Create an in-memory database with the full schema.
    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    /// Insert a test object and return its ID
    fn insert_object(conn: &RusqliteConnection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test source and return its ID
    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        present: bool,
        excluded: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at, present, excluded)
             VALUES (?, ?, ?, 0, 0, 1000, 1704067200, 'hash', 0, 0, ?, ?)",
            rusqlite::params![root_id, rel_path, object_id, present as i64, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // insert_destination tests
    // =========================================================================

    #[test]
    fn insert_destination_fresh_insert() {
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        let new = NewSource {
            root_id,
            rel_path: "2024/photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify returned Source has correct values
        assert_eq!(source.root_id, root_id);
        assert_eq!(source.rel_path, "2024/photo.jpg");
        assert_eq!(source.size, 1024);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "partial123");
        assert_eq!(source.object_id, Some(obj_id));
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 12345);
        // Fresh insert should have basis_rev = 0
        assert_eq!(source.basis_rev, 0);
        // Should not be excluded
        assert!(!source.excluded);
    }

    #[test]
    fn insert_destination_stale_record_update() {
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert a stale record (present=0) with basis_rev=5
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, device, inode)
             VALUES (?, ?, ?, 500, 1700000000, 'oldhash', 5, 0, 0, 0, 1, 100, 200)",
            rusqlite::params![root_id, "revived.jpg", obj_id],
        )
        .unwrap();

        let new = NewSource {
            root_id,
            rel_path: "revived.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(99999),
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify stale record was updated, not inserted
        assert_eq!(source.rel_path, "revived.jpg");
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "newhash");
        // basis_rev should be incremented from 5 to 6
        assert_eq!(source.basis_rev, 6);
        // device/inode should be updated
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 99999);
        // excluded should be reset to false
        assert!(!source.excluded);

        // Verify only one record exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "revived.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_destination_null_device_inode() {
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Simulate non-Unix platform where device/inode are not available
        let new = NewSource {
            root_id,
            rel_path: "nonunix.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: None, // Not available
            inode: None,  // Not available
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Should succeed with device/inode defaulting to 0
        assert_eq!(source.rel_path, "nonunix.jpg");
        assert_eq!(source.device, 0);
        assert_eq!(source.inode, 0);
        assert_eq!(source.size, 1024);
    }

    #[test]
    fn insert_destination_update_active_record() {
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert an active record (present=1) — simulates a scan that ran between apply runs
        let existing_id = insert_source(&conn, root_id, "existing.jpg", Some(obj_id), true, false);

        let new = NewSource {
            root_id,
            rel_path: "existing.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        // Should succeed — UPDATE fires on the active record, no UNIQUE error
        let source = insert_destination(&conn, &new).unwrap();

        // Verify the active record was updated with new metadata
        assert_eq!(source.id, existing_id);
        assert_eq!(source.rel_path, "existing.jpg");
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "newhash");
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 12345);
        // basis_rev should be incremented
        assert!(source.basis_rev > 0);
        // excluded should be reset
        assert!(!source.excluded);

        // Verify only one record exists (no duplicate)
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "existing.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_destination_idempotent() {
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        let new = NewSource {
            root_id,
            rel_path: "idempotent.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        // First call — INSERT path
        let source1 = insert_destination(&conn, &new).unwrap();
        assert_eq!(source1.size, 1024);
        assert_eq!(source1.basis_rev, 0);

        // Second call — UPDATE path (same data)
        let source2 = insert_destination(&conn, &new).unwrap();
        assert_eq!(source2.size, 1024);
        // basis_rev increments because UPDATE always increments
        assert_eq!(source2.basis_rev, 1);
        assert_eq!(source2.id, source1.id);

        // Verify only one record exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "idempotent.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_destination_returns_complete_source() {
        // Verify the returned Source has all joined fields populated
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", true); // object is excluded

        let new = NewSource {
            root_id,
            rel_path: "complete.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify joined fields from roots table
        assert_eq!(source.root_path, "/archive");
        assert_eq!(source.root_role, "archive");
        assert!(!source.root_suspended);

        // Verify joined fields from objects table
        assert_eq!(source.object_id, Some(obj_id));
        assert_eq!(source.object_excluded, Some(true));

        // Verify domain predicate works with joined data
        assert!(source.is_excluded()); // object is excluded
        assert!(source.is_active()); // root is not suspended
        assert!(source.is_from_role("archive"));

        // Verify path() works
        assert_eq!(source.path(), "/archive/complete.jpg");
    }

    // =========================================================================
    // insert_destination decision_id tests
    // =========================================================================

    #[test]
    fn test_insert_destination_sets_decision_id() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: Some(42),
        };

        let source = insert_destination(&conn, &new).unwrap();
        assert_eq!(source.decision_id, Some(42));

        // Verify it's in the DB
        let db_val: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                [source.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(db_val, Some(42));
    }

    #[test]
    fn test_insert_destination_updates_decision_id() {
        // Re-inserting the same path with a new decision_id overwrites it
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash2", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: Some(10),
        };
        insert_destination(&conn, &new).unwrap();

        // Re-insert with a new decision_id
        let new2 = NewSource {
            decision_id: Some(20),
            ..new
        };
        let source = insert_destination(&conn, &new2).unwrap();
        assert_eq!(source.decision_id, Some(20));
    }

    #[test]
    fn test_insert_destination_null_decision_id() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash3", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();
        assert_eq!(source.decision_id, None);
    }

    // =========================================================================
    // fetch_decision_id_at_path tests
    // =========================================================================

    #[test]
    fn fetch_decision_id_at_path_returns_value() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0, 42)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn fetch_decision_id_at_path_null_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fetch_decision_id_at_path_missing_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let result = fetch_decision_id_at_path(&conn, root_id, "nonexistent.jpg").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fetch_decision_id_at_path_not_present_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 0, 0, 99)",
            rusqlite::params![root_id],
        )
        .unwrap();
        // present = 0, should not be returned
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, None);
    }

    // =========================================================================
    // batch_check_paths_exist tests
    // =========================================================================

    #[test]
    fn batch_check_paths_exist_empty_input() {
        let conn = setup_test_db();
        let _root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let result = batch_check_paths_exist(&conn, 1, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_paths_exist_none_found() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        // No sources exist, query for paths that don't exist
        let result = batch_check_paths_exist(&conn, root_id, &["a.jpg", "b.jpg"]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_paths_exist_all_found() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "a.jpg", None, true, false);
        insert_source(&conn, root_id, "b.jpg", None, true, false);

        let result = batch_check_paths_exist(&conn, root_id, &["a.jpg", "b.jpg"]).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains("a.jpg"));
        assert!(result.contains("b.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_mixed() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "exists.jpg", None, true, false);
        // "missing.jpg" is not inserted

        let result =
            batch_check_paths_exist(&conn, root_id, &["exists.jpg", "missing.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("exists.jpg"));
        assert!(!result.contains("missing.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_ignores_not_present() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=0

        let result =
            batch_check_paths_exist(&conn, root_id, &["present.jpg", "deleted.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("present.jpg"));
        assert!(!result.contains("deleted.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_different_root() {
        let conn = setup_test_db();
        let root1 = crate::core::repo::insert_test_root(&conn, "/archive1", "archive", false);
        let root2 = crate::core::repo::insert_test_root(&conn, "/archive2", "archive", false);

        // Insert in root1
        insert_source(&conn, root1, "file.jpg", None, true, false);

        // Query against root2 - should not find it
        let result = batch_check_paths_exist(&conn, root2, &["file.jpg"]).unwrap();
        assert!(result.is_empty());

        // Query against root1 - should find it
        let result = batch_check_paths_exist(&conn, root1, &["file.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("file.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_handles_999_paths() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create 999 sources (just under BATCH_SIZE)
        let mut paths = Vec::new();
        for i in 0..999 {
            let path = format!("file_{i}.jpg");
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 999);
    }

    #[test]
    fn batch_check_paths_exist_handles_1000_paths() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create exactly BATCH_SIZE sources
        let mut paths = Vec::new();
        for i in 0..1000 {
            let path = format!("file_{i}.jpg");
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 1000);
    }

    #[test]
    fn batch_check_paths_exist_handles_1001_paths() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create more than BATCH_SIZE sources (requires 2 batches)
        let mut paths = Vec::new();
        for i in 0..1001 {
            let path = format!("file_{i}.jpg");
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 1001);

        // Verify samples from both batches
        assert!(result.contains("file_0.jpg"));
        assert!(result.contains("file_999.jpg"));
        assert!(result.contains("file_1000.jpg"));
    }

    // =========================================================================
    // update_location tests
    // =========================================================================

    #[test]
    fn update_location_updates_fields() {
        let conn = setup_test_db();

        let source_root = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
        let archive_root = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);
        let source_id = insert_source(&conn, source_root, "original.jpg", None, true, false);

        let now = 1700000001i64;
        update_location(
            &conn,
            source_id,
            archive_root,
            "new/path.jpg",
            now,
            Some(55),
        )
        .unwrap();

        // Verify fields updated
        let (root_id, rel_path, scanned_at, last_seen_at, decision_id): (i64, String, i64, i64, Option<i64>) = conn
            .query_row(
                "SELECT root_id, rel_path, scanned_at, last_seen_at, decision_id FROM sources WHERE id = ?",
                rusqlite::params![source_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();

        assert_eq!(root_id, archive_root);
        assert_eq!(rel_path, "new/path.jpg");
        assert_eq!(scanned_at, now);
        assert_eq!(last_seen_at, now);
        assert_eq!(decision_id, Some(55));
    }

    #[test]
    fn update_location_nonexistent_source() {
        let conn = setup_test_db();
        let root_id = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Should not error when source doesn't exist (0 rows affected)
        let result = update_location(&conn, 99999, root_id, "path.jpg", 1700000001, None);
        assert!(result.is_ok());
    }
}
