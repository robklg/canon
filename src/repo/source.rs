//! Source repository — infrastructure layer for fetching sources.
//!
//! This module provides batch fetch functions that return `Source` structs
//! from the database. It is intentionally "dumb" — no domain logic here,
//! just data access.
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic in WHERE clauses
//! 2. **Batch fetching**: Avoid N+1 queries by fetching in chunks of BATCH_SIZE
//! 3. **Returns domain types**: Functions return `Source` structs, not raw rows
//! 4. **present=1 baked in**: Only fetches present (non-deleted) sources
//!
//! ## Usage
//!
//! ```ignore
//! use canon::source_repo;
//!
//! // Fetch all sources for specific roots
//! let sources = source_repo::batch_fetch_by_roots(conn, &[1, 2, 3])?;
//! ```

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::types::Value;
use rusqlite::OptionalExtension;

use super::db::Connection;
use crate::domain::source::{NewSource, Source};

/// Batch size for SQL IN clauses. Consistent across all repositories.
pub const BATCH_SIZE: usize = 1000;

/// The columns we SELECT for Source construction.
/// Kept as a constant to ensure consistency across fetch functions.
const SOURCE_COLUMNS: &str = r#"
    s.id,
    s.root_id,
    r.path as root_path,
    s.rel_path,
    s.object_id,
    s.size,
    s.mtime,
    s.excluded,
    o.excluded as object_excluded,
    s.device,
    s.inode,
    s.partial_hash,
    s.basis_rev,
    r.role as root_role,
    r.suspended as root_suspended
"#;

/// The base FROM/JOIN clause for Source queries.
const SOURCE_FROM: &str = r#"
    FROM sources s
    JOIN roots r ON s.root_id = r.id
    LEFT JOIN objects o ON s.object_id = o.id
"#;

/// Construct a Source from a row. Column order must match SOURCE_COLUMNS.
fn source_from_row(row: &rusqlite::Row) -> rusqlite::Result<Source> {
    Ok(Source {
        id: row.get(0)?,
        root_id: row.get(1)?,
        root_path: row.get(2)?,
        rel_path: row.get(3)?,
        object_id: row.get(4)?,
        size: row.get(5)?,
        mtime: row.get(6)?,
        excluded: row.get(7)?,
        object_excluded: row.get(8)?,
        device: row.get(9)?,
        inode: row.get(10)?,
        partial_hash: row.get(11)?,
        basis_rev: row.get(12)?,
        root_role: row.get(13)?,
        root_suspended: row.get(14)?,
    })
}

/// Fetch all present sources for the given root IDs.
///
/// Returns sources in no particular order. Callers should sort if needed.
///
/// This is a simple fetch with no filtering beyond `present = 1`.
/// Domain filtering (scope, exclusion, role) should be done in Rust
/// using the Source predicates.
pub fn batch_fetch_by_roots(conn: &Connection, root_ids: &[i64]) -> Result<Vec<Source>> {
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut sources = Vec::new();

    // Process root_ids in batches
    for chunk in root_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 1 AND s.root_id IN ({})",
            SOURCE_COLUMNS,
            SOURCE_FROM,
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), source_from_row)?;

        for row in rows {
            sources.push(row?);
        }
    }

    Ok(sources)
}

/// Fetch sources by their IDs, returning a HashMap for O(1) lookup.
///
/// This is useful when you have a list of source IDs (e.g., from filter results)
/// and need to fetch the full Source data for each.
///
/// Only present sources are returned. If an ID doesn't exist or the source
/// is not present, it won't appear in the result map.
pub fn batch_fetch_by_ids(conn: &Connection, source_ids: &[i64]) -> Result<HashMap<i64, Source>> {
    if source_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sources = HashMap::with_capacity(source_ids.len());

    // Process source_ids in batches
    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 1 AND s.id IN ({})",
            SOURCE_COLUMNS,
            SOURCE_FROM,
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), source_from_row)?;

        for row in rows {
            let source = row?;
            sources.insert(source.id, source);
        }
    }

    Ok(sources)
}

/// Fetch a single source by root_id and rel_path.
///
/// Returns None if no present source exists at that path.
/// Used internally after insert/update to return the complete Source.
fn fetch_by_root_and_path(conn: &Connection, root_id: i64, rel_path: &str) -> Result<Option<Source>> {
    let sql = format!(
        "SELECT {} {} WHERE s.present = 1 AND s.root_id = ? AND s.rel_path = ?",
        SOURCE_COLUMNS,
        SOURCE_FROM,
    );

    let result = conn
        .query_row(&sql, rusqlite::params![root_id, rel_path], source_from_row)
        .optional()?;

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
/// let created = repo::source::insert_destination(conn, &new_source)?;
/// println!("Created source {} at {}", created.id, created.path());
/// ```
pub fn insert_destination(conn: &Connection, new: &NewSource) -> Result<Source> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    // First try to update an existing stale record (present=0).
    // This preserves the row and increments basis_rev to reflect new content at this path.
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
            excluded = 0
         WHERE root_id = ? AND rel_path = ? AND present = 0",
        rusqlite::params![
            new.device,
            new.inode,
            new.size,
            new.mtime,
            new.partial_hash,
            new.object_id,
            now,
            now,
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
                object_id, basis_rev, scanned_at, last_seen_at, present, excluded
             ) VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?, ?, ?, 0, ?, ?, 1, 0)",
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
            ],
        )?;
    }

    // Fetch the complete Source record with all joined fields.
    // This ensures the returned Source accurately reflects database state.
    fetch_by_root_and_path(conn, new.root_id, &new.rel_path)?
        .ok_or_else(|| anyhow::anyhow!(
            "Failed to fetch source after insert: root_id={}, rel_path={}",
            new.root_id,
            new.rel_path
        ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the canon schema and test data.
    fn setup_test_db() -> RusqliteConnection {
        let conn = RusqliteConnection::open_in_memory().unwrap();

        // Create minimal schema needed for tests
        conn.execute_batch(
            r#"
            CREATE TABLE roots (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'source',
                suspended INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE objects (
                id INTEGER PRIMARY KEY,
                hash_type TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE sources (
                id INTEGER PRIMARY KEY,
                root_id INTEGER NOT NULL REFERENCES roots(id),
                rel_path TEXT NOT NULL,
                object_id INTEGER REFERENCES objects(id),
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                device INTEGER NOT NULL DEFAULT 0,
                inode INTEGER NOT NULL DEFAULT 0,
                partial_hash TEXT NOT NULL DEFAULT '',
                basis_rev INTEGER NOT NULL DEFAULT 0,
                scanned_at INTEGER NOT NULL DEFAULT 0,
                last_seen_at INTEGER NOT NULL DEFAULT 0,
                present INTEGER NOT NULL DEFAULT 1,
                excluded INTEGER NOT NULL DEFAULT 0,
                UNIQUE(root_id, rel_path)
            );
            "#,
        )
        .unwrap();

        conn
    }

    /// Insert a test root and return its ID
    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
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
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, present, excluded)
             VALUES (?, ?, ?, 1000, 1704067200, ?, ?)",
            rusqlite::params![root_id, rel_path, object_id, present as i64, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // batch_fetch_by_roots tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_roots_empty_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_roots(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_roots_no_matching_roots() {
        let conn = setup_test_db();
        // Query for non-existent root IDs
        let result = batch_fetch_by_roots(&conn, &[999, 1000]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_roots_single_root() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg", None, true, false);
        insert_source(&conn, root_id, "b.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify source data is populated correctly
        let source = sources.iter().find(|s| s.rel_path == "a.jpg").unwrap();
        assert_eq!(source.root_path, "/photos");
        assert_eq!(source.root_role, "source");
        assert!(!source.root_suspended);
    }

    #[test]
    fn batch_fetch_by_roots_multiple_roots() {
        let conn = setup_test_db();

        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root1, "photo.jpg", None, true, false);
        insert_source(&conn, root2, "backup.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root1, root2]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify roles are correct
        let photo = sources.iter().find(|s| s.rel_path == "photo.jpg").unwrap();
        assert_eq!(photo.root_role, "source");

        let backup = sources.iter().find(|s| s.rel_path == "backup.jpg").unwrap();
        assert_eq!(backup.root_role, "archive");
    }

    #[test]
    fn batch_fetch_by_roots_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=false

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].rel_path, "present.jpg");
    }

    #[test]
    fn batch_fetch_by_roots_includes_excluded_sources() {
        // Repository layer fetches ALL present sources, including excluded ones.
        // Filtering by exclusion is done in the domain layer.
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "normal.jpg", None, true, false);
        insert_source(&conn, root_id, "excluded.jpg", None, true, true); // excluded=true

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 2);

        let excluded = sources.iter().find(|s| s.rel_path == "excluded.jpg").unwrap();
        assert!(excluded.excluded);
    }

    #[test]
    fn batch_fetch_by_roots_includes_object_excluded() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abc123", true); // object excluded
        insert_source(&conn, root_id, "file.jpg", Some(obj_id), true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);

        let source = &sources[0];
        assert!(!source.excluded); // source not excluded
        assert_eq!(source.object_excluded, Some(true)); // but object is
        assert!(source.is_excluded()); // domain predicate catches both
    }

    #[test]
    fn batch_fetch_by_roots_suspended_root() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", true); // suspended
        insert_source(&conn, root_id, "file.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources[0].root_suspended);
        assert!(!sources[0].is_active()); // domain predicate
    }

    // =========================================================================
    // batch_fetch_by_ids tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_ids_empty_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_no_matching_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[999, 1000]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_returns_hashmap() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "a.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "b.jpg", None, true, false);

        let sources = batch_fetch_by_ids(&conn, &[id1, id2]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify O(1) lookup works
        assert_eq!(sources.get(&id1).unwrap().rel_path, "a.jpg");
        assert_eq!(sources.get(&id2).unwrap().rel_path, "b.jpg");
    }

    #[test]
    fn batch_fetch_by_ids_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        let present_id = insert_source(&conn, root_id, "present.jpg", None, true, false);
        let deleted_id = insert_source(&conn, root_id, "deleted.jpg", None, false, false);

        let sources = batch_fetch_by_ids(&conn, &[present_id, deleted_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&present_id));
        assert!(!sources.contains_key(&deleted_id));
    }

    #[test]
    fn batch_fetch_by_ids_partial_match() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "exists.jpg", None, true, false);

        // Query for mix of existing and non-existing IDs
        let sources = batch_fetch_by_ids(&conn, &[id1, 999, 1000]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&id1));
    }

    // =========================================================================
    // insert_destination tests
    // =========================================================================

    #[test]
    fn insert_destination_fresh_insert() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/archive", "archive", false);
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

        let root_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert a stale record (present=0) with basis_rev=5
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash,
             basis_rev, present, excluded, device, inode)
             VALUES (?, ?, ?, 500, 1700000000, 'oldhash', 5, 0, 1, 100, 200)",
            rusqlite::params![root_id, "revived.jpg", obj_id],
        ).unwrap();

        let new = NewSource {
            root_id,
            rel_path: "revived.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(99999),
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

        let root_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Simulate non-Unix platform where device/inode are not available
        let new = NewSource {
            root_id,
            rel_path: "nonunix.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: None,  // Not available
            inode: None,   // Not available
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Should succeed with device/inode defaulting to 0
        assert_eq!(source.rel_path, "nonunix.jpg");
        assert_eq!(source.device, 0);
        assert_eq!(source.inode, 0);
        assert_eq!(source.size, 1024);
    }

    #[test]
    fn insert_destination_already_present_fails() {
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert an active record (present=1)
        insert_source(&conn, root_id, "existing.jpg", Some(obj_id), true, false);

        let new = NewSource {
            root_id,
            rel_path: "existing.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
        };

        // Should fail due to UNIQUE constraint on (root_id, rel_path)
        let result = insert_destination(&conn, &new);
        assert!(result.is_err());

        // Verify the error mentions constraint violation
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("UNIQUE") || err_msg.contains("constraint"));
    }

    #[test]
    fn insert_destination_returns_complete_source() {
        // Verify the returned Source has all joined fields populated
        let conn = setup_test_db();

        let root_id = insert_root(&conn, "/archive", "archive", false);
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
        assert!(source.is_active());   // root is not suspended
        assert!(source.is_from_role("archive"));

        // Verify path() works
        assert_eq!(source.path(), "/archive/complete.jpg");
    }

}
