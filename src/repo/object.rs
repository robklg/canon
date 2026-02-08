//! Object repository — infrastructure layer for fetching objects.
//!
//! This module provides batch fetch functions for objects and archive detection.
//! Archive detection answers: "Is this content in any archive root?"
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic in WHERE clauses
//! 2. **Returns domain types**: Functions return `Object` structs, not raw rows
//! 3. **Batch operations**: All functions handle large ID sets via chunking
//!
//! ## Usage
//!
//! ```ignore
//! use canon::object_repo;
//!
//! // Fetch objects by ID
//! let objects = object_repo::batch_fetch_by_ids(conn, &object_ids)?;
//!
//! // Check which objects are in any archive
//! let archived = object_repo::batch_check_archived(conn, &object_ids, None)?;
//!
//! // Check which objects are in a specific archive
//! let in_archive = object_repo::batch_check_archived(conn, &object_ids, Some(archive_root_id))?;
//!
//! // Get archive paths for objects (by object_id)
//! let paths = object_repo::batch_find_archive_paths(conn, &object_ids)?;
//!
//! // Get archive info by content hash (for manifest workflows)
//! let info = object_repo::batch_find_archive_info_by_hash(conn, &["abc123", "def456"])?;
//! ```

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::OptionalExtension;

use super::db::Connection;
use crate::domain::object::Object;

/// Batch size for SQL IN clauses (consistent with other repos).
pub const BATCH_SIZE: usize = 1000;

/// The columns we SELECT for Object construction.
const OBJECT_COLUMNS: &str = "id, hash_type, hash_value, excluded";

/// Construct an Object from a row. Column order must match OBJECT_COLUMNS.
fn object_from_row(row: &rusqlite::Row) -> rusqlite::Result<Object> {
    Ok(Object {
        id: row.get(0)?,
        hash_type: row.get(1)?,
        hash_value: row.get(2)?,
        excluded: row.get(3)?,
    })
}

/// Fetch objects by their IDs.
///
/// Returns HashMap for O(1) lookup. Missing IDs are not included in the result.
pub fn batch_fetch_by_ids(conn: &Connection, object_ids: &[i64]) -> Result<HashMap<i64, Object>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::with_capacity(object_ids.len());

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} FROM objects WHERE id IN ({})",
            OBJECT_COLUMNS,
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&id| rusqlite::types::Value::from(id))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), object_from_row)?;

        for row in rows {
            let obj = row?;
            result.insert(obj.id, obj);
        }
    }

    Ok(result)
}

/// Fetch an object by its hash value.
///
/// # Returns
/// - `Ok(Some(Object))` if found
/// - `Ok(None)` if no object with that hash exists
///
/// This is a single-row lookup, not a batch operation.
pub fn fetch_by_hash(conn: &Connection, hash: &str) -> Result<Option<Object>> {
    let sql = format!("SELECT {} FROM objects WHERE hash_value = ?", OBJECT_COLUMNS);

    let result = conn
        .query_row(&sql, [hash], object_from_row)
        .optional()?;

    Ok(result)
}

/// Check which objects have copies in archive root(s).
///
/// Returns set of object IDs that have at least one source in an archive root.
/// An object is "archived" if EXISTS a source with that object_id under a
/// root with role='archive' and present=1.
///
/// If `archive_root_id` is Some, checks only that specific archive.
/// If `archive_root_id` is None, checks all archive roots.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_check_archived(
    conn: &Connection,
    object_ids: &[i64],
    archive_root_id: Option<i64>,
) -> Result<HashSet<i64>> {
    if object_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let mut result = HashSet::new();

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();

        let (sql, params): (String, Vec<rusqlite::types::Value>) = if let Some(root_id) = archive_root_id {
            // Specific archive root - no need to join roots table
            let sql = format!(
                "SELECT DISTINCT s.object_id
                 FROM sources s
                 WHERE s.root_id = ? AND s.present = 1
                   AND s.object_id IN ({})",
                placeholders.join(",")
            );
            let mut params = vec![rusqlite::types::Value::from(root_id)];
            params.extend(chunk.iter().map(|&id| rusqlite::types::Value::from(id)));
            (sql, params)
        } else {
            // Any archive root - need to join roots table
            let sql = format!(
                "SELECT DISTINCT s.object_id
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE r.role = 'archive' AND s.present = 1
                   AND s.object_id IN ({})",
                placeholders.join(",")
            );
            let params: Vec<rusqlite::types::Value> = chunk
                .iter()
                .map(|&id| rusqlite::types::Value::from(id))
                .collect();
            (sql, params)
        };

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| row.get::<_, i64>(0))?;

        for row in rows {
            result.insert(row?);
        }
    }

    Ok(result)
}

/// Find archive paths for objects.
///
/// Returns map from object_id to list of archive paths where that content exists.
/// Only includes objects that have archive copies. Objects without archive copies
/// are not included in the result map.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_find_archive_paths(
    conn: &Connection,
    object_ids: &[i64],
) -> Result<HashMap<i64, Vec<String>>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<i64, Vec<String>> = HashMap::new();

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT s.object_id, r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE r.role = 'archive' AND s.present = 1
               AND s.object_id IN ({})
             ORDER BY s.object_id, r.path, s.rel_path",
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&id| rusqlite::types::Value::from(id))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let object_id: i64 = row.get(0)?;
            let root_path: String = row.get(1)?;
            let rel_path: String = row.get(2)?;
            Ok((object_id, root_path, rel_path))
        })?;

        for row in rows {
            let (object_id, root_path, rel_path) = row?;
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{}/{}", root_path, rel_path)
            };
            result.entry(object_id).or_default().push(full_path);
        }
    }

    Ok(result)
}

/// Find archive info for objects identified by content hash.
///
/// This function is designed for manifest workflows where `hash_value` is the
/// content identifier (see D3 in write-infrastructure spec). It returns archive
/// location information needed for conflict detection.
///
/// # Behavior
/// - Looks up objects by hash_value (sha256), finds all archive copies
/// - Returns the archive root_id along with the full path
/// - Only includes present sources in archive-role roots
/// - Handles large inputs via chunking (BATCH_SIZE)
///
/// # Returns
/// Map from hash_value to list of (archive_root_id, full_path) tuples.
/// Hashes not found in any archive are not included in the result.
/// Results are ordered by archive root_id, then rel_path within each hash.
///
/// # Caller Responsibilities
/// - Filter out sources without hash values before calling
/// - Use the archive_root_id to distinguish destination archive from others
pub fn batch_find_archive_info_by_hash(
    conn: &Connection,
    hash_values: &[&str],
) -> Result<HashMap<String, Vec<(i64, String)>>> {
    if hash_values.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<String, Vec<(i64, String)>> = HashMap::new();

    for chunk in hash_values.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT o.hash_value, r.id, r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.role = 'archive' AND s.present = 1
               AND o.hash_value IN ({})
             ORDER BY o.hash_value, r.id, s.rel_path",
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&h| rusqlite::types::Value::from(h.to_string()))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let hash_value: String = row.get(0)?;
            let archive_root_id: i64 = row.get(1)?;
            let root_path: String = row.get(2)?;
            let rel_path: String = row.get(3)?;
            Ok((hash_value, archive_root_id, root_path, rel_path))
        })?;

        for row in rows {
            let (hash_value, archive_root_id, root_path, rel_path) = row?;
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{}/{}", root_path, rel_path)
            };
            result
                .entry(hash_value)
                .or_default()
                .push((archive_root_id, full_path));
        }
    }

    Ok(result)
}

/// Set the exclusion flag for an object.
///
/// # Behavior
/// - Updates `excluded` column to the specified value
/// - No error if object doesn't exist (0 rows affected)
/// - Affects all sources linked to this object (via Source::is_excluded() predicate)
///
/// # Returns
/// Ok(()) on success.
pub fn set_excluded(conn: &Connection, object_id: i64, excluded: bool) -> Result<()> {
    conn.execute(
        "UPDATE objects SET excluded = ? WHERE id = ?",
        rusqlite::params![excluded as i64, object_id],
    )?;
    Ok(())
}

/// Fetch all excluded objects.
///
/// Returns a Vec of Object structs where excluded = 1, ordered by id.
/// Used by `exclude list --objects` to show all excluded objects.
pub fn fetch_excluded(conn: &Connection) -> Result<Vec<Object>> {
    let sql = format!(
        "SELECT {} FROM objects WHERE excluded = 1 ORDER BY id",
        OBJECT_COLUMNS
    );

    let mut stmt = conn.prepare(&sql)?;
    let objects = stmt
        .query_map([], object_from_row)?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(objects)
}

/// Get or create an object by hash, returning the complete Object.
///
/// This is an idempotent operation: if an object with the given hash exists,
/// it is returned; otherwise a new object is created and returned.
///
/// # Arguments
/// * `conn` - Database connection
/// * `hash_type` - Type of hash (e.g., "sha256")
/// * `hash_value` - The hash value
///
/// # Returns
/// The existing or newly created Object with all fields populated.
pub fn get_or_create(conn: &Connection, hash_type: &str, hash_value: &str) -> Result<Object> {
    // Try to find existing object - return full Object if found
    let sql = format!(
        "SELECT {} FROM objects WHERE hash_type = ? AND hash_value = ?",
        OBJECT_COLUMNS
    );
    let existing = conn
        .query_row(&sql, rusqlite::params![hash_type, hash_value], object_from_row)
        .optional()?;

    if let Some(obj) = existing {
        return Ok(obj);
    }

    // Create new object
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES (?, ?)",
        rusqlite::params![hash_type, hash_value],
    )?;
    let id = conn.last_insert_rowid();

    // Fetch the complete Object to ensure consistency with database state.
    // This follows the insert_destination() pattern from source.rs.
    let fetch_sql = format!("SELECT {} FROM objects WHERE id = ?", OBJECT_COLUMNS);
    let obj = conn.query_row(&fetch_sql, [id], object_from_row)?;
    Ok(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the required schema.
    fn setup_test_db() -> RusqliteConnection {
        let conn = RusqliteConnection::open_in_memory().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE roots (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL DEFAULT 'source',
                comment TEXT,
                last_scanned_at INTEGER,
                suspended INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE objects (
                id INTEGER PRIMARY KEY,
                hash_type TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                excluded INTEGER NOT NULL DEFAULT 0,
                UNIQUE(hash_type, hash_value)
            );

            CREATE TABLE sources (
                id INTEGER PRIMARY KEY,
                root_id INTEGER NOT NULL REFERENCES roots(id),
                rel_path TEXT NOT NULL,
                device INTEGER NOT NULL DEFAULT 0,
                inode INTEGER NOT NULL DEFAULT 0,
                size INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER NOT NULL DEFAULT 0,
                partial_hash TEXT NOT NULL DEFAULT '',
                basis_rev INTEGER NOT NULL DEFAULT 0,
                scanned_at INTEGER NOT NULL DEFAULT 0,
                last_seen_at INTEGER NOT NULL DEFAULT 0,
                present INTEGER NOT NULL DEFAULT 1,
                object_id INTEGER REFERENCES objects(id),
                excluded INTEGER NOT NULL DEFAULT 0,
                UNIQUE(root_id, rel_path)
            );
            "#,
        )
        .unwrap();

        conn
    }

    /// Insert a test root and return its ID.
    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role) VALUES (?, ?)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test object and return its ID.
    fn insert_object(conn: &RusqliteConnection, hash_value: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash_value, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test source and return its ID.
    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        present: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, present) VALUES (?, ?, ?, ?)",
            rusqlite::params![root_id, rel_path, object_id, present as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // batch_fetch_by_ids tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_ids_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_found() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", false);

        let result = batch_fetch_by_ids(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let obj = result.get(&obj_id).unwrap();
        assert_eq!(obj.id, obj_id);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123");
        assert!(!obj.excluded);
    }

    #[test]
    fn batch_fetch_by_ids_partial_missing_ids_ignored() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", false);

        // Query for existing and non-existing IDs
        let result = batch_fetch_by_ids(&conn, &[obj_id, 999, 1000]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&obj_id));
        assert!(!result.contains_key(&999));
    }

    #[test]
    fn batch_fetch_by_ids_includes_excluded_objects() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", true);

        let result = batch_fetch_by_ids(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let obj = result.get(&obj_id).unwrap();
        assert!(obj.is_excluded());
    }

    // =========================================================================
    // batch_check_archived tests
    // =========================================================================

    #[test]
    fn batch_check_archived_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_check_archived(&conn, &[], None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_finds_archived_objects() {
        let conn = setup_test_db();

        // Setup: archive root with a source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source root (not archive) with a source
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should not find it because root is not an archive
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_requires_present_source() {
        let conn = setup_test_db();

        // Setup: archive root with a non-present source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should not find it because source is not present
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_deduplicates_multiple_archive_sources() {
        let conn = setup_test_db();

        // Setup: same object in two different archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file1.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "file2.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should only contain the object_id once
        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_specific_root_filters_correctly() {
        let conn = setup_test_db();

        // Setup: object in specific archive
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let _archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);

        // Check with specific archive root
        let result = batch_check_archived(&conn, &[obj_id], Some(archive1_id)).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_specific_root_ignores_other_archives() {
        let conn = setup_test_db();

        // Setup: object in archive1, but we query archive2
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);

        // Check with different archive root
        let result = batch_check_archived(&conn, &[obj_id], Some(archive2_id)).unwrap();

        // Should not find it because object is in archive1, not archive2
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_handles_large_id_sets() {
        let conn = setup_test_db();

        // Setup: one archive with many objects
        let archive_id = insert_root(&conn, "/archive", "archive");

        // Create more than BATCH_SIZE objects (1000+)
        let mut object_ids = Vec::new();
        for i in 0..1050 {
            let obj_id = insert_object(&conn, &format!("hash_{}", i), false);
            object_ids.push(obj_id);
            // Put every 10th object in archive
            if i % 10 == 0 {
                insert_source(&conn, archive_id, &format!("file_{}.jpg", i), Some(obj_id), true);
            }
        }

        let result = batch_check_archived(&conn, &object_ids, None).unwrap();

        // Should find 105 objects (every 10th from 0 to 1040)
        assert_eq!(result.len(), 105);
    }

    // =========================================================================
    // batch_find_archive_paths tests
    // =========================================================================

    #[test]
    fn batch_find_archive_paths_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_find_archive_paths(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_paths_returns_correct_path_format() {
        let conn = setup_test_db();

        // Setup: archive with source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "subdir/file.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "/archive/subdir/file.jpg");
    }

    #[test]
    fn batch_find_archive_paths_empty_rel_path() {
        let conn = setup_test_db();

        // Setup: source at root of archive (empty rel_path)
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths[0], "/archive"); // No trailing slash
    }

    #[test]
    fn batch_find_archive_paths_multiple_paths_per_object() {
        let conn = setup_test_db();

        // Setup: same object in two archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "copy.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths.len(), 2);
        // Ordered by root path, then rel_path
        assert_eq!(paths[0], "/archive1/file.jpg");
        assert_eq!(paths[1], "/archive2/copy.jpg");
    }

    #[test]
    fn batch_find_archive_paths_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source in non-archive root
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        // Should not include the source root path
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_paths_excludes_non_present() {
        let conn = setup_test_db();

        // Setup: non-present source in archive
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert!(result.is_empty());
    }

    // =========================================================================
    // batch_find_archive_info_by_hash tests
    // =========================================================================

    #[test]
    fn batch_find_archive_info_by_hash_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_find_archive_info_by_hash(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_single_hash_single_archive() {
        let conn = setup_test_db();

        // Setup: archive with source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "subdir/file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert_eq!(result.len(), 1);
        let info = result.get("abc123").unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].0, archive_id); // archive_root_id
        assert_eq!(info[0].1, "/archive/subdir/file.jpg"); // full_path
    }

    #[test]
    fn batch_find_archive_info_by_hash_returns_archive_root_id() {
        let conn = setup_test_db();

        // Setup: two archives with different content
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj1_id = insert_object(&conn, "hash1", false);
        let obj2_id = insert_object(&conn, "hash2", false);
        insert_source(&conn, archive1_id, "file1.jpg", Some(obj1_id), true);
        insert_source(&conn, archive2_id, "file2.jpg", Some(obj2_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["hash1", "hash2"]).unwrap();

        // Verify we can distinguish which archive each hash is in
        let info1 = result.get("hash1").unwrap();
        assert_eq!(info1[0].0, archive1_id);

        let info2 = result.get("hash2").unwrap();
        assert_eq!(info2[0].0, archive2_id);
    }

    #[test]
    fn batch_find_archive_info_by_hash_multiple_archives_per_hash() {
        let conn = setup_test_db();

        // Setup: same hash in two archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "copy.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert_eq!(result.len(), 1);
        let info = result.get("abc123").unwrap();
        assert_eq!(info.len(), 2);
        // Ordered by archive root_id
        assert_eq!(info[0].0, archive1_id);
        assert_eq!(info[0].1, "/archive1/file.jpg");
        assert_eq!(info[1].0, archive2_id);
        assert_eq!(info[1].1, "/archive2/copy.jpg");
    }

    #[test]
    fn batch_find_archive_info_by_hash_empty_rel_path() {
        let conn = setup_test_db();

        // Setup: source at root of archive (empty rel_path)
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        let info = result.get("abc123").unwrap();
        assert_eq!(info[0].1, "/archive"); // No trailing slash
    }

    #[test]
    fn batch_find_archive_info_by_hash_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source in non-archive root
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        // Should not include the source root
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_excludes_non_present() {
        let conn = setup_test_db();

        // Setup: non-present source in archive
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_not_found_hashes_excluded() {
        let conn = setup_test_db();

        // Setup: one hash exists, one doesn't
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "exists", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["exists", "missing"]).unwrap();

        // Only the existing hash should be in results
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("exists"));
        assert!(!result.contains_key("missing"));
    }

    #[test]
    fn batch_find_archive_info_by_hash_handles_large_hash_sets() {
        let conn = setup_test_db();

        // Setup: one archive with many objects
        let archive_id = insert_root(&conn, "/archive", "archive");

        // Create more than BATCH_SIZE hashes (1000+)
        let mut hashes: Vec<String> = Vec::new();
        for i in 0..1050 {
            let hash = format!("hash_{}", i);
            let obj_id = insert_object(&conn, &hash, false);
            hashes.push(hash);
            // Put every 10th object in archive
            if i % 10 == 0 {
                insert_source(&conn, archive_id, &format!("file_{}.jpg", i), Some(obj_id), true);
            }
        }

        let hash_refs: Vec<&str> = hashes.iter().map(|s| s.as_str()).collect();
        let result = batch_find_archive_info_by_hash(&conn, &hash_refs).unwrap();

        // Should find 105 hashes (every 10th from 0 to 1040)
        assert_eq!(result.len(), 105);
    }

    // =========================================================================
    // set_excluded tests
    // =========================================================================

    #[test]
    fn set_excluded_marks_object() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", false);

        // Verify initially not excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            rusqlite::params![obj_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 0);

        // Set excluded
        set_excluded(&conn, obj_id, true).unwrap();

        // Verify now excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            rusqlite::params![obj_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 1);
    }

    #[test]
    fn set_excluded_clears_object() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", true); // starts excluded

        // Verify initially excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            rusqlite::params![obj_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 1);

        // Clear excluded
        set_excluded(&conn, obj_id, false).unwrap();

        // Verify now not excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            rusqlite::params![obj_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 0);
    }

    #[test]
    fn set_excluded_nonexistent_object() {
        let conn = setup_test_db();

        // Should not error when object doesn't exist
        let result = set_excluded(&conn, 99999, true);
        assert!(result.is_ok());
    }

    // =========================================================================
    // fetch_by_hash tests
    // =========================================================================

    #[test]
    fn fetch_by_hash_returns_object() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123def456", false);

        let result = fetch_by_hash(&conn, "abc123def456").unwrap();

        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.id, obj_id);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123def456");
        assert!(!obj.excluded);
    }

    #[test]
    fn fetch_by_hash_not_found() {
        let conn = setup_test_db();

        let result = fetch_by_hash(&conn, "nonexistent_hash").unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_hash_returns_excluded_flag() {
        let conn = setup_test_db();
        insert_object(&conn, "excluded_hash", true);

        let result = fetch_by_hash(&conn, "excluded_hash").unwrap();

        assert!(result.is_some());
        let obj = result.unwrap();
        assert!(obj.excluded);
    }

    #[test]
    fn fetch_excluded_returns_only_excluded() {
        let conn = setup_test_db();

        // Insert mix of excluded and non-excluded
        insert_object(&conn, "excluded1", true);
        insert_object(&conn, "not_excluded", false);
        insert_object(&conn, "excluded2", true);

        let result = fetch_excluded(&conn).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|o| o.excluded));
        // Ordered by id
        assert_eq!(result[0].hash_value, "excluded1");
        assert_eq!(result[1].hash_value, "excluded2");
    }

    #[test]
    fn fetch_excluded_empty_when_none_excluded() {
        let conn = setup_test_db();

        insert_object(&conn, "not_excluded1", false);
        insert_object(&conn, "not_excluded2", false);

        let result = fetch_excluded(&conn).unwrap();

        assert!(result.is_empty());
    }

    // =========================================================================
    // get_or_create tests
    // =========================================================================

    #[test]
    fn get_or_create_creates_new_returns_complete_object() {
        let conn = setup_test_db();

        let obj = get_or_create(&conn, "sha256", "abc123").unwrap();

        // Verify returned Object has all fields populated correctly
        assert!(obj.id > 0);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123");
        assert!(!obj.excluded); // New objects are not excluded

        // Verify object was created in database
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE hash_type = 'sha256' AND hash_value = 'abc123'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_returns_existing_with_excluded_flag() {
        let conn = setup_test_db();

        // Create an excluded object first
        let id1 = insert_object(&conn, "existing_hash", true);

        // get_or_create should return the same object with excluded=true
        let obj = get_or_create(&conn, "sha256", "existing_hash").unwrap();
        assert_eq!(obj.id, id1);
        assert_eq!(obj.hash_value, "existing_hash");
        assert!(obj.excluded); // Preserved from existing object

        // Verify only one object exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE hash_value = 'existing_hash'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_is_idempotent() {
        let conn = setup_test_db();

        // Call multiple times with same hash
        let obj1 = get_or_create(&conn, "sha256", "same_hash").unwrap();
        let obj2 = get_or_create(&conn, "sha256", "same_hash").unwrap();
        let obj3 = get_or_create(&conn, "sha256", "same_hash").unwrap();

        // All should return same Object
        assert_eq!(obj1.id, obj2.id);
        assert_eq!(obj2.id, obj3.id);
        assert_eq!(obj1.hash_value, "same_hash");

        // Verify only one object exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE hash_value = 'same_hash'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_different_hashes() {
        let conn = setup_test_db();

        let obj1 = get_or_create(&conn, "sha256", "hash1").unwrap();
        let obj2 = get_or_create(&conn, "sha256", "hash2").unwrap();

        // Different IDs and hash values
        assert_ne!(obj1.id, obj2.id);
        assert_eq!(obj1.hash_value, "hash1");
        assert_eq!(obj2.hash_value, "hash2");

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 2);
    }
}
