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
//! // Get archive paths for objects
//! let paths = object_repo::batch_find_archive_paths(conn, &object_ids)?;
//! ```

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::db::Connection;
use crate::object::Object;

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
}
