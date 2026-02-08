//! Root repository — infrastructure layer for fetching roots.
//!
//! This module provides fetch functions that return `Root` structs from the
//! database. It is intentionally simple — no domain logic here, just data access.
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic in WHERE clauses
//! 2. **Returns domain types**: Functions return `Root` structs, not raw rows
//! 3. **No filtering**: Fetch all roots; domain predicates handle filtering
//!
//! ## Usage
//!
//! ```ignore
//! use canon::root_repo;
//!
//! // Fetch all roots
//! let roots = root_repo::fetch_all(conn)?;
//!
//! // Filter with domain predicates
//! let active_sources: Vec<_> = roots.iter()
//!     .filter(|r| r.is_active())
//!     .filter(|r| r.is_source())
//!     .collect();
//! ```

use std::collections::HashMap;

use anyhow::Result;

use super::db::Connection;
use crate::domain::root::Root;

/// The columns we SELECT for Root construction.
const ROOT_COLUMNS: &str = "id, path, role, comment, last_scanned_at, suspended";

/// Construct a Root from a row. Column order must match ROOT_COLUMNS.
fn root_from_row(row: &rusqlite::Row) -> rusqlite::Result<Root> {
    Ok(Root {
        id: row.get(0)?,
        path: row.get(1)?,
        role: row.get(2)?,
        comment: row.get(3)?,
        last_scanned_at: row.get(4)?,
        suspended: row.get(5)?,
    })
}

/// Fetch all roots.
///
/// Returns roots ordered by ID. No filtering is applied — callers should use
/// domain predicates like `is_active()`, `is_source()`, etc. to filter.
pub fn fetch_all(conn: &Connection) -> Result<Vec<Root>> {
    let sql = format!("SELECT {} FROM roots ORDER BY id", ROOT_COLUMNS);
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], root_from_row)?;

    let mut roots = Vec::new();
    for row in rows {
        roots.push(row?);
    }

    Ok(roots)
}

/// Fetch roots by their IDs, returning a HashMap for O(1) lookup.
///
/// This is useful when you have a list of root IDs and need to fetch
/// the full Root data for each.
///
/// If an ID doesn't exist, it won't appear in the result map.
// Part of the domain model API but not currently used. Kept for API completeness.
#[allow(dead_code)]
pub fn batch_fetch_by_ids(conn: &Connection, root_ids: &[i64]) -> Result<HashMap<i64, Root>> {
    if root_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders: Vec<&str> = root_ids.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT {} FROM roots WHERE id IN ({})",
        ROOT_COLUMNS,
        placeholders.join(",")
    );

    let params: Vec<rusqlite::types::Value> = root_ids
        .iter()
        .map(|&id| rusqlite::types::Value::from(id))
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), root_from_row)?;

    let mut roots = HashMap::with_capacity(root_ids.len());
    for row in rows {
        let root = row?;
        roots.insert(root.id, root);
    }

    Ok(roots)
}

/// Create a new root in the database.
///
/// # Arguments
/// * `conn` - Database connection
/// * `path` - Canonical path of the root directory
/// * `role` - Role of the root ("source" or "archive")
/// * `comment` - Optional comment/description
///
/// # Returns
/// The newly created Root with all fields populated.
pub fn create(conn: &Connection, path: &str, role: &str, comment: Option<&str>) -> Result<Root> {
    conn.execute(
        "INSERT INTO roots (path, role, comment) VALUES (?, ?, ?)",
        rusqlite::params![path, role, comment],
    )?;
    let id = conn.last_insert_rowid();

    // Fetch the complete Root to ensure consistency with database state.
    // This follows the insert_destination() pattern from source.rs.
    let sql = format!("SELECT {} FROM roots WHERE id = ?", ROOT_COLUMNS);
    let root = conn.query_row(&sql, [id], root_from_row)?;
    Ok(root)
}

/// Update the last_scanned_at timestamp for a root.
///
/// Called after a full root scan completes (not for subdirectory scans).
pub fn update_last_scanned_at(conn: &Connection, root_id: i64, timestamp: i64) -> Result<()> {
    conn.execute(
        "UPDATE roots SET last_scanned_at = ? WHERE id = ?",
        rusqlite::params![timestamp, root_id],
    )?;
    Ok(())
}

/// Insert a root for testing purposes.
///
/// This function is only available in test builds. It provides a simple way
/// to set up test data without duplicating INSERT SQL across test modules.
#[cfg(test)]
pub fn insert_test_root(
    conn: &Connection,
    path: &str,
    role: &str,
    suspended: bool,
) -> i64 {
    conn.execute(
        "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
        rusqlite::params![path, role, suspended as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the full schema.
    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    /// Insert a test root and return its ID.
    fn insert_root(
        conn: &RusqliteConnection,
        path: &str,
        role: &str,
        comment: Option<&str>,
        last_scanned_at: Option<i64>,
        suspended: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, comment, last_scanned_at, suspended) VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![path, role, comment, last_scanned_at, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // fetch_all tests
    // =========================================================================

    #[test]
    fn fetch_all_empty() {
        let conn = setup_test_db();
        let roots = fetch_all(&conn).unwrap();
        assert!(roots.is_empty());
    }

    #[test]
    fn fetch_all_returns_all() {
        let conn = setup_test_db();

        insert_root(&conn, "/photos", "source", None, None, false);
        insert_root(&conn, "/archive", "archive", Some("backup"), Some(1704067200), false);

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots.len(), 2);

        // Verify order (by ID)
        assert_eq!(roots[0].path, "/photos");
        assert_eq!(roots[1].path, "/archive");

        // Verify all fields populated
        let archive = &roots[1];
        assert_eq!(archive.role, "archive");
        assert_eq!(archive.comment, Some("backup".to_string()));
        assert_eq!(archive.last_scanned_at, Some(1704067200));
        assert!(!archive.suspended);
    }

    #[test]
    fn fetch_all_includes_suspended() {
        let conn = setup_test_db();

        insert_root(&conn, "/active", "source", None, None, false);
        insert_root(&conn, "/suspended", "source", None, None, true);

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots.len(), 2);

        // Both are returned; filtering is caller's job
        let suspended = roots.iter().find(|r| r.path == "/suspended").unwrap();
        assert!(suspended.is_suspended());
    }

    #[test]
    fn fetch_all_with_domain_predicates() {
        let conn = setup_test_db();

        insert_root(&conn, "/photos", "source", None, None, false);
        insert_root(&conn, "/archive", "archive", None, None, false);
        insert_root(&conn, "/suspended", "source", None, None, true);

        let roots = fetch_all(&conn).unwrap();

        // Use domain predicates to filter
        let active_sources: Vec<_> = roots
            .iter()
            .filter(|r| r.is_active())
            .filter(|r| r.is_source())
            .collect();

        assert_eq!(active_sources.len(), 1);
        assert_eq!(active_sources[0].path, "/photos");
    }

    // =========================================================================
    // batch_fetch_by_ids tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_ids_empty() {
        let conn = setup_test_db();
        let roots = batch_fetch_by_ids(&conn, &[]).unwrap();
        assert!(roots.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_found() {
        let conn = setup_test_db();

        let id1 = insert_root(&conn, "/photos", "source", None, None, false);
        let id2 = insert_root(&conn, "/archive", "archive", None, None, false);

        let roots = batch_fetch_by_ids(&conn, &[id1, id2]).unwrap();
        assert_eq!(roots.len(), 2);

        // Verify O(1) lookup works
        assert_eq!(roots.get(&id1).unwrap().path, "/photos");
        assert_eq!(roots.get(&id2).unwrap().path, "/archive");
    }

    #[test]
    fn batch_fetch_by_ids_partial() {
        let conn = setup_test_db();

        let id1 = insert_root(&conn, "/photos", "source", None, None, false);

        // Query for mix of existing and non-existing IDs
        let roots = batch_fetch_by_ids(&conn, &[id1, 999, 1000]).unwrap();
        assert_eq!(roots.len(), 1);
        assert!(roots.contains_key(&id1));
        assert!(!roots.contains_key(&999));
    }

    #[test]
    fn batch_fetch_by_ids_no_matching() {
        let conn = setup_test_db();

        insert_root(&conn, "/photos", "source", None, None, false);

        let roots = batch_fetch_by_ids(&conn, &[999, 1000]).unwrap();
        assert!(roots.is_empty());
    }

    // =========================================================================
    // create tests
    // =========================================================================

    #[test]
    fn create_returns_complete_root() {
        let conn = setup_test_db();

        let root = create(&conn, "/photos", "source", None).unwrap();

        // Verify returned Root has all fields populated correctly
        assert!(root.id > 0);
        assert_eq!(root.path, "/photos");
        assert_eq!(root.role, "source");
        assert_eq!(root.comment, None);
        assert_eq!(root.last_scanned_at, None);
        assert!(!root.suspended);

        // Verify it matches what's in the database
        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, root.id);
    }

    #[test]
    fn create_with_comment() {
        let conn = setup_test_db();

        let root = create(&conn, "/archive", "archive", Some("My archive")).unwrap();

        // Verify returned Root includes comment
        assert_eq!(root.path, "/archive");
        assert_eq!(root.role, "archive");
        assert_eq!(root.comment, Some("My archive".to_string()));

        // Verify it matches what's in the database
        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, root.id);
        assert_eq!(roots[0].comment, Some("My archive".to_string()));
    }

    #[test]
    fn create_multiple_roots() {
        let conn = setup_test_db();

        let root1 = create(&conn, "/photos", "source", None).unwrap();
        let root2 = create(&conn, "/archive", "archive", None).unwrap();

        // Verify different IDs
        assert_ne!(root1.id, root2.id);

        // Verify returned objects have correct data
        assert_eq!(root1.path, "/photos");
        assert_eq!(root2.path, "/archive");

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots.len(), 2);
    }

    // =========================================================================
    // update_last_scanned_at tests
    // =========================================================================

    #[test]
    fn update_last_scanned_at_sets_timestamp() {
        let conn = setup_test_db();
        let id = insert_root(&conn, "/photos", "source", None, None, false);

        // Initially None
        let roots = fetch_all(&conn).unwrap();
        assert!(roots[0].last_scanned_at.is_none());

        // Update timestamp
        update_last_scanned_at(&conn, id, 1700000001).unwrap();

        // Verify updated
        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots[0].last_scanned_at, Some(1700000001));
    }

    #[test]
    fn update_last_scanned_at_overwrites() {
        let conn = setup_test_db();
        let id = insert_root(&conn, "/photos", "source", None, Some(1700000000), false);

        update_last_scanned_at(&conn, id, 1700000001).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots[0].last_scanned_at, Some(1700000001));
    }

    #[test]
    fn update_last_scanned_at_nonexistent_root() {
        let conn = setup_test_db();

        // Should not error when root doesn't exist
        let result = update_last_scanned_at(&conn, 99999, 1700000001);
        assert!(result.is_ok());
    }
}
