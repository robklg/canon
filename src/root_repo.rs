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

use crate::db::Connection;
use crate::root::Root;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the roots schema and optional test data.
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
            "#,
        )
        .unwrap();

        conn
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
}
