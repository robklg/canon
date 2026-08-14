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

use anyhow::Result;

use super::db::Connection;
use crate::domain::root::Root;

/// The columns we SELECT for Root construction.
pub(crate) const ROOT_COLUMNS: &str = "id, path, role, comment, last_scanned_at, suspended";

/// Construct a Root from a row. Column order must match ROOT_COLUMNS.
pub(crate) fn root_from_row(row: &rusqlite::Row) -> rusqlite::Result<Root> {
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
    let sql = format!("SELECT {ROOT_COLUMNS} FROM roots ORDER BY id");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], root_from_row)?;

    let mut roots = Vec::new();
    for row in rows {
        roots.push(row?);
    }

    Ok(roots)
}

/// Insert a root for testing purposes.
///
/// This function is only available in test builds. It provides a simple way
/// to set up test data without duplicating INSERT SQL across test modules.
#[cfg(test)]
pub fn insert_test_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
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
        insert_root(
            &conn,
            "/archive",
            "archive",
            Some("backup"),
            Some(1704067200),
            false,
        );

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
}
