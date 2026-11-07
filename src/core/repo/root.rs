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
//! use crate::core::repo;
//!
//! // Fetch all roots
//! let roots = repo::root::fetch_all(conn)?;
//!
//! // Filter with domain predicates
//! let active_sources: Vec<_> = roots.iter()
//!     .filter(|r| !r.is_suspended())
//!     .filter(|r| r.is_source())
//!     .collect();
//! ```

use anyhow::Result;

use super::db::Connection;
use crate::core::domain::root::Root;

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
/// domain predicates like `is_suspended()`, `is_source()`, etc. to filter.
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

/// The next root number: one above every number this index has given.
///
/// The root-number law — *a root's number is never reused* — spoken once. A
/// removed root's number lives on in the records that outlive it (scopes,
/// extraction rows on both ends, receipt pointers), and a root that takes it
/// inherits that history into its story and its book, which is never
/// rewritten. So the next number is bounded by every column that names a
/// root, and by the mark [`remember_root_number`] leaves at removal for the
/// case where no record names it (`recording = "Off"`). The columns are the
/// ones named `root_id` or ending `_root_id`, pinned by
/// `every_column_naming_a_root_bounds_the_next_number`.
///
/// A scalar expression, so it runs inside the one `INSERT` that creates the
/// root ([`insert_numbered`]) and concurrent creators serialise on the write
/// lock.
///
/// Where it does not apply: an index that already gave a number twice (it
/// repairs nothing), and numbers across two indexes (a number is local to one).
const NEXT_ROOT_NUMBER_SQL: &str = "1 + MAX(
    COALESCE((SELECT MAX(id) FROM roots), 0),
    COALESCE((SELECT MAX(root_id) FROM sources), 0),
    COALESCE((SELECT MAX(root_id) FROM notes), 0),
    COALESCE((SELECT MAX(root_id) FROM decision_scopes), 0),
    COALESCE((SELECT MAX(root_id) FROM decision_extractions), 0),
    COALESCE((SELECT MAX(destination_root_id) FROM decision_extractions), 0),
    COALESCE((SELECT MAX(receipt_root_id) FROM decisions), 0),
    COALESCE((SELECT value_int FROM _meta WHERE key = 'root_number_high_water'), 0)
)";

/// Insert a root under the next number ([`NEXT_ROOT_NUMBER_SQL`]) and return
/// that number. One statement: the number is chosen and taken in the same
/// write, and `path UNIQUE` still refuses a second root at a path.
pub fn insert_numbered(
    conn: &Connection,
    path: &str,
    role: &str,
    comment: Option<&str>,
) -> Result<i64> {
    conn.execute(
        &format!(
            "INSERT INTO roots (id, path, role, comment) SELECT ({NEXT_ROOT_NUMBER_SQL}), ?, ?, ?"
        ),
        rusqlite::params![path, role, comment],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Remember that `root_id` was given, before the root that holds it is removed.
///
/// The mark only ever rises. Written before the delete, in whatever
/// transaction the caller holds: a removal that fails after the mark leaves a
/// mark above a live root, which is harmless; the other order could lose it.
pub fn remember_root_number(conn: &Connection, root_id: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO _meta (key, value_int) VALUES ('root_number_high_water', ?)
         ON CONFLICT(key) DO UPDATE SET value_int = MAX(value_int, excluded.value_int)",
        [root_id],
    )?;
    Ok(())
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
    use crate::core::repo::open_in_memory_for_test;
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
            .filter(|r| !r.is_suspended())
            .filter(|r| r.is_source())
            .collect();

        assert_eq!(active_sources.len(), 1);
        assert_eq!(active_sources[0].path, "/photos");
    }

    // =========================================================================
    // the root-number law
    // =========================================================================

    #[test]
    fn a_fresh_index_gives_number_one() {
        let conn = setup_test_db();
        assert_eq!(insert_numbered(&conn, "/a", "source", None).unwrap(), 1);
        assert_eq!(insert_numbered(&conn, "/b", "source", None).unwrap(), 2);
    }

    #[test]
    fn every_column_naming_a_root_bounds_the_next_number() {
        // Derived from the claim, not the fragment: every column the schema
        // has that is named `root_id` or ends `_root_id`, plus `roots.id`,
        // is a record a removed root's number can survive in. Each must
        // bound the next number, or a number it holds can be given again.
        let conn = setup_test_db();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let mut naming: Vec<(String, String)> = vec![("roots".into(), "id".into())];
        for table in &tables {
            let columns: Vec<String> = conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| row.get(1))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            for column in columns {
                if column == "root_id" || column.ends_with("_root_id") {
                    naming.push((table.clone(), column));
                }
            }
        }
        assert!(naming.len() >= 7, "the schema scan found {naming:?}");
        for (table, column) in &naming {
            let needle = format!("SELECT MAX({column}) FROM {table})");
            assert!(
                NEXT_ROOT_NUMBER_SQL.contains(&needle),
                "{table}.{column} names a root but does not bound the next number"
            );
        }
        assert!(NEXT_ROOT_NUMBER_SQL.contains("root_number_high_water"));
    }

    #[test]
    fn each_record_naming_a_number_keeps_it_taken() {
        // The behavioural half of the column pin: a number held only by one
        // kind of record is still never given again. `sources` and `notes`
        // reference `roots(id)`, so neither can name a removed root here;
        // the column pin keeps them in the bound regardless.
        let records = [
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (1, 7, '')",
            "INSERT INTO decision_extractions (decision_id, root_id, root_path, rel_prefix, files,
                                               destination_path)
             VALUES (1, 7, '/gone', '', 1, '/x')",
            "INSERT INTO decision_extractions (decision_id, root_id, root_path, rel_prefix, files,
                                               destination_root_id, destination_path)
             VALUES (1, 1, '/gone', '', 1, 7, '/x')",
            "INSERT INTO decisions (command, command_line, canon_version, created_at, receipt_root_id)
             VALUES ('apply', 'x', '0', 0, 7)",
            "INSERT INTO _meta (key, value_int) VALUES ('root_number_high_water', 7)",
        ];
        for record in records {
            let conn = setup_test_db();
            conn.execute(record, []).unwrap();
            assert_eq!(
                insert_numbered(&conn, "/next", "source", None).unwrap(),
                8,
                "after: {record}"
            );
        }
    }

    #[test]
    fn the_mark_only_rises() {
        let conn = setup_test_db();
        remember_root_number(&conn, 5).unwrap();
        remember_root_number(&conn, 3).unwrap();
        let mark: i64 = conn
            .query_row(
                "SELECT value_int FROM _meta WHERE key = 'root_number_high_water'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mark, 5);
    }
}
