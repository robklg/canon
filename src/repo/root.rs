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

/// Fetch file counts (present sources) for a set of root IDs.
///
/// Returns a HashMap from root_id to count of present sources.
/// Roots with no present sources will not appear in the result.
pub fn fetch_file_counts(conn: &Connection, root_ids: &[i64]) -> Result<HashMap<i64, i64>> {
    if root_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders: Vec<&str> = root_ids.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT root_id, COUNT(*) FROM sources WHERE present = 1 AND root_id IN ({}) GROUP BY root_id",
        placeholders.join(",")
    );

    let params: Vec<rusqlite::types::Value> = root_ids
        .iter()
        .map(|&id| rusqlite::types::Value::from(id))
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut counts = HashMap::new();
    for row in rows {
        let (root_id, count) = row?;
        counts.insert(root_id, count);
    }

    Ok(counts)
}

/// Set the suspended state of a root.
pub fn set_suspended(conn: &Connection, root_id: i64, suspended: bool) -> Result<()> {
    conn.execute(
        "UPDATE roots SET suspended = ? WHERE id = ?",
        rusqlite::params![suspended as i64, root_id],
    )?;
    Ok(())
}

/// Set or clear the comment on a root.
pub fn set_comment(conn: &Connection, root_id: i64, comment: Option<&str>) -> Result<()> {
    conn.execute(
        "UPDATE roots SET comment = ? WHERE id = ?",
        rusqlite::params![comment, root_id],
    )?;
    Ok(())
}

/// Remove a root and all its sources and facts.
///
/// Deletes in order: facts for sources → sources → root.
/// Returns the number of sources deleted.
pub fn remove(conn: &Connection, root_id: i64) -> Result<i64> {
    // Delete facts for sources in this root
    conn.execute(
        "DELETE FROM facts WHERE entity_type = 'source' AND entity_id IN (
            SELECT id FROM sources WHERE root_id = ?
        )",
        [root_id],
    )?;

    // Delete sources
    let deleted_sources = conn.execute("DELETE FROM sources WHERE root_id = ?", [root_id])?;

    // Delete the root
    conn.execute("DELETE FROM roots WHERE id = ?", [root_id])?;

    Ok(deleted_sources as i64)
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
    // fetch_file_counts tests
    // =========================================================================

    /// Insert a test source and return its ID.
    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        present: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, present, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at)
             VALUES (?, ?, ?, 1, 1, 100, 1700000000, 'testhash', 0, 0)",
            rusqlite::params![root_id, rel_path, present as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn fetch_file_counts_empty_ids() {
        let conn = setup_test_db();
        let counts = fetch_file_counts(&conn, &[]).unwrap();
        assert!(counts.is_empty());
    }

    #[test]
    fn fetch_file_counts_root_with_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        insert_source(&conn, root_id, "a.jpg", true);
        insert_source(&conn, root_id, "b.jpg", true);
        insert_source(&conn, root_id, "c.jpg", true);

        let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
        assert_eq!(counts.get(&root_id), Some(&3));
    }

    #[test]
    fn fetch_file_counts_root_no_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
        // Root with no sources is not in the result
        assert!(!counts.contains_key(&root_id));
    }

    #[test]
    fn fetch_file_counts_multiple_roots() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", None, None, false);
        let root2 = insert_root(&conn, "/archive", "archive", None, None, false);

        insert_source(&conn, root1, "a.jpg", true);
        insert_source(&conn, root1, "b.jpg", true);
        insert_source(&conn, root2, "c.jpg", true);

        let counts = fetch_file_counts(&conn, &[root1, root2]).unwrap();
        assert_eq!(counts.get(&root1), Some(&2));
        assert_eq!(counts.get(&root2), Some(&1));
    }

    #[test]
    fn fetch_file_counts_only_present() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        insert_source(&conn, root_id, "a.jpg", true);
        insert_source(&conn, root_id, "b.jpg", true);
        insert_source(&conn, root_id, "missing.jpg", false); // Not present

        let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
        assert_eq!(counts.get(&root_id), Some(&2)); // Only counts present=1
    }

    // =========================================================================
    // set_suspended tests
    // =========================================================================

    #[test]
    fn set_suspended_activates() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        set_suspended(&conn, root_id, true).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert!(roots[0].is_suspended());
    }

    #[test]
    fn set_suspended_deactivates() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, true);

        set_suspended(&conn, root_id, false).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert!(!roots[0].is_suspended());
    }

    #[test]
    fn set_suspended_idempotent() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, true);

        // Setting same value should not error
        set_suspended(&conn, root_id, true).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert!(roots[0].is_suspended());
    }

    // =========================================================================
    // set_comment tests
    // =========================================================================

    #[test]
    fn set_comment_adds() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        set_comment(&conn, root_id, Some("My photos")).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots[0].comment, Some("My photos".to_string()));
    }

    #[test]
    fn set_comment_updates() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", Some("Old comment"), None, false);

        set_comment(&conn, root_id, Some("New comment")).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots[0].comment, Some("New comment".to_string()));
    }

    #[test]
    fn set_comment_clears() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", Some("Comment"), None, false);

        set_comment(&conn, root_id, None).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert_eq!(roots[0].comment, None);
    }

    // =========================================================================
    // remove tests
    // =========================================================================

    #[test]
    fn remove_empty_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        let deleted = remove(&conn, root_id).unwrap();

        assert_eq!(deleted, 0);
        let roots = fetch_all(&conn).unwrap();
        assert!(roots.is_empty());
    }

    #[test]
    fn remove_with_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        insert_source(&conn, root_id, "a.jpg", true);
        insert_source(&conn, root_id, "b.jpg", true);
        insert_source(&conn, root_id, "c.jpg", true);

        let deleted = remove(&conn, root_id).unwrap();

        assert_eq!(deleted, 3);
        let roots = fetch_all(&conn).unwrap();
        assert!(roots.is_empty());
    }

    #[test]
    fn remove_deletes_facts() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);
        let source_id = insert_source(&conn, root_id, "a.jpg", true);

        // Insert a fact for this source
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('source', ?, 'test.key', 'test value', 0, 1)",
            [source_id],
        )
        .unwrap();

        // Verify fact exists
        let fact_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fact_count, 1);

        remove(&conn, root_id).unwrap();

        // Verify fact was deleted
        let fact_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fact_count, 0);
    }

    #[test]
    fn remove_deletes_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", None, None, false);

        remove(&conn, root_id).unwrap();

        let roots = fetch_all(&conn).unwrap();
        assert!(roots.is_empty());
    }
}
