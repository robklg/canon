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

use anyhow::Result;
use rusqlite::types::Value;

use crate::db::Connection;
use crate::source::Source;

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
                present INTEGER NOT NULL DEFAULT 1,
                excluded INTEGER NOT NULL DEFAULT 0
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

}
