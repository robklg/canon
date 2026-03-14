//! Shared test helpers for ops layer tests.
//!
//! Consolidates duplicated insert/setup helpers that were independently
//! maintained in each ops module's test section.

use crate::repo::Connection;

pub fn setup_test_db() -> Connection {
    crate::repo::db::open_in_memory_for_test()
}

pub fn insert_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
    conn.execute(
        "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
        rusqlite::params![path, role, suspended as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub fn insert_object(conn: &Connection, hash: &str, excluded: bool) -> i64 {
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
        rusqlite::params![hash, excluded as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// Insert a source with default metadata.
/// Uses size=1000, mtime=1704067200, partial_hash="testhash", excluded=false.
pub fn insert_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, 1000, 1704067200, "testhash")
}

/// Insert a source marked as excluded.
pub fn insert_source_excluded(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, true, 1000, 1704067200, "testhash")
}

/// Insert a source with a specific size.
pub fn insert_source_with_size(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    size: i64,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, size, 1704067200, "testhash")
}

/// Insert a source with specific size and mtime (for staleness tests).
pub fn insert_source_with_metadata(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    size: i64,
    mtime: i64,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, size, mtime, "testhash")
}

/// Full-control source insertion with all parameters.
pub fn insert_source_full(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    excluded: bool,
    size: i64,
    mtime: i64,
    partial_hash: &str,
) -> i64 {
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
         VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?)",
        rusqlite::params![root_id, rel_path, object_id, size, mtime, partial_hash, excluded as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// Insert a fact for a source.
pub fn insert_fact(conn: &Connection, source_id: i64, key: &str, value: &str) {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev) VALUES ('source', ?, ?, ?, 0, 0)",
        rusqlite::params![source_id, key, value],
    )
    .unwrap();
}

/// Check if a source is excluded in the DB.
pub fn is_source_excluded(conn: &Connection, source_id: i64) -> bool {
    conn.query_row(
        "SELECT excluded FROM sources WHERE id = ?",
        [source_id],
        |row| row.get::<_, bool>(0),
    )
    .unwrap()
}

/// Check if an object is excluded in the DB.
pub fn is_object_excluded(conn: &Connection, object_id: i64) -> bool {
    conn.query_row(
        "SELECT excluded FROM objects WHERE id = ?",
        [object_id],
        |row| row.get::<_, bool>(0),
    )
    .unwrap()
}
