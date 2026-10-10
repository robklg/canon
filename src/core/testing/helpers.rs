//! The insert/setup helpers themselves. Consolidates fixtures that were
//! once independently maintained in each module's test section, so a schema
//! change lands in one place.

use crate::core::repo::Connection;

pub fn setup_test_db() -> Connection {
    crate::core::repo::db::open_in_memory_for_test()
}

/// Make exactly one statement kind fail against one table, from the test side.
///
/// A trigger that raises on `BEFORE <event> ON <table>` fails every statement
/// of that kind against that table and leaves every other statement working —
/// the recorder's own UPDATE on `decisions` included. That is the whole point:
/// a site can be failed *inside* its window, so what the row says afterwards is
/// observable, without production code growing an injection seam for the test's
/// benefit. Dropping the table (what the recorder's own battery does) cannot
/// serve here — the row has to survive to be read.
///
/// `event` is `INSERT`, `UPDATE` or `DELETE`.
pub fn plant_failure(conn: &Connection, event: &str, table: &str) {
    conn.execute_batch(&format!(
        "CREATE TRIGGER plant_{event}_{table} BEFORE {event} ON {table} \
         BEGIN SELECT RAISE(ABORT, 'planted'); END"
    ))
    .unwrap();
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
    insert_source_full(
        conn, root_id, rel_path, object_id, false, 1000, 1704067200, "testhash",
    )
}

/// Insert a source marked as excluded.
pub fn insert_source_excluded(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    insert_source_full(
        conn, root_id, rel_path, object_id, true, 1000, 1704067200, "testhash",
    )
}

/// Insert a source with a specific size.
pub fn insert_source_with_size(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    size: i64,
) -> i64 {
    insert_source_full(
        conn, root_id, rel_path, object_id, false, size, 1704067200, "testhash",
    )
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
    insert_source_full(
        conn, root_id, rel_path, object_id, false, size, mtime, "testhash",
    )
}

/// Full-control source insertion with all parameters. Private: the named
/// helpers above are the surface, and each one exists because some test
/// needed exactly that combination.
#[allow(clippy::too_many_arguments)]
fn insert_source_full(
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

/// Insert a text fact on content (an object), the other half of a source's two rows.
pub fn insert_object_fact(conn: &Connection, object_id: i64, key: &str, value: &str) {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at) VALUES ('object', ?, ?, ?, 0)",
        rusqlite::params![object_id, key, value],
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

/// Insert a note with an explicit timestamp (for ordering tests).
pub fn insert_note(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    text: &str,
    created_at: i64,
) -> i64 {
    conn.execute(
        "INSERT INTO notes (root_id, rel_path, text, created_at) VALUES (?, ?, ?, ?)",
        rusqlite::params![root_id, rel_path, text, created_at],
    )
    .unwrap();
    conn.last_insert_rowid()
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

/// A time fact as migration 8 carries an old one over: the stored number read as a clock
/// reading, counted as if it were UTC, at second precision.
pub fn migrated_time(secs: i64) -> crate::core::domain::time::value::TimeValue {
    let at = jiff::Timestamp::from_second(secs).expect("a test time in range");
    crate::core::domain::time::value::TimeValue::clock(
        jiff::tz::Offset::UTC.to_datetime(at),
        crate::core::domain::time::value::Precision::Second,
    )
    .expect("a whole-second clock reading")
}

/// Store a time fact on a source, written as `written` (`2024`, `2024-06-15T14:30:05`, …),
/// through the one conversion pair the import uses.
pub fn insert_time_fact(conn: &Connection, source_id: i64, key: &str, written: &str) {
    use crate::core::domain::fact::FactValue;
    let value = FactValue::Time(crate::core::domain::time::reading::read_time(written).unwrap());
    let p = crate::core::repo::fact::fact_value_params(&value).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO facts (entity_type, entity_id, key, {}, observed_at, observed_basis_rev)
             VALUES ('source', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0)",
            crate::core::repo::fact::FACT_VALUE_COLUMNS
        ),
        rusqlite::params![source_id, key, p[0], p[1], p[2], p[3], p[4], p[5], p[6]],
    )
    .unwrap();
}
