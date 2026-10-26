//! Facts-exclusive repository fns: type-map lookups, upserts, promotion
//! reads/writes, and the criteria-based delete/prune families that back
//! `canon facts`.

use anyhow::Result;
use rusqlite::OptionalExtension;

use crate::core::domain::fact::{FactType, FactValue};
use crate::core::repo::db::{populate_temp_sources, Connection};
use crate::core::repo::fact::{fact_value_at, fact_value_params, FACT_VALUE_COLUMNS};
use crate::facts::domain::ContentFactOnSource;

/// Count fact keys across sources: each stored key with how many of the given sources hold
/// it, on the source or on its content. Used by `canon facts` (no --key) for each key's Count
/// and Coverage; a key's type is read from all its facts, never from these
/// (`core::repo::fact::key_types`).
///
/// Only counts stored facts from the facts table. Source facts and built-in
/// keys are handled separately by the command layer.
pub fn count_fact_keys(conn: &mut Connection, source_ids: &[i64]) -> Result<Vec<(String, usize)>> {
    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Populate temp table with source IDs
    populate_temp_sources(conn, source_ids)?;

    // Count distinct source IDs per key.
    // We count sources (not entities) because multiple sources can share an object.
    // A source with the same key at both source-level and object-level is counted
    // once (COUNT(DISTINCT id)), not twice.
    let query = r#"
        SELECT key, COUNT(DISTINCT id) as cnt
        FROM (
            SELECT ts.id, f.key
            FROM temp_sources ts
            JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

            UNION ALL

            SELECT ts.id, f.key
            FROM temp_sources ts
            JOIN sources s ON s.id = ts.id
            JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
            WHERE s.object_id IS NOT NULL
        )
        GROUP BY key
        ORDER BY key
    "#;

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| {
        let key: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        Ok((key, count as usize))
    })?;

    let result: Vec<_> = rows.collect::<Result<Vec<_>, _>>()?;

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(result)
}

/// Upsert a fact (insert or update on conflict).
///
/// Works for facts stored on a source or on an object. Uses INSERT ON CONFLICT to
/// atomically insert or update the fact; the value is written through the one
/// column conversion, so every value column is replaced together.
///
/// # Arguments
/// * `conn` - Database connection
/// * `entity_type` - "source" or "object"
/// * `entity_id` - The source_id or object_id
/// * `key` - Fact key (e.g., "content.Make")
/// * `value` - The value to store
/// * `observed_at` - When this fact was observed
/// * `observed_basis_rev` - For a fact stored on a source, the basis_rev at observation time
pub fn upsert(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
    value: &FactValue,
    observed_at: i64,
    observed_basis_rev: Option<i64>,
) -> Result<()> {
    let [text, num, time, nanos, form, offset, precision] = fact_value_params(value)?;
    conn.execute(
        &format!(
            "INSERT INTO facts (entity_type, entity_id, key, {FACT_VALUE_COLUMNS}, observed_at, observed_basis_rev)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
               value_text = excluded.value_text,
               value_num = excluded.value_num,
               value_time = excluded.value_time,
               time_nanos = excluded.time_nanos,
               time_form = excluded.time_form,
               time_offset = excluded.time_offset,
               time_precision = excluded.time_precision,
               observed_at = excluded.observed_at,
               observed_basis_rev = excluded.observed_basis_rev"
        ),
        rusqlite::params![
            entity_type,
            entity_id,
            key,
            text,
            num,
            time,
            nanos,
            form,
            offset,
            precision,
            observed_at,
            observed_basis_rev,
        ],
    )?;
    Ok(())
}

/// Fetch every fact stored on a source (entity_type = 'source') — content facts
/// stored on the source, not those stored on its object.
///
/// Used during fact promotion when linking a source to an object. A stored time Canon
/// cannot read is left out, and so stays where it is.
pub fn fetch_content_facts_on_source(
    conn: &Connection,
    source_id: i64,
) -> Result<Vec<ContentFactOnSource>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT id, key, observed_at, {FACT_VALUE_COLUMNS}
         FROM facts
         WHERE entity_type = 'source' AND entity_id = ?"
    ))?;

    let facts = stmt
        .query_map([source_id], |row| {
            let Some(value) = fact_value_at(row, 3)?.readable() else {
                return Ok(None);
            };
            Ok(Some(ContentFactOnSource {
                id: row.get(0)?,
                key: row.get(1)?,
                value,
                observed_at: row.get(2)?,
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(facts.into_iter().flatten().collect())
}

/// Check if an object has a specific fact key.
///
/// Used during fact promotion to avoid creating duplicate facts on the object.
pub fn object_has_fact(conn: &Connection, object_id: i64, key: &str) -> Result<bool> {
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
            rusqlite::params![object_id, key],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    Ok(exists)
}

/// Delete a fact by its ID.
///
/// Used during fact promotion to remove the fact stored on the source after copying it to the object.
pub fn delete_by_id(conn: &Connection, fact_id: i64) -> Result<()> {
    conn.execute("DELETE FROM facts WHERE id = ?", [fact_id])?;
    Ok(())
}

/// Write each fact's new value by id: the seven value columns only, so when it was observed and
/// the revision it was observed at stay as they are.
pub fn update_fact_values(conn: &Connection, changes: &[(i64, &FactValue)]) -> Result<()> {
    let mut stmt = conn.prepare(
        "UPDATE facts SET value_text = ?, value_num = ?, value_time = ?, time_nanos = ?,
                          time_form = ?, time_offset = ?, time_precision = ?
         WHERE id = ?",
    )?;
    for (id, value) in changes {
        let [text, num, time, nanos, form, offset, precision] = fact_value_params(value)?;
        stmt.execute(rusqlite::params![
            text, num, time, nanos, form, offset, precision, id
        ])?;
    }
    Ok(())
}

/// Delete facts by id, in chunks.
pub fn delete_facts_by_id(conn: &Connection, ids: &[i64]) -> Result<()> {
    for chunk in ids.chunks(1000) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        conn.execute(
            &format!("DELETE FROM facts WHERE id IN ({placeholders})"),
            rusqlite::params_from_iter(chunk.iter()),
        )?;
    }
    Ok(())
}

/// Insert a fact on an object (for promotion from source).
///
/// Similar to upsert but specifically for facts moved onto an object during promotion.
/// Does not update on conflict — callers should check `object_has_fact` first.
pub fn insert_object_fact(
    conn: &Connection,
    object_id: i64,
    key: &str,
    value: &FactValue,
    observed_at: i64,
) -> Result<()> {
    let [text, num, time, nanos, form, offset, precision] = fact_value_params(value)?;
    conn.execute(
        &format!(
            "INSERT INTO facts (entity_type, entity_id, key, {FACT_VALUE_COLUMNS}, observed_at, observed_basis_rev)
             VALUES ('object', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)"
        ),
        rusqlite::params![
            object_id,
            key,
            text,
            num,
            time,
            nanos,
            form,
            offset,
            precision,
            observed_at,
        ],
    )?;
    Ok(())
}

// ============================================================================
// Criteria-based deletion (for `canon facts delete`)
// ============================================================================

/// Build SQL clause for value type filter.
fn value_type_clause(value_type: Option<FactType>) -> &'static str {
    match value_type {
        Some(FactType::Text) => "AND value_text IS NOT NULL",
        Some(FactType::Num) => "AND value_num IS NOT NULL",
        Some(FactType::Time) => "AND value_time IS NOT NULL",
        // Unreachable: `--value-type`'s parser offers no `path`, because a path is
        // derived and never stored — so no fact matches it.
        Some(FactType::Path) => "AND 0",
        None => "",
    }
}

/// Count facts matching criteria for the given sources.
///
/// # Arguments
/// * `conn` - Database connection (must be mutable for temp table)
/// * `source_ids` - Source IDs to scope the operation
/// * `key` - Fact key to match
/// * `entity_type` - "source" or "object"
/// * `value_type` - Optional filter: the stored type
///
/// # Returns
/// Tuple of (total_fact_count, distinct_entity_count)
pub fn count_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<FactType>,
) -> Result<(i64, i64)> {
    if source_ids.is_empty() {
        return Ok((0, 0));
    }

    populate_temp_sources(conn, source_ids)?;
    let vt_clause = value_type_clause(value_type);

    let (fact_count, entity_count) = if entity_type == "source" {
        let count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {vt_clause}"
            ),
            [key],
            |row| row.get(0),
        )?;

        let entities: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {vt_clause}"
            ),
            [key],
            |row| row.get(0),
        )?;

        (count, entities)
    } else {
        // Object entity type - need to map source IDs to object IDs
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS temp_objects (id INTEGER PRIMARY KEY)",
            [],
        )?;
        conn.execute("DELETE FROM temp_objects", [])?;
        conn.execute(
            "INSERT OR IGNORE INTO temp_objects (id)
             SELECT DISTINCT object_id FROM sources
             WHERE id IN (SELECT id FROM temp_sources) AND object_id IS NOT NULL",
            [],
        )?;

        let count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {vt_clause}"
            ),
            [key],
            |row| row.get(0),
        )?;

        let entities: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {vt_clause}"
            ),
            [key],
            |row| row.get(0),
        )?;

        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;

        (count, entities)
    };

    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok((fact_count, entity_count))
}

/// Delete facts matching criteria for the given sources.
///
/// # Arguments
/// * `conn` - Database connection (must be mutable for temp table)
/// * `source_ids` - Source IDs to scope the operation
/// * `key` - Fact key to match
/// * `entity_type` - "source" or "object"
/// * `value_type` - Optional filter: the stored type
///
/// # Returns
/// Number of fact rows deleted
pub fn delete_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<FactType>,
) -> Result<usize> {
    if source_ids.is_empty() {
        return Ok(0);
    }

    populate_temp_sources(conn, source_ids)?;
    let vt_clause = value_type_clause(value_type);

    let deleted = if entity_type == "source" {
        conn.execute(
            &format!(
                "DELETE FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {vt_clause}"
            ),
            [key],
        )?
    } else {
        // Object entity type - need to map source IDs to object IDs
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS temp_objects (id INTEGER PRIMARY KEY)",
            [],
        )?;
        conn.execute("DELETE FROM temp_objects", [])?;
        conn.execute(
            "INSERT OR IGNORE INTO temp_objects (id)
             SELECT DISTINCT object_id FROM sources
             WHERE id IN (SELECT id FROM temp_sources) AND object_id IS NOT NULL",
            [],
        )?;

        let deleted = conn.execute(
            &format!(
                "DELETE FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {vt_clause}"
            ),
            [key],
        )?;

        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;

        deleted
    };

    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(deleted)
}

// ============================================================================
// Stale fact pruning (for `canon facts prune --stale`)
// ============================================================================

/// Count stale facts stored on sources, where observed_basis_rev != current basis_rev.
///
/// A fact is considered stale when its `observed_basis_rev` (the file's basis_rev
/// at the time the fact was recorded) doesn't match the source's current `basis_rev`.
/// This indicates the file has changed since the fact was observed.
///
/// Only facts stored on a source with non-null observed_basis_rev are considered.
/// Facts stored on an object don't have basis_rev tracking.
pub fn count_stale(conn: &Connection) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         JOIN sources s ON f.entity_type = 'source' AND f.entity_id = s.id
         WHERE f.observed_basis_rev IS NOT NULL
           AND f.observed_basis_rev != s.basis_rev",
        [],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// Delete stale facts stored on sources, where observed_basis_rev != current basis_rev.
///
/// Returns the number of facts deleted.
///
/// See `count_stale()` for the definition of staleness.
pub fn delete_stale(conn: &Connection) -> Result<usize> {
    // The DELETE uses a subquery to find stale entity_ids, then verifies
    // each fact individually to avoid race conditions with concurrent updates.
    let deleted = conn.execute(
        "DELETE FROM facts
         WHERE entity_type = 'source'
           AND entity_id IN (
               SELECT f.entity_id FROM facts f
               JOIN sources s ON f.entity_type = 'source' AND f.entity_id = s.id
               WHERE f.observed_basis_rev IS NOT NULL
                 AND f.observed_basis_rev != s.basis_rev
           )
           AND observed_basis_rev IS NOT NULL
           AND observed_basis_rev != (
               SELECT basis_rev FROM sources WHERE id = facts.entity_id
           )",
        [],
    )?;
    Ok(deleted)
}

// ============================================================================
// Excluded entity fact pruning (for `canon facts prune --excluded-facts`)
// ============================================================================

/// Count facts for excluded entities.
///
/// # Arguments
/// * `conn` - Database connection
/// * `scope` - Which facts to count: "source", "object", or "all"
///
/// # Returns
/// Tuple of (stored_on_source_count, stored_on_content_count).
/// If scope is "source", stored_on_content_count will be 0.
/// If scope is "object", stored_on_source_count will be 0.
pub fn count_excluded(conn: &Connection, scope: &str) -> Result<(i64, i64)> {
    let count_sources = scope == "all" || scope == "source";
    let count_objects = scope == "all" || scope == "object";

    let stored_on_source_count: i64 = if count_sources {
        conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (SELECT id FROM sources WHERE excluded = 1)",
            [],
            |row| row.get(0),
        )?
    } else {
        0
    };

    let stored_on_content_count: i64 = if count_objects {
        conn.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (SELECT id FROM objects WHERE excluded = 1)",
            [],
            |row| row.get(0),
        )?
    } else {
        0
    };

    Ok((stored_on_source_count, stored_on_content_count))
}

/// Delete facts for excluded entities.
///
/// # Arguments
/// * `conn` - Database connection
/// * `scope` - Which facts to delete: "source", "object", or "all"
///
/// # Returns
/// Tuple of (source_facts_deleted, object_facts_deleted).
pub fn delete_excluded(conn: &Connection, scope: &str) -> Result<(usize, usize)> {
    let delete_sources = scope == "all" || scope == "source";
    let delete_objects = scope == "all" || scope == "object";

    let source_facts_deleted = if delete_sources {
        conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'source'
               AND entity_id IN (SELECT id FROM sources WHERE excluded = 1)",
            [],
        )?
    } else {
        0
    };

    let object_facts_deleted = if delete_objects {
        conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'object'
               AND entity_id IN (SELECT id FROM objects WHERE excluded = 1)",
            [],
        )?
    } else {
        0
    };

    Ok((source_facts_deleted, object_facts_deleted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &Connection, id: i64, path: &str) {
        conn.execute(
            "INSERT INTO roots (id, path) VALUES (?1, ?2)",
            [&id as &dyn rusqlite::ToSql, &path],
        )
        .unwrap();
    }

    fn insert_object(conn: &Connection, id: i64, hash: &str) {
        conn.execute(
            "INSERT INTO objects (id, hash_type, hash_value) VALUES (?1, 'sha256', ?2)",
            [&id as &dyn rusqlite::ToSql, &hash],
        )
        .unwrap();
    }

    fn insert_source(
        conn: &Connection,
        id: i64,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?1, ?2, ?3, ?4, 0, 0, '', 0, 0, 0, 0)",
            rusqlite::params![id, root_id, rel_path, object_id],
        )
        .unwrap();
    }

    fn insert_fact_text(
        conn: &Connection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: &str,
    ) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    fn insert_fact_num(
        conn: &Connection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: f64,
    ) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    fn insert_fact_time(
        conn: &Connection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: i64,
    ) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 'clock', 'second', 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    // =========================================================================
    // count_fact_keys tests
    // =========================================================================

    #[test]
    fn count_fact_keys_empty_ids() {
        let mut conn = setup_test_db();
        let result = count_fact_keys(&mut conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn count_fact_keys_basic() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = count_fact_keys(&mut conn, &[1]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "content.Make");
        assert_eq!(result[0].1, 1); // count
    }

    #[test]
    fn count_fact_keys_multiple_sources() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_object(&conn, 101, "def456");
        insert_source(&conn, 1, 1, "file1.txt", Some(100));
        insert_source(&conn, 2, 1, "file2.txt", Some(101));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 101, "content.Make", "Nikon");

        let result = count_fact_keys(&mut conn, &[1, 2]).unwrap();
        let make = result.iter().find(|(k, _)| k == "content.Make").unwrap();
        assert_eq!(make.1, 2); // Both sources have this key
    }

    #[test]
    fn count_fact_keys_shared_object() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        // Two sources sharing the same object
        insert_source(&conn, 1, 1, "file1.txt", Some(100));
        insert_source(&conn, 2, 1, "file2.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = count_fact_keys(&mut conn, &[1, 2]).unwrap();
        let make = result.iter().find(|(k, _)| k == "content.Make").unwrap();
        // Should count 2 (both sources) even though fact is on one object
        assert_eq!(make.1, 2);
    }

    /// Regression test: a source with the same key at both source-level and
    /// object-level must be counted once, not twice. This was the root cause
    /// of coverage >100% (e.g., content.mime at 101.8%).
    /// See commits dc6d301, 0374f5e, 50aacad for the history of this bug.
    #[test]
    fn count_fact_keys_no_double_count_on_source_and_object() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        // Same key at both source-level and object-level
        insert_fact_text(&conn, "source", 1, "content.mime", "image/jpeg");
        insert_fact_text(&conn, "object", 100, "content.mime", "image/jpeg");

        let result = count_fact_keys(&mut conn, &[1]).unwrap();
        let mime = result.iter().find(|(k, _)| k == "content.mime").unwrap();
        // Must be 1, not 2 — one source, one key
        assert_eq!(mime.1, 1);
    }

    // =========================================================================
    // upsert tests
    // =========================================================================

    #[test]
    fn upsert_inserts_text_fact() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        upsert(
            &conn,
            "object",
            100,
            "content.Make",
            &FactValue::Text("Canon".into()),
            1700000000,
            None,
        )
        .unwrap();

        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Canon");
    }

    #[test]
    fn upsert_inserts_num_fact() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        upsert(
            &conn,
            "object",
            100,
            "content.Width",
            &FactValue::Num(4000.0),
            1700000000,
            None,
        )
        .unwrap();

        let value: f64 = conn.query_row(
            "SELECT value_num FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Width'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, 4000.0);
    }

    #[test]
    fn upsert_inserts_time_fact() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        upsert(
            &conn,
            "object",
            100,
            "content.DateTimeOriginal",
            &FactValue::Time(crate::core::testing::migrated_time(1704067200)),
            1700000000,
            None,
        )
        .unwrap();

        let value: i64 = conn.query_row(
            "SELECT value_time FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.DateTimeOriginal'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, 1704067200);
    }

    #[test]
    fn upsert_updates_existing() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        // Insert initial
        upsert(
            &conn,
            "object",
            100,
            "content.Make",
            &FactValue::Text("Canon".into()),
            1700000000,
            None,
        )
        .unwrap();

        // Update
        upsert(
            &conn,
            "object",
            100,
            "content.Make",
            &FactValue::Text("Nikon".into()),
            1700000001,
            None,
        )
        .unwrap();

        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Nikon");

        // Only one fact
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_on_source_with_basis_rev() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        upsert(
            &conn,
            "source",
            1,
            "content.Make",
            &FactValue::Text("Canon".into()),
            1700000000,
            Some(5), // basis_rev
        )
        .unwrap();

        let (value, basis_rev): (String, i64) = conn.query_row(
            "SELECT value_text, observed_basis_rev FROM facts WHERE entity_type = 'source' AND entity_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(value, "Canon");
        assert_eq!(basis_rev, 5);
    }

    // =========================================================================
    // fetch_content_facts_on_source tests
    // =========================================================================

    #[test]
    fn fetch_content_facts_on_source_empty() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        let result = fetch_content_facts_on_source(&conn, 1).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn fetch_content_facts_on_source_returns_all() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_num(&conn, "source", 1, "content.Width", 4000.0);

        let result = fetch_content_facts_on_source(&conn, 1).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|f| f.key == "content.Make"));
        assert!(result.iter().any(|f| f.key == "content.Width"));
    }

    #[test]
    fn fetch_content_facts_on_source_excludes_object_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "source", 1, "source.policy", "reviewed");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = fetch_content_facts_on_source(&conn, 1).unwrap();

        // Only the fact stored on the source is returned
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "source.policy");
    }

    // =========================================================================
    // object_has_fact tests
    // =========================================================================

    #[test]
    fn object_has_fact_returns_true() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = object_has_fact(&conn, 100, "content.Make").unwrap();
        assert!(result);
    }

    #[test]
    fn object_has_fact_returns_false_missing() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        let result = object_has_fact(&conn, 100, "content.Make").unwrap();
        assert!(!result);
    }

    #[test]
    fn object_has_fact_returns_false_wrong_entity() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        // Fact is on source, not object
        let result = object_has_fact(&conn, 1, "content.Make").unwrap();
        assert!(!result);
    }

    // =========================================================================
    // delete_by_id tests
    // =========================================================================

    #[test]
    fn delete_by_id_removes_fact() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        // Get the fact ID
        let fact_id: i64 = conn
            .query_row(
                "SELECT id FROM facts WHERE entity_type = 'source' AND entity_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        delete_by_id(&conn, fact_id).unwrap();

        // Verify fact is gone
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE id = ?",
                [fact_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_by_id_nonexistent_ok() {
        let conn = setup_test_db();

        // Should not error even if fact doesn't exist
        let result = delete_by_id(&conn, 99999);
        assert!(result.is_ok());
    }

    // =========================================================================
    // insert_object_fact tests
    // =========================================================================

    #[test]
    fn insert_object_fact_creates() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        insert_object_fact(
            &conn,
            100,
            "content.Make",
            &FactValue::Text("Canon".into()),
            1700000000,
        )
        .unwrap();

        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Canon");
    }

    #[test]
    fn insert_object_fact_with_num() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        insert_object_fact(
            &conn,
            100,
            "content.Width",
            &FactValue::Num(4000.0),
            1700000000,
        )
        .unwrap();

        let value: f64 = conn.query_row(
            "SELECT value_num FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.Width'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, 4000.0);
    }

    #[test]
    fn insert_object_fact_with_time() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        insert_object_fact(
            &conn,
            100,
            "content.DateTimeOriginal",
            &FactValue::Time(crate::core::testing::migrated_time(1704067200)),
            1700000000,
        )
        .unwrap();

        let value: i64 = conn.query_row(
            "SELECT value_time FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.DateTimeOriginal'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, 1704067200);
    }

    // =========================================================================
    // count_by_criteria tests
    // =========================================================================

    #[test]
    fn count_by_criteria_empty_sources() {
        let mut conn = setup_test_db();
        let (count, entities) =
            count_by_criteria(&mut conn, &[], "content.Make", "source", None).unwrap();
        assert_eq!(count, 0);
        assert_eq!(entities, 0);
    }

    #[test]
    fn count_by_criteria_source_entity() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file1.txt", None);
        insert_source(&conn, 2, 1, "file2.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "source", 2, "content.Make", "Nikon");

        let (count, entities) =
            count_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
        assert_eq!(count, 2);
        assert_eq!(entities, 2);
    }

    #[test]
    fn count_by_criteria_object_entity() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_object(&conn, 101, "def456");
        insert_source(&conn, 1, 1, "file1.txt", Some(100));
        insert_source(&conn, 2, 1, "file2.txt", Some(101));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 101, "content.Make", "Nikon");

        let (count, entities) =
            count_by_criteria(&mut conn, &[1, 2], "content.Make", "object", None).unwrap();
        assert_eq!(count, 2);
        assert_eq!(entities, 2);
    }

    #[test]
    fn count_by_criteria_value_type_filter() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_num(&conn, "source", 1, "content.Width", 4000.0);
        insert_fact_time(&conn, "source", 1, "content.DateTimeOriginal", 1704067200);

        // Only text
        let (count, _) = count_by_criteria(
            &mut conn,
            &[1],
            "content.Make",
            "source",
            Some(FactType::Text),
        )
        .unwrap();
        assert_eq!(count, 1);

        // Only num - should not match text key
        let (count, _) = count_by_criteria(
            &mut conn,
            &[1],
            "content.Make",
            "source",
            Some(FactType::Num),
        )
        .unwrap();
        assert_eq!(count, 0);

        // Num key with num filter
        let (count, _) = count_by_criteria(
            &mut conn,
            &[1],
            "content.Width",
            "source",
            Some(FactType::Num),
        )
        .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn count_by_criteria_no_matching_key() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        let (count, entities) =
            count_by_criteria(&mut conn, &[1], "content.Model", "source", None).unwrap();
        assert_eq!(count, 0);
        assert_eq!(entities, 0);
    }

    // =========================================================================
    // delete_by_criteria tests
    // =========================================================================

    #[test]
    fn delete_by_criteria_empty_sources() {
        let mut conn = setup_test_db();
        let deleted = delete_by_criteria(&mut conn, &[], "content.Make", "source", None).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn delete_by_criteria_source_entity() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file1.txt", None);
        insert_source(&conn, 2, 1, "file2.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "source", 2, "content.Make", "Nikon");
        insert_fact_text(&conn, "source", 1, "content.Model", "EOS"); // different key, should not be deleted

        let deleted =
            delete_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
        assert_eq!(deleted, 2);

        // Verify deleted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE key = 'content.Make'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);

        // Verify other key still exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE key = 'content.Model'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_by_criteria_object_entity() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let deleted = delete_by_criteria(&mut conn, &[1], "content.Make", "object", None).unwrap();
        assert_eq!(deleted, 1);

        // Verify deleted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND key = 'content.Make'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_by_criteria_value_type_filter() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_num(&conn, "source", 1, "content.Width", 4000.0);

        // Delete only num facts with key content.Width
        let deleted = delete_by_criteria(
            &mut conn,
            &[1],
            "content.Width",
            "source",
            Some(FactType::Num),
        )
        .unwrap();
        assert_eq!(deleted, 1);

        // Text fact should still exist
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE key = 'content.Make'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_by_criteria_respects_source_scope() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file1.txt", None);
        insert_source(&conn, 2, 1, "file2.txt", None);
        insert_source(&conn, 3, 1, "file3.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "source", 2, "content.Make", "Nikon");
        insert_fact_text(&conn, "source", 3, "content.Make", "Sony");

        // Only delete for sources 1 and 2
        let deleted =
            delete_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
        assert_eq!(deleted, 2);

        // Source 3's fact should still exist
        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'source' AND entity_id = 3 AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Sony");
    }

    // =========================================================================
    // count_stale / delete_stale tests
    // =========================================================================

    /// Helper to insert a source with a specific basis_rev
    fn insert_source_with_basis_rev(
        conn: &Connection,
        id: i64,
        root_id: i64,
        rel_path: &str,
        basis_rev: i64,
    ) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, basis_rev, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?1, ?2, ?3, ?4, 0, 0, '', 0, 0, 0, 0)",
            rusqlite::params![id, root_id, rel_path, basis_rev],
        )
        .unwrap();
    }

    /// Helper to insert a fact stored on a source, with a specific observed_basis_rev
    fn insert_fact_with_basis_rev(
        conn: &Connection,
        source_id: i64,
        key: &str,
        value: &str,
        observed_basis_rev: Option<i64>,
    ) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('source', ?1, ?2, ?3, 0, ?4)",
            rusqlite::params![source_id, key, value, observed_basis_rev],
        )
        .unwrap();
    }

    #[test]
    fn count_stale_no_facts() {
        let conn = setup_test_db();
        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn count_stale_no_stale_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file.txt", 5);
        // Fact with matching basis_rev
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5));

        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn count_stale_detects_mismatch() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file.txt", 10); // Current basis_rev = 10
                                                                   // Fact was observed at basis_rev = 5 (stale)
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5));

        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 1);
    }

    // Note: count_stale_ignores_null_basis_rev was removed because the production
    // schema's CHECK constraint prevents facts stored on a source with NULL observed_basis_rev.

    #[test]
    fn count_stale_multiple_stale_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file1.txt", 10);
        insert_source_with_basis_rev(&conn, 2, 1, "file2.txt", 20);
        // Both facts are stale
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5));
        insert_fact_with_basis_rev(&conn, 2, "content.Make", "Nikon", Some(15));

        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn count_stale_mixed_stale_and_fresh() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file1.txt", 10);
        insert_source_with_basis_rev(&conn, 2, 1, "file2.txt", 20);
        // Source 1: stale fact
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5));
        // Source 2: fresh fact
        insert_fact_with_basis_rev(&conn, 2, "content.Make", "Nikon", Some(20));

        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_stale_no_facts() {
        let conn = setup_test_db();
        let deleted = delete_stale(&conn).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn delete_stale_removes_mismatched() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file.txt", 10);
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5)); // stale

        let deleted = delete_stale(&conn).unwrap();
        assert_eq!(deleted, 1);

        // Verify fact is gone
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'source' AND entity_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_stale_preserves_fresh_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file1.txt", 10);
        insert_source_with_basis_rev(&conn, 2, 1, "file2.txt", 20);
        // Source 1: stale fact
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", Some(5));
        // Source 2: fresh fact
        insert_fact_with_basis_rev(&conn, 2, "content.Make", "Nikon", Some(20));

        let deleted = delete_stale(&conn).unwrap();
        assert_eq!(deleted, 1);

        // Verify only source 2's fact remains
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'source'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 1);

        let value: String = conn
            .query_row(
                "SELECT value_text FROM facts WHERE entity_type = 'source' AND entity_id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "Nikon");
    }

    // Note: delete_stale_preserves_null_basis_rev was removed because the production
    // schema's CHECK constraint prevents facts stored on a source with NULL observed_basis_rev.

    #[test]
    fn delete_stale_ignores_object_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        // Object fact - should never be considered stale (no basis_rev tracking)
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let deleted = delete_stale(&conn).unwrap();
        assert_eq!(deleted, 0);

        // Verify object fact still exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'object'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    // =========================================================================
    // count_excluded / delete_excluded tests
    // =========================================================================

    /// Helper to insert an excluded source
    fn insert_excluded_source(conn: &Connection, id: i64, root_id: i64, rel_path: &str) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, excluded, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?1, ?2, ?3, 1, 0, 0, '', 0, 0, 0, 0)",
            rusqlite::params![id, root_id, rel_path],
        )
        .unwrap();
    }

    fn insert_excluded_object(conn: &Connection, id: i64, hash: &str) {
        conn.execute(
            "INSERT INTO objects (id, hash_type, hash_value, excluded) VALUES (?1, 'sha256', ?2, 1)",
            [&id as &dyn rusqlite::ToSql, &hash],
        )
        .unwrap();
    }

    #[test]
    fn count_excluded_no_excluded() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        let (source_count, object_count) = count_excluded(&conn, "all").unwrap();

        assert_eq!(source_count, 0);
        assert_eq!(object_count, 0);
    }

    #[test]
    fn count_excluded_source_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "source", 1, "content.Model", "EOS");

        let (source_count, object_count) = count_excluded(&conn, "all").unwrap();

        assert_eq!(source_count, 2);
        assert_eq!(object_count, 0);
    }

    #[test]
    fn count_excluded_object_facts() {
        let conn = setup_test_db();
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let (source_count, object_count) = count_excluded(&conn, "all").unwrap();

        assert_eq!(source_count, 0);
        assert_eq!(object_count, 1);
    }

    #[test]
    fn count_excluded_source_scope_only() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let (source_count, object_count) = count_excluded(&conn, "source").unwrap();

        assert_eq!(source_count, 1);
        assert_eq!(object_count, 0); // Not counted because scope is "source"
    }

    #[test]
    fn count_excluded_object_scope_only() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let (source_count, object_count) = count_excluded(&conn, "object").unwrap();

        assert_eq!(source_count, 0); // Not counted because scope is "object"
        assert_eq!(object_count, 1);
    }

    #[test]
    fn delete_excluded_source_only() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let (source_deleted, object_deleted) = delete_excluded(&conn, "source").unwrap();

        assert_eq!(source_deleted, 1);
        assert_eq!(object_deleted, 0);

        // Object fact should still exist
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'object'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_excluded_object_only() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let (source_deleted, object_deleted) = delete_excluded(&conn, "object").unwrap();

        assert_eq!(source_deleted, 0);
        assert_eq!(object_deleted, 1);

        // The fact stored on the source should still exist
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'source'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_excluded_all() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_excluded_source(&conn, 1, 1, "file.txt");
        insert_excluded_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let (source_deleted, object_deleted) = delete_excluded(&conn, "all").unwrap();

        assert_eq!(source_deleted, 1);
        assert_eq!(object_deleted, 1);

        // All facts should be gone
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_excluded_preserves_non_excluded() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");

        // Non-excluded source with fact
        insert_source(&conn, 1, 1, "file1.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        // Excluded source with fact
        insert_excluded_source(&conn, 2, 1, "file2.txt");
        insert_fact_text(&conn, "source", 2, "content.Make", "Nikon");

        let (source_deleted, _) = delete_excluded(&conn, "all").unwrap();

        assert_eq!(source_deleted, 1);

        // Non-excluded fact should still exist
        let value: String = conn
            .query_row(
                "SELECT value_text FROM facts WHERE entity_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "Canon");
    }
}
