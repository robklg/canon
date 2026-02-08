//! Fact repository — infrastructure layer for fetching facts.
//!
//! This module provides batch fetch functions that return typed fact data.
//! It transparently handles the source/object fact distinction, associating
//! all facts with their source ID for easy consumption.
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic
//! 2. **Batch fetching**: Use temp tables for large source sets
//! 3. **Returns domain types**: Functions return `FactEntry` structs
//! 4. **Source-centric**: Object facts are keyed by source_id in results
//!
//! ## Usage
//!
//! ```ignore
//! use canon::fact_repo;
//!
//! // Fetch a specific fact key for sources
//! let facts = fact_repo::batch_fetch_key_for_sources(conn, &source_ids, "content.Make")?;
//! for (source_id, entry) in &facts {
//!     if let Some(fact) = entry {
//!         println!("{}: {:?}", source_id, fact.value);
//!     }
//! }
//! ```

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::OptionalExtension;

use super::db::{populate_temp_sources, Connection};
use crate::domain::fact::{FactEntry, FactType, FactValue};

// Note: We use temp tables (populate_temp_sources) instead of IN clause chunking,
// so BATCH_SIZE is not needed here. The temp table pattern handles large sets better.

/// Fetch all facts for the given source IDs.
///
/// Returns a map from source_id to list of FactEntry.
/// Each source's facts include:
/// - Direct source facts (entity_type = 'source', entity_id = source_id)
/// - Object facts (entity_type = 'object', entity_id = object_id) if source has object_id
///
/// Object facts are associated with the SOURCE id in the result map,
/// making it easy to get "all facts for this source" without separate lookups.
pub fn batch_fetch_for_sources(
    conn: &mut Connection,
    source_ids: &[i64],
) -> Result<HashMap<i64, Vec<FactEntry>>> {
    if source_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Populate temp table with source IDs
    populate_temp_sources(conn, source_ids)?;

    // Query all facts using UNION ALL
    let query = r#"
        SELECT ts.id as source_id, f.key, f.value_text, f.value_num, f.value_time,
               f.entity_type, f.entity_id
        FROM temp_sources ts
        JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

        UNION ALL

        SELECT ts.id as source_id, f.key, f.value_text, f.value_num, f.value_time,
               f.entity_type, f.entity_id
        FROM temp_sources ts
        JOIN sources s ON s.id = ts.id
        JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
        WHERE s.object_id IS NOT NULL
    "#;

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| {
        let source_id: i64 = row.get(0)?;
        let key: String = row.get(1)?;
        let value_text: Option<String> = row.get(2)?;
        let value_num: Option<f64> = row.get(3)?;
        let value_time: Option<i64> = row.get(4)?;
        let entity_type: String = row.get(5)?;
        let entity_id: i64 = row.get(6)?;

        let value = fact_value_from_columns(value_text, value_num, value_time);

        Ok((source_id, FactEntry::new(key, value, entity_type, entity_id)))
    })?;

    // Group facts by source_id
    let mut result: HashMap<i64, Vec<FactEntry>> = HashMap::new();
    for row in rows {
        let (source_id, entry) = row?;
        result.entry(source_id).or_default().push(entry);
    }

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(result)
}

/// Fetch facts for a specific key only.
///
/// Efficient for fetching one fact key across many sources.
/// Returns map from source_id to Option<FactEntry>.
/// None indicates the source lacks this fact.
pub fn batch_fetch_key_for_sources(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
) -> Result<HashMap<i64, Option<FactEntry>>> {
    if source_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Populate temp table with source IDs
    populate_temp_sources(conn, source_ids)?;

    // Query specific key using UNION ALL
    let query = r#"
        SELECT ts.id as source_id, f.key, f.value_text, f.value_num, f.value_time,
               f.entity_type, f.entity_id
        FROM temp_sources ts
        JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

        UNION ALL

        SELECT ts.id as source_id, f.key, f.value_text, f.value_num, f.value_time,
               f.entity_type, f.entity_id
        FROM temp_sources ts
        JOIN sources s ON s.id = ts.id
        JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
        WHERE s.object_id IS NOT NULL
    "#;

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([key], |row| {
        let source_id: i64 = row.get(0)?;
        let key: String = row.get(1)?;
        let value_text: Option<String> = row.get(2)?;
        let value_num: Option<f64> = row.get(3)?;
        let value_time: Option<i64> = row.get(4)?;
        let entity_type: String = row.get(5)?;
        let entity_id: i64 = row.get(6)?;

        let value = fact_value_from_columns(value_text, value_num, value_time);

        Ok((source_id, FactEntry::new(key, value, entity_type, entity_id)))
    })?;

    // Initialize result with None for all source IDs
    let mut result: HashMap<i64, Option<FactEntry>> = source_ids.iter().map(|&id| (id, None)).collect();

    // Fill in found facts
    for row in rows {
        let (source_id, entry) = row?;
        result.insert(source_id, Some(entry));
    }

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(result)
}

/// Count fact keys across sources.
///
/// Returns (key, count, fact_type) tuples for all stored facts.
/// Used by `canon facts` (no --key) to show fact coverage.
///
/// Only counts stored facts from the facts table. Built-in/derived facts
/// are handled separately by the command layer.
pub fn count_fact_keys(
    conn: &mut Connection,
    source_ids: &[i64],
) -> Result<Vec<(String, usize, FactType)>> {
    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Populate temp table with source IDs
    populate_temp_sources(conn, source_ids)?;

    // Count distinct source IDs per key, determine dominant type
    // We count sources (not entities) because multiple sources can share an object
    // The outer SELECT DISTINCT ensures each source is counted once per key
    let query = r#"
        SELECT key,
               COUNT(*) as cnt,
               MAX(CASE WHEN value_time IS NOT NULL THEN 1 ELSE 0 END) as is_time,
               MAX(CASE WHEN value_num IS NOT NULL THEN 1 ELSE 0 END) as is_num
        FROM (
            SELECT DISTINCT ts.id, f.key, f.value_text, f.value_num, f.value_time
            FROM temp_sources ts
            JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

            UNION ALL

            SELECT DISTINCT ts.id, f.key, f.value_text, f.value_num, f.value_time
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
        let is_time: i64 = row.get(2)?;
        let is_num: i64 = row.get(3)?;

        // Determine fact type from the MAX flags
        let fact_type = if is_time == 1 {
            FactType::Time
        } else if is_num == 1 {
            FactType::Num
        } else {
            FactType::Text
        };

        Ok((key, count as usize, fact_type))
    })?;

    let result: Vec<_> = rows.collect::<Result<Vec<_>, _>>()?;

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(result)
}

/// Convert database columns to FactValue.
///
/// The facts table stores values in mutually exclusive columns:
/// - value_text for Text
/// - value_num for Num
/// - value_time for Time
///
/// Note: Path variant is not stored in the database (it's derived).
fn fact_value_from_columns(
    value_text: Option<String>,
    value_num: Option<f64>,
    value_time: Option<i64>,
) -> FactValue {
    if let Some(t) = value_time {
        FactValue::Time(t)
    } else if let Some(n) = value_num {
        FactValue::Num(n)
    } else if let Some(s) = value_text {
        FactValue::Text(s)
    } else {
        // This shouldn't happen due to CHECK constraint, but handle gracefully
        FactValue::Text(String::new())
    }
}

/// Store a fact for an object (upsert).
///
/// Used to record content-derived facts like `content.hash.sha256`.
/// If the fact already exists, it is updated with the new value and timestamp.
///
/// # Arguments
/// * `conn` - Database connection
/// * `object_id` - ID of the object
/// * `key` - Fact key (e.g., "content.hash.sha256")
/// * `value` - Fact value (text)
/// * `timestamp` - When the fact was observed
pub fn store_object_fact(
    conn: &Connection,
    object_id: i64,
    key: &str,
    value: &str,
    timestamp: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at)
         VALUES ('object', ?, ?, ?, ?)
         ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
           value_text = excluded.value_text,
           observed_at = excluded.observed_at",
        rusqlite::params![object_id, key, value, timestamp],
    )?;
    Ok(())
}

/// Fetch the type map for all existing facts.
///
/// Returns a map from fact key to its storage type (Text, Num, or Time).
/// This is used for type consistency checking during import — if a key
/// already exists with type X, new values must also be type X.
///
/// # Returns
/// HashMap where key is the fact key (e.g., "content.Make") and value is
/// the detected FactValueType based on which column has data.
pub fn fetch_type_map(conn: &Connection) -> Result<HashMap<String, crate::domain::fact::FactValueType>> {
    use crate::domain::fact::FactValueType;

    let mut type_map = HashMap::new();

    let mut stmt = conn.prepare(
        "SELECT DISTINCT key,
                CASE
                    WHEN value_time IS NOT NULL THEN 'time'
                    WHEN value_num IS NOT NULL THEN 'num'
                    ELSE 'text'
                END as type
         FROM facts"
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (key, type_str) = row?;
        let fact_type = match type_str.as_str() {
            "time" => FactValueType::Time,
            "num" => FactValueType::Num,
            _ => FactValueType::Text,
        };
        type_map.insert(key, fact_type);
    }

    Ok(type_map)
}

/// Upsert a fact (insert or update on conflict).
///
/// Works for both source and object facts. Uses INSERT ON CONFLICT to
/// atomically insert or update the fact.
///
/// # Arguments
/// * `conn` - Database connection
/// * `entity_type` - "source" or "object"
/// * `entity_id` - The source_id or object_id
/// * `key` - Fact key (e.g., "content.Make")
/// * `value_text` - Text value (mutually exclusive with value_num/value_time)
/// * `value_num` - Numeric value
/// * `value_time` - Timestamp value
/// * `observed_at` - When this fact was observed
/// * `observed_basis_rev` - For source facts, the basis_rev at observation time
pub fn upsert(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
    value_text: Option<&str>,
    value_num: Option<f64>,
    value_time: Option<i64>,
    observed_at: i64,
    observed_basis_rev: Option<i64>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, value_num, value_time, observed_at, observed_basis_rev)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
           value_text = excluded.value_text,
           value_num = excluded.value_num,
           value_time = excluded.value_time,
           observed_at = excluded.observed_at,
           observed_basis_rev = excluded.observed_basis_rev",
        rusqlite::params![
            entity_type,
            entity_id,
            key,
            value_text,
            value_num,
            value_time,
            observed_at,
            observed_basis_rev,
        ],
    )?;
    Ok(())
}

/// Fetch all facts for a source (entity_type = 'source').
///
/// Returns all facts directly attached to the source (not object facts).
/// Used during fact promotion when linking a source to an object.
pub fn fetch_source_facts(conn: &Connection, source_id: i64) -> Result<Vec<crate::domain::fact::SourceFact>> {
    use crate::domain::fact::SourceFact;

    let mut stmt = conn.prepare(
        "SELECT id, key, value_text, value_num, value_time, observed_at
         FROM facts
         WHERE entity_type = 'source' AND entity_id = ?"
    )?;

    let facts = stmt
        .query_map([source_id], |row| {
            Ok(SourceFact {
                id: row.get(0)?,
                key: row.get(1)?,
                value_text: row.get(2)?,
                value_num: row.get(3)?,
                value_time: row.get(4)?,
                observed_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(facts)
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
/// Used during fact promotion to remove the source fact after copying to object.
pub fn delete_by_id(conn: &Connection, fact_id: i64) -> Result<()> {
    conn.execute("DELETE FROM facts WHERE id = ?", [fact_id])?;
    Ok(())
}

/// Insert a fact on an object (for promotion from source).
///
/// Similar to upsert but specifically for object facts during promotion.
/// Does not update on conflict — callers should check `object_has_fact` first.
pub fn insert_object_fact(
    conn: &Connection,
    object_id: i64,
    key: &str,
    value_text: Option<&str>,
    value_num: Option<f64>,
    value_time: Option<i64>,
    observed_at: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, value_num, value_time, observed_at, observed_basis_rev)
         VALUES ('object', ?, ?, ?, ?, ?, ?, NULL)",
        rusqlite::params![
            object_id,
            key,
            value_text,
            value_num,
            value_time,
            observed_at,
        ],
    )?;
    Ok(())
}

// ============================================================================
// Criteria-based deletion (for `canon facts delete`)
// ============================================================================

/// Build SQL clause for value type filter.
fn value_type_clause(value_type: Option<&str>) -> &'static str {
    match value_type {
        Some("text") => "AND value_text IS NOT NULL",
        Some("num") => "AND value_num IS NOT NULL",
        Some("time") => "AND value_time IS NOT NULL",
        _ => "",
    }
}

/// Count facts matching criteria for the given sources.
///
/// # Arguments
/// * `conn` - Database connection (must be mutable for temp table)
/// * `source_ids` - Source IDs to scope the operation
/// * `key` - Fact key to match
/// * `entity_type` - "source" or "object"
/// * `value_type` - Optional filter: "text", "num", or "time"
///
/// # Returns
/// Tuple of (total_fact_count, distinct_entity_count)
pub fn count_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
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
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        let entities: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'source'
                   AND entity_id IN (SELECT id FROM temp_sources)
                   AND key = ? {}",
                vt_clause
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
                   AND key = ? {}",
                vt_clause
            ),
            [key],
            |row| row.get(0),
        )?;

        let entities: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT entity_id) FROM facts
                 WHERE entity_type = 'object'
                   AND entity_id IN (SELECT id FROM temp_objects)
                   AND key = ? {}",
                vt_clause
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
/// * `value_type` - Optional filter: "text", "num", or "time"
///
/// # Returns
/// Number of fact rows deleted
pub fn delete_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
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
                   AND key = ? {}",
                vt_clause
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
                   AND key = ? {}",
                vt_clause
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

/// Count stale source facts where observed_basis_rev != current basis_rev.
///
/// A fact is considered stale when its `observed_basis_rev` (the file's basis_rev
/// at the time the fact was recorded) doesn't match the source's current `basis_rev`.
/// This indicates the file has changed since the fact was observed.
///
/// Only source facts with non-null observed_basis_rev are considered.
/// Object facts don't have basis_rev tracking.
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

/// Delete stale source facts where observed_basis_rev != current basis_rev.
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
/// Tuple of (source_fact_count, object_fact_count).
/// If scope is "source", object_fact_count will be 0.
/// If scope is "object", source_fact_count will be 0.
pub fn count_excluded(conn: &Connection, scope: &str) -> Result<(i64, i64)> {
    let count_sources = scope == "all" || scope == "source";
    let count_objects = scope == "all" || scope == "object";

    let source_fact_count: i64 = if count_sources {
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

    let object_fact_count: i64 = if count_objects {
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

    Ok((source_fact_count, object_fact_count))
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
    use rusqlite::Connection as RawConnection;

    /// Set up an in-memory database with schema for testing
    fn setup_test_db() -> Connection {
        let conn = RawConnection::open_in_memory().unwrap();

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
                hash_value TEXT,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE sources (
                id INTEGER PRIMARY KEY,
                root_id INTEGER NOT NULL,
                rel_path TEXT NOT NULL,
                object_id INTEGER,
                present INTEGER NOT NULL DEFAULT 1,
                excluded INTEGER NOT NULL DEFAULT 0,
                size INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER NOT NULL DEFAULT 0,
                device INTEGER NOT NULL DEFAULT 0,
                inode INTEGER NOT NULL DEFAULT 0,
                partial_hash TEXT NOT NULL DEFAULT '',
                basis_rev INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (root_id) REFERENCES roots(id),
                FOREIGN KEY (object_id) REFERENCES objects(id)
            );

            CREATE TABLE facts (
                id INTEGER PRIMARY KEY,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('source', 'object')),
                entity_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value_text TEXT,
                value_num REAL,
                value_time INTEGER,
                observed_at INTEGER NOT NULL DEFAULT 0,
                observed_basis_rev INTEGER,
                CHECK (
                    (value_text IS NOT NULL) + (value_num IS NOT NULL) +
                    (value_time IS NOT NULL) = 1
                ),
                UNIQUE (entity_type, entity_id, key)
            );
            "#,
        )
        .unwrap();

        conn
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
            "INSERT INTO objects (id, hash_value) VALUES (?1, ?2)",
            [&id as &dyn rusqlite::ToSql, &hash],
        )
        .unwrap();
    }

    fn insert_source(conn: &Connection, id: i64, root_id: i64, rel_path: &str, object_id: Option<i64>) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, object_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, root_id, rel_path, object_id],
        )
        .unwrap();
    }

    fn insert_fact_text(conn: &Connection, entity_type: &str, entity_id: i64, key: &str, value: &str) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    fn insert_fact_num(conn: &Connection, entity_type: &str, entity_id: i64, key: &str, value: f64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    fn insert_fact_time(conn: &Connection, entity_type: &str, entity_id: i64, key: &str, value: i64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value],
        )
        .unwrap();
    }

    // =========================================================================
    // batch_fetch_for_sources tests
    // =========================================================================

    #[test]
    fn batch_fetch_for_sources_empty_ids() {
        let mut conn = setup_test_db();
        let result = batch_fetch_for_sources(&mut conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_for_sources_no_facts() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        let result = batch_fetch_for_sources(&mut conn, &[1]).unwrap();
        // Source exists but has no facts - not in result map
        assert!(result.get(&1).map(|v| v.is_empty()).unwrap_or(true));
    }

    #[test]
    fn batch_fetch_for_sources_source_facts() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "source.policy", "reviewed");

        let result = batch_fetch_for_sources(&mut conn, &[1]).unwrap();
        let facts = result.get(&1).unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].key, "source.policy");
    }

    #[test]
    fn batch_fetch_for_sources_object_facts() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = batch_fetch_for_sources(&mut conn, &[1]).unwrap();
        let facts = result.get(&1).unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].key, "content.Make");
    }

    #[test]
    fn batch_fetch_for_sources_mixed_facts() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "source", 1, "source.policy", "reviewed");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = batch_fetch_for_sources(&mut conn, &[1]).unwrap();
        let facts = result.get(&1).unwrap();
        assert_eq!(facts.len(), 2);
    }

    #[test]
    fn batch_fetch_for_sources_multiple_sources() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_object(&conn, 101, "def456");
        insert_source(&conn, 1, 1, "file1.txt", Some(100));
        insert_source(&conn, 2, 1, "file2.txt", Some(101));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 101, "content.Make", "Nikon");

        let result = batch_fetch_for_sources(&mut conn, &[1, 2]).unwrap();
        assert_eq!(result.get(&1).unwrap().len(), 1);
        assert_eq!(result.get(&2).unwrap().len(), 1);
    }

    // =========================================================================
    // batch_fetch_key_for_sources tests
    // =========================================================================

    #[test]
    fn batch_fetch_key_for_sources_empty_ids() {
        let mut conn = setup_test_db();
        let result = batch_fetch_key_for_sources(&mut conn, &[], "content.Make").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_key_for_sources_found() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_text(&conn, "object", 100, "content.Model", "EOS");

        let result = batch_fetch_key_for_sources(&mut conn, &[1], "content.Make").unwrap();
        let entry = result.get(&1).unwrap().as_ref().unwrap();
        assert_eq!(entry.key, "content.Make");
        match &entry.value {
            FactValue::Text(s) => assert_eq!(s, "Canon"),
            _ => panic!("Expected Text variant"),
        }
    }

    #[test]
    fn batch_fetch_key_for_sources_missing() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        let result = batch_fetch_key_for_sources(&mut conn, &[1], "content.Make").unwrap();
        assert!(result.get(&1).unwrap().is_none());
    }

    #[test]
    fn batch_fetch_key_for_sources_partial() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file1.txt", Some(100));
        insert_source(&conn, 2, 1, "file2.txt", None);
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = batch_fetch_key_for_sources(&mut conn, &[1, 2], "content.Make").unwrap();
        assert!(result.get(&1).unwrap().is_some());
        assert!(result.get(&2).unwrap().is_none());
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
        assert!(matches!(result[0].2, FactType::Text));
    }

    #[test]
    fn count_fact_keys_type_detection() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_num(&conn, "object", 100, "content.Width", 4000.0);
        insert_fact_time(&conn, "object", 100, "content.DateTimeOriginal", 1704067200);

        let result = count_fact_keys(&mut conn, &[1]).unwrap();
        assert_eq!(result.len(), 3);

        // Find each key and check type
        let make = result.iter().find(|(k, _, _)| k == "content.Make").unwrap();
        assert!(matches!(make.2, FactType::Text));

        let width = result.iter().find(|(k, _, _)| k == "content.Width").unwrap();
        assert!(matches!(width.2, FactType::Num));

        let date = result.iter().find(|(k, _, _)| k == "content.DateTimeOriginal").unwrap();
        assert!(matches!(date.2, FactType::Time));
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
        let make = result.iter().find(|(k, _, _)| k == "content.Make").unwrap();
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
        let make = result.iter().find(|(k, _, _)| k == "content.Make").unwrap();
        // Should count 2 (both sources) even though fact is on one object
        assert_eq!(make.1, 2);
    }

    // =========================================================================
    // store_object_fact tests
    // =========================================================================

    #[test]
    fn store_object_fact_inserts_new() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        store_object_fact(&conn, 100, "content.hash.sha256", "abc123", 1700000000).unwrap();

        // Verify fact was created
        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "abc123");
    }

    #[test]
    fn store_object_fact_upserts_existing() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        // Insert initial fact
        store_object_fact(&conn, 100, "content.hash.sha256", "old_hash", 1700000000).unwrap();

        // Upsert with new value
        store_object_fact(&conn, 100, "content.hash.sha256", "new_hash", 1700000001).unwrap();

        // Verify fact was updated
        let (value, timestamp): (String, i64) = conn.query_row(
            "SELECT value_text, observed_at FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(value, "new_hash");
        assert_eq!(timestamp, 1700000001);

        // Verify only one fact exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn store_object_fact_different_keys() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");

        store_object_fact(&conn, 100, "content.hash.sha256", "hash1", 1700000000).unwrap();
        store_object_fact(&conn, 100, "content.Make", "Canon", 1700000000).unwrap();

        // Verify both facts exist
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND entity_id = 100",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 2);
    }

    // =========================================================================
    // fetch_type_map tests
    // =========================================================================

    #[test]
    fn fetch_type_map_empty() {
        let conn = setup_test_db();
        let result = fetch_type_map(&conn).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn fetch_type_map_detects_text() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = fetch_type_map(&conn).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.get("content.Make"),
            Some(&crate::domain::fact::FactValueType::Text)
        );
    }

    #[test]
    fn fetch_type_map_detects_num() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");
        insert_fact_num(&conn, "object", 100, "content.Width", 4000.0);

        let result = fetch_type_map(&conn).unwrap();

        assert_eq!(
            result.get("content.Width"),
            Some(&crate::domain::fact::FactValueType::Num)
        );
    }

    #[test]
    fn fetch_type_map_detects_time() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");
        insert_fact_time(&conn, "object", 100, "content.DateTimeOriginal", 1704067200);

        let result = fetch_type_map(&conn).unwrap();

        assert_eq!(
            result.get("content.DateTimeOriginal"),
            Some(&crate::domain::fact::FactValueType::Time)
        );
    }

    #[test]
    fn fetch_type_map_multiple_types() {
        let conn = setup_test_db();
        insert_object(&conn, 100, "abc123");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");
        insert_fact_num(&conn, "object", 100, "content.Width", 4000.0);
        insert_fact_time(&conn, "object", 100, "content.DateTimeOriginal", 1704067200);

        let result = fetch_type_map(&conn).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result.get("content.Make"),
            Some(&crate::domain::fact::FactValueType::Text)
        );
        assert_eq!(
            result.get("content.Width"),
            Some(&crate::domain::fact::FactValueType::Num)
        );
        assert_eq!(
            result.get("content.DateTimeOriginal"),
            Some(&crate::domain::fact::FactValueType::Time)
        );
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
            Some("Canon"),
            None,
            None,
            1700000000,
            None,
        ).unwrap();

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
            None,
            Some(4000.0),
            None,
            1700000000,
            None,
        ).unwrap();

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
            None,
            None,
            Some(1704067200),
            1700000000,
            None,
        ).unwrap();

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
        upsert(&conn, "object", 100, "content.Make", Some("Canon"), None, None, 1700000000, None).unwrap();

        // Update
        upsert(&conn, "object", 100, "content.Make", Some("Nikon"), None, None, 1700000001, None).unwrap();

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
    fn upsert_source_fact_with_basis_rev() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        upsert(
            &conn,
            "source",
            1,
            "content.Make",
            Some("Canon"),
            None,
            None,
            1700000000,
            Some(5), // basis_rev
        ).unwrap();

        let (value, basis_rev): (String, i64) = conn.query_row(
            "SELECT value_text, observed_basis_rev FROM facts WHERE entity_type = 'source' AND entity_id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(value, "Canon");
        assert_eq!(basis_rev, 5);
    }

    // =========================================================================
    // fetch_source_facts tests
    // =========================================================================

    #[test]
    fn fetch_source_facts_empty() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);

        let result = fetch_source_facts(&conn, 1).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn fetch_source_facts_returns_all() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");
        insert_fact_num(&conn, "source", 1, "content.Width", 4000.0);

        let result = fetch_source_facts(&conn, 1).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|f| f.key == "content.Make"));
        assert!(result.iter().any(|f| f.key == "content.Width"));
    }

    #[test]
    fn fetch_source_facts_excludes_object_facts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_object(&conn, 100, "abc123");
        insert_source(&conn, 1, 1, "file.txt", Some(100));
        insert_fact_text(&conn, "source", 1, "source.policy", "reviewed");
        insert_fact_text(&conn, "object", 100, "content.Make", "Canon");

        let result = fetch_source_facts(&conn, 1).unwrap();

        // Only the source fact should be returned
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
        let fact_id: i64 = conn.query_row(
            "SELECT id FROM facts WHERE entity_type = 'source' AND entity_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();

        delete_by_id(&conn, fact_id).unwrap();

        // Verify fact is gone
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE id = ?",
            [fact_id],
            |row| row.get(0),
        ).unwrap();
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

        insert_object_fact(&conn, 100, "content.Make", Some("Canon"), None, None, 1700000000).unwrap();

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

        insert_object_fact(&conn, 100, "content.Width", None, Some(4000.0), None, 1700000000).unwrap();

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

        insert_object_fact(&conn, 100, "content.DateTimeOriginal", None, None, Some(1704067200), 1700000000).unwrap();

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
        let (count, entities) = count_by_criteria(&mut conn, &[], "content.Make", "source", None).unwrap();
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

        let (count, entities) = count_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
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

        let (count, entities) = count_by_criteria(&mut conn, &[1, 2], "content.Make", "object", None).unwrap();
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
        let (count, _) = count_by_criteria(&mut conn, &[1], "content.Make", "source", Some("text")).unwrap();
        assert_eq!(count, 1);

        // Only num - should not match text key
        let (count, _) = count_by_criteria(&mut conn, &[1], "content.Make", "source", Some("num")).unwrap();
        assert_eq!(count, 0);

        // Num key with num filter
        let (count, _) = count_by_criteria(&mut conn, &[1], "content.Width", "source", Some("num")).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn count_by_criteria_no_matching_key() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source(&conn, 1, 1, "file.txt", None);
        insert_fact_text(&conn, "source", 1, "content.Make", "Canon");

        let (count, entities) = count_by_criteria(&mut conn, &[1], "content.Model", "source", None).unwrap();
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

        let deleted = delete_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
        assert_eq!(deleted, 2);

        // Verify deleted
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 0);

        // Verify other key still exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE key = 'content.Model'",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let deleted = delete_by_criteria(&mut conn, &[1], "content.Width", "source", Some("num")).unwrap();
        assert_eq!(deleted, 1);

        // Text fact should still exist
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE key = 'content.Make'",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let deleted = delete_by_criteria(&mut conn, &[1, 2], "content.Make", "source", None).unwrap();
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
    fn insert_source_with_basis_rev(conn: &Connection, id: i64, root_id: i64, rel_path: &str, basis_rev: i64) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, basis_rev) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, root_id, rel_path, basis_rev],
        )
        .unwrap();
    }

    /// Helper to insert a source fact with a specific observed_basis_rev
    fn insert_fact_with_basis_rev(conn: &Connection, source_id: i64, key: &str, value: &str, observed_basis_rev: Option<i64>) {
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

    #[test]
    fn count_stale_ignores_null_basis_rev() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file.txt", 10);
        // Fact with null observed_basis_rev (not trackable, shouldn't be counted)
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", None);

        let count = count_stale(&conn).unwrap();
        assert_eq!(count, 0);
    }

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
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'source' AND entity_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'source'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(remaining, 1);

        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_type = 'source' AND entity_id = 2",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Nikon");
    }

    #[test]
    fn delete_stale_preserves_null_basis_rev() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/root");
        insert_source_with_basis_rev(&conn, 1, 1, "file.txt", 10);
        // Fact with null observed_basis_rev - should not be deleted
        insert_fact_with_basis_rev(&conn, 1, "content.Make", "Canon", None);

        let deleted = delete_stale(&conn).unwrap();
        assert_eq!(deleted, 0);

        // Verify fact still exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'source' AND entity_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

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
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    // =========================================================================
    // count_excluded / delete_excluded tests
    // =========================================================================

    /// Helper to insert an excluded source
    fn insert_excluded_source(conn: &Connection, id: i64, root_id: i64, rel_path: &str) {
        conn.execute(
            "INSERT INTO sources (id, root_id, rel_path, excluded) VALUES (?1, ?2, ?3, 1)",
            rusqlite::params![id, root_id, rel_path],
        )
        .unwrap();
    }

    /// Helper to insert an excluded object
    fn insert_excluded_object(conn: &Connection, id: i64, hash: &str) {
        conn.execute(
            "INSERT INTO objects (id, hash_value, excluded) VALUES (?1, ?2, 1)",
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
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'object'",
            [],
            |row| row.get(0),
        ).unwrap();
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

        // Source fact should still exist
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'source'",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM facts",
            [],
            |row| row.get(0),
        ).unwrap();
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
        let value: String = conn.query_row(
            "SELECT value_text FROM facts WHERE entity_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "Canon");
    }
}
