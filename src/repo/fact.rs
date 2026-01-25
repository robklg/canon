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
                )
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
}
