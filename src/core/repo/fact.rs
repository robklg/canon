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
//! use crate::core::repo;
//!
//! // Fetch a specific fact key for sources
//! let facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, "content.Make")?;
//! for (source_id, entry) in &facts {
//!     if let Some(fact) = entry {
//!         println!("{}: {:?}", source_id, fact.value);
//!     }
//! }
//! ```

use std::collections::HashMap;

use anyhow::Result;

use super::db::{populate_temp_sources, Connection};
use crate::core::domain::fact::{FactEntry, FactValue};

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

        Ok((
            source_id,
            FactEntry::new(key, value, entity_type, entity_id),
        ))
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

        Ok((
            source_id,
            FactEntry::new(key, value, entity_type, entity_id),
        ))
    })?;

    // Initialize result with None for all source IDs
    let mut result: HashMap<i64, Option<FactEntry>> =
        source_ids.iter().map(|&id| (id, None)).collect();

    // Fill in found facts
    for row in rows {
        let (source_id, entry) = row?;
        result.insert(source_id, Some(entry));
    }

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
}
