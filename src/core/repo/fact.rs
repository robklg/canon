//! Fact repository — infrastructure layer for fetching facts.
//!
//! This module provides batch fetch functions that return typed fact data.
//! It transparently handles facts stored on a source and on its object, associating
//! all facts with their source ID for easy consumption.
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic
//! 2. **Batch fetching**: Use temp tables for large source sets
//! 3. **Returns domain types**: Functions return `FactEntry` structs
//! 4. **Source-centric**: Facts stored on an object are keyed by source_id in results
//!
//! ## Usage
//!
//! ```ignore
//! use crate::core::repo;
//!
//! // Every fact each source carries, on the source and on its content.
//! let facts = repo::fact::batch_fetch_for_sources(conn, &source_ids)?;
//! for (source_id, entries) in &facts {
//!     for fact in entries {
//!         println!("{}: {} = {:?}", source_id, fact.key, fact.value);
//!     }
//! }
//! ```

use std::collections::HashMap;

use anyhow::Result;

use jiff::civil::{self, DateTime};
use jiff::tz::Offset;
use jiff::{SignedDuration, Timestamp};
use rusqlite::types::Value as SqlValue;

use super::db::{populate_temp_sources, Connection};
use crate::core::domain::fact::{FactEntry, FactType, FactValue, KeyTypes};
use crate::core::domain::time::value::{Precision, TimeForm, TimeValue};

// Source-keyed fetches fill a temp table (populate_temp_sources) rather than chunk an IN
// list; `key_types`, keyed by fact key, chunks its key list instead (KEY_TYPES_CHUNK).

/// Fetch all facts for the given source IDs.
///
/// Returns a map from source_id to list of FactEntry.
/// Each source's facts include:
/// - Facts stored on the source (entity_type = 'source', entity_id = source_id)
/// - Facts stored on its object (entity_type = 'object', entity_id = object_id) if source has object_id
///
/// Facts stored on the object are associated with the SOURCE id in the result map,
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
    let query = format!(
        r#"
        SELECT ts.id as source_id, f.key, f.entity_type, f.entity_id, {cols}
        FROM temp_sources ts
        JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id

        UNION ALL

        SELECT ts.id as source_id, f.key, f.entity_type, f.entity_id, {cols}
        FROM temp_sources ts
        JOIN sources s ON s.id = ts.id
        JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
        WHERE s.object_id IS NOT NULL
    "#,
        cols = fact_value_columns_of("f")
    );

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let source_id: i64 = row.get(0)?;
        let key: String = row.get(1)?;
        let entity_type: String = row.get(2)?;
        let entity_id: i64 = row.get(3)?;
        let value = fact_value_at(row, 4)?;

        Ok(value.map(|value| {
            (
                source_id,
                FactEntry::new(key, value, entity_type, entity_id),
            )
        }))
    })?;

    // Group facts by source_id
    let mut result: HashMap<i64, Vec<FactEntry>> = HashMap::new();
    for row in rows {
        if let Some((source_id, entry)) = row? {
            result.entry(source_id).or_default().push(entry);
        }
    }

    // Clean up temp table
    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(result)
}

/// Keys per `key_types` query, under SQLite's bound-variable limit.
const KEY_TYPES_CHUNK: usize = 900;

/// The types each key's facts hold — read from the key's row set: every fact of the key,
/// on every root, of every source, of any content, present or not. No join, so suspension,
/// scope, presence and role can never enter a key's type. A key with no facts is absent
/// from the map: it has no type.
///
/// The grouping expressions are written exactly as the `facts_key_type` index holds them,
/// so SQLite answers from the index alone.
pub fn key_types(conn: &Connection, keys: &[&str]) -> Result<HashMap<String, KeyTypes>> {
    let mut counts: HashMap<String, Vec<(FactType, u64)>> = HashMap::new();
    for chunk in keys.chunks(KEY_TYPES_CHUNK) {
        let mut stmt = conn.prepare(&key_types_sql(chunk.len()))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            let key: String = row.get(0)?;
            let no_time: bool = row.get(1)?;
            let no_num: bool = row.get(2)?;
            let n: i64 = row.get(3)?;
            let t = match (no_time, no_num) {
                (false, _) => FactType::Time,
                (true, false) => FactType::Num,
                (true, true) => FactType::Text,
            };
            Ok((key, t, n as u64))
        })?;
        for row in rows {
            let (key, t, n) = row?;
            counts.entry(key).or_default().push((t, n));
        }
    }
    Ok(counts
        .into_iter()
        .filter_map(|(key, c)| KeyTypes::from_counts(c).map(|types| (key, types)))
        .collect())
}

/// `key_types`' query for `n` keys. Its expressions repeat `facts_key_type`'s, which is what
/// lets SQLite answer it from that index.
fn key_types_sql(n: usize) -> String {
    let placeholders = vec!["?"; n].join(", ");
    format!(
        "SELECT key, value_time IS NULL, value_num IS NULL, COUNT(*) FROM facts
         WHERE key IN ({placeholders})
         GROUP BY key, value_time IS NULL, value_num IS NULL"
    )
}

/// The value columns of `facts`, in the order [`fact_value_at`] reads them and
/// [`fact_value_params`] binds them. Every SELECT that reads a fact's value spells this list.
pub const FACT_VALUE_COLUMNS: &str =
    "value_text, value_num, value_time, time_nanos, time_form, time_offset, time_precision";

/// The value columns of `facts` prefixed with a table alias (`f.value_text, …`), for a
/// SELECT that joins.
pub fn fact_value_columns_of(alias: &str) -> String {
    FACT_VALUE_COLUMNS
        .split(", ")
        .map(|c| format!("{alias}.{c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Read a fact's value from the seven columns starting at `first`.
///
/// A row the table's CHECKs forbid is an error. A stored time Canon cannot read — outside
/// the years every zone reads in 0000–9999, or with a part its constructor refuses — is no
/// value for that fact (`Ok(None)`), never an error that fails the command.
pub fn fact_value_at(row: &rusqlite::Row, first: usize) -> rusqlite::Result<Option<FactValue>> {
    let text: Option<String> = row.get(first)?;
    let num: Option<f64> = row.get(first + 1)?;
    let time: Option<i64> = row.get(first + 2)?;
    let nanos: Option<i32> = row.get(first + 3)?;
    let form: Option<String> = row.get(first + 4)?;
    let offset: Option<i32> = row.get(first + 5)?;
    let precision: Option<String> = row.get(first + 6)?;
    let forbidden = |what: &str| {
        rusqlite::Error::FromSqlConversionFailure(
            first,
            rusqlite::types::Type::Null,
            format!("facts row the schema forbids: {what}").into(),
        )
    };
    match (text, num, time) {
        (Some(t), None, None) => Ok(Some(FactValue::Text(t))),
        (None, Some(n), None) => Ok(Some(FactValue::Num(n))),
        (None, None, Some(secs)) => {
            let precision = precision
                .as_deref()
                .and_then(precision_from_column)
                .ok_or_else(|| forbidden("time precision"))?;
            let nanos = match (precision, nanos) {
                (Precision::Fraction, Some(n)) => n,
                (Precision::Fraction, None) | (_, Some(_)) => {
                    return Err(forbidden("time fraction"))
                }
                (_, None) => 0,
            };
            let value = match (form.as_deref(), offset) {
                (Some("clock"), None) => clock_from_column(secs, nanos)
                    .and_then(|at| TimeValue::clock(at, precision).ok()),
                (Some("instant"), None) => Timestamp::new(secs, nanos)
                    .ok()
                    .and_then(|at| TimeValue::instant(at, None, precision).ok()),
                (Some("instant_offset"), Some(o)) => Timestamp::new(secs, nanos)
                    .ok()
                    .zip(Offset::from_seconds(o).ok())
                    .and_then(|(at, o)| TimeValue::instant(at, Some(o), precision).ok()),
                _ => return Err(forbidden("time form")),
            };
            Ok(value.map(FactValue::Time))
        }
        _ => Err(forbidden("one value column")),
    }
}

/// The seven column values a fact's value is written as, in [`FACT_VALUE_COLUMNS`]' order.
/// A path is derived, never stored.
pub fn fact_value_params(value: &FactValue) -> Result<[SqlValue; 7]> {
    use SqlValue::{Integer, Null, Real, Text};
    Ok(match value {
        FactValue::Text(t) => [Text(t.clone()), Null, Null, Null, Null, Null, Null],
        FactValue::Num(n) => [Null, Real(*n), Null, Null, Null, Null, Null],
        FactValue::Time(tv) => [
            Null,
            Null,
            Integer(tv.legacy_epoch_number()),
            tv.fraction_nanos().map_or(Null, |n| Integer(n.into())),
            Text(form_column(tv.form()).to_string()),
            tv.recorded_offset()
                .map_or(Null, |o| Integer(o.seconds().into())),
            Text(precision_column(tv.precision()).to_string()),
        ],
        FactValue::Path(_) => anyhow::bail!("a path value is derived and never stored"),
    })
}

fn precision_column(p: Precision) -> &'static str {
    match p {
        Precision::Year => "year",
        Precision::Month => "month",
        Precision::Day => "day",
        Precision::Minute => "minute",
        Precision::Second => "second",
        Precision::Fraction => "fraction",
    }
}

fn precision_from_column(s: &str) -> Option<Precision> {
    Some(match s {
        "year" => Precision::Year,
        "month" => Precision::Month,
        "day" => Precision::Day,
        "minute" => Precision::Minute,
        "second" => Precision::Second,
        "fraction" => Precision::Fraction,
        _ => return None,
    })
}

fn form_column(f: TimeForm) -> &'static str {
    match f {
        TimeForm::Instant => "instant",
        TimeForm::InstantWithOffset => "instant_offset",
        TimeForm::Clock => "clock",
    }
}

/// A clock reading stored as if it were UTC: the seconds and fraction since 1970-01-01T00:00
/// on the reading's own clock. `None` past the civil range.
fn clock_from_column(secs: i64, nanos: i32) -> Option<DateTime> {
    civil::date(1970, 1, 1)
        .at(0, 0, 0, 0)
        .checked_add(SignedDuration::new(secs, nanos))
        .ok()
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
    fn batch_fetch_for_sources_facts_on_the_source() {
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
    // key_types — the key's row set
    // =========================================================================

    fn insert_fact_num(conn: &Connection, entity_type: &str, entity_id: i64, key: &str, n: f64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
             VALUES (?1, ?2, ?3, ?4, 0, CASE WHEN ?1 = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, n],
        )
        .unwrap();
    }

    /// Every fact of the key counts, wherever it sits: on a suspended root, on a source no
    /// longer present, on an excluded source, and on content no source holds.
    #[test]
    fn key_types_reads_every_row_of_the_key() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/open");
        insert_root(&conn, 2, "/parked");
        conn.execute("UPDATE roots SET suspended = 1 WHERE id = 2", [])
            .unwrap();
        insert_source(&conn, 1, 1, "present.jpg", None);
        insert_source(&conn, 2, 2, "parked.jpg", None);
        insert_source(&conn, 3, 1, "gone.jpg", None);
        conn.execute("UPDATE sources SET present = 0 WHERE id = 3", [])
            .unwrap();
        insert_source(&conn, 4, 1, "dismissed.jpg", None);
        conn.execute("UPDATE sources SET excluded = 1 WHERE id = 4", [])
            .unwrap();
        insert_object(&conn, 100, "held-by-nothing");
        for id in 1..=4 {
            insert_fact_num(&conn, "source", id, "content.W", 1.0);
        }
        insert_fact_num(&conn, "object", 100, "content.W", 1.0);
        insert_fact_text(&conn, "source", 1, "content.Other", "x");

        let types = key_types(&conn, &["content.W", "content.Missing"]).unwrap();
        assert_eq!(types["content.W"].counts(), &[(FactType::Num, 5)]);
        assert!(
            !types.contains_key("content.Missing"),
            "a key with no facts has no type"
        );
        assert!(!types.contains_key("content.Other"), "only the keys asked");
    }

    #[test]
    fn key_types_reports_two_types_with_their_counts() {
        let conn = setup_test_db();
        insert_root(&conn, 1, "/r");
        insert_source(&conn, 1, 1, "a", None);
        insert_source(&conn, 2, 1, "b", None);
        insert_source(&conn, 3, 1, "c", None);
        insert_fact_text(&conn, "source", 1, "content.D", "yesterday");
        insert_fact_num(&conn, "source", 2, "content.D", 7.0);
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision, observed_at, observed_basis_rev)
             VALUES ('source', 3, 'content.D', 0, 'clock', 'second', 0, 0)",
            [],
        )
        .unwrap();

        let types = key_types(&conn, &["content.D"]).unwrap();
        let d = &types["content.D"];
        assert_eq!(d.word(), "mixed");
        assert_eq!(
            d.counts(),
            &[(FactType::Text, 1), (FactType::Num, 1), (FactType::Time, 1)]
        );
    }

    /// The lookup and its grouping are answered from `facts_key_type`: the query's
    /// expressions repeat the index's, so no step sorts the rows again.
    #[test]
    fn the_type_lookup_uses_its_index() {
        let conn = setup_test_db();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {}", key_types_sql(1)))
            .unwrap()
            .query_map(["content.D"], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(
            plan.iter().any(|step| step.contains("facts_key_type")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
    }

    /// More keys than SQLite binds in one statement (32,766), so the list must be cut, and
    /// the types merged back across the cuts.
    #[test]
    fn key_types_batches_large_key_lists() {
        const KEYS: usize = 33_000;
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/r");
        insert_source(&conn, 1, 1, "a", None);
        let keys: Vec<String> = (0..KEYS).map(|i| format!("content.K{i}")).collect();
        let tx = conn.transaction().unwrap();
        for key in &keys {
            insert_fact_text(&tx, "source", 1, key, "v");
        }
        tx.commit().unwrap();
        // The list is past what one statement binds, so an uncut query would fail.
        assert!(conn.prepare(&key_types_sql(KEYS)).is_err());
        let asked: Vec<&str> = keys.iter().map(String::as_str).collect();
        let types = key_types(&conn, &asked).unwrap();
        assert_eq!(types.len(), KEYS);
        assert!(types.values().all(|t| t.single() == Some(FactType::Text)));
    }

    // =========================================================================
    // The one conversion pair
    // =========================================================================

    use crate::core::domain::time::reading::read_time;

    fn write_and_read(conn: &Connection, value: &FactValue) -> Option<FactValue> {
        conn.execute("DELETE FROM facts", []).unwrap();
        let params = fact_value_params(value).unwrap();
        conn.execute(
            &format!(
                "INSERT INTO facts (entity_type, entity_id, key, observed_at, observed_basis_rev, {FACT_VALUE_COLUMNS})
                 VALUES ('object', 1, 'content.T', 0, NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            ),
            rusqlite::params_from_iter(params.iter()),
        )
        .unwrap();
        conn.query_row(
            &format!("SELECT {FACT_VALUE_COLUMNS} FROM facts"),
            [],
            |r| fact_value_at(r, 0),
        )
        .unwrap()
    }

    #[test]
    fn every_time_form_and_precision_round_trips_through_storage() {
        let conn = setup_test_db();
        for text in [
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "2024-06-15T14:30:05.45",
            "1969-12-31T23:59:59.5",
            "2023-12-31T21:00-05:00",
            "2023-12-31T21:00:00-05:00",
            "2024-06-15T14:30:05.123456789+02:00",
            "1969-12-31T23:59:59.5+01:00",
            "2023-12-31T20:00:00-00:00",
            "2023-12-31T20:00:00.5-00:00",
            "2023-12-31T23:30:00Z",
            "9999-12-31T23:59:59",
            "0000-01-01",
        ] {
            let value = read_time(text).unwrap();
            match write_and_read(&conn, &FactValue::Time(value)) {
                Some(FactValue::Time(back)) => assert_eq!(back, value, "{text}"),
                other => panic!("{text}: {other:?}"),
            }
        }
        let saved =
            crate::core::domain::time::value::TimeValue::from_epoch_secs(1_718_452_800).unwrap();
        assert!(
            matches!(write_and_read(&conn, &FactValue::Time(saved)), Some(FactValue::Time(t)) if t == saved)
        );
        assert!(
            matches!(write_and_read(&conn, &FactValue::Text("x".into())), Some(FactValue::Text(t)) if t == "x")
        );
        assert!(
            matches!(write_and_read(&conn, &FactValue::Num(4.5)), Some(FactValue::Num(n)) if n == 4.5)
        );
        assert!(fact_value_params(&FactValue::Path("/a".into())).is_err());
    }

    /// Rows the schema forbids, fed to the decoder as literal columns: each is an error.
    #[test]
    fn a_row_the_schema_forbids_reads_as_an_error() {
        let conn = setup_test_db();
        let forbidden = [
            "NULL, NULL, NULL, NULL, NULL, NULL, NULL",
            "'x', 1.0, NULL, NULL, NULL, NULL, NULL",
            "NULL, NULL, 0, NULL, NULL, NULL, NULL",
            "NULL, NULL, 0, NULL, 'clock', NULL, 'hour'",
            "NULL, NULL, 0, NULL, 'local', NULL, 'second'",
            "NULL, NULL, 0, NULL, 'clock', 3600, 'second'",
            "NULL, NULL, 0, NULL, 'instant_offset', NULL, 'second'",
            "NULL, NULL, 0, 5, 'clock', NULL, 'second'",
            "NULL, NULL, 0, NULL, 'clock', NULL, 'fraction'",
        ];
        for columns in forbidden {
            let read = conn.query_row(&format!("SELECT {columns}"), [], |r| fact_value_at(r, 0));
            assert!(
                read.is_err(),
                "({columns}) must read as an error, got {read:?}"
            );
        }
    }

    /// A stored time outside the years every zone reads in 0000–9999 — reachable only by a
    /// pre-upgrade import late in year 9999 with a negative offset, which the old parser
    /// stored as a second in year 10000 — is no value for that fact, and the fetch over it
    /// completes.
    #[test]
    fn a_stored_time_outside_the_readable_line_is_no_value_and_the_fetch_completes() {
        let mut conn = setup_test_db();
        insert_root(&conn, 1, "/photos");
        insert_source(&conn, 1, 1, "a.jpg", None);
        insert_source(&conn, 2, 1, "b.jpg", None);
        // 10000-01-01T04:00:00 counted as if UTC, as the old parser stored
        // `9999-12-31T23:00:00-05:00`.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision, observed_at, observed_basis_rev)
             VALUES ('source', 1, 'content.T', 253402315200, 'clock', 'second', 0, 0),
                    ('source', 2, 'content.T', 1704067200, 'clock', 'second', 0, 0)",
            [],
        )
        .unwrap();
        // A seconds offset the constructor refuses reads as no value too.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_offset, time_precision, observed_at, observed_basis_rev)
             VALUES ('source', 2, 'content.U', 1704067200, 'instant_offset', 3601, 'second', 0, 0)",
            [],
        )
        .unwrap();

        let all = batch_fetch_for_sources(&mut conn, &[1, 2]).unwrap();
        assert!(!all.contains_key(&1));
        assert_eq!(all[&2].len(), 1, "only the readable fact");
    }
}
