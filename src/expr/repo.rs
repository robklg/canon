//! The expression facility's SQL: what the language needs read out of the
//! database before it can be evaluated.
//!
//! Two prefetches fill the cache the evaluation half reads — one for facts,
//! one for the status predicates — one point read answers whether a key is
//! known, one reads the source row a per-source comparison works from, and
//! one reads a key's rows per source, on the source and on its content, for the
//! one rule that picks between them. Batch in, domain types out.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

use super::domain::cache::FactCache;
use super::domain::filter::{StatusPredicate, UsedStatus};
use super::domain::key::Key;
use super::domain::value::SourceAttributes;
use crate::core::domain::fact::{FactEntry, FactValue};
use crate::core::repo::db::populate_temp_sources;
use crate::core::repo::fact::{fact_value_at, fact_value_columns_of};

// ============================================================================
// Fact Prefetching
// ============================================================================

/// A fact row: the entity it hangs off, and its value where it has one Canon can read.
type FactRow = (i64, Option<FactValue>);

/// Prefetch facts for a batch of sources and keys
pub fn prefetch_facts(
    conn: &mut Connection,
    source_ids: &[i64],
    keys: &[String],
) -> Result<FactCache> {
    let mut cache = FactCache::new();

    if source_ids.is_empty() || keys.is_empty() {
        return Ok(cache);
    }

    // The keys arrive as base keys, already read (accessor and modifiers stripped, an alias
    // read as its stored key) — never re-read here, so a quoted name holding `|` or `[` stays
    // one name.
    let base_keys: Vec<String> = keys.to_vec();

    // A key the source row computes needs no lookup; every other key is read
    // from the facts table under its stored name, the content hash included
    // (the reader has already read `hash` as `content.hash.sha256`).
    let stored_keys: Vec<&String> = base_keys
        .iter()
        .filter(|k| !Key::read(k).is_some_and(|key| key.is_computed()))
        .collect();

    for key in &base_keys {
        cache.prefetched_keys.insert(key.clone());
    }

    if stored_keys.is_empty() {
        return Ok(cache);
    }

    // Filling the temp source table opens and commits a transaction of its
    // own, and clears whatever a previous fill left behind. Two consequences
    // hold for every query in this module that uses it, here and below: none
    // of them may run inside an outer transaction, and the drops that follow
    // each one are tidiness rather than correctness. Hoisting the fills into
    // a single shared setup looks like an obvious simplification and would
    // break the first of those.
    populate_temp_sources(conn, source_ids)?;

    // Fetch source->object mappings
    let mappings: Vec<(i64, i64)> = conn
        .prepare(
            "SELECT ts.id, s.object_id
             FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             WHERE s.object_id IS NOT NULL",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (source_id, object_id) in mappings {
        cache.source_objects.insert(source_id, object_id);
    }

    // Fetch the facts stored on the sources, for all keys
    for key in &stored_keys {
        let facts: Vec<FactRow> = conn
            .prepare(&format!(
                "SELECT ts.id, {}
                 FROM temp_sources ts
                 JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?",
                fact_value_columns_of("f")
            ))?
            .query_map([*key], |row| Ok((row.get(0)?, fact_value_at(row, 1)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        for (source_id, value) in facts {
            if let Some(fv) = value {
                cache.on_sources.insert((source_id, (*key).clone()), fv);
            }
        }
    }

    // Fetch the facts stored on their objects, for all keys
    // Get unique object IDs (multiple sources may share the same object)
    let object_ids: Vec<i64> = cache
        .source_objects
        .values()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if !object_ids.is_empty() {
        // Create temp table for object IDs
        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;
        conn.execute(
            "CREATE TEMP TABLE temp_objects (id INTEGER PRIMARY KEY)",
            [],
        )?;
        {
            let mut stmt = conn.prepare("INSERT INTO temp_objects (id) VALUES (?)")?;
            for oid in &object_ids {
                stmt.execute([oid])?;
            }
        }

        for key in &stored_keys {
            let facts: Vec<FactRow> = conn
                .prepare(&format!(
                    "SELECT t.id, {}
                     FROM temp_objects t
                     JOIN facts f ON f.entity_type = 'object' AND f.entity_id = t.id AND f.key = ?",
                    fact_value_columns_of("f")
                ))?
                .query_map([*key], |row| Ok((row.get(0)?, fact_value_at(row, 1)?)))?
                .collect::<Result<Vec<_>, _>>()?;

            for (object_id, value) in facts {
                if let Some(fv) = value {
                    cache.on_objects.insert((object_id, (*key).clone()), fv);
                }
            }
        }

        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;
    }

    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(cache)
}

/// Prefetch status predicate data into the cache based on which predicates are used.
///
/// Evaluation reads each predicate's prefetched set without checking that it
/// is there, so every predicate the expression asked for must be loaded here
/// before evaluation runs. The walk covers every variant and matches
/// exhaustively so that stays true of predicates added later.
pub fn prefetch_status_data(
    conn: &mut Connection,
    source_ids: &[i64],
    used: &UsedStatus,
    cache: &mut FactCache,
) -> Result<()> {
    use strum::IntoEnumIterator;

    if source_ids.is_empty() {
        return Ok(());
    }

    // Ensure source_objects is populated when hashed? or archived? need it.
    // prefetch_facts only populates this when there are stored fact keys to fetch.
    if (used.hashed || used.archived) && cache.source_objects.is_empty() {
        populate_temp_sources(conn, source_ids)?;
        let mappings: Vec<(i64, i64)> = conn
            .prepare(
                "SELECT ts.id, s.object_id
                 FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 WHERE s.object_id IS NOT NULL",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for (source_id, object_id) in mappings {
            cache.source_objects.insert(source_id, object_id);
        }
        conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
    }

    for predicate in StatusPredicate::iter() {
        if !used.uses(predicate) {
            continue;
        }
        match predicate {
            // Answered from the source-to-object map populated above.
            StatusPredicate::Hashed => {}

            StatusPredicate::Archived => {
                // Collect object_ids from the cache's source_objects mapping
                let object_ids: Vec<i64> = cache
                    .source_objects
                    .values()
                    .copied()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                let archived =
                    crate::core::repo::object::batch_check_archived(conn, &object_ids, None)?;
                cache.archived_objects = Some(archived);
            }

            StatusPredicate::Excluded => {
                populate_temp_sources(conn, source_ids)?;
                let excluded: HashSet<i64> = conn
                    .prepare(
                        "SELECT DISTINCT s.id FROM temp_sources ts
                         JOIN sources s ON s.id = ts.id
                         WHERE s.excluded = 1
                         UNION
                         SELECT DISTINCT s.id FROM temp_sources ts
                         JOIN sources s ON s.id = ts.id
                         JOIN objects o ON o.id = s.object_id
                         WHERE o.excluded = 1",
                    )?
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<HashSet<_>, _>>()?;
                conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
                cache.excluded_sources = Some(excluded);
            }

            StatusPredicate::Enriched => {
                populate_temp_sources(conn, source_ids)?;
                let enriched: HashSet<i64> = conn
                    .prepare(
                        "SELECT DISTINCT ts.id FROM temp_sources ts
                         JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id
                             AND f.key != 'content.hash.sha256'
                         UNION
                         SELECT DISTINCT s.id FROM temp_sources ts
                         JOIN sources s ON s.id = ts.id
                         JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
                             AND f.key != 'content.hash.sha256'",
                    )?
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<HashSet<_>, _>>()?;
                conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
                cache.enriched_sources = Some(enriched);
            }
        }
    }

    Ok(())
}

/// A source row as a per-source comparison reads it: the attributes the built-in keys are
/// resolved from, and whether its device and inode columns hold a value (a NULL one has no
/// value to read).
pub struct SourceRow {
    pub attributes: SourceAttributes,
    pub has_device: bool,
    pub has_inode: bool,
}

/// One source's row, for a per-source comparison; `None` for an id with no row.
pub fn fetch_source_row(conn: &Connection, source_id: i64) -> Result<Option<SourceRow>> {
    use rusqlite::OptionalExtension;
    Ok(conn
        .query_row(
            "SELECT s.id, s.root_id, r.path, s.rel_path, s.size, s.mtime, s.device, s.inode
             FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
            [source_id],
            |row| {
                let device: Option<i64> = row.get(6)?;
                let inode: Option<i64> = row.get(7)?;
                Ok(SourceRow {
                    attributes: SourceAttributes {
                        id: row.get(0)?,
                        root_id: row.get(1)?,
                        root_path: row.get(2)?,
                        rel_path: row.get(3)?,
                        size: row.get(4)?,
                        mtime: row.get(5)?,
                        device: device.unwrap_or(0),
                        inode: inode.unwrap_or(0),
                    },
                    has_device: device.is_some(),
                    has_inode: inode.is_some(),
                })
            },
        )
        .optional()?)
}

// ============================================================================
// A key's rows per source
// ============================================================================

/// One source's rows for one key: the fact on the source and the fact on its content —
/// everything the rule that picks a source's value reads.
pub(super) struct KeyRowsOf {
    pub on_source: Option<FactEntry>,
    pub on_content: Option<FactEntry>,
}

/// A key's rows for a batch of sources. A source holding neither row is absent from the
/// map, and so is a stored value Canon cannot read: it is no row.
pub(super) fn fetch_key_rows(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
) -> Result<HashMap<i64, KeyRowsOf>> {
    if source_ids.is_empty() {
        return Ok(HashMap::new());
    }

    populate_temp_sources(conn, source_ids)?;

    let query = format!(
        r#"
        SELECT ts.id, f.key, f.entity_type, f.entity_id, {cols}
        FROM temp_sources ts
        JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?1

        UNION ALL

        SELECT ts.id, f.key, f.entity_type, f.entity_id, {cols}
        FROM temp_sources ts
        JOIN sources s ON s.id = ts.id
        JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id AND f.key = ?1
        WHERE s.object_id IS NOT NULL
    "#,
        cols = fact_value_columns_of("f")
    );

    let mut rows: HashMap<i64, KeyRowsOf> = HashMap::new();
    {
        let mut stmt = conn.prepare(&query)?;
        let fetched = stmt.query_map([key], |row| {
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
        for row in fetched {
            let Some((source_id, entry)) = row? else {
                continue;
            };
            let of = rows.entry(source_id).or_insert(KeyRowsOf {
                on_source: None,
                on_content: None,
            });
            if entry.is_on_source() {
                of.on_source = Some(entry);
            } else {
                of.on_content = Some(entry);
            }
        }
    }

    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{
        insert_fact, insert_object, insert_object_fact, insert_root, insert_source, setup_test_db,
    };

    fn text(entry: &Option<FactEntry>) -> Option<&str> {
        match entry.as_ref().map(|e| &e.value) {
            Some(FactValue::Text(s)) => Some(s.as_str()),
            Some(other) => panic!("expected text, got {other:?}"),
            None => None,
        }
    }

    #[test]
    fn fetch_key_rows_empty_ids() {
        let mut conn = setup_test_db();
        let rows = fetch_key_rows(&mut conn, &[], "content.Make").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn fetch_key_rows_found() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let obj = insert_object(&conn, "abc123", false);
        let source = insert_source(&conn, root, "file.txt", Some(obj));
        insert_object_fact(&conn, obj, "content.Make", "Canon");
        insert_object_fact(&conn, obj, "content.Model", "EOS");
        insert_fact(&conn, source, "content.Make", "Nikon");

        let rows = fetch_key_rows(&mut conn, &[source], "content.Make").unwrap();
        let of = &rows[&source];
        assert_eq!(text(&of.on_content), Some("Canon"));
        assert_eq!(text(&of.on_source), Some("Nikon"));
        assert_eq!(of.on_content.as_ref().unwrap().key, "content.Make");
    }

    #[test]
    fn fetch_key_rows_missing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let source = insert_source(&conn, root, "file.txt", None);

        let rows = fetch_key_rows(&mut conn, &[source], "content.Make").unwrap();
        assert!(!rows.contains_key(&source));
    }

    #[test]
    fn fetch_key_rows_partial() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let obj = insert_object(&conn, "abc123", false);
        let hashed = insert_source(&conn, root, "file1.txt", Some(obj));
        let unhashed = insert_source(&conn, root, "file2.txt", None);
        insert_object_fact(&conn, obj, "content.Make", "Canon");

        let rows = fetch_key_rows(&mut conn, &[hashed, unhashed], "content.Make").unwrap();
        assert_eq!(text(&rows[&hashed].on_content), Some("Canon"));
        assert!(rows[&hashed].on_source.is_none());
        assert!(!rows.contains_key(&unhashed));
    }

    /// A stored time Canon cannot read — a clock reading stored as a second
    /// in year 10000 — is no row for that key, and the fetch over it completes.
    #[test]
    fn a_stored_time_outside_the_readable_line_is_no_row_and_the_key_fetch_completes() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);
        let b = insert_source(&conn, root, "b.jpg", None);
        // 10000-01-01T04:00:00 counted as if UTC: a second past the last year Canon reads.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision, observed_at, observed_basis_rev)
             VALUES ('source', ?1, 'content.T', 253402315200, 'clock', 'second', 0, 0),
                    ('source', ?2, 'content.T', 1704067200, 'clock', 'second', 0, 0)",
            [a, b],
        )
        .unwrap();

        let rows = fetch_key_rows(&mut conn, &[a, b], "content.T").unwrap();
        assert!(!rows.contains_key(&a), "the unreadable time is no row");
        assert!(rows[&b].on_source.is_some());
    }
}
