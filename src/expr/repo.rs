//! The expression facility's SQL: what the language needs read out of the
//! database before it can be evaluated.
//!
//! Two prefetches fill the cache the evaluation half reads — one for facts,
//! one for the status predicates — and two point reads answer a single
//! question about a single key. Batch in, domain types out.

use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashSet;

use super::domain::cache::{to_fact_value, FactCache, FactValue};
use super::domain::filter::{StatusPredicate, UsedStatus};
use super::domain::key::{is_builtin_key, parse_key_with_modifiers};
use crate::core::repo::db::populate_temp_sources;

// ============================================================================
// Fact Prefetching
// ============================================================================

/// A fact row: (entity_id, value_text, value_num, value_time).
type FactRow = (i64, Option<String>, Option<f64>, Option<i64>);

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

    // Parse keys to get base keys (without accessors/modifiers)
    let base_keys: Vec<String> = keys
        .iter()
        .filter_map(|k| parse_key_with_modifiers(k).ok().map(|(base, _, _)| base))
        .collect();

    // Skip built-in keys (they don't need DB lookups)
    let stored_keys: Vec<&String> = base_keys.iter().filter(|k| !is_builtin_key(k)).collect();

    for key in &base_keys {
        cache.prefetched_keys.insert(key.clone());
    }

    if stored_keys.is_empty() {
        // content.hash.sha256? needs source_objects even with no stored keys.
        if base_keys.iter().any(|k| k == "content.hash.sha256") {
            // Filling the temp source table opens and commits a transaction
            // of its own, and clears whatever a previous fill left behind.
            // Two consequences hold for every query in this module that uses
            // it, here and below: none of them may run inside an outer
            // transaction, and the drops that follow each one are tidiness
            // rather than correctness. Hoisting the fills into a single
            // shared setup looks like an obvious simplification and would
            // break the first of those.
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
        return Ok(cache);
    }

    // Create temp table for source IDs
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

    // Fetch source facts for all keys
    for key in &stored_keys {
        let facts: Vec<FactRow> = conn
            .prepare(
                "SELECT ts.id, f.value_text, f.value_num, f.value_time
                 FROM temp_sources ts
                 LEFT JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?"
            )?
            .query_map([*key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        for (source_id, text_val, num_val, time_val) in facts {
            if let Some(fv) = to_fact_value(text_val, num_val, time_val) {
                cache.source_facts.insert((source_id, (*key).clone()), fv);
            }
        }
    }

    // Fetch object facts for all keys
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
                .prepare(
                    "SELECT t.id, f.value_text, f.value_num, f.value_time
                     FROM temp_objects t
                     LEFT JOIN facts f ON f.entity_type = 'object' AND f.entity_id = t.id AND f.key = ?"
                )?
                .query_map([*key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
                .collect::<Result<Vec<_>, _>>()?;

            for (object_id, text_val, num_val, time_val) in facts {
                if let Some(fv) = to_fact_value(text_val, num_val, time_val) {
                    cache.object_facts.insert((object_id, (*key).clone()), fv);
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

/// Check if a key is known (either a built-in or exists in the facts table)
pub fn is_known_key(conn: &Connection, base_key: &str) -> Result<bool> {
    // Check built-in keys first
    if is_builtin_key(base_key) {
        return Ok(true);
    }

    // Check if key exists in facts table (for any entity)
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE key = ? LIMIT 1",
            [base_key],
            |_| Ok(true),
        )
        .unwrap_or(false);

    Ok(exists)
}

// ============================================================================
// Value Handling
// ============================================================================

pub fn get_fact_value(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
) -> Result<Option<FactValue>> {
    let result: Option<(Option<String>, Option<f64>, Option<i64>)> = conn
        .query_row(
            "SELECT value_text, value_num, value_time FROM facts
             WHERE entity_type = ? AND entity_id = ? AND key = ?",
            params![entity_type, entity_id, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok();

    Ok(result.and_then(|(text, num, time)| {
        if let Some(t) = text {
            Some(FactValue::Text(t))
        } else if let Some(n) = num {
            Some(FactValue::Num(n))
        } else {
            time.map(FactValue::Time)
        }
    }))
}
