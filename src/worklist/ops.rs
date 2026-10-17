//! Worklist operations — JSONL entry construction and unique-content filtering.
//!
//! `build_entries()` converts selected sources into serializable worklist entries,
//! optionally fetching facts and deduplicating by content identity — the object
//! a source resolves to, not the hash string, though an object owns exactly one
//! hash so the two answer the same question.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::Serialize;

use crate::core::domain::fact::FactValue;
use crate::core::domain::number::spelled;
use crate::core::domain::source::Source;
use crate::core::repo::Connection;
use crate::expr::fetch_source_values;

/// A single worklist entry, ready for JSONL serialization.
#[derive(Serialize)]
pub struct WorklistEntry {
    pub source_id: i64,
    pub path: String,
    pub root_id: i64,
    pub size: i64,
    pub mtime: i64,
    pub basis_rev: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facts: Option<HashMap<String, serde_json::Value>>,
}

/// Result of building worklist entries from a selection.
pub struct WorklistResult {
    pub entries: Vec<WorklistEntry>,
    /// Sources skipped because they have no object_id (unique_content mode).
    pub skipped_unhashed: u64,
    /// Sources skipped because another source with the same object was already emitted.
    pub skipped_duplicate: u64,
}

/// Build worklist entries from selected sources.
///
/// When `unique_content` is true, only the first source per object_id is included.
/// When `emit_keys` is non-empty, each emitted source's one value per key is attached,
/// read in one batch per key, by the key as given.
pub fn build_entries(
    conn: &mut Connection,
    sources: &[Source],
    unique_content: bool,
    emit_keys: &[String],
) -> Result<WorklistResult> {
    let mut emitted: Vec<&Source> = Vec::new();
    let mut seen_objects: HashSet<i64> = HashSet::new();
    let mut skipped_unhashed: u64 = 0;
    let mut skipped_duplicate: u64 = 0;

    for source in sources {
        if unique_content {
            if source.object_id.is_none() {
                skipped_unhashed += 1;
                continue;
            }
            let object_id = source.object_id.unwrap();
            if !seen_objects.insert(object_id) {
                skipped_duplicate += 1;
                continue;
            }
        }
        emitted.push(source);
    }

    let ids: Vec<i64> = emitted.iter().map(|s| s.id).collect();
    let mut values_by_key = Vec::with_capacity(emit_keys.len());
    for key in emit_keys {
        values_by_key.push((key, fetch_source_values(conn, &ids, key)?));
    }

    let entries = emitted
        .into_iter()
        .map(|source| {
            let facts = (!emit_keys.is_empty()).then(|| {
                values_by_key
                    .iter()
                    .map(|(key, values)| {
                        let value = values
                            .get(&source.id)
                            .and_then(Option::as_ref)
                            .map(|entry| emit_value(&entry.value))
                            .unwrap_or(serde_json::Value::Null);
                        ((*key).clone(), value)
                    })
                    .collect()
            });
            WorklistEntry {
                source_id: source.id,
                path: source.path(),
                root_id: source.root_id,
                size: source.size,
                mtime: source.mtime,
                basis_rev: source.basis_rev,
                facts,
            }
        })
        .collect();

    Ok(WorklistResult {
        entries,
        skipped_unhashed,
        skipped_duplicate,
    })
}

/// A fact value as `--emit` writes it into a worklist entry. One key always emits one JSON
/// type: a time is its ISO 8601 text, at its own precision with the offset it was stored
/// with — a machine channel speaks what is stored, never the user's clock.
fn emit_value(value: &FactValue) -> serde_json::Value {
    match value {
        FactValue::Text(s) | FactValue::Path(s) => serde_json::Value::String(s.clone()),
        // Through the one spelling of a number, so a whole number is written `5`, never `5.0`.
        FactValue::Num(n) => serde_json::from_str::<serde_json::Number>(&spelled(*n))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        FactValue::Time(tv) => serde_json::Value::String(tv.iso_text()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::reading::read_time;
    use crate::core::repo::fact::{fact_value_params, FACT_VALUE_COLUMNS};
    use crate::core::repo::source::fetch_by_id;
    use crate::core::testing::{
        insert_fact, insert_object, insert_object_fact, insert_root, insert_source,
        insert_source_with_size, setup_test_db,
    };

    fn emitted(written: &str) -> serde_json::Value {
        emit_value(&FactValue::Time(read_time(written).unwrap()))
    }

    /// A number is written as Canon spells it: a whole number with no fraction (`5`, not
    /// `5.0`), any other as the shortest decimal that reads back the same. The defect it names:
    /// every number went through `f64`'s JSON form, so a size imported as `5` came back `5.0`.
    #[test]
    fn a_number_emits_as_canon_spells_it() {
        for (n, written) in [
            (5.0, "5"),
            (-3.0, "-3"),
            (0.0, "0"),
            (1e6, "1000000"),
            (2.5, "2.5"),
            (0.1, "0.1"),
            (52.37, "52.37"),
        ] {
            let emitted = emit_value(&FactValue::Num(n));
            assert_eq!(serde_json::to_string(&emitted).unwrap(), written, "{n}");
        }
        assert_eq!(
            emit_value(&FactValue::Num(f64::NAN)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn a_time_emits_its_iso_text_at_its_own_precision_with_its_stored_offset() {
        for written in [
            "2023-12-31T21:00:00-05:00",
            "2023-12-31T20:00:00-00:00",
            "2023-12-31T21:00:00",
            "2024",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05.45+02:00",
        ] {
            assert_eq!(emitted(written), serde_json::json!(written));
        }
        // A zero offset is written `+00:00`, never `Z`.
        assert_eq!(
            emitted("2023-12-31T23:30:00Z"),
            serde_json::json!("2023-12-31T23:30:00+00:00")
        );
    }

    /// Store a time fact on an object through the one conversion pair.
    fn insert_object_time(conn: &Connection, object_id: i64, key: &str, written: &str) {
        let value = FactValue::Time(read_time(written).unwrap());
        let params = fact_value_params(&value).unwrap();
        conn.execute(
            &format!(
                "INSERT INTO facts (entity_type, entity_id, key, {FACT_VALUE_COLUMNS}, observed_at)
                 VALUES ('object', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0)"
            ),
            rusqlite::params![
                object_id, key, params[0], params[1], params[2], params[3], params[4], params[5],
                params[6]
            ],
        )
        .unwrap();
    }

    #[test]
    fn a_time_key_always_emits_strings_and_mtime_stays_epoch_seconds() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let written = [
            "2024",
            "2023-12-31T21:00:00-05:00",
            "2024-06-15T14:30:05.45+02:00",
        ];
        let mut sources = Vec::new();
        for (i, w) in written.iter().enumerate() {
            let obj = insert_object(&conn, &format!("hash{i}"), false);
            let id = insert_source(&conn, root, &format!("{i}.jpg"), Some(obj));
            insert_object_time(&conn, obj, "content.When", w);
            sources.push(fetch_by_id(&conn, id).unwrap().unwrap());
        }

        let result =
            build_entries(&mut conn, &sources, false, &["content.When".to_string()]).unwrap();

        assert_eq!(result.entries.len(), written.len());
        for (entry, source) in result.entries.iter().zip(&sources) {
            let json = serde_json::to_value(entry).unwrap();
            assert!(json["facts"]["content.When"].is_string(), "{json}");
            assert_eq!(json["mtime"], serde_json::json!(source.mtime));
        }
    }

    /// `--emit` writes each source's one value: the content's fact over the source's own, an
    /// empty file's as well, and a file with only its own fact that one. A `source.` key
    /// reads through the same rule, and no Canon writer stores one, so it emits null.
    #[test]
    fn worklist_emits_one_value_per_source() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let content = insert_object(&conn, "datahash", false);
        let empty = insert_object(&conn, "emptyhash", false);
        let two_rows = insert_source_with_size(&conn, root, "a.jpg", Some(content), 100);
        let empty_file = insert_source_with_size(&conn, root, "e.jpg", Some(empty), 0);
        let own_only = insert_source(&conn, root, "o.jpg", None);
        for (object, source) in [(content, two_rows), (empty, empty_file)] {
            insert_object_fact(&conn, object, "content.Make", "Canon");
            insert_fact(&conn, source, "content.Make", "Nikon");
        }
        insert_fact(&conn, own_only, "content.Make", "Sony");
        let sources: Vec<Source> = [two_rows, empty_file, own_only]
            .iter()
            .map(|&id| fetch_by_id(&conn, id).unwrap().unwrap())
            .collect();

        let keys = ["content.Make".to_string(), "source.size".to_string()];
        let result = build_entries(&mut conn, &sources, false, &keys).unwrap();

        let emitted: Vec<(serde_json::Value, serde_json::Value)> = result
            .entries
            .iter()
            .map(|entry| {
                let facts = entry.facts.as_ref().unwrap();
                (facts["content.Make"].clone(), facts["source.size"].clone())
            })
            .collect();
        assert_eq!(
            emitted,
            vec![
                (serde_json::json!("Canon"), serde_json::Value::Null),
                (serde_json::json!("Canon"), serde_json::Value::Null),
                (serde_json::json!("Sony"), serde_json::Value::Null),
            ]
        );
    }
}
