//! Worklist operations — JSONL entry construction and unique-content filtering.
//!
//! `build_entries()` converts selected sources into serializable worklist entries,
//! optionally fetching facts and deduplicating by content identity — the object
//! a source resolves to, not the hash string, though an object owns exactly one
//! hash so the two answer the same question.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::Serialize;

use crate::core::domain::source::Source;
use crate::core::repo::Connection;
use crate::expr::get_fact_value;

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
/// When `emit_keys` is non-empty, fact values are fetched and attached to each entry.
pub fn build_entries(
    conn: &Connection,
    sources: &[Source],
    unique_content: bool,
    emit_keys: &[String],
) -> Result<WorklistResult> {
    let mut entries = Vec::new();
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

        let facts = if emit_keys.is_empty() {
            None
        } else {
            let mut map = HashMap::new();
            for key in emit_keys {
                let (entity_type, entity_id) = if key.starts_with("source.") {
                    ("source", Some(source.id))
                } else {
                    ("object", source.object_id)
                };
                let value = match entity_id {
                    Some(eid) => get_fact_value(conn, entity_type, eid, key)?
                        .map(|v| v.into())
                        .unwrap_or(serde_json::Value::Null),
                    None => serde_json::Value::Null,
                };
                map.insert(key.clone(), value);
            }
            Some(map)
        };

        entries.push(WorklistEntry {
            source_id: source.id,
            path: source.path(),
            root_id: source.root_id,
            size: source.size,
            mtime: source.mtime,
            basis_rev: source.basis_rev,
            facts,
        });
    }

    Ok(WorklistResult {
        entries,
        skipped_unhashed,
        skipped_duplicate,
    })
}
