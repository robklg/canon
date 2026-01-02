use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db;

#[derive(Deserialize)]
struct FactImport {
    source_id: i64,
    basis_rev: i64,
    #[serde(default = "current_timestamp")]
    observed_at: i64,
    facts: HashMap<String, Value>,
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

#[derive(Default)]
struct ImportStats {
    lines_processed: u64,
    facts_imported: u64,
    skipped_stale: u64,
    skipped_reserved: u64,
    skipped_archived: u64,
    objects_created: u64,
    facts_promoted: u64,
}

/// Normalize a fact key to use the content.* namespace.
/// - Keys starting with "source." are rejected (reserved namespace)
/// - Keys already starting with "content." are left as-is
/// - All other keys are prefixed with "content."
fn normalize_fact_key(key: &str) -> Result<String, &'static str> {
    if key.starts_with("source.") {
        return Err("source.* namespace is reserved for built-in facts");
    }
    if key.starts_with("content.") {
        return Ok(key.to_string());
    }
    Ok(format!("content.{}", key))
}

pub fn run(db_path: &Path, allow_archived: bool) -> Result<()> {
    let conn = db::open(db_path)?;
    let stdin = io::stdin();
    let mut stats = ImportStats::default();

    for line in stdin.lock().lines() {
        let line = line.context("Failed to read line from stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        stats.lines_processed += 1;

        let import: FactImport = match serde_json::from_str(&line) {
            Ok(i) => i,
            Err(e) => {
                eprintln!("Warning: Failed to parse line {}: {}", stats.lines_processed, e);
                continue;
            }
        };

        match process_import(&conn, &import, &mut stats, allow_archived) {
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "Warning: Failed to process source_id {}: {}",
                    import.source_id, e
                );
            }
        }
    }

    println!(
        "Processed {} lines: {} facts imported, {} skipped (stale), {} skipped (reserved), {} skipped (archived), {} objects created, {} facts promoted",
        stats.lines_processed,
        stats.facts_imported,
        stats.skipped_stale,
        stats.skipped_reserved,
        stats.skipped_archived,
        stats.objects_created,
        stats.facts_promoted
    );

    Ok(())
}

fn process_import(conn: &Connection, import: &FactImport, stats: &mut ImportStats, allow_archived: bool) -> Result<()> {
    // Check if source exists and get its basis_rev and role
    let current: Option<(i64, Option<i64>, String)> = conn
        .query_row(
            "SELECT s.basis_rev, s.object_id, r.role
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [import.source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    let (current_basis_rev, current_object_id, role) = match current {
        Some(c) => c,
        None => {
            eprintln!("Warning: source_id {} not found", import.source_id);
            return Ok(());
        }
    };

    // Check if source is in an archive root
    if role == "archive" && !allow_archived {
        stats.skipped_archived += 1;
        return Ok(());
    }

    if current_basis_rev != import.basis_rev {
        eprintln!(
            "Warning: source_id {} has basis_rev {} but import has {}, skipping",
            import.source_id, current_basis_rev, import.basis_rev
        );
        stats.skipped_stale += 1;
        return Ok(());
    }

    // Normalize all fact keys first, collecting valid ones
    let mut normalized_facts: Vec<(String, &Value)> = Vec::new();
    for (key, value) in &import.facts {
        match normalize_fact_key(key) {
            Ok(normalized_key) => normalized_facts.push((normalized_key, value)),
            Err(msg) => {
                eprintln!("Warning: skipping fact '{}': {}", key, msg);
                stats.skipped_reserved += 1;
            }
        }
    }

    // Check for content hash and process it first
    // Support both old format (hash.sha256) and new format (content.hash.sha256)
    let mut object_id = current_object_id;
    let hash_value = normalized_facts
        .iter()
        .find(|(k, _)| k == "content.hash.sha256")
        .map(|(_, v)| *v);

    if let Some(hash_val) = hash_value {
        if let Some(hash_str) = hash_val.as_str() {
            object_id = Some(get_or_create_object(conn, "sha256", hash_str, stats)?);

            // Link source to object if not already linked
            if current_object_id != object_id {
                conn.execute(
                    "UPDATE sources SET object_id = ? WHERE id = ?",
                    params![object_id, import.source_id],
                )?;
            }
        }
    }

    // Import facts - all imported facts are content facts (stored on object when available)
    for (key, value) in &normalized_facts {
        if object_id.is_some() {
            // Store as object fact
            insert_fact(
                conn,
                "object",
                object_id.unwrap(),
                key,
                value,
                import.observed_at,
                None, // object facts don't have observed_basis_rev
            )?;
            stats.facts_imported += 1;
            stats.facts_promoted += 1;
        } else {
            // Store as source fact for now (will be promoted later when hash is known)
            insert_fact(
                conn,
                "source",
                import.source_id,
                key,
                value,
                import.observed_at,
                Some(import.basis_rev),
            )?;
            stats.facts_imported += 1;
        }
    }

    // If we just linked an object, promote any existing content facts from source to object
    if object_id.is_some() && current_object_id.is_none() {
        let promoted = promote_content_facts(conn, import.source_id, object_id.unwrap())?;
        stats.facts_promoted += promoted;
    }

    Ok(())
}

fn get_or_create_object(
    conn: &Connection,
    hash_type: &str,
    hash_value: &str,
    stats: &mut ImportStats,
) -> Result<i64> {
    // Try to find existing object
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM objects WHERE hash_type = ? AND hash_value = ?",
            params![hash_type, hash_value],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(id) = existing {
        return Ok(id);
    }

    // Create new object
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES (?, ?)",
        params![hash_type, hash_value],
    )?;
    stats.objects_created += 1;
    Ok(conn.last_insert_rowid())
}

fn is_content_fact(key: &str) -> bool {
    // Content facts use the content.* namespace
    // All imported facts are content facts (auto-namespaced on import)
    key.starts_with("content.")
}

fn insert_fact(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
    value: &Value,
    observed_at: i64,
    observed_basis_rev: Option<i64>,
) -> Result<()> {
    let (value_text, value_num, value_time, value_json) = classify_value(value);

    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, value_num, value_time, value_json, observed_at, observed_basis_rev)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            entity_type,
            entity_id,
            key,
            value_text,
            value_num,
            value_time,
            value_json,
            observed_at,
            observed_basis_rev,
        ],
    )?;

    Ok(())
}

fn classify_value(value: &Value) -> (Option<String>, Option<f64>, Option<i64>, Option<String>) {
    match value {
        Value::String(s) => {
            // Try to parse as timestamp (ISO 8601 format)
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return (None, None, Some(dt.timestamp()), None);
            }
            // Try simpler datetime formats
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return (None, None, Some(dt.and_utc().timestamp()), None);
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y:%m:%d %H:%M:%S") {
                // EXIF format
                return (None, None, Some(dt.and_utc().timestamp()), None);
            }
            (Some(s.clone()), None, None, None)
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                (None, Some(i as f64), None, None)
            } else if let Some(f) = n.as_f64() {
                (None, Some(f), None, None)
            } else {
                (Some(n.to_string()), None, None, None)
            }
        }
        Value::Bool(b) => (None, Some(if *b { 1.0 } else { 0.0 }), None, None),
        Value::Null => (Some(String::new()), None, None, None),
        Value::Array(_) | Value::Object(_) => (None, None, None, Some(value.to_string())),
    }
}

fn promote_content_facts(conn: &Connection, source_id: i64, object_id: i64) -> Result<u64> {
    // Find content facts on this source that should be promoted
    let mut stmt = conn.prepare(
        "SELECT id, key, value_text, value_num, value_time, value_json, observed_at
         FROM facts
         WHERE entity_type = 'source' AND entity_id = ?"
    )?;

    let facts: Vec<(i64, String, Option<String>, Option<f64>, Option<i64>, Option<String>, i64)> = stmt
        .query_map([source_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut promoted = 0u64;
    for (fact_id, key, value_text, value_num, value_time, value_json, observed_at) in facts {
        if is_content_fact(&key) {
            // Check if object already has this fact
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
                    params![object_id, key],
                    |_| Ok(true),
                )
                .optional()?
                .unwrap_or(false);

            if !exists {
                // Copy to object
                conn.execute(
                    "INSERT INTO facts (entity_type, entity_id, key, value_text, value_num, value_time, value_json, observed_at, observed_basis_rev)
                     VALUES ('object', ?, ?, ?, ?, ?, ?, ?, NULL)",
                    params![object_id, key, value_text, value_num, value_time, value_json, observed_at],
                )?;
                promoted += 1;
            }

            // Delete from source
            conn.execute("DELETE FROM facts WHERE id = ?", [fact_id])?;
        }
    }

    Ok(promoted)
}
