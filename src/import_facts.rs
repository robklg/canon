use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::domain::fact::{is_content_fact, normalize_fact_key, FactValueType};
use crate::repo::{self, Connection, Db};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // Worklist pass-through fields are intentionally accepted but not read
struct FactImport {
    source_id: i64,
    basis_rev: i64,
    #[serde(default = "current_timestamp")]
    observed_at: i64,
    facts: HashMap<String, Value>,
    // Optional fields from worklist pass-through (allowed but ignored)
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    root_id: Option<i64>,
    #[serde(default)]
    size: Option<i64>,
    #[serde(default)]
    mtime: Option<i64>,
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
    skipped_type_mismatch: u64,
    objects_created: u64,
    facts_promoted: u64,
}

/// A fact value with optional type hint
enum TypedValue {
    Plain(Value),
    Hinted { value: Value, type_hint: String },
}

impl TypedValue {
    fn parse(v: &Value) -> Self {
        if let Value::Object(obj) = v {
            if let (Some(value), Some(Value::String(type_hint))) =
                (obj.get("value"), obj.get("type"))
            {
                return TypedValue::Hinted {
                    value: value.clone(),
                    type_hint: type_hint.clone(),
                };
            }
        }
        TypedValue::Plain(v.clone())
    }
}

/// Build a map of known fact keys to their established types
fn build_fact_type_map(conn: &Connection) -> Result<HashMap<String, FactValueType>> {
    repo::fact::fetch_type_map(conn)
}

pub fn run(db: &mut Db, allow_archived: bool, verbose: bool) -> Result<()> {
    let conn = db.conn_mut();
    let stdin = io::stdin();
    let mut stats = ImportStats::default();

    // Build type map once at start for efficient type checking
    // This map is updated during import to catch mixed types in the input stream
    let mut fact_type_map = build_fact_type_map(conn)?;

    // Track which keys had type mismatches (for summary)
    let mut type_mismatch_keys: HashMap<String, (FactValueType, FactValueType)> = HashMap::new();

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

        match process_import(conn, &import, &mut stats, &mut fact_type_map, &mut type_mismatch_keys, allow_archived, verbose) {
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "Warning: Failed to process source_id {}: {}",
                    import.source_id, e
                );
            }
        }
    }

    // Print type mismatch warnings with remediation hint
    if !type_mismatch_keys.is_empty() {
        eprintln!("\nType mismatch warnings:");
        let mut keys: Vec<_> = type_mismatch_keys.iter().collect();
        keys.sort_by_key(|(k, _)| *k);
        for (key, (existing, attempted)) in keys {
            eprintln!("  {}: existing type is {}, attempted to import {}", key, existing, attempted);
        }
        eprintln!("\nTo change the type, first delete existing facts:");
        eprintln!("  canon facts delete --key <key>");
        eprintln!("Then re-import with the new type.");
    }

    println!(
        "Processed {} lines: {} facts imported, {} skipped (stale), {} skipped (reserved), {} skipped (archived), {} skipped (type mismatch), {} objects created, {} facts promoted",
        stats.lines_processed,
        stats.facts_imported,
        stats.skipped_stale,
        stats.skipped_reserved,
        stats.skipped_archived,
        stats.skipped_type_mismatch,
        stats.objects_created,
        stats.facts_promoted
    );

    // Update query planner statistics after bulk changes
    db.run_analyze()?;

    Ok(())
}

fn process_import(
    conn: &mut Connection,
    import: &FactImport,
    stats: &mut ImportStats,
    fact_type_map: &mut HashMap<String, FactValueType>,
    type_mismatch_keys: &mut HashMap<String, (FactValueType, FactValueType)>,
    allow_archived: bool,
    verbose: bool,
) -> Result<()> {
    // Check if source exists and get its data
    let source = match repo::source::fetch_by_id(conn, import.source_id)? {
        Some(s) => s,
        None => {
            eprintln!("Warning: source_id {} not found", import.source_id);
            return Ok(());
        }
    };

    let current_basis_rev = source.basis_rev;
    let current_object_id = source.object_id;
    let root_path = &source.root_path;
    let rel_path = &source.rel_path;

    // Check if source is in an archive root
    if source.is_from_role("archive") && !allow_archived {
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
            let obj = repo::object::get_or_create(conn, "sha256", hash_str)?;
            // Check if this was a new object (not previously linked to this source)
            if current_object_id.is_none() || current_object_id != Some(obj.id) {
                stats.objects_created += 1;
            }
            object_id = Some(obj.id);

            // Link source to object AND promote existing facts in a single transaction
            // This ensures atomicity: either both succeed or neither does
            if current_object_id.is_none() {
                let tx = conn.transaction()?;
                repo::source::set_object_id(&tx, import.source_id, obj.id)?;
                let promoted = promote_content_facts(&tx, import.source_id, obj.id)?;
                tx.commit()?;
                stats.facts_promoted += promoted;
            } else if current_object_id != object_id {
                // Just re-linking to a different object (rare case)
                repo::source::set_object_id(conn, import.source_id, obj.id)?;
            }
        }
    }

    // Import facts - all imported facts are content facts (stored on object when available)
    if verbose && !normalized_facts.is_empty() {
        eprintln!("[{}] {}", root_path, rel_path);
    }
    for (key, value) in &normalized_facts {
        // Parse as typed value (plain or with hint)
        let typed = TypedValue::parse(value);

        // Check type consistency
        let new_type = match get_typed_value_type(&typed) {
            Some(t) => t,
            None => {
                // Type hint parsing failed - classify_typed_value will give details
                if let Err(e) = classify_typed_value(&typed) {
                    eprintln!("Warning: {} for '{}' in {}/{}", e, key, root_path, rel_path);
                }
                stats.skipped_type_mismatch += 1;
                continue;
            }
        };

        if let Some(&existing_type) = fact_type_map.get(key) {
            if existing_type != new_type {
                // Type mismatch - skip this fact
                let is_new_key = !type_mismatch_keys.contains_key(key);
                type_mismatch_keys.entry(key.clone()).or_insert((existing_type, new_type));
                if is_new_key {
                    // First occurrence of this mismatch - show the path and value
                    eprintln!(
                        "Warning: type mismatch for '{}' in {}/{}: existing {}, got {} (value: {})",
                        key, root_path, rel_path, existing_type, new_type, value
                    );
                }
                stats.skipped_type_mismatch += 1;
                continue;
            }
        } else {
            // New key - register its type for subsequent records in this import session
            fact_type_map.insert(key.clone(), new_type);
        }

        // Classify for storage
        let (value_text, value_num, value_time) = match classify_typed_value(&typed) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Warning: {}", e);
                stats.skipped_type_mismatch += 1;
                continue;
            }
        };

        if object_id.is_some() {
            if verbose {
                eprintln!("  {}: {} (on object)", key, value);
            }
            // Store as object fact
            repo::fact::upsert(
                conn,
                "object",
                object_id.unwrap(),
                key,
                value_text.as_deref(),
                value_num,
                value_time,
                import.observed_at,
                None, // object facts don't have observed_basis_rev
            )?;
            stats.facts_imported += 1;
            stats.facts_promoted += 1;
        } else {
            if verbose {
                eprintln!("  {}: {} (on source)", key, value);
            }
            // Store as source fact for now (will be promoted later when hash is known)
            repo::fact::upsert(
                conn,
                "source",
                import.source_id,
                key,
                value_text.as_deref(),
                value_num,
                value_time,
                import.observed_at,
                Some(import.basis_rev),
            )?;
            stats.facts_imported += 1;
        }
    }

    Ok(())
}

/// Try to parse a string as a datetime, returning the Unix timestamp if successful
fn try_parse_datetime(s: &str) -> Option<i64> {
    // RFC3339 format (2020-01-15T10:30:00+00:00)
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    // ISO format without timezone (2020-01-15T10:30:00)
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp());
    }

    // EXIF format uses colons in date: "2020:07:23 11:06:32"
    // Normalize to ISO format: "2020-07-23T11:06:32" then parse
    if s.len() >= 19 && s.chars().nth(4) == Some(':') && s.chars().nth(7) == Some(':') {
        // Convert "2020:07:23 11:06:32..." to "2020-07-23T11:06:32..."
        let iso: String = s
            .chars()
            .enumerate()
            .map(|(i, c)| match i {
                4 | 7 => '-',            // date colons → dashes
                10 if c == ' ' => 'T',   // space → T
                _ => c,
            })
            .collect();

        // Try with timezone (handles subseconds too: 2020-07-23T11:06:32.023+02:00)
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&iso) {
            return Some(dt.timestamp());
        }
        // Try with Z suffix
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&format!("{}Z", &iso)) {
            return Some(dt.timestamp());
        }
        // Try without timezone (assume UTC)
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&iso[..19], "%Y-%m-%dT%H:%M:%S") {
            return Some(dt.and_utc().timestamp());
        }
    }

    // Plain 4-digit year string (e.g., "2005") - Jan 1 midnight UTC
    if s.len() == 4 {
        if let Ok(year) = s.parse::<i32>() {
            return year_to_timestamp(year);
        }
    }

    None
}

/// Convert a year to a Unix timestamp (Jan 1 midnight UTC)
fn year_to_timestamp(year: i32) -> Option<i64> {
    use chrono::TimeZone;
    // Sanity check for reasonable year range
    if (1900..=2100).contains(&year) {
        chrono::Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .single()
            .map(|dt| dt.timestamp())
    } else {
        None
    }
}

/// Parse duration string to seconds. Supports:
/// - "H:MM:SS" or "HH:MM:SS" (hours:minutes:seconds)
/// - "M:SS" or "MM:SS" (minutes:seconds)
/// - "SS" or "SS.ms" (seconds with optional decimals)
/// - Numeric values (already in seconds)
fn try_parse_duration(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => {
            let s = s.trim();
            let parts: Vec<&str> = s.split(':').collect();
            match parts.len() {
                3 => {
                    // H:MM:SS
                    let hours: f64 = parts[0].parse().ok()?;
                    let mins: f64 = parts[1].parse().ok()?;
                    let secs: f64 = parts[2].parse().ok()?;
                    Some(hours * 3600.0 + mins * 60.0 + secs)
                }
                2 => {
                    // M:SS
                    let mins: f64 = parts[0].parse().ok()?;
                    let secs: f64 = parts[1].parse().ok()?;
                    Some(mins * 60.0 + secs)
                }
                1 => {
                    // Plain seconds (possibly with decimals)
                    s.parse().ok()
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Classify a plain value (no type hint) - NO date guessing
fn classify_value(value: &Value) -> (Option<String>, Option<f64>, Option<i64>) {
    match value {
        Value::String(s) => (Some(s.clone()), None, None), // Just store as text
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                (None, Some(i as f64), None)
            } else if let Some(f) = n.as_f64() {
                (None, Some(f), None)
            } else {
                (Some(n.to_string()), None, None)
            }
        }
        Value::Bool(b) => (None, Some(if *b { 1.0 } else { 0.0 }), None),
        Value::Null => (Some(String::new()), None, None),
        // Store arrays/objects as their JSON string representation
        Value::Array(_) | Value::Object(_) => (Some(value.to_string()), None, None),
    }
}

/// Classify a value with optional type hint
fn classify_typed_value(typed: &TypedValue) -> Result<(Option<String>, Option<f64>, Option<i64>), String> {
    match typed {
        TypedValue::Plain(v) => Ok(classify_value(v)),
        TypedValue::Hinted { value, type_hint } => match type_hint.as_str() {
            "duration" => {
                if let Some(secs) = try_parse_duration(value) {
                    Ok((None, Some(secs), None))
                } else {
                    Err(format!("cannot parse as duration: {}", value))
                }
            }
            "datetime" => {
                if let Value::String(s) = value {
                    if let Some(ts) = try_parse_datetime(s) {
                        Ok((None, None, Some(ts)))
                    } else {
                        Err(format!("cannot parse as datetime: {}", value))
                    }
                } else if let Value::Number(n) = value {
                    // Handle numeric year (e.g., 2005 from audio metadata)
                    if let Some(year) = n.as_i64() {
                        if let Some(ts) = year_to_timestamp(year as i32) {
                            Ok((None, None, Some(ts)))
                        } else {
                            Err(format!("year out of range: {}", year))
                        }
                    } else {
                        Err(format!("datetime requires integer year, got: {}", value))
                    }
                } else {
                    Err(format!("datetime requires string or year, got: {}", value))
                }
            }
            unknown => Err(format!("unknown type hint: {}", unknown)),
        },
    }
}

/// Determine storage type for plain value - NO date guessing
fn get_value_type(value: &Value) -> FactValueType {
    match value {
        Value::String(_) => FactValueType::Text, // Always text
        Value::Number(_) | Value::Bool(_) => FactValueType::Num,
        Value::Null | Value::Array(_) | Value::Object(_) => FactValueType::Text,
    }
}

/// Determine storage type with type hint
fn get_typed_value_type(typed: &TypedValue) -> Option<FactValueType> {
    match typed {
        TypedValue::Plain(v) => Some(get_value_type(v)),
        TypedValue::Hinted { value, type_hint } => match type_hint.as_str() {
            "duration" => {
                if try_parse_duration(value).is_some() {
                    Some(FactValueType::Num)
                } else {
                    None
                }
            }
            "datetime" => {
                if let Value::String(s) = value {
                    if try_parse_datetime(s).is_some() {
                        Some(FactValueType::Time)
                    } else {
                        None
                    }
                } else if let Value::Number(n) = value {
                    // Numeric year (e.g., 2005)
                    if let Some(year) = n.as_i64() {
                        if year_to_timestamp(year as i32).is_some() {
                            Some(FactValueType::Time)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None, // Unknown hint
        },
    }
}

/// Promote content facts from a source to its newly-linked object.
///
/// This function is called within a transaction that also links the source to the object,
/// ensuring atomicity. It:
/// 1. Fetches all facts on the source
/// 2. For each content fact (content.* namespace):
///    - If the object doesn't already have this fact, copies it to the object
///    - Deletes the source fact
fn promote_content_facts(conn: &Connection, source_id: i64, object_id: i64) -> Result<u64> {
    // Fetch all facts on this source
    let facts = repo::fact::fetch_source_facts(conn, source_id)?;

    let mut promoted = 0u64;
    for fact in facts {
        if is_content_fact(&fact.key) {
            // Check if object already has this fact
            if !repo::fact::object_has_fact(conn, object_id, &fact.key)? {
                // Copy to object
                repo::fact::insert_object_fact(
                    conn,
                    object_id,
                    &fact.key,
                    fact.value_text.as_deref(),
                    fact.value_num,
                    fact.value_time,
                    fact.observed_at,
                )?;
                promoted += 1;
            }

            // Delete from source
            repo::fact::delete_by_id(conn, fact.id)?;
        }
    }

    Ok(promoted)
}
