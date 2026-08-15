//! Import facts operations — per-record processing for JSONL fact import.
//!
//! `init_state()` loads the existing fact type map from the database.
//! `process_record()` handles all business logic for a single import record:
//! source validation, key normalization, hash linking, fact promotion, type
//! checking, value classification, and fact upsert.
//!
//! No stdout/stderr/stdin — the interface reads stdin and displays outcomes.

use std::collections::{BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;

use crate::facts::domain::{is_content_fact, normalize_fact_key, FactValueType};
use crate::facts::repo as facts_repo;
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

/// Deserialized JSONL record from stdin.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // Worklist pass-through fields are intentionally accepted but not read
pub struct ImportRecord {
    pub source_id: i64,
    pub basis_rev: i64,
    #[serde(default = "current_timestamp")]
    pub observed_at: i64,
    pub facts: HashMap<String, Value>,
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

/// Mutable state accumulated across records during an import session.
pub struct ImportState {
    pub fact_type_map: HashMap<String, FactValueType>,
    pub type_mismatch_keys: HashMap<String, (FactValueType, FactValueType)>,
    pub stats: ImportStats,
    /// Roots whose sources this import acted on — the decision's durable
    /// scope (one whole-root entry per root, recorded at completion).
    pub touched_roots: BTreeSet<i64>,
}

/// Counters for the import summary.
#[derive(Default)]
pub struct ImportStats {
    pub lines_processed: u64,
    pub facts_imported: u64,
    pub skipped_stale: u64,
    pub skipped_reserved: u64,
    pub skipped_archived: u64,
    pub skipped_type_mismatch: u64,
    pub objects_created: u64,
    pub facts_promoted: u64,
}

impl ImportStats {
    /// Compose the import summary message.
    pub fn compose_summary(&self) -> String {
        use crate::domain::format_count;
        format!(
            "Processed {} lines: {} facts imported, {} skipped (stale), {} skipped (reserved), {} skipped (archived), {} skipped (type mismatch), {} objects created, {} facts promoted",
            format_count(self.lines_processed),
            format_count(self.facts_imported),
            format_count(self.skipped_stale),
            format_count(self.skipped_reserved),
            format_count(self.skipped_archived),
            format_count(self.skipped_type_mismatch),
            format_count(self.objects_created),
            format_count(self.facts_promoted),
        )
    }
}

/// Outcome of processing a single import record.
pub struct RecordOutcome {
    /// Warning messages for display.
    pub warnings: Vec<String>,
    /// Verbose progress lines.
    pub verbose_lines: Vec<String>,
}

// ============================================================================
// Public functions
// ============================================================================

/// Initialize import state by loading the existing fact type map from the database.
pub fn init_state(conn: &Connection) -> Result<ImportState> {
    let fact_type_map = facts_repo::fetch_type_map(conn)?;
    Ok(ImportState {
        fact_type_map,
        type_mismatch_keys: HashMap::new(),
        stats: ImportStats::default(),
        touched_roots: BTreeSet::new(),
    })
}

/// Process a single import record. Returns structured outcome — no stdout/stderr.
pub fn process_record(
    conn: &mut Connection,
    record: &ImportRecord,
    state: &mut ImportState,
    allow_archived: bool,
) -> Result<RecordOutcome> {
    let mut outcome = RecordOutcome {
        warnings: Vec::new(),
        verbose_lines: Vec::new(),
    };

    // Check if source exists and get its data
    let source = match repo::source::fetch_by_id(conn, record.source_id)? {
        Some(s) => s,
        None => {
            outcome
                .warnings
                .push(format!("source_id {} not found", record.source_id));
            return Ok(outcome);
        }
    };

    let current_basis_rev = source.basis_rev;
    let current_object_id = source.object_id;
    let root_path = &source.root_path;
    let rel_path = &source.rel_path;

    // Check if source is in an archive root
    if source.is_from_role("archive") && !allow_archived {
        state.stats.skipped_archived += 1;
        return Ok(outcome);
    }

    if current_basis_rev != record.basis_rev {
        outcome.warnings.push(format!(
            "source_id {} has basis_rev {} but import has {}, skipping",
            record.source_id, current_basis_rev, record.basis_rev
        ));
        state.stats.skipped_stale += 1;
        return Ok(outcome);
    }

    // Past the gates — this record will act on the source's root.
    state.touched_roots.insert(source.root_id);

    // Normalize all fact keys first, collecting valid ones
    let mut normalized_facts: Vec<(String, &Value)> = Vec::new();
    for (key, value) in &record.facts {
        match normalize_fact_key(key) {
            Ok(normalized_key) => normalized_facts.push((normalized_key, value)),
            Err(msg) => {
                outcome
                    .warnings
                    .push(format!("skipping fact '{key}': {msg}"));
                state.stats.skipped_reserved += 1;
            }
        }
    }

    // Check for content hash and process it first
    let mut object_id = current_object_id;
    let hash_value = normalized_facts
        .iter()
        .find(|(k, _)| k == "content.hash.sha256")
        .map(|(_, v)| *v);

    if let Some(hash_val) = hash_value {
        if let Some(hash_str) = hash_val.as_str() {
            let obj = repo::object::get_or_create(conn, "sha256", hash_str)?;
            if current_object_id.is_none() || current_object_id != Some(obj.id) {
                state.stats.objects_created += 1;
            }
            object_id = Some(obj.id);

            // Link source to object AND promote existing facts in a single transaction
            if current_object_id.is_none() {
                let tx = conn.transaction()?;
                repo::source::set_object_id(&tx, record.source_id, obj.id)?;
                let promoted = promote_content_facts(&tx, record.source_id, obj.id)?;
                tx.commit()?;
                state.stats.facts_promoted += promoted;
            } else if current_object_id != object_id {
                repo::source::set_object_id(conn, record.source_id, obj.id)?;
            }
        }
    }

    // Import facts
    if !normalized_facts.is_empty() {
        outcome
            .verbose_lines
            .push(format!("[{root_path}] {rel_path}"));
    }
    for (key, value) in &normalized_facts {
        let typed = TypedValue::parse(value);

        // Check type consistency
        let new_type = match get_typed_value_type(&typed) {
            Some(t) => t,
            None => {
                if let Err(e) = classify_typed_value(&typed) {
                    outcome
                        .warnings
                        .push(format!("{e} for '{key}' in {root_path}/{rel_path}"));
                }
                state.stats.skipped_type_mismatch += 1;
                continue;
            }
        };

        if let Some(&existing_type) = state.fact_type_map.get(key) {
            if existing_type != new_type {
                let is_new_key = !state.type_mismatch_keys.contains_key(key);
                state
                    .type_mismatch_keys
                    .entry(key.clone())
                    .or_insert((existing_type, new_type));
                if is_new_key {
                    outcome.warnings.push(format!(
                        "type mismatch for '{key}' in {root_path}/{rel_path}: existing {existing_type}, got {new_type} (value: {value})"
                    ));
                }
                state.stats.skipped_type_mismatch += 1;
                continue;
            }
        } else {
            state.fact_type_map.insert(key.clone(), new_type);
        }

        // Classify for storage
        let (value_text, value_num, value_time) = match classify_typed_value(&typed) {
            Ok(v) => v,
            Err(e) => {
                outcome.warnings.push(e);
                state.stats.skipped_type_mismatch += 1;
                continue;
            }
        };

        if let Some(oid) = object_id {
            outcome
                .verbose_lines
                .push(format!("  {key}: {value} (on object)"));
            facts_repo::upsert(
                conn,
                "object",
                oid,
                key,
                value_text.as_deref(),
                value_num,
                value_time,
                record.observed_at,
                None,
            )?;
            state.stats.facts_imported += 1;
            state.stats.facts_promoted += 1;
        } else {
            outcome
                .verbose_lines
                .push(format!("  {key}: {value} (on source)"));
            facts_repo::upsert(
                conn,
                "source",
                record.source_id,
                key,
                value_text.as_deref(),
                value_num,
                value_time,
                record.observed_at,
                Some(record.basis_rev),
            )?;
            state.stats.facts_imported += 1;
        }
    }

    Ok(outcome)
}

// ============================================================================
// Private helpers
// ============================================================================

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

/// A fact value with optional type hint.
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

/// Try to parse a string as a datetime, returning the Unix timestamp if successful.
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
    if s.len() >= 19 && s.chars().nth(4) == Some(':') && s.chars().nth(7) == Some(':') {
        let iso: String = s
            .chars()
            .enumerate()
            .map(|(i, c)| match i {
                4 | 7 => '-',
                10 if c == ' ' => 'T',
                _ => c,
            })
            .collect();

        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&iso) {
            return Some(dt.timestamp());
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&format!("{}Z", &iso)) {
            return Some(dt.timestamp());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&iso[..19], "%Y-%m-%dT%H:%M:%S") {
            return Some(dt.and_utc().timestamp());
        }
    }

    // Plain 4-digit year string (e.g., "2005")
    if s.len() == 4 {
        if let Ok(year) = s.parse::<i32>() {
            return year_to_timestamp(year);
        }
    }

    None
}

/// Convert a year to a Unix timestamp (Jan 1 midnight UTC).
fn year_to_timestamp(year: i32) -> Option<i64> {
    use chrono::TimeZone;
    if (1900..=2100).contains(&year) {
        chrono::Utc
            .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .single()
            .map(|dt| dt.timestamp())
    } else {
        None
    }
}

/// Parse duration string to seconds.
fn try_parse_duration(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => {
            let s = s.trim();
            let parts: Vec<&str> = s.split(':').collect();
            match parts.len() {
                3 => {
                    let hours: f64 = parts[0].parse().ok()?;
                    let mins: f64 = parts[1].parse().ok()?;
                    let secs: f64 = parts[2].parse().ok()?;
                    Some(hours * 3600.0 + mins * 60.0 + secs)
                }
                2 => {
                    let mins: f64 = parts[0].parse().ok()?;
                    let secs: f64 = parts[1].parse().ok()?;
                    Some(mins * 60.0 + secs)
                }
                1 => s.parse().ok(),
                _ => None,
            }
        }
        _ => None,
    }
}

/// A classified fact value: (value_text, value_num, value_time).
type ClassifiedValue = (Option<String>, Option<f64>, Option<i64>);

/// Classify a plain value (no type hint).
fn classify_value(value: &Value) -> ClassifiedValue {
    match value {
        Value::String(s) => (Some(s.clone()), None, None),
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
        Value::Array(_) | Value::Object(_) => (Some(value.to_string()), None, None),
    }
}

/// Classify a value with optional type hint.
fn classify_typed_value(typed: &TypedValue) -> Result<ClassifiedValue, String> {
    match typed {
        TypedValue::Plain(v) => Ok(classify_value(v)),
        TypedValue::Hinted { value, type_hint } => match type_hint.as_str() {
            "duration" => {
                if let Some(secs) = try_parse_duration(value) {
                    Ok((None, Some(secs), None))
                } else {
                    Err(format!("cannot parse as duration: {value}"))
                }
            }
            "datetime" => {
                if let Value::String(s) = value {
                    if let Some(ts) = try_parse_datetime(s) {
                        Ok((None, None, Some(ts)))
                    } else {
                        Err(format!("cannot parse as datetime: {value}"))
                    }
                } else if let Value::Number(n) = value {
                    if let Some(year) = n.as_i64() {
                        if let Some(ts) = year_to_timestamp(year as i32) {
                            Ok((None, None, Some(ts)))
                        } else {
                            Err(format!("year out of range: {year}"))
                        }
                    } else {
                        Err(format!("datetime requires integer year, got: {value}"))
                    }
                } else {
                    Err(format!("datetime requires string or year, got: {value}"))
                }
            }
            unknown => Err(format!("unknown type hint: {unknown}")),
        },
    }
}

/// Determine storage type for plain value.
fn get_value_type(value: &Value) -> FactValueType {
    match value {
        Value::String(_) => FactValueType::Text,
        Value::Number(_) | Value::Bool(_) => FactValueType::Num,
        Value::Null | Value::Array(_) | Value::Object(_) => FactValueType::Text,
    }
}

/// Determine storage type with type hint.
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
            _ => None,
        },
    }
}

/// Promote content facts from a source to its newly-linked object.
///
/// The source copy is deleted whichever way the branch goes: content facts
/// describe the content, so once the object holds the key the source's row is
/// redundant, and the object's existing value wins over an arriving duplicate.
/// The counter follows the insert, not the delete — it counts facts the object
/// gained, not source rows removed.
fn promote_content_facts(conn: &Connection, source_id: i64, object_id: i64) -> Result<u64> {
    let facts = facts_repo::fetch_source_facts(conn, source_id)?;

    let mut promoted = 0u64;
    for fact in facts {
        if is_content_fact(&fact.key) {
            if !facts_repo::object_has_fact(conn, object_id, &fact.key)? {
                facts_repo::insert_object_fact(
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
            facts_repo::delete_by_id(conn, fact.id)?;
        }
    }

    Ok(promoted)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_object, insert_root, insert_source, setup_test_db};

    // =========================================================================
    // try_parse_datetime
    // =========================================================================

    #[test]
    fn test_try_parse_datetime_rfc3339() {
        let ts = try_parse_datetime("2020-01-15T10:30:00+00:00").unwrap();
        assert_eq!(ts, 1579084200);
    }

    #[test]
    fn test_try_parse_datetime_iso() {
        let ts = try_parse_datetime("2020-01-15T10:30:00").unwrap();
        assert_eq!(ts, 1579084200);
    }

    #[test]
    fn test_try_parse_datetime_exif() {
        let ts = try_parse_datetime("2020:07:23 11:06:32").unwrap();
        // 2020-07-23T11:06:32 UTC
        assert_eq!(ts, 1595502392);
    }

    #[test]
    fn test_try_parse_datetime_plain_year() {
        let ts = try_parse_datetime("2005").unwrap();
        // Jan 1 2005 00:00:00 UTC
        assert_eq!(ts, 1104537600);
    }

    #[test]
    fn test_try_parse_datetime_invalid() {
        assert!(try_parse_datetime("not a date").is_none());
        assert!(try_parse_datetime("").is_none());
        assert!(try_parse_datetime("99999").is_none());
    }

    // =========================================================================
    // try_parse_duration
    // =========================================================================

    #[test]
    fn test_try_parse_duration_hms() {
        let secs = try_parse_duration(&Value::String("1:30:45".to_string())).unwrap();
        assert!((secs - 5445.0).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_ms() {
        let secs = try_parse_duration(&Value::String("3:30".to_string())).unwrap();
        assert!((secs - 210.0).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_seconds() {
        let secs = try_parse_duration(&Value::String("45.5".to_string())).unwrap();
        assert!((secs - 45.5).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_numeric_json() {
        let secs = try_parse_duration(&serde_json::json!(120)).unwrap();
        assert!((secs - 120.0).abs() < 0.01);
    }

    #[test]
    fn test_try_parse_duration_invalid() {
        assert!(try_parse_duration(&Value::String("not:a:duration:at:all".to_string())).is_none());
        assert!(try_parse_duration(&Value::Null).is_none());
    }

    // =========================================================================
    // classify_value
    // =========================================================================

    #[test]
    fn test_classify_value_string() {
        let (text, num, time) = classify_value(&serde_json::json!("hello"));
        assert_eq!(text.as_deref(), Some("hello"));
        assert!(num.is_none());
        assert!(time.is_none());
    }

    #[test]
    fn test_classify_value_number() {
        let (text, num, time) = classify_value(&serde_json::json!(42));
        assert!(text.is_none());
        assert_eq!(num, Some(42.0));
        assert!(time.is_none());
    }

    #[test]
    fn test_classify_value_bool() {
        let (text, num, _) = classify_value(&serde_json::json!(true));
        assert!(text.is_none());
        assert_eq!(num, Some(1.0));
    }

    // =========================================================================
    // classify_typed_value
    // =========================================================================

    #[test]
    fn test_classify_typed_datetime() {
        let typed = TypedValue::Hinted {
            value: Value::String("2020-01-15T10:30:00+00:00".to_string()),
            type_hint: "datetime".to_string(),
        };
        let (text, num, time) = classify_typed_value(&typed).unwrap();
        assert!(text.is_none());
        assert!(num.is_none());
        assert_eq!(time, Some(1579084200));
    }

    #[test]
    fn test_classify_typed_duration() {
        let typed = TypedValue::Hinted {
            value: Value::String("3:30".to_string()),
            type_hint: "duration".to_string(),
        };
        let (text, num, time) = classify_typed_value(&typed).unwrap();
        assert!(text.is_none());
        assert!((num.unwrap() - 210.0).abs() < 0.01);
        assert!(time.is_none());
    }

    #[test]
    fn test_classify_typed_unknown_hint() {
        let typed = TypedValue::Hinted {
            value: Value::String("hello".to_string()),
            type_hint: "bogus".to_string(),
        };
        let result = classify_typed_value(&typed);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown type hint"));
    }

    // =========================================================================
    // get_typed_value_type
    // =========================================================================

    #[test]
    fn test_get_typed_value_type_plain() {
        assert_eq!(
            get_typed_value_type(&TypedValue::Plain(serde_json::json!("hello"))),
            Some(FactValueType::Text)
        );
        assert_eq!(
            get_typed_value_type(&TypedValue::Plain(serde_json::json!(42))),
            Some(FactValueType::Num)
        );
    }

    #[test]
    fn test_get_typed_value_type_hinted() {
        let dt = TypedValue::Hinted {
            value: Value::String("2020-01-15T10:30:00+00:00".to_string()),
            type_hint: "datetime".to_string(),
        };
        assert_eq!(get_typed_value_type(&dt), Some(FactValueType::Time));

        let dur = TypedValue::Hinted {
            value: Value::String("3:30".to_string()),
            type_hint: "duration".to_string(),
        };
        assert_eq!(get_typed_value_type(&dur), Some(FactValueType::Num));
    }

    // =========================================================================
    // process_record (integration)
    // =========================================================================

    fn make_record(source_id: i64, basis_rev: i64, facts: HashMap<String, Value>) -> ImportRecord {
        ImportRecord {
            source_id,
            basis_rev,
            observed_at: 1700000000,
            facts,
            path: None,
            root_id: None,
            size: None,
            mtime: None,
        }
    }

    #[test]
    fn test_process_record_source_not_found() {
        let mut conn = setup_test_db();
        let mut state = init_state(&conn).unwrap();
        let record = make_record(999, 0, HashMap::new());

        let outcome = process_record(&mut conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings[0].contains("not found"));
    }

    #[test]
    fn test_process_record_stale_skipped() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        // Source has basis_rev 0 (default), import has 99
        let record = make_record(1, 99, HashMap::new());
        let outcome = process_record(&mut conn, &record, &mut state, false).unwrap();

        assert!(outcome.warnings[0].contains("basis_rev"));
        assert_eq!(state.stats.skipped_stale, 1);
        // A gated record touches no root — no scope entry for it.
        assert!(state.touched_roots.is_empty());
    }

    #[test]
    fn test_touched_roots_collects_acted_on_roots() {
        let mut conn = setup_test_db();
        let root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "source", false);
        let s1 = insert_source(&conn, root_a, "1.jpg", None);
        let s2 = insert_source(&conn, root_b, "2.jpg", None);
        let mut state = init_state(&conn).unwrap();

        for id in [s1, s2] {
            let record = make_record(
                id,
                0,
                HashMap::from([("Make".to_string(), Value::String("Canon".to_string()))]),
            );
            process_record(&mut conn, &record, &mut state, false).unwrap();
        }
        // Missing source: gated, must not touch.
        process_record(
            &mut conn,
            &make_record(999, 0, HashMap::new()),
            &mut state,
            false,
        )
        .unwrap();

        assert_eq!(
            state.touched_roots.iter().copied().collect::<Vec<_>>(),
            vec![root_a, root_b]
        );
    }

    #[test]
    fn test_process_record_archived_skipped() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        let record = make_record(1, 0, HashMap::new());

        // Without allow_archived
        let outcome = process_record(&mut conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.skipped_archived, 1);
    }

    #[test]
    fn test_process_record_imports_fact() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        let mut facts = HashMap::new();
        facts.insert("content.Make".to_string(), serde_json::json!("Canon"));
        let record = make_record(1, 0, facts);

        let outcome = process_record(&mut conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.facts_imported, 1);
    }

    #[test]
    fn test_process_record_hash_links_object() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None); // unhashed
        let mut state = init_state(&conn).unwrap();

        let mut facts = HashMap::new();
        facts.insert(
            "content.hash.sha256".to_string(),
            serde_json::json!("abc123def456"),
        );
        let record = make_record(1, 0, facts);

        let outcome = process_record(&mut conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.objects_created, 1);
    }

    #[test]
    fn test_process_record_type_mismatch() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        // First import: "content.Width" as number
        let mut facts1 = HashMap::new();
        facts1.insert("content.Width".to_string(), serde_json::json!(1920));
        let record1 = make_record(1, 0, facts1);
        process_record(&mut conn, &record1, &mut state, false).unwrap();
        assert_eq!(state.stats.facts_imported, 1);

        // Second import: "content.Width" as string (type mismatch)
        let mut facts2 = HashMap::new();
        facts2.insert("content.Width".to_string(), serde_json::json!("wide"));
        let record2 = make_record(1, 0, facts2);
        let outcome = process_record(&mut conn, &record2, &mut state, false).unwrap();

        assert!(!outcome.warnings.is_empty());
        assert!(outcome.warnings[0].contains("type mismatch"));
        assert_eq!(state.stats.skipped_type_mismatch, 1);
    }

    // =========================================================================
    // promote_content_facts
    // =========================================================================

    fn source_fact_count(conn: &Connection, source_id: i64) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_type = 'source' AND entity_id = ?1",
            [source_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn object_fact_value(conn: &Connection, object_id: i64, key: &str) -> Option<String> {
        conn.query_row(
            "SELECT value_text FROM facts
             WHERE entity_type = 'object' AND entity_id = ?1 AND key = ?2",
            rusqlite::params![object_id, key],
            |r| r.get(0),
        )
        .ok()
    }

    #[test]
    fn promote_content_facts_moves_an_absent_content_fact() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        let src = insert_source(&conn, root, "photo.jpg", Some(obj));
        crate::ops::test_helpers::insert_fact(&conn, src, "content.Make", "Canon");

        let promoted = promote_content_facts(&conn, src, obj).unwrap();

        assert_eq!(promoted, 1);
        assert_eq!(
            object_fact_value(&conn, obj, "content.Make").as_deref(),
            Some("Canon")
        );
        assert_eq!(source_fact_count(&conn, src), 0);
    }

    #[test]
    fn promote_content_facts_deletes_the_source_copy_when_the_object_already_has_it() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        let src = insert_source(&conn, root, "photo.jpg", Some(obj));
        crate::ops::test_helpers::insert_fact(&conn, src, "content.Make", "Nikon");
        facts_repo::insert_object_fact(&conn, obj, "content.Make", Some("Canon"), None, None, 0)
            .unwrap();

        let promoted = promote_content_facts(&conn, src, obj).unwrap();

        // Nothing was gained, so nothing is counted...
        assert_eq!(promoted, 0);
        // ...the object keeps the value it already had...
        assert_eq!(
            object_fact_value(&conn, obj, "content.Make").as_deref(),
            Some("Canon")
        );
        // ...and the redundant source copy goes regardless.
        assert_eq!(source_fact_count(&conn, src), 0);
    }
}
