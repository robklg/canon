//! Import facts operations — per-record processing for JSONL fact import.
//!
//! `init_state()` loads the existing fact type map from the database.
//! `process_record()` handles all business logic for a single import record:
//! source validation, key normalization, hash linking, fact promotion, type
//! checking, reading each value once as its hint says, and fact upsert.
//!
//! No stdout/stderr/stdin — the interface reads stdin and displays outcomes.

use std::collections::{BTreeSet, HashMap};

use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;

use crate::core::domain::fact::{FactType, FactValue};
use crate::core::domain::time::now_epoch_secs;
use crate::core::domain::time::reading::{self, TimeReadError};
use crate::core::repo::{self, Connection};
use crate::facts::domain::{is_content_fact, normalize_fact_key};
use crate::facts::repo as facts_repo;

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
    #[serde(default = "now_epoch_secs")]
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
    pub fact_type_map: HashMap<String, FactType>,
    pub type_mismatch_keys: HashMap<String, (FactType, FactType)>,
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
    /// Records naming a source that stands behind a door the user closed.
    ///
    /// A batch surface takes the closed default per **record**, not per run:
    /// refusing the whole import would break a pipeline over one parked root,
    /// while writing the facts would be a write inside the closet. So the
    /// record is set aside, counted, and stated in the summary — never
    /// silent, never fatal.
    pub skipped_suspended: u64,
    pub skipped_type_mismatch: u64,
    pub objects_created: u64,
    pub facts_promoted: u64,
}

impl ImportStats {
    /// Compose the import summary message.
    pub fn compose_summary(&self) -> String {
        use crate::core::domain::format_count;
        format!(
            "Processed {} lines: {} facts imported, {} skipped (stale), {} skipped (reserved), {} skipped (archived), {} skipped (on suspended roots), {} skipped (type mismatch), {} objects created, {} facts promoted",
            format_count(self.lines_processed),
            format_count(self.facts_imported),
            format_count(self.skipped_stale),
            format_count(self.skipped_reserved),
            format_count(self.skipped_archived),
            format_count(self.skipped_suspended),
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
/// Takes a shared connection so the command's armed recorder can hold one for
/// the whole run. The one transaction below opens with `unchecked_transaction`
/// for that reason; nesting is what SQLite refuses at runtime, and nothing
/// here is reached from inside another transaction.
pub fn process_record(
    conn: &Connection,
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

    // The door, per record. Writing a fact onto a source behind a root the
    // user closed is a write inside the closet; refusing the whole batch over
    // one parked root would break an import that is otherwise entirely
    // legitimate. So this record is set aside and counted, and the root is
    // never touched — including the object link and the fact promotion below,
    // which is why this stands ahead of every one of them.
    if !source.is_active() {
        state.stats.skipped_suspended += 1;
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
            let (obj, created) =
                repo::object::get_or_create_reporting_creation(conn, "sha256", hash_str)?;
            if created {
                state.stats.objects_created += 1;
            }
            object_id = Some(obj.id);

            // Link source to object AND promote existing facts in a single transaction
            if current_object_id.is_none() {
                let tx = conn.unchecked_transaction()?;
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
        let fact_value = match read_hinted(&TypedValue::parse(value)) {
            Ok(v) => v,
            Err(refusal) => {
                outcome
                    .warnings
                    .push(refusal.warning(key, &format!("{root_path}/{rel_path}")));
                state.stats.skipped_type_mismatch += 1;
                continue;
            }
        };

        // Check type consistency
        let new_type = fact_value.kind();

        if let Some(&existing_type) = state.fact_type_map.get(key) {
            if existing_type != new_type {
                let is_new_key = !state.type_mismatch_keys.contains_key(key);
                state
                    .type_mismatch_keys
                    .entry(key.clone())
                    .or_insert((existing_type, new_type));
                if is_new_key {
                    outcome.warnings.push(format!(
                        "type mismatch for '{key}' in {root_path}/{rel_path}: existing {}, got {} (value: {value})",
                        existing_type.as_str(),
                        new_type.as_str()
                    ));
                }
                state.stats.skipped_type_mismatch += 1;
                continue;
            }
        } else {
            state.fact_type_map.insert(key.clone(), new_type);
        }

        if let Some(oid) = object_id {
            outcome
                .verbose_lines
                .push(format!("  {key}: {value} (on object)"));
            facts_repo::upsert(
                conn,
                "object",
                oid,
                key,
                &fact_value,
                record.observed_at,
                None,
            )?;
            // Counted as imported, never as promoted: this fact arrived on the
            // object directly. Promotion is the separate event above — facts
            // that already stood on the source, moved onto the object when the
            // source gained its content identity.
            state.stats.facts_imported += 1;
        } else {
            outcome
                .verbose_lines
                .push(format!("  {key}: {value} (on source)"));
            facts_repo::upsert(
                conn,
                "source",
                record.source_id,
                key,
                &fact_value,
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

/// Why a value could not be read: a time the list of spellings refused, or anything else.
enum Refusal {
    Time { value: Value, why: TimeReadError },
    Other(String),
}

impl Refusal {
    /// The warning, with the place named before the reason so a reason sentence ends the line.
    fn warning(&self, key: &str, at: &str) -> String {
        match self {
            Refusal::Time { value, why } => {
                format!("{value} is not a time for '{key}' in {at}: {why}")
            }
            Refusal::Other(e) => format!("{e} for '{key}' in {at}"),
        }
    }
}

/// Read an imported value once, as what its hint says it is. A time is read through the one
/// list of spellings, never by a parser of import's own; a number is never epoch seconds.
fn read_hinted(typed: &TypedValue) -> Result<FactValue, Refusal> {
    match typed {
        TypedValue::Plain(v) => Ok(read_plain(v)),
        TypedValue::Hinted { value, type_hint } => match type_hint.as_str() {
            "duration" => try_parse_duration(value)
                .map(FactValue::Num)
                .ok_or_else(|| Refusal::Other(format!("cannot parse as duration: {value}"))),
            "datetime" => {
                let read = match value {
                    Value::String(s) => reading::read_time(s),
                    Value::Number(n) => n.as_f64().map_or(
                        Err(TimeReadError::YearOutOfRange),
                        reading::read_year_number,
                    ),
                    _ => {
                        return Err(Refusal::Other(format!(
                            "datetime requires string or year, got: {value}"
                        )))
                    }
                };
                read.map(FactValue::Time).map_err(|why| Refusal::Time {
                    value: value.clone(),
                    why,
                })
            }
            unknown => Err(Refusal::Other(format!("unknown type hint: {unknown}"))),
        },
    }
}

/// A plain value (no type hint): a string is text, a number or a boolean a number, anything
/// else its JSON text.
fn read_plain(value: &Value) -> FactValue {
    match value {
        Value::String(s) => FactValue::Text(s.clone()),
        Value::Number(n) => match n.as_i64() {
            Some(i) => FactValue::Num(i as f64),
            None => n
                .as_f64()
                .map_or_else(|| FactValue::Text(n.to_string()), FactValue::Num),
        },
        Value::Bool(b) => FactValue::Num(if *b { 1.0 } else { 0.0 }),
        Value::Null => FactValue::Text(String::new()),
        Value::Array(_) | Value::Object(_) => FactValue::Text(value.to_string()),
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
                    &fact.value,
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
    use crate::core::domain::time::value::{Precision, TimeForm, TimeValue};
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};

    // =========================================================================
    // read_hinted: times
    // =========================================================================

    fn hinted(value: Value) -> TypedValue {
        TypedValue::Hinted {
            value,
            type_hint: "datetime".to_string(),
        }
    }

    fn read_as_time(value: Value) -> TimeValue {
        match read_hinted(&hinted(value)) {
            Ok(FactValue::Time(tv)) => tv,
            Ok(other) => panic!("read as {}, not a time", other.kind()),
            Err(refusal) => panic!("refused: {}", refusal.warning("k", "here")),
        }
    }

    fn refusal_of(value: Value) -> TimeReadError {
        match read_hinted(&hinted(value)) {
            Err(Refusal::Time { why, .. }) => why,
            Err(Refusal::Other(e)) => panic!("refused, but not as a time: {e}"),
            Ok(v) => panic!("read as {}", v.kind()),
        }
    }

    #[test]
    fn an_rfc3339_time_with_an_offset_is_an_instant_that_keeps_it() {
        let tv = read_as_time(Value::from("2020-01-15T10:30:00+00:00"));
        assert_eq!(tv.form(), TimeForm::InstantWithOffset);
        assert_eq!(tv.recorded_offset(), Some(jiff::tz::Offset::UTC));
        assert_eq!(tv.precision(), Precision::Second);
        assert_eq!(tv.legacy_epoch_number(), 1579084200);
    }

    #[test]
    fn an_iso_time_without_an_offset_is_a_clock_reading() {
        let tv = read_as_time(Value::from("2020-01-15T10:30:00"));
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.precision(), Precision::Second);
        assert_eq!(tv.legacy_epoch_number(), 1579084200);
    }

    #[test]
    fn an_exif_time_without_an_offset_is_a_clock_reading() {
        let tv = read_as_time(Value::from("2020:07:23 11:06:32"));
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.legacy_epoch_number(), 1595502392);
    }

    /// Text after a time is never cut off and dropped — a multi-byte character included, at
    /// any position — so an import never stops on it and never keeps part of it.
    #[test]
    fn an_exif_shaped_value_with_trailing_text_is_refused() {
        assert_eq!(
            refusal_of(Value::from("2020:07:23 11:06:3é")),
            TimeReadError::NotInList
        );
        assert_eq!(
            refusal_of(Value::from("2020:07:23 11:06:32é")),
            TimeReadError::NotInList
        );
    }

    #[test]
    fn a_plain_year_string_is_a_clock_reading_at_year_precision() {
        let tv = read_as_time(Value::from("2005"));
        assert_eq!(tv.form(), TimeForm::Clock);
        assert_eq!(tv.precision(), Precision::Year);
        assert_eq!(tv.legacy_epoch_number(), 1104537600);
    }

    #[test]
    fn text_that_is_not_a_time_is_refused() {
        for text in ["not a date", "", "99999"] {
            assert_eq!(
                refusal_of(Value::from(text)),
                TimeReadError::NotInList,
                "{text:?}"
            );
        }
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
    // read_hinted: plain values and the other hints
    // =========================================================================

    #[test]
    fn a_plain_string_is_text() {
        let read = read_hinted(&TypedValue::Plain(serde_json::json!("hello")));
        assert!(matches!(read, Ok(FactValue::Text(t)) if t == "hello"));
    }

    #[test]
    fn a_plain_number_is_a_number() {
        let read = read_hinted(&TypedValue::Plain(serde_json::json!(42)));
        assert!(matches!(read, Ok(FactValue::Num(n)) if n == 42.0));
    }

    #[test]
    fn a_plain_boolean_is_a_number() {
        let read = read_hinted(&TypedValue::Plain(serde_json::json!(true)));
        assert!(matches!(read, Ok(FactValue::Num(n)) if n == 1.0));
    }

    #[test]
    fn the_datetime_hint_on_a_string_reads_a_time() {
        let read = read_hinted(&hinted(Value::from("2020-01-15T10:30:00+00:00")));
        assert_eq!(read.ok().map(|v| v.kind()), Some(FactType::Time));
    }

    #[test]
    fn the_duration_hint_reads_a_number_of_seconds() {
        let typed = TypedValue::Hinted {
            value: Value::String("3:30".to_string()),
            type_hint: "duration".to_string(),
        };
        let read = read_hinted(&typed);
        assert!(matches!(read, Ok(FactValue::Num(n)) if (n - 210.0).abs() < 0.01));
    }

    #[test]
    fn an_unknown_hint_is_refused() {
        let typed = TypedValue::Hinted {
            value: Value::String("hello".to_string()),
            type_hint: "bogus".to_string(),
        };
        match read_hinted(&typed) {
            Err(refusal) => assert!(refusal.warning("k", "here").contains("unknown type hint")),
            Ok(v) => panic!("read as {}", v.kind()),
        }
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
        let conn = setup_test_db();
        let mut state = init_state(&conn).unwrap();
        let record = make_record(999, 0, HashMap::new());

        let outcome = process_record(&conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings[0].contains("not found"));
    }

    #[test]
    fn test_process_record_stale_skipped() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        // Source has basis_rev 0 (default), import has 99
        let record = make_record(1, 99, HashMap::new());
        let outcome = process_record(&conn, &record, &mut state, false).unwrap();

        assert!(outcome.warnings[0].contains("basis_rev"));
        assert_eq!(state.stats.skipped_stale, 1);
        // A gated record touches no root — no scope entry for it.
        assert!(state.touched_roots.is_empty());
    }

    #[test]
    fn test_touched_roots_collects_acted_on_roots() {
        let conn = setup_test_db();
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
            process_record(&conn, &record, &mut state, false).unwrap();
        }
        // Missing source: gated, must not touch.
        process_record(
            &conn,
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
        let conn = setup_test_db();
        let root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        let record = make_record(1, 0, HashMap::new());

        // Without allow_archived
        let outcome = process_record(&conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.skipped_archived, 1);
    }

    /// A record naming a source behind a closed door is set aside and
    /// counted — never written, never fatal to the batch, and the root is not
    /// touched at all: no fact, no object link, and no scope row claiming this
    /// import acted there.
    #[test]
    fn an_import_record_on_a_parked_root_is_counted_not_written() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/photos", "source", true);
        let live = insert_root(&conn, "/live", "source", false);
        let behind = insert_source(&conn, parked, "photo.jpg", None);
        let keeper = insert_source(&conn, live, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        let facts = || {
            HashMap::from([(
                "content.hash.sha256".to_string(),
                Value::String("abc123def456".to_string()),
            )])
        };
        let outcome =
            process_record(&conn, &make_record(behind, 0, facts()), &mut state, false).unwrap();

        assert!(outcome.warnings.is_empty(), "set aside is not a warning");
        assert_eq!(state.stats.skipped_suspended, 1);
        assert_eq!(state.stats.facts_imported, 0);
        assert_eq!(state.stats.objects_created, 0, "not even the object link");
        assert!(
            !state.touched_roots.contains(&parked),
            "a set-aside record never claims its root as acted on"
        );

        // And the batch runs on: the next record imports as it always did.
        process_record(&conn, &make_record(keeper, 0, facts()), &mut state, false).unwrap();
        assert_eq!(state.stats.objects_created, 1);
        assert!(state.touched_roots.contains(&live));
    }

    #[test]
    fn test_process_record_imports_fact() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        let mut facts = HashMap::new();
        facts.insert("content.Make".to_string(), serde_json::json!("Canon"));
        let record = make_record(1, 0, facts);

        let outcome = process_record(&conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.facts_imported, 1);
    }

    #[test]
    fn test_process_record_hash_links_object() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None); // unhashed
        let mut state = init_state(&conn).unwrap();

        let mut facts = HashMap::new();
        facts.insert(
            "content.hash.sha256".to_string(),
            serde_json::json!("abc123def456"),
        );
        let record = make_record(1, 0, facts);

        let outcome = process_record(&conn, &record, &mut state, false).unwrap();
        assert!(outcome.warnings.is_empty());
        assert_eq!(state.stats.objects_created, 1);
    }

    /// The two counters that go into the printed *and* recorded summary must
    /// count the events they name. Two sources sharing one hash is one object
    /// created; a fact arriving straight onto an object was never promoted.
    #[test]
    fn the_import_counters_count_the_events_they_name() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let first = insert_source(&conn, root, "photo.jpg", None);
        let second = insert_source(&conn, root, "copy.jpg", None);
        let mut state = init_state(&conn).unwrap();

        // A fact stands on the first source before it has a content identity;
        // giving it one promotes that fact onto the object.
        let mut with_fact = HashMap::new();
        with_fact.insert("content.Make".to_string(), serde_json::json!("Canon"));
        process_record(&conn, &make_record(first, 0, with_fact), &mut state, false).unwrap();
        assert_eq!(state.stats.facts_imported, 1);
        assert_eq!(state.stats.facts_promoted, 0, "nothing promoted yet");

        let mut hash_and_fact = HashMap::new();
        hash_and_fact.insert(
            "content.hash.sha256".to_string(),
            serde_json::json!("abc123def456"),
        );
        hash_and_fact.insert("content.Model".to_string(), serde_json::json!("R6"));
        process_record(
            &conn,
            &make_record(first, 0, hash_and_fact.clone()),
            &mut state,
            false,
        )
        .unwrap();
        assert_eq!(state.stats.objects_created, 1);
        assert_eq!(
            state.stats.facts_promoted, 1,
            "the fact that already stood on the source"
        );

        // The second source carries the same content: it links to the object
        // that already exists, and the facts it brings land on that object
        // directly — imported, not promoted.
        process_record(
            &conn,
            &make_record(second, 0, hash_and_fact),
            &mut state,
            false,
        )
        .unwrap();
        assert_eq!(
            state.stats.objects_created, 1,
            "one object, however many sources share it"
        );
        assert_eq!(
            state.stats.facts_promoted, 1,
            "an object fact imported directly was never promoted"
        );
    }

    #[test]
    fn test_process_record_type_mismatch() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));
        let mut state = init_state(&conn).unwrap();

        // First import: "content.Width" as number
        let mut facts1 = HashMap::new();
        facts1.insert("content.Width".to_string(), serde_json::json!(1920));
        let record1 = make_record(1, 0, facts1);
        process_record(&conn, &record1, &mut state, false).unwrap();
        assert_eq!(state.stats.facts_imported, 1);

        // Second import: "content.Width" as string (type mismatch)
        let mut facts2 = HashMap::new();
        facts2.insert("content.Width".to_string(), serde_json::json!("wide"));
        let record2 = make_record(1, 0, facts2);
        let outcome = process_record(&conn, &record2, &mut state, false).unwrap();

        assert!(!outcome.warnings.is_empty());
        assert!(outcome.warnings[0].contains("type mismatch"));
        assert_eq!(state.stats.skipped_type_mismatch, 1);
    }

    /// The columns one imported time is stored with: form, offset in seconds, precision, and
    /// the fraction in nanoseconds.
    type TimeColumns = (String, Option<i64>, String, Option<i64>);

    fn time_columns(conn: &Connection, source_id: i64, key: &str) -> Option<TimeColumns> {
        conn.query_row(
            "SELECT time_form, time_offset, time_precision, time_nanos FROM facts
             WHERE entity_type = 'source' AND entity_id = ?1 AND key = ?2",
            rusqlite::params![source_id, key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .ok()
    }

    fn import_one(
        conn: &Connection,
        state: &mut ImportState,
        source_id: i64,
        facts: &[(String, Value)],
    ) -> RecordOutcome {
        let record = make_record(source_id, 0, facts.iter().cloned().collect());
        process_record(conn, &record, state, false).unwrap()
    }

    #[test]
    fn each_written_form_is_stored_with_its_form_offset_and_precision() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        // What was written, then the form, offset, precision and fraction it is stored with.
        type Case = (Value, &'static str, Option<i64>, &'static str, Option<i64>);
        let cases: [Case; 13] = [
            (
                "2023-12-31T21:00:00-05:00".into(),
                "instant_offset",
                Some(-18000),
                "second",
                None,
            ),
            (
                "2023-12-31T23:30:00Z".into(),
                "instant_offset",
                Some(0),
                "second",
                None,
            ),
            (
                "2023-12-31T23:30:00+00:00".into(),
                "instant_offset",
                Some(0),
                "second",
                None,
            ),
            (
                "2023-12-31T23:30:00-00:00".into(),
                "instant",
                None,
                "second",
                None,
            ),
            ("2023:12:31 21:00:00".into(), "clock", None, "second", None),
            ("2024".into(), "clock", None, "year", None),
            ("2024-06".into(), "clock", None, "month", None),
            ("2024-06-15".into(), "clock", None, "day", None),
            ("2024-06-15T14:30".into(), "clock", None, "minute", None),
            (
                "2024-06-15T14:30:05.45+02:00".into(),
                "instant_offset",
                Some(7200),
                "fraction",
                Some(450_000_000),
            ),
            (
                "2024:07:23 11:06:32.45+02:00".into(),
                "instant_offset",
                Some(7200),
                "fraction",
                Some(450_000_000),
            ),
            (serde_json::json!(2019), "clock", None, "year", None),
            (serde_json::json!(2019.0), "clock", None, "year", None),
        ];
        let facts: Vec<(String, Value)> = cases
            .iter()
            .enumerate()
            .map(|(i, (v, ..))| {
                let wrapped = serde_json::json!({"value": v, "type": "datetime"});
                (format!("content.form{i}"), wrapped)
            })
            .collect();

        let outcome = import_one(&conn, &mut state, src, &facts);

        assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
        for (i, (written, form, offset, precision, nanos)) in cases.iter().enumerate() {
            assert_eq!(
                time_columns(&conn, src, &format!("content.form{i}")),
                Some((form.to_string(), *offset, precision.to_string(), *nanos)),
                "{written}"
            );
        }
    }

    #[test]
    fn an_unreadable_time_is_skipped_counted_and_warned_never_stored() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        let cases: [(Value, TimeReadError); 6] = [
            ("2020:07:23 11:06:32é".into(), TimeReadError::NotInList),
            ("2024-02-30".into(), TimeReadError::ImpossibleDate),
            ("2024-06-15+02:00".into(), TimeReadError::OffsetOnDateAlone),
            (
                "9999-12-31T23:00:00-05:00".into(),
                TimeReadError::OutOfRange,
            ),
            // A number with the time hint is a year or nothing — never epoch seconds.
            (serde_json::json!(1.5e9), TimeReadError::YearOutOfRange),
            (serde_json::json!(2019.5), TimeReadError::YearOutOfRange),
        ];
        let facts: Vec<(String, Value)> = cases
            .iter()
            .enumerate()
            .map(|(i, (v, _))| {
                let wrapped = serde_json::json!({"value": v, "type": "datetime"});
                (format!("content.bad{i}"), wrapped)
            })
            .collect();

        let outcome = import_one(&conn, &mut state, src, &facts);

        assert_eq!(state.stats.skipped_type_mismatch, cases.len() as u64);
        assert_eq!(state.stats.facts_imported, 0);
        assert_eq!(
            source_fact_count(&conn, src),
            0,
            "nothing is stored, not even as a clock reading"
        );
        assert_eq!(outcome.warnings.len(), cases.len());
        for (i, (written, why)) in cases.iter().enumerate() {
            let key = format!("content.bad{i}");
            let expected =
                format!("{written} is not a time for '{key}' in /photos/photo.jpg: {why}");
            assert!(
                outcome.warnings.contains(&expected),
                "{expected}\n{:?}",
                outcome.warnings
            );
        }
    }

    #[test]
    fn an_unhinted_number_stays_a_number() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);
        let mut state = init_state(&conn).unwrap();

        import_one(
            &conn,
            &mut state,
            src,
            &[("content.When".to_string(), serde_json::json!(1500000000))],
        );

        let stored: (Option<f64>, Option<i64>) = conn
            .query_row(
                "SELECT value_num, value_time FROM facts WHERE key = 'content.When'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (Some(1500000000.0), None));
    }

    #[test]
    fn one_key_holding_every_form_is_never_a_type_change() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let mut state = init_state(&conn).unwrap();
        let written: [Value; 6] = [
            "2023-12-31T21:00:00-05:00".into(),
            "2023-12-31T23:30:00-00:00".into(),
            "2023:12:31 21:00:00".into(),
            "2024".into(),
            "2024-06-15T14:30:05.45+02:00".into(),
            serde_json::json!(2019),
        ];

        for (i, v) in written.iter().enumerate() {
            let src = insert_source(&conn, root, &format!("{i}.jpg"), None);
            let wrapped = serde_json::json!({"value": v, "type": "datetime"});
            let outcome = import_one(
                &conn,
                &mut state,
                src,
                &[("content.When".to_string(), wrapped)],
            );
            assert!(outcome.warnings.is_empty(), "{v}: {:?}", outcome.warnings);
        }

        assert_eq!(state.stats.facts_imported, written.len() as u64);
        assert_eq!(state.stats.skipped_type_mismatch, 0);
        assert!(state.type_mismatch_keys.is_empty());
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
        crate::core::testing::insert_fact(&conn, src, "content.Make", "Canon");

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
        crate::core::testing::insert_fact(&conn, src, "content.Make", "Nikon");
        facts_repo::insert_object_fact(
            &conn,
            obj,
            "content.Make",
            &FactValue::Text("Canon".into()),
            0,
        )
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
