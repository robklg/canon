//! Import facts operations — batches of JSONL records, each one write transaction.
//!
//! `import_batch()` takes the write lock before it reads anything, reads the types of the
//! batch's keys under it, and runs each record inside a savepoint: source validation, key
//! reading, hash linking, fact promotion, and each value decided by its key's type
//! (`facts::domain::decide`) before it is stored. A record that fails is undone whole — its
//! facts, its counts and any type it gave a key — and the batch goes on.
//!
//! No stdout/stderr/stdin — the interface reads stdin, closes batches and displays outcomes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use anyhow::Result;
use rusqlite::{Transaction, TransactionBehavior};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::core::domain::fact::KeyTypes;
use crate::core::domain::format_count;
use crate::core::domain::time::now_epoch_secs;
use crate::core::repo::{self, Connection};
use crate::expr::CONTENT_HASH;
use crate::facts::domain::{
    decide, import_key, is_content_fact, keys_named_twice, reserved_name_cause, Disposition,
    JsonFact, SkipCause,
};
use crate::facts::repo as facts_repo;

/// A batch closes at this many records…
pub const BATCH_RECORDS: usize = 500;
/// …or this long after its first record, whichever comes first.
pub const BATCH_WINDOW: Duration = Duration::from_secs(1);

// ============================================================================
// Types
// ============================================================================

/// Deserialized JSONL record from stdin. Each fact's value is kept as the JSON its tool
/// wrote, so a number arrives in its own spelling.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // Worklist pass-through fields are intentionally accepted but not read
pub struct ImportRecord {
    pub source_id: i64,
    pub basis_rev: i64,
    #[serde(default = "now_epoch_secs")]
    pub observed_at: i64,
    pub facts: BTreeMap<String, Box<RawValue>>,
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

/// Why a whole record is set aside, in the order the summary names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordSkip {
    /// Written for a version of the file that has since changed.
    Stale,
    /// Naming a source behind a door the user closed.
    ///
    /// A batch surface takes the closed default per **record**, not per run: refusing the
    /// whole import would break a pipeline over one parked root, while writing the facts
    /// would be a write inside the closet. So the record is set aside, counted, and stated in
    /// the summary — never silent, never fatal.
    Suspended,
    /// Naming a source in an archive root, without `--allow archived`.
    Archived,
}

/// The values of one key skipped for one cause: how many, and how many times each was
/// written as it was (its raw JSON), so a line can name the most frequent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Skipped {
    pub count: u64,
    pub values: BTreeMap<String, u64>,
}

impl Skipped {
    /// The value written most often; between equally frequent values, the first in text order.
    pub fn most_frequent(&self) -> Option<&str> {
        let mut best: Option<(&str, u64)> = None;
        for (value, &n) in &self.values {
            if best.is_none_or(|(_, top)| n > top) {
                best = Some((value, n));
            }
        }
        best.map(|(value, _)| value)
    }
}

/// Counters for the import summary.
#[derive(Debug, Default)]
pub struct ImportStats {
    /// Records read, in committed batches.
    pub records: u64,
    pub facts_imported: u64,
    pub objects_created: u64,
    /// Facts stored on a source before its content was known, moved to the content.
    pub facts_moved: u64,
    /// Such facts dropped instead, the content already having a value for their key.
    pub facts_dropped: u64,
    pub skipped_records: BTreeMap<RecordSkip, u64>,
    /// Keyed by the key (the name as written, for a name no content fact has) and the cause.
    pub skipped_values: BTreeMap<(String, SkipCause), Skipped>,
}

/// What the summary calls a skip cause, in the order it names them; `None` outside the
/// skipped group (a null value and a source fact passed back are nothing to fix).
fn summary_class(cause: &SkipCause) -> Option<(u8, &'static str, &'static str)> {
    Some(match cause {
        SkipCause::NoValue | SkipCause::SourceFact => return None,
        SkipCause::DoesNotRead(_) => (
            0,
            "that does not read as its key's type",
            "that do not read as their key's type",
        ),
        SkipCause::WouldLose(_) => (
            1,
            "that would lose what the text carried",
            "that would lose what the text carried",
        ),
        SkipCause::NumberForTime => (
            2,
            "that is a number for a time key",
            "that are numbers for a time key",
        ),
        SkipCause::NotExact => (
            3,
            "that is a number Canon cannot hold exactly",
            "that are numbers Canon cannot hold exactly",
        ),
        SkipCause::BoolForTime => (
            4,
            "that is true or false for a time key",
            "that are true or false for a time key",
        ),
        SkipCause::Structure(_) => (
            5,
            "that is a list or object for a number or time key",
            "that are lists or objects for a number or time key",
        ),
        SkipCause::HintMisfit(_) => (
            6,
            "with a hint its key does not take",
            "with a hint their key does not take",
        ),
        SkipCause::HintDoesNotRead(_) => (
            7,
            "that does not read as its hint",
            "that do not read as their hint",
        ),
        SkipCause::UnknownHint(_) => (
            8,
            "with a hint Canon does not read",
            "with a hint Canon does not read",
        ),
        SkipCause::MixedKey => (9, "for a key of mixed type", "for keys of mixed type"),
        SkipCause::NamedTwice => (
            10,
            "for a key named twice in one record",
            "for keys named twice in one record",
        ),
        SkipCause::SourceName => (
            11,
            "under a name reserved for source facts",
            "under names reserved for source facts",
        ),
    })
}

/// `n` with its unit, singular for one.
fn counted(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", format_count(n), if n == 1 { one } else { many })
}

impl ImportStats {
    /// Add a committed batch's counts.
    pub fn add(&mut self, other: ImportStats) {
        self.records += other.records;
        self.facts_imported += other.facts_imported;
        self.objects_created += other.objects_created;
        self.facts_moved += other.facts_moved;
        self.facts_dropped += other.facts_dropped;
        for (skip, n) in other.skipped_records {
            *self.skipped_records.entry(skip).or_default() += n;
        }
        for (at, skipped) in other.skipped_values {
            let mine = self.skipped_values.entry(at).or_default();
            mine.count += skipped.count;
            for (value, n) in skipped.values {
                *mine.values.entry(value).or_default() += n;
            }
        }
    }

    /// Records set aside whole.
    pub fn records_skipped(&self) -> u64 {
        self.skipped_records.values().sum()
    }

    /// Values skipped for a cause there is something to fix about.
    pub fn values_skipped(&self) -> u64 {
        self.skipped_values
            .iter()
            .filter(|((_, cause), _)| summary_class(cause).is_some())
            .map(|(_, skipped)| skipped.count)
            .sum()
    }

    fn values_for(&self, wanted: &SkipCause) -> u64 {
        self.skipped_values
            .iter()
            .filter(|((_, cause), _)| cause == wanted)
            .map(|(_, skipped)| skipped.count)
            .sum()
    }

    /// The import summary: one line, only counts above zero named, each with its unit.
    pub fn compose_summary(&self) -> String {
        let mut clauses = vec![format!(
            "imported {}",
            counted(self.facts_imported, "fact", "facts")
        )];
        let records = self.records_skipped();
        if records > 0 {
            let parts: Vec<String> = self
                .skipped_records
                .iter()
                .filter(|(_, &n)| n > 0)
                .map(|(skip, &n)| {
                    let phrase = match skip {
                        RecordSkip::Stale => "stale",
                        RecordSkip::Suspended => "on suspended roots",
                        RecordSkip::Archived => "in archive roots",
                    };
                    format!("{} {phrase}", format_count(n))
                })
                .collect();
            clauses.push(format!(
                "skipped {}: {}",
                counted(records, "record", "records"),
                parts.join(", ")
            ));
        }
        let values = self.values_skipped();
        if values > 0 {
            let mut by_class: BTreeMap<u8, (u64, &str, &str)> = BTreeMap::new();
            for ((_, cause), skipped) in &self.skipped_values {
                if let Some((order, one, many)) = summary_class(cause) {
                    by_class.entry(order).or_insert((0, one, many)).0 += skipped.count;
                }
            }
            let parts: Vec<String> = by_class
                .values()
                .map(|&(n, one, many)| {
                    format!("{} {}", format_count(n), if n == 1 { one } else { many })
                })
                .collect();
            clauses.push(format!(
                "skipped {}: {}",
                counted(values, "value", "values"),
                parts.join(", ")
            ));
        }
        let nulls = self.values_for(&SkipCause::NoValue);
        if nulls > 0 {
            clauses.push(format!(
                "{} (nothing stored)",
                counted(nulls, "null value", "null values")
            ));
        }
        let source_facts = self.values_for(&SkipCause::SourceFact);
        if source_facts > 0 {
            clauses.push(format!(
                "{}, which Canon records itself",
                counted(
                    source_facts,
                    "value for a source fact",
                    "values for source facts"
                )
            ));
        }
        if self.objects_created > 0 {
            clauses.push(counted(
                self.objects_created,
                "object created",
                "objects created",
            ));
        }
        let dropped = (self.facts_dropped > 0).then(|| {
            format!(
                "{} (the content already had a value for {})",
                counted(self.facts_dropped, "fact dropped", "facts dropped"),
                if self.facts_dropped == 1 {
                    "its key"
                } else {
                    "their key"
                }
            )
        });
        match (self.facts_moved > 0, dropped) {
            (true, Some(dropped)) => clauses.push(format!(
                "{}, {dropped}",
                counted(
                    self.facts_moved,
                    "fact moved to the content",
                    "facts moved to the content"
                )
            )),
            (true, None) => clauses.push(counted(
                self.facts_moved,
                "fact moved to the content",
                "facts moved to the content",
            )),
            (false, Some(dropped)) => clauses.push(dropped),
            (false, None) => {}
        }
        format!(
            "Read {}: {}.",
            counted(self.records, "record", "records"),
            clauses.join("; ")
        )
    }

    /// The summary of an import that ended before its input did: what the committed batches
    /// hold, and the error.
    pub fn compose_early_stop(&self, error: &anyhow::Error) -> String {
        if self.records == 0 {
            return format!("Import ended early before anything was imported: {error:#}");
        }
        let mut skipped = Vec::new();
        let records = self.records_skipped();
        if records > 0 {
            skipped.push(counted(records, "record", "records"));
        }
        let values = self.values_skipped();
        if values > 0 {
            skipped.push(counted(values, "value", "values"));
        }
        let skipped = match skipped.as_slice() {
            [] => String::new(),
            [one] => format!("; {one} skipped"),
            [first, second, ..] => format!("; {first} and {second} skipped"),
        };
        format!(
            "Import ended early after {} ({} imported in committed batches, which stand{skipped}): {error:#}",
            counted(self.records, "record", "records"),
            counted(self.facts_imported, "fact", "facts"),
        )
    }
}

/// One line of `--verbose` detail, as data: the interface words it.
#[derive(Debug, Clone, PartialEq)]
pub enum VerboseLine {
    /// The file the lines below it are about.
    Source { root_path: String, rel_path: String },
    /// A value stored, as the tool wrote it.
    Stored {
        key: String,
        written: String,
        on_content: bool,
    },
    /// A value skipped, as the tool wrote it, and why.
    Skipped {
        key: String,
        written: String,
        cause: SkipCause,
    },
}

/// What a committed batch did.
#[derive(Debug, Default)]
pub struct BatchOutcome {
    pub stats: ImportStats,
    /// Roots whose sources this batch acted on — the decision's durable scope (one
    /// whole-root entry per root, recorded at completion).
    pub touched_roots: BTreeSet<i64>,
    /// Warning messages for display.
    pub warnings: Vec<String>,
    pub verbose: Vec<VerboseLine>,
}

impl BatchOutcome {
    fn absorb(&mut self, other: BatchOutcome) {
        self.stats.add(other.stats);
        self.touched_roots.extend(other.touched_roots);
        self.warnings.extend(other.warnings);
        self.verbose.extend(other.verbose);
    }

    fn skip_value(&mut self, key: &str, cause: SkipCause, written: &str) {
        let skipped = self
            .stats
            .skipped_values
            .entry((key.to_string(), cause.clone()))
            .or_default();
        skipped.count += 1;
        *skipped.values.entry(written.to_string()).or_default() += 1;
        self.verbose.push(VerboseLine::Skipped {
            key: key.to_string(),
            written: written.to_string(),
            cause,
        });
    }
}

// ============================================================================
// Public functions
// ============================================================================

/// Import one batch of records in one write transaction.
///
/// `IMMEDIATE` takes the write lock before anything is read, so the key types read here are
/// the committed ones and nothing changes them before the commit: a conversion committed
/// before the batch is seen, and one waiting for the lock sees what the batch wrote. Each
/// record runs between a savepoint and its release; a record whose processing fails is rolled
/// back to its savepoint — its facts, counts and any first type it gave a key with it —
/// reported, and the batch goes on. Takes a shared connection so the command's armed recorder
/// can hold one for the whole run.
pub fn import_batch(
    conn: &Connection,
    records: &[ImportRecord],
    allow_archived: bool,
) -> Result<BatchOutcome> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let keys: BTreeSet<String> = records
        .iter()
        .flat_map(|record| record.facts.keys())
        .filter_map(|name| import_key(name))
        .collect();
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut types = repo::fact::key_types(&tx, &keys)?;

    let mut outcome = BatchOutcome::default();
    for record in records {
        outcome.stats.records += 1;
        tx.execute_batch("SAVEPOINT record")?;
        let mut staged = BatchOutcome::default();
        let mut first_types = HashMap::new();
        match process_record(
            &tx,
            record,
            allow_archived,
            &types,
            &mut staged,
            &mut first_types,
        ) {
            Ok(()) => {
                tx.execute_batch("RELEASE record")?;
                for (key, first) in first_types {
                    types.entry(key).or_insert(first);
                }
                outcome.absorb(staged);
            }
            Err(e) => {
                tx.execute_batch("ROLLBACK TO record; RELEASE record")?;
                outcome.warnings.push(format!(
                    "Failed to process source_id {}: {e:#}",
                    record.source_id
                ));
            }
        }
    }
    tx.commit()?;
    Ok(outcome)
}

/// The types the named keys hold now, read after the last committed batch — what a key ended
/// the run as, which decides the way out of a number Canon could not hold exactly.
pub fn types_at_end(conn: &Connection, keys: &[&str]) -> Result<HashMap<String, KeyTypes>> {
    repo::fact::key_types(conn, keys)
}

// ============================================================================
// Private helpers
// ============================================================================

/// One record, inside its savepoint. What it does is staged in `out`, and each key it gives
/// a first type in `first_types`; the caller keeps both only once the savepoint is released.
fn process_record(
    conn: &Connection,
    record: &ImportRecord,
    allow_archived: bool,
    types: &HashMap<String, KeyTypes>,
    out: &mut BatchOutcome,
    first_types: &mut HashMap<String, KeyTypes>,
) -> Result<()> {
    // Check if source exists and get its data
    let source = match repo::source::fetch_by_id(conn, record.source_id)? {
        Some(s) => s,
        None => {
            out.warnings
                .push(format!("source_id {} not found", record.source_id));
            return Ok(());
        }
    };

    let current_object_id = source.object_id;

    let set_aside = |out: &mut BatchOutcome, skip: RecordSkip| {
        *out.stats.skipped_records.entry(skip).or_default() += 1;
    };

    // Check if source is in an archive root
    if source.is_from_role("archive") && !allow_archived {
        set_aside(out, RecordSkip::Archived);
        return Ok(());
    }

    // The door, per record. Writing a fact onto a source behind a root the
    // user closed is a write inside the closet; refusing the whole batch over
    // one parked root would break an import that is otherwise entirely
    // legitimate. So this record is set aside and counted, and the root is
    // never touched — including the object link and the fact promotion below,
    // which is why this stands ahead of every one of them.
    if !source.is_active() {
        set_aside(out, RecordSkip::Suspended);
        return Ok(());
    }

    if source.basis_rev != record.basis_rev {
        out.warnings.push(format!(
            "source_id {} has basis_rev {} but import has {}, skipping",
            record.source_id, source.basis_rev, record.basis_rev
        ));
        set_aside(out, RecordSkip::Stale);
        return Ok(());
    }

    // Past the gates — this record will act on the source's root.
    out.touched_roots.insert(source.root_id);

    // The file's header comes before any line about its values, the skipped names included.
    if !record.facts.is_empty() {
        out.verbose.push(VerboseLine::Source {
            root_path: source.root_path.clone(),
            rel_path: source.rel_path.clone(),
        });
    }

    // Read every name under its content reading first: a name starting with `source.` has
    // none, and is skipped by what it names.
    let mut arrived: Vec<(String, JsonFact, &RawValue)> = Vec::new();
    for (name, raw) in &record.facts {
        let fact = JsonFact::from_raw(raw)?;
        match import_key(name) {
            Some(key) => arrived.push((key, fact, raw)),
            None => out.skip_value(name, reserved_name_cause(name), fact.written(raw)),
        }
    }

    let named_twice = keys_named_twice(arrived.iter().map(|(key, fact, _)| (key.as_str(), fact)));
    let (twice, arrived): (Vec<_>, Vec<_>) = arrived
        .into_iter()
        .partition(|(key, _, _)| named_twice.contains(key));
    for (key, fact, raw) in &twice {
        out.skip_value(key, SkipCause::NamedTwice, fact.written(raw));
    }

    // Check for content hash and process it first
    let mut object_id = current_object_id;
    let hash = arrived.iter().find_map(|(key, fact, _)| match fact {
        JsonFact::Text(hash) if key == CONTENT_HASH => Some(hash.as_str()),
        JsonFact::Text(_)
        | JsonFact::Number(_)
        | JsonFact::Bool(_)
        | JsonFact::Null
        | JsonFact::Structure(_)
        | JsonFact::Hinted { .. } => None,
    });
    if let Some(hash) = hash {
        let (obj, created) = repo::object::get_or_create_reporting_creation(conn, "sha256", hash)?;
        if created {
            out.stats.objects_created += 1;
        }
        object_id = Some(obj.id);

        // Link source to object AND promote existing facts, inside the record's savepoint.
        if current_object_id.is_none() {
            repo::source::set_object_id(conn, record.source_id, obj.id)?;
            let (moved, dropped) = promote_content_facts(conn, record.source_id, obj.id)?;
            out.stats.facts_moved += moved;
            out.stats.facts_dropped += dropped;
        } else if current_object_id != object_id {
            repo::source::set_object_id(conn, record.source_id, obj.id)?;
        }
    }

    for (key, fact, raw) in &arrived {
        let written = fact.written(raw);
        let held = types.get(key);
        let first = held.is_none();
        let value = match decide(held, fact) {
            Disposition::Store(value) => value,
            Disposition::Skip(cause) => {
                out.skip_value(key, cause, written);
                continue;
            }
        };
        if first {
            if let Some(gave) = KeyTypes::from_counts([(value.kind(), 1)]) {
                first_types.insert(key.clone(), gave);
            }
        }
        match object_id {
            Some(oid) => {
                facts_repo::upsert(conn, "object", oid, key, &value, record.observed_at, None)?
            }
            None => facts_repo::upsert(
                conn,
                "source",
                record.source_id,
                key,
                &value,
                record.observed_at,
                Some(record.basis_rev),
            )?,
        }
        // Counted as imported, never as moved: this fact arrived where it is stored.
        // Moving is the separate event above — facts that already stood on the source,
        // moved to the content when the source gained its content identity.
        out.stats.facts_imported += 1;
        out.verbose.push(VerboseLine::Stored {
            key: key.clone(),
            written: written.to_string(),
            on_content: object_id.is_some(),
        });
    }

    Ok(())
}

/// Move content facts from a source to its newly-linked object: how many moved, and how many
/// were dropped instead.
///
/// The source copy is deleted whichever way the branch goes: content facts
/// describe the content, so once the object holds the key the source's row is
/// redundant, and the object's existing value wins over an arriving duplicate.
fn promote_content_facts(conn: &Connection, source_id: i64, object_id: i64) -> Result<(u64, u64)> {
    let facts = facts_repo::fetch_content_facts_on_source(conn, source_id)?;

    let (mut moved, mut dropped) = (0u64, 0u64);
    for fact in facts {
        if is_content_fact(&fact.key) {
            if facts_repo::object_has_fact(conn, object_id, &fact.key)? {
                dropped += 1;
            } else {
                facts_repo::insert_object_fact(
                    conn,
                    object_id,
                    &fact.key,
                    &fact.value,
                    fact.observed_at,
                )?;
                moved += 1;
            }
            facts_repo::delete_by_id(conn, fact.id)?;
        }
    }

    Ok((moved, dropped))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::conversion::Loss;
    use crate::core::domain::fact::{FactType, FactValue};
    use crate::core::repo::{open_with_options, Db, DbOptions};
    use crate::core::testing::{
        insert_fact, insert_number_fact, insert_object, insert_root, insert_source, setup_test_db,
    };
    use crate::facts::domain::{KnownHint, Reading};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A database in a file, as the command opens one, so a second connection can share it.
    fn open_file(path: &std::path::Path) -> Db {
        open_with_options(
            path,
            DbOptions {
                debug_sql: false,
                profile: false,
            },
        )
        .unwrap()
    }

    /// A record whose facts are given as each name and the JSON its tool wrote.
    fn record(source_id: i64, basis_rev: i64, facts: &[(&str, &str)]) -> ImportRecord {
        ImportRecord {
            source_id,
            basis_rev,
            observed_at: 1700000000,
            facts: facts
                .iter()
                .map(|(name, raw)| {
                    let raw = RawValue::from_string(raw.to_string())
                        .unwrap_or_else(|e| panic!("{raw}: {e}"));
                    (name.to_string(), raw)
                })
                .collect(),
            path: None,
            root_id: None,
            size: None,
            mtime: None,
        }
    }

    fn import(conn: &Connection, records: &[ImportRecord]) -> BatchOutcome {
        import_batch(conn, records, false).unwrap()
    }

    fn skipped(outcome: &BatchOutcome, key: &str, cause: SkipCause) -> u64 {
        outcome
            .stats
            .skipped_values
            .get(&(key.to_string(), cause))
            .map_or(0, |s| s.count)
    }

    fn set_aside(outcome: &BatchOutcome, skip: RecordSkip) -> u64 {
        outcome
            .stats
            .skipped_records
            .get(&skip)
            .copied()
            .unwrap_or_default()
    }

    fn stored_count(conn: &Connection, key: &str) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM facts WHERE key = ?1", [key], |r| {
            r.get(0)
        })
        .unwrap()
    }

    #[test]
    fn test_process_record_source_not_found() {
        let conn = setup_test_db();
        let outcome = import(&conn, &[record(999, 0, &[])]);
        assert!(outcome.warnings[0].contains("not found"));
    }

    #[test]
    fn test_process_record_stale_skipped() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None);

        // Source has basis_rev 0 (default), import has 99
        let outcome = import(&conn, &[record(1, 99, &[])]);

        assert!(outcome.warnings[0].contains("basis_rev"));
        assert_eq!(set_aside(&outcome, RecordSkip::Stale), 1);
        // A gated record touches no root — no scope entry for it.
        assert!(outcome.touched_roots.is_empty());
    }

    #[test]
    fn test_touched_roots_collects_acted_on_roots() {
        let conn = setup_test_db();
        let root_a = insert_root(&conn, "/a", "source", false);
        let root_b = insert_root(&conn, "/b", "source", false);
        let s1 = insert_source(&conn, root_a, "1.jpg", None);
        let s2 = insert_source(&conn, root_b, "2.jpg", None);

        let outcome = import(
            &conn,
            &[
                record(s1, 0, &[("Make", r#""Canon""#)]),
                record(s2, 0, &[("Make", r#""Canon""#)]),
                // Missing source: gated, must not touch.
                record(999, 0, &[]),
            ],
        );

        assert_eq!(
            outcome.touched_roots.iter().copied().collect::<Vec<_>>(),
            vec![root_a, root_b]
        );
    }

    #[test]
    fn test_process_record_archived_skipped() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        // Without allow_archived
        let outcome = import(&conn, &[record(1, 0, &[])]);
        assert!(outcome.warnings.is_empty());
        assert_eq!(set_aside(&outcome, RecordSkip::Archived), 1);
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
        let facts = [("content.hash.sha256", r#""abc123def456""#)];

        let outcome = import(&conn, &[record(behind, 0, &facts)]);

        assert!(outcome.warnings.is_empty(), "set aside is not a warning");
        assert_eq!(set_aside(&outcome, RecordSkip::Suspended), 1);
        assert_eq!(outcome.stats.facts_imported, 0);
        assert_eq!(outcome.stats.objects_created, 0, "not even the object link");
        assert!(
            !outcome.touched_roots.contains(&parked),
            "a set-aside record never claims its root as acted on"
        );

        // And the batch runs on: the next record imports as it always did.
        let outcome = import(&conn, &[record(keeper, 0, &facts)]);
        assert_eq!(outcome.stats.objects_created, 1);
        assert!(outcome.touched_roots.contains(&live));
    }

    #[test]
    fn test_process_record_imports_fact() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let outcome = import(&conn, &[record(1, 0, &[("content.Make", r#""Canon""#)])]);
        assert!(outcome.warnings.is_empty());
        assert_eq!(outcome.stats.facts_imported, 1);
    }

    /// A name is imported under its content reading (`Make` → `content.Make`); a name
    /// starting with `source.` has none, and is skipped and counted — as a source fact passed
    /// back where it names one, with no warning.
    #[test]
    fn a_source_name_is_skipped_and_a_plain_name_stored_under_content() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let outcome = import(
            &conn,
            &[record(
                1,
                0,
                &[
                    ("source.size", "10"),
                    ("source.foo", "1"),
                    ("Make", r#""Canon""#),
                ],
            )],
        );

        assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
        assert_eq!(skipped(&outcome, "source.size", SkipCause::SourceFact), 1);
        assert_eq!(skipped(&outcome, "source.foo", SkipCause::SourceName), 1);
        assert_eq!(outcome.stats.facts_imported, 1);
        let keys: Vec<String> = conn
            .prepare("SELECT key FROM facts")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(keys, ["content.Make"]);
    }

    #[test]
    fn test_process_record_hash_links_object() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "photo.jpg", None); // unhashed

        let outcome = import(
            &conn,
            &[record(
                1,
                0,
                &[("content.hash.sha256", r#""abc123def456""#)],
            )],
        );
        assert!(outcome.warnings.is_empty());
        assert_eq!(outcome.stats.objects_created, 1);
    }

    /// The counters that go into the printed *and* recorded summary must count the events
    /// they name. Two sources sharing one hash is one object created; a fact arriving
    /// straight onto an object was never moved there.
    #[test]
    fn the_import_counters_count_the_events_they_name() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let first = insert_source(&conn, root, "photo.jpg", None);
        let second = insert_source(&conn, root, "copy.jpg", None);

        // A fact stands on the first source before it has a content identity;
        // giving it one moves that fact to the content.
        let outcome = import(
            &conn,
            &[record(first, 0, &[("content.Make", r#""Canon""#)])],
        );
        assert_eq!(outcome.stats.facts_imported, 1);
        assert_eq!(outcome.stats.facts_moved, 0, "nothing moved yet");

        let hash_and_fact = [
            ("content.hash.sha256", r#""abc123def456""#),
            ("content.Model", r#""R6""#),
        ];
        let outcome = import(&conn, &[record(first, 0, &hash_and_fact)]);
        assert_eq!(outcome.stats.objects_created, 1);
        assert_eq!(
            outcome.stats.facts_moved, 1,
            "the fact that already stood on the source"
        );

        // The second source carries the same content: it links to the object
        // that already exists, and the facts it brings land on that object
        // directly — imported, not moved.
        let outcome = import(&conn, &[record(second, 0, &hash_and_fact)]);
        assert_eq!(
            outcome.stats.objects_created, 0,
            "one object, however many sources share it"
        );
        assert_eq!(
            outcome.stats.facts_moved, 0,
            "an object fact imported directly was never moved"
        );
        assert_eq!(outcome.stats.facts_imported, 2);
    }

    /// A string that does not read as a number key's type is skipped and counted, never
    /// stored as a second type.
    #[test]
    fn test_process_record_type_mismatch() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let outcome = import(&conn, &[record(1, 0, &[("content.Width", "1920")])]);
        assert_eq!(outcome.stats.facts_imported, 1);

        let outcome = import(&conn, &[record(1, 0, &[("content.Width", r#""wide""#)])]);
        assert_eq!(
            skipped(
                &outcome,
                "content.Width",
                SkipCause::DoesNotRead(Reading::Number)
            ),
            1
        );
        assert_eq!(outcome.stats.facts_imported, 0);
        let num: Option<f64> = conn
            .query_row(
                "SELECT value_num FROM facts WHERE key = 'content.Width'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            num,
            Some(1920.0),
            "the number stands, and no text beside it"
        );
    }

    /// **One type per key, whichever root holds it**: a key whose every fact sits on a
    /// suspended root still has its type, and an import from an open root must fit it. The
    /// key's types are read from its whole row set; parking never enters them.
    #[test]
    fn an_import_into_a_parked_key_must_fit_its_type() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/parked", "source", true);
        let open = insert_root(&conn, "/open", "source", false);
        let behind = insert_source(&conn, parked, "a.jpg", None);
        insert_number_fact(&conn, behind, "content.Serial", 12.0);
        let here = insert_source(&conn, open, "b.jpg", None);

        let outcome = import(
            &conn,
            &[record(here, 0, &[("content.Serial", r#""SN12AB""#)])],
        );

        assert_eq!(
            skipped(
                &outcome,
                "content.Serial",
                SkipCause::DoesNotRead(Reading::Number)
            ),
            1
        );
        assert_eq!(
            stored_count(&conn, "content.Serial"),
            1,
            "only the parked number"
        );

        // A string that reads as a number lands as one.
        import(&conn, &[record(here, 0, &[("content.Serial", r#""12""#)])]);
        let types = types_at_end(&conn, &["content.Serial"]).unwrap();
        assert_eq!(types["content.Serial"].single(), Some(FactType::Num));
    }

    /// **One record never gives a key two types.** Two names reading to one new key, a string
    /// and a number, then a later record in the same batch: the key ends with one type and
    /// nothing is skipped as a key of mixed type.
    #[test]
    fn two_names_for_one_new_key_never_give_it_two_types() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);
        let b = insert_source(&conn, root, "b.jpg", None);

        let outcome = import(
            &conn,
            &[
                record(a, 0, &[("Make", r#""x""#), ("content.Make", "5")]),
                record(b, 0, &[("Make", r#""y""#)]),
            ],
        );

        let types = types_at_end(&conn, &["content.Make"]).unwrap();
        assert!(
            types["content.Make"].single().is_some(),
            "{:?}",
            types["content.Make"]
        );
        assert_eq!(skipped(&outcome, "content.Make", SkipCause::MixedKey), 0);
    }

    /// **A key named twice in one record takes neither value.** A tool's fresh `Model` beside
    /// the `content.Model` passed back: the stored value stands, both are skipped and counted,
    /// and nothing is counted imported. A content hash named twice links no content.
    #[test]
    fn a_key_named_twice_in_one_record_takes_neither_value() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);
        import(&conn, &[record(a, 0, &[("Model", r#""old""#)])]);

        let outcome = import(
            &conn,
            &[record(
                a,
                0,
                &[
                    ("content.Model", r#""old""#),
                    ("Model", r#""new""#),
                    ("hash.sha256", r#""aaa""#),
                    ("content.hash.sha256", r#""bbb""#),
                ],
            )],
        );

        assert_eq!(skipped(&outcome, "content.Model", SkipCause::NamedTwice), 2);
        assert_eq!(
            skipped(&outcome, "content.hash.sha256", SkipCause::NamedTwice),
            2
        );
        assert_eq!(outcome.stats.facts_imported, 0);
        assert_eq!(outcome.stats.objects_created, 0);
        let model: String = conn
            .query_row(
                "SELECT value_text FROM facts WHERE key = 'content.Model'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(model, "old");
    }

    /// **A `null` beside a value under a second name names no key.** `worklist --emit` passes
    /// back `null` for a file without the key; the tool's value under the other name is stored,
    /// and the null is counted as a null.
    #[test]
    fn a_null_beside_a_value_under_a_second_name_names_no_key() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);

        let outcome = import(
            &conn,
            &[record(
                a,
                0,
                &[("content.Lens", "null"), ("Lens", r#""fresh""#)],
            )],
        );

        assert_eq!(outcome.stats.facts_imported, 1);
        assert_eq!(skipped(&outcome, "content.Lens", SkipCause::NoValue), 1);
        assert_eq!(skipped(&outcome, "content.Lens", SkipCause::NamedTwice), 0);
        let lens: String = conn
            .query_row(
                "SELECT value_text FROM facts WHERE key = 'content.Lens'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lens, "fresh");
    }

    /// **A skipped `source.` name is listed under its own file.** A record holding only such
    /// names, after one that stored a value: its line follows its own file's header.
    #[test]
    fn a_skipped_source_name_is_listed_under_its_own_file() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let b = insert_source(&conn, root, "b.jpg", None);
        let c = insert_source(&conn, root, "c.jpg", None);

        let outcome = import(
            &conn,
            &[
                record(b, 0, &[("Lens", r#""x""#)]),
                record(c, 0, &[("source.foo", r#""y""#)]),
            ],
        );

        let order: Vec<String> = outcome
            .verbose
            .iter()
            .map(|line| match line {
                VerboseLine::Source { rel_path, .. } => format!("file {rel_path}"),
                VerboseLine::Stored { key, .. } | VerboseLine::Skipped { key, .. } => key.clone(),
            })
            .collect();
        assert_eq!(
            order,
            ["file b.jpg", "content.Lens", "file c.jpg", "source.foo"]
        );
    }

    /// **The key's type is read under the write lock.** Another writer changes the key from
    /// text to number while the import waits for the lock; once it has the lock, the import
    /// reads the key as it now is and stores `"12"` as a number. Two connections to one file,
    /// since an in-memory database has one.
    #[test]
    fn an_import_and_a_type_change_never_interleave_on_one_key() {
        static WAITING: AtomicBool = AtomicBool::new(false);
        fn wait_for_the_lock(_: i32) -> bool {
            WAITING.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
            true
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("canon.db");
        let changer = open_file(&path);
        let root = insert_root(&changer, "/photos", "source", false);
        let before = insert_source(&changer, root, "a.jpg", None);
        let arriving = insert_source(&changer, root, "b.jpg", None);
        insert_fact(&changer, before, "content.K", "5");

        changer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let importer = open_file(&path);
        importer.busy_handler(Some(wait_for_the_lock)).unwrap();
        let import = std::thread::spawn(move || {
            import_batch(
                &importer,
                &[record(arriving, 0, &[("content.K", r#""12""#)])],
                false,
            )
            .unwrap()
        });
        let started = std::time::Instant::now();
        while !WAITING.load(Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the import never waited for the lock"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        changer
            .execute_batch(
                "UPDATE facts SET value_num = CAST(value_text AS REAL), value_text = NULL \
                 WHERE key = 'content.K'; COMMIT",
            )
            .unwrap();

        let outcome = import.join().unwrap();
        assert_eq!(outcome.stats.facts_imported, 1);
        let types = types_at_end(&changer, &["content.K"]).unwrap();
        assert_eq!(
            types["content.K"].single(),
            Some(FactType::Num),
            "one type: the import read the key after the change"
        );
        let stored: Option<f64> = changer
            .query_row(
                "SELECT value_num FROM facts WHERE key = 'content.K' AND entity_id = ?1",
                [arriving],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stored, Some(12.0));
    }

    /// **A record that fails is undone whole, the type it gave a key included.** The first
    /// record gives a new key its first type (a number) and then fails on its next value; the
    /// second record's text for the key finds a key with no type, and lands as text.
    #[test]
    fn a_record_that_fails_mid_batch_leaves_no_type_and_no_count() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let failing = insert_source(&conn, root, "a.jpg", None);
        let next = insert_source(&conn, root, "b.jpg", None);
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_b BEFORE INSERT ON facts WHEN NEW.key = 'content.B' \
             BEGIN SELECT RAISE(ABORT, 'planted'); END",
        )
        .unwrap();

        let outcome = import(
            &conn,
            &[
                record(failing, 0, &[("content.A", "5"), ("content.B", r#""x""#)]),
                record(next, 0, &[("content.A", r#""abc""#)]),
            ],
        );

        assert_eq!(outcome.warnings.len(), 1);
        assert!(
            outcome.warnings[0]
                .starts_with(&format!("Failed to process source_id {failing}: planted")),
            "{:?}",
            outcome.warnings
        );
        assert_eq!(outcome.stats.records, 2);
        assert_eq!(
            outcome.stats.facts_imported, 1,
            "the failed record's number is not counted"
        );
        assert!(outcome.stats.skipped_values.is_empty());
        let stored: Vec<(i64, Option<String>)> = conn
            .prepare("SELECT entity_id, value_text FROM facts WHERE key = 'content.A'")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(stored, [(next, Some("abc".to_string()))]);
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

    #[test]
    fn each_written_form_is_stored_with_its_form_offset_and_precision() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);

        // What was written, then the form, offset, precision and fraction it is stored with.
        type Case = (
            &'static str,
            &'static str,
            Option<i64>,
            &'static str,
            Option<i64>,
        );
        let cases: [Case; 13] = [
            (
                r#""2023-12-31T21:00:00-05:00""#,
                "instant_offset",
                Some(-18000),
                "second",
                None,
            ),
            (
                r#""2023-12-31T23:30:00Z""#,
                "instant_offset",
                Some(0),
                "second",
                None,
            ),
            (
                r#""2023-12-31T23:30:00+00:00""#,
                "instant_offset",
                Some(0),
                "second",
                None,
            ),
            (
                r#""2023-12-31T23:30:00-00:00""#,
                "instant",
                None,
                "second",
                None,
            ),
            (r#""2023:12:31 21:00:00""#, "clock", None, "second", None),
            (r#""2024""#, "clock", None, "year", None),
            (r#""2024-06""#, "clock", None, "month", None),
            (r#""2024-06-15""#, "clock", None, "day", None),
            (r#""2024-06-15T14:30""#, "clock", None, "minute", None),
            (
                r#""2024-06-15T14:30:05.45+02:00""#,
                "instant_offset",
                Some(7200),
                "fraction",
                Some(450_000_000),
            ),
            (
                r#""2024:07:23 11:06:32.45+02:00""#,
                "instant_offset",
                Some(7200),
                "fraction",
                Some(450_000_000),
            ),
            ("2019", "clock", None, "year", None),
            ("2019.0", "clock", None, "year", None),
        ];
        let names: Vec<String> = (0..cases.len())
            .map(|i| format!("content.form{i}"))
            .collect();
        let wrapped: Vec<String> = cases
            .iter()
            .map(|(v, ..)| format!(r#"{{"value": {v}, "type": "datetime"}}"#))
            .collect();
        let facts: Vec<(&str, &str)> = names
            .iter()
            .zip(&wrapped)
            .map(|(n, w)| (n.as_str(), w.as_str()))
            .collect();

        let outcome = import(&conn, &[record(src, 0, &facts)]);

        assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
        assert!(outcome.stats.skipped_values.is_empty());
        for (i, (written, form, offset, precision, nanos)) in cases.iter().enumerate() {
            assert_eq!(
                time_columns(&conn, src, &format!("content.form{i}")),
                Some((form.to_string(), *offset, precision.to_string(), *nanos)),
                "{written}"
            );
        }
    }

    /// A value that does not read as its datetime hint is skipped and counted under that
    /// cause, its `--verbose` line naming it as written — never stored, not even as a clock
    /// reading, and never read as epoch seconds.
    #[test]
    fn an_unreadable_time_is_skipped_counted_and_warned_never_stored() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);

        let written = [
            r#""2020:07:23 11:06:32é""#,
            r#""2024-02-30""#,
            r#""2024-06-15+02:00""#,
            r#""9999-12-31T23:00:00-05:00""#,
            // A number with the time hint is a year or nothing — never epoch seconds.
            "1.5e9",
            "2019.5",
        ];
        let names: Vec<String> = (0..written.len())
            .map(|i| format!("content.bad{i}"))
            .collect();
        let wrapped: Vec<String> = written
            .iter()
            .map(|v| format!(r#"{{"value": {v}, "type": "datetime"}}"#))
            .collect();
        let facts: Vec<(&str, &str)> = names
            .iter()
            .zip(&wrapped)
            .map(|(n, w)| (n.as_str(), w.as_str()))
            .collect();

        let outcome = import(&conn, &[record(src, 0, &facts)]);

        assert_eq!(outcome.stats.facts_imported, 0);
        assert_eq!(outcome.stats.values_skipped(), written.len() as u64);
        let cause = SkipCause::HintDoesNotRead(KnownHint::Datetime);
        for (name, value) in names.iter().zip(written) {
            assert_eq!(skipped(&outcome, name, cause.clone()), 1, "{name}");
            assert!(
                outcome.verbose.contains(&VerboseLine::Skipped {
                    key: name.clone(),
                    written: value.to_string(),
                    cause: cause.clone(),
                }),
                "{name}: {:?}",
                outcome.verbose
            );
        }
        assert_eq!(
            stored_on_source_count(&conn, src),
            0,
            "nothing is stored, not even as a clock reading"
        );
    }

    #[test]
    fn an_unhinted_number_stays_a_number() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let src = insert_source(&conn, root, "photo.jpg", None);

        import(&conn, &[record(src, 0, &[("content.When", "1500000000")])]);

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
        let written = [
            r#""2023-12-31T21:00:00-05:00""#,
            r#""2023-12-31T23:30:00-00:00""#,
            r#""2023:12:31 21:00:00""#,
            r#""2024""#,
            r#""2024-06-15T14:30:05.45+02:00""#,
            "2019",
        ];

        let mut imported = 0;
        for (i, v) in written.iter().enumerate() {
            let src = insert_source(&conn, root, &format!("{i}.jpg"), None);
            let wrapped = format!(r#"{{"value": {v}, "type": "datetime"}}"#);
            let outcome = import(&conn, &[record(src, 0, &[("content.When", &wrapped)])]);
            assert!(outcome.warnings.is_empty(), "{v}: {:?}", outcome.warnings);
            assert!(outcome.stats.skipped_values.is_empty(), "{v}");
            imported += outcome.stats.facts_imported;
        }

        assert_eq!(imported, written.len() as u64);
    }

    /// Facts stored on a source before its content was known are moved to the content when it
    /// becomes known, or dropped where the content already has a value for their key — each
    /// counted under its own word.
    #[test]
    fn facts_moved_to_the_content_and_dropped_are_counted() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let first = insert_source(&conn, root, "a.jpg", None);
        let second = insert_source(&conn, root, "b.jpg", None);
        let hash = [("content.hash.sha256", r#""feed""#)];

        import(
            &conn,
            &[
                record(first, 0, &[("content.Make", r#""Canon""#)]),
                record(
                    second,
                    0,
                    &[
                        ("content.Make", r#""Nikon""#),
                        ("content.Lens", r#""50mm""#),
                    ],
                ),
            ],
        );
        let outcome = import(&conn, &[record(first, 0, &hash), record(second, 0, &hash)]);

        // The first source's Make moves; the second's Make is dropped (the content has one)
        // and its Lens moves.
        assert_eq!(outcome.stats.facts_moved, 2);
        assert_eq!(outcome.stats.facts_dropped, 1);
        assert_eq!(stored_on_source_count(&conn, first), 0);
        assert_eq!(stored_on_source_count(&conn, second), 0);
    }

    /// Only counts above zero are named, each with its unit, singular for one; a null value
    /// and a source fact passed back stand outside the skipped group.
    #[test]
    fn the_summary_names_only_counts_above_zero_with_their_units() {
        let mut stats = ImportStats {
            records: 1204,
            facts_imported: 3880,
            objects_created: 12,
            facts_moved: 9,
            facts_dropped: 2,
            ..ImportStats::default()
        };
        stats.skipped_records.insert(RecordSkip::Stale, 3);
        stats.skipped_records.insert(RecordSkip::Suspended, 2);
        let mut skip = |key: &str, cause: SkipCause, n: u64| {
            stats.skipped_values.insert(
                (key.to_string(), cause),
                Skipped {
                    count: n,
                    values: BTreeMap::from([("x".to_string(), n)]),
                },
            );
        };
        skip("content.A", SkipCause::DoesNotRead(Reading::Time), 40);
        skip("content.B", SkipCause::DoesNotRead(Reading::Number), 1);
        skip("content.B", SkipCause::WouldLose(Loss::LeadingZero), 3);
        skip("content.C", SkipCause::WouldLose(Loss::TrailingZero), 1);
        skip("content.D", SkipCause::NoValue, 2);
        skip("source.mtime", SkipCause::SourceFact, 3);
        assert_eq!(
            stats.compose_summary(),
            "Read 1,204 records: imported 3,880 facts; skipped 5 records: 3 stale, 2 on \
             suspended roots; skipped 45 values: 41 that do not read as their key's type, 4 \
             that would lose what the text carried; 2 null values (nothing stored); 3 values \
             for source facts, which Canon records itself; 12 objects created; 9 facts moved \
             to the content, 2 facts dropped (the content already had a value for their key)."
        );

        let nothing = ImportStats {
            records: 3,
            ..ImportStats::default()
        };
        assert_eq!(
            nothing.compose_summary(),
            "Read 3 records: imported 0 facts."
        );

        let mut ones = ImportStats {
            records: 1,
            facts_imported: 1,
            objects_created: 1,
            facts_dropped: 1,
            ..ImportStats::default()
        };
        ones.skipped_records.insert(RecordSkip::Archived, 1);
        for cause in [
            SkipCause::NotExact,
            SkipCause::NoValue,
            SkipCause::SourceFact,
        ] {
            ones.skipped_values.insert(
                ("content.K".to_string(), cause),
                Skipped {
                    count: 1,
                    values: BTreeMap::new(),
                },
            );
        }
        assert_eq!(
            ones.compose_summary(),
            "Read 1 record: imported 1 fact; skipped 1 record: 1 in archive roots; skipped 1 \
             value: 1 that is a number Canon cannot hold exactly; 1 null value (nothing \
             stored); 1 value for a source fact, which Canon records itself; 1 object created; \
             1 fact dropped (the content already had a value for its key)."
        );
    }

    #[test]
    fn the_early_stop_names_what_the_committed_batches_hold() {
        let error = anyhow::anyhow!("database is locked");
        assert_eq!(
            ImportStats::default().compose_early_stop(&error),
            "Import ended early before anything was imported: database is locked"
        );
        let mut stats = ImportStats {
            records: 1204,
            facts_imported: 3880,
            ..ImportStats::default()
        };
        assert_eq!(
            stats.compose_early_stop(&error),
            "Import ended early after 1,204 records (3,880 facts imported in committed \
             batches, which stand): database is locked"
        );
        stats.skipped_records.insert(RecordSkip::Stale, 5);
        stats.skipped_values.insert(
            ("content.A".to_string(), SkipCause::NotExact),
            Skipped {
                count: 45,
                values: BTreeMap::new(),
            },
        );
        assert_eq!(
            stats.compose_early_stop(&error),
            "Import ended early after 1,204 records (3,880 facts imported in committed \
             batches, which stand; 5 records and 45 values skipped): database is locked"
        );
    }

    #[test]
    fn the_most_frequent_value_breaks_a_tie_by_text_order() {
        let skipped = Skipped {
            count: 5,
            values: BTreeMap::from([
                ("\"b\"".to_string(), 2),
                ("\"a\"".to_string(), 2),
                ("\"c\"".to_string(), 1),
            ]),
        };
        assert_eq!(skipped.most_frequent(), Some("\"a\""));
    }

    // =========================================================================
    // promote_content_facts
    // =========================================================================

    fn stored_on_source_count(conn: &Connection, source_id: i64) -> i64 {
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

        let (moved, dropped) = promote_content_facts(&conn, src, obj).unwrap();

        assert_eq!((moved, dropped), (1, 0));
        assert_eq!(
            object_fact_value(&conn, obj, "content.Make").as_deref(),
            Some("Canon")
        );
        assert_eq!(stored_on_source_count(&conn, src), 0);
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

        let (moved, dropped) = promote_content_facts(&conn, src, obj).unwrap();

        // Nothing was gained, so nothing moved; the source's copy is dropped...
        assert_eq!((moved, dropped), (0, 1));
        // ...the object keeps the value it already had...
        assert_eq!(
            object_fact_value(&conn, obj, "content.Make").as_deref(),
            Some("Canon")
        );
        // ...and the redundant source copy goes regardless.
        assert_eq!(stored_on_source_count(&conn, src), 0);
    }
}
