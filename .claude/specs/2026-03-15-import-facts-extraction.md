# Refactoring Spec: import_facts Extraction to Ops Layer

## Overview

Extract all business logic from `src/import_facts.rs` (interface, 579 lines, zero tests) to `src/ops/import_facts.rs` (new module). The last command module with significant business logic in the interface layer. After extraction, the interface is a thin stdin reader + display layer.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` — Phase 3, Step 2

## Scope

### In scope
- Create `src/ops/import_facts.rs` with all business logic
- Move: `FactImport`, `ImportStats`, `TypedValue`, `process_import`, `try_parse_datetime`, `year_to_timestamp`, `try_parse_duration`, `classify_value`, `classify_typed_value`, `get_value_type`, `get_typed_value_type`, `promote_content_facts`, `build_fact_type_map`, `current_timestamp`
- Replace inline `eprintln!` calls with structured return data (`RecordOutcome`)
- Thin interface to: read stdin, parse JSONL, call ops, display warnings/stats
- Add tests for pure functions (currently zero coverage)
- Register module in `ops/mod.rs`
- Update CLAUDE.md

### Non-goals
- Changing import behavior or semantics
- Adding batch/transaction scope redesign
- Changing the streaming (per-record) processing model
- Adding new validation logic

## Design

### New types in ops/import_facts

```rust
/// Deserialized JSONL record from stdin.
#[derive(Deserialize)]
pub struct ImportRecord {
    pub source_id: i64,
    pub basis_rev: i64,
    #[serde(default = "current_timestamp")]
    pub observed_at: i64,
    pub facts: HashMap<String, Value>,
    // Worklist pass-through fields (accepted, ignored)
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

/// Outcome of processing a single import record.
pub struct RecordOutcome {
    /// Warning messages for display (e.g., "source_id 42 not found", "type mismatch for key")
    pub warnings: Vec<String>,
    /// Verbose progress lines (e.g., "[/photos] file.jpg", "  content.Make: Canon (on object)")
    pub verbose_lines: Vec<String>,
}
```

### New functions in ops/import_facts

```rust
/// Initialize import state by loading the existing fact type map from the database.
pub fn init_state(conn: &Connection) -> Result<ImportState>;

/// Process a single import record. All business logic lives here:
/// - Source lookup and validation (exists, not stale, not archived)
/// - Key normalization (content.* prefix)
/// - Hash detection and object creation/linking
/// - Fact promotion (source → object) on first hash
/// - Type consistency checking
/// - Value classification (plain, datetime hint, duration hint)
/// - Fact upsert (object fact or source fact)
///
/// Returns structured outcome — no stdout/stderr.
pub fn process_record(
    conn: &mut Connection,
    record: &ImportRecord,
    state: &mut ImportState,
    allow_archived: bool,
) -> Result<RecordOutcome>;
```

### Private helpers (moved from interface, stay private in ops)

All these move unchanged except `eprintln!` calls become entries in `RecordOutcome.warnings`/`verbose_lines`:

- `TypedValue::parse()` — unchanged
- `try_parse_datetime()` — unchanged
- `year_to_timestamp()` — unchanged
- `try_parse_duration()` — unchanged
- `classify_value()` — unchanged
- `classify_typed_value()` — unchanged
- `get_value_type()` — unchanged
- `get_typed_value_type()` — unchanged
- `promote_content_facts()` — unchanged
- `current_timestamp()` — unchanged

### Interface changes to `src/import_facts.rs`

The interface becomes ~60 lines:

```rust
pub fn run(db: &mut Db, allow_archived: bool, verbose: bool) -> Result<()> {
    let conn = db.conn_mut();
    let mut state = ops::import_facts::init_state(conn)?;

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.context("Failed to read line from stdin")?;
        if line.trim().is_empty() { continue; }
        state.stats.lines_processed += 1;

        let record: ImportRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: Failed to parse line {}: {}", state.stats.lines_processed, e);
                continue;
            }
        };

        let outcome = ops::import_facts::process_record(conn, &record, &mut state, allow_archived)?;

        // Display warnings and verbose output
        for warning in &outcome.warnings {
            eprintln!("Warning: {warning}");
        }
        if verbose {
            for line in &outcome.verbose_lines {
                eprintln!("{line}");
            }
        }
    }

    // Display type mismatch summary
    // ... (same as current)

    // Print stats summary
    // ... (same as current)

    db.run_analyze()?;
    Ok(())
}
```

### Warning handling transformation

Current inline warnings in `process_import` become structured data:

| Current (eprintln in ops) | After (RecordOutcome field) |
|---|---|
| `"source_id {} not found"` | `warnings.push(...)` |
| `"source_id {} has basis_rev {} but import has {}"` | `warnings.push(...)` |
| `"skipping fact '{}': {}"` (reserved key) | `warnings.push(...)` |
| `"type mismatch for '{}' in {}: ..."` | `warnings.push(...)` |
| `"cannot parse as duration/datetime: {}"` | `warnings.push(...)` |
| `"[root] rel_path"` (verbose) | `verbose_lines.push(...)` |
| `"  key: value (on object/source)"` (verbose) | `verbose_lines.push(...)` |

The `process_record` function returns `Ok(outcome)` for all cases including skips. The `Err` path is reserved for actual I/O failures (DB errors).

Note: `lines_processed` is incremented by the interface (it counts parsed lines), not by `process_record`. All other stats are incremented by `process_record` via `state.stats`.

## Test Requirements

### Existing tests
None — zero test coverage for import_facts.

### New tests to add in ops/import_facts

**Pure parsing functions:**
1. `test_try_parse_datetime_rfc3339` — "2020-01-15T10:30:00+00:00" → timestamp
2. `test_try_parse_datetime_iso` — "2020-01-15T10:30:00" → timestamp (UTC)
3. `test_try_parse_datetime_exif` — "2020:07:23 11:06:32" → timestamp
4. `test_try_parse_datetime_plain_year` — "2005" → Jan 1 2005 UTC
5. `test_try_parse_datetime_invalid` — "not a date" → None
6. `test_try_parse_duration_hms` — "1:30:45" → 5445.0
7. `test_try_parse_duration_ms` — "3:30" → 210.0
8. `test_try_parse_duration_seconds` — "45.5" → 45.5
9. `test_try_parse_duration_numeric_json` — JSON number 120 → 120.0
10. `test_classify_value_string` — JSON string → (Some(text), None, None)
11. `test_classify_value_number` — JSON number → (None, Some(f64), None)
12. `test_classify_typed_datetime` — hinted datetime → (None, None, Some(timestamp))
13. `test_classify_typed_duration` — hinted duration → (None, Some(seconds), None)
14. `test_classify_typed_unknown_hint` — unknown hint → Err
15. `test_get_typed_value_type_plain` — plain string → Text, plain number → Num
16. `test_get_typed_value_type_hinted` — datetime hint → Time, duration hint → Num

**Integration tests (with DB):**
17. `test_process_record_source_not_found` — returns warning, no crash
18. `test_process_record_stale_skipped` — mismatched basis_rev → skipped_stale + warning
19. `test_process_record_archived_skipped` — archive source without allow → skipped_archived
20. `test_process_record_imports_fact` — basic fact import, verify in DB
21. `test_process_record_hash_links_object` — hash fact creates object, links source
22. `test_process_record_type_mismatch` — existing num, import text → skipped + mismatch recorded

## Design Decisions
- Streaming model preserved — `process_record()` per line, not bulk
- `ImportState` bundles all mutable accumulation (type map, mismatches, stats) into one struct
- `RecordOutcome` carries warnings and verbose lines — ops never writes to stderr
- `lines_processed` is the only stat incremented by the interface (it counts stdin lines, not record processing)
- `process_record` returns `Ok` for all business-level skips; `Err` only for I/O failures
- Verbose lines are always generated — the interface decides whether to display them based on the `--verbose` flag
