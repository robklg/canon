# Story: Receipt Infrastructure Foundation

**Design Spec**: [~/store/claude-designs/2026-04-11-decision-receipts.md](~/store/claude-designs/2026-04-11-decision-receipts.md)
**Lineage Spec**: [~/store/claude-designs/2026-04-12-destination-provenance.md](~/store/claude-designs/2026-04-12-destination-provenance.md)
**Epic**: [epic-decision-receipts.md](epic-decision-receipts.md)
**Status**: Complete
**Created**: 2026-04-12

## Objective

Establish the infrastructure foundation for decision receipts and source lineage. This story builds config loading, schema changes for all three tables (sources, decisions, decision_scopes), the DecisionParams/DecisionRecorder rework for the new recording mode, the source `decision_id` column with scan's conservative behavior, CLI flag migration (`--no-record` → config, `--no-receipt`), scan exclusion of `.canon-ledger/`, and the atomic write primitives.

No receipt files are written in this story. The infrastructure is in place but inert — receipt writing is Story 2.

## Functional Requirements Summary

**Config**: `$CANON_HOME/config.toml` with `[ledger]` section. Three settings: `recording` (full/records/off), `layout` (central/alongside), `root` (archive root ID). Created on first run with inline documentation. Invalid values fall back to defaults with warnings. Config errors never prevent Canon from operating.

**Recording mode**: Config `recording` setting controls whether DB records are written. `--no-receipt` (new global flag) suppresses receipt generation only. `--no-record` is removed (clean break, pre-1.0).

**Schema**: `sources.decision_id` (nullable), `decisions.receipt_root_id` + `decisions.receipt_rel_path` (nullable), new `decision_scopes` table.

**Source decision_id**: Set by `insert_destination()` for apply. Set on INSERT only for scan (new files). Scan UPDATEs (Modified, Moved, Unchanged) preserve existing value. Semantics: "the decision that caused the most recent state transition."

**Scan exclusion**: `.canon-ledger/` directories excluded from scanning via WalkDir `filter_entry()`.

**Atomic write**: `write_file_incomplete()` and `finalize_file()` primitives in `ops/fs.rs` for Story 2 to use.

## Current State

**DecisionRecorder** (`ops/decision.rs`): Works with `DecisionParams.enabled: bool`. `start()` checks enabled, INSERTs "started" record, returns recorder. `complete()` UPDATEs with status/counts/summary. Catches own errors, collects warnings.

**`insert_started()`** (`repo/decision.rs`): Takes command, scope, command_line, reason, canon_version. Returns row ID. No receipt columns.

**`insert_destination()`** (`repo/source.rs`): Two-step UPDATE→INSERT→SELECT pattern. No decision_id parameter.

**`apply_reconciliation()`** (`repo/source.rs`): Handles New (INSERT), Unchanged (UPDATE last_seen_at), Modified (UPDATE metadata), Moved (UPDATE path). No decision_id parameter.

**CLI**: `--no-record` is a global flag on `Cli` struct. `no_record: bool` threaded to all command modules.

**Scan walk**: `WalkDir::new(&walk_path).follow_links(false)` in `src/scan.rs`. No directory exclusion.

**Config**: No config.toml exists. `aliases.toml` is the closest pattern.

**Dependencies**: `toml = "0.8"`, `serde` with derive, `serde_json = "1"` — all present in Cargo.toml. No new crates needed.

## Design

### Phase 1: Config types + loading

- **Goal**: Introduce `LedgerConfig` and config file loading. Config is available in main.rs before dispatch.

#### Changes

**New `src/domain/config.rs`**:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum RecordingMode {
    Full,
    Records,
    Off,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReceiptLayout {
    Central,
    Alongside,
}

#[derive(Debug, Clone)]
pub struct LedgerConfig {
    pub recording: RecordingMode,
    pub layout: ReceiptLayout,
    pub root: Option<i64>,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            recording: RecordingMode::Full,
            layout: ReceiptLayout::Central,
            root: None,
        }
    }
}

/// Parse a config file's content into LedgerConfig.
/// Returns defaults for any invalid or missing fields, with warnings.
pub fn parse_ledger_config(content: &str) -> (LedgerConfig, Vec<String>) {
    // Uses toml::Value for flexible parsing (unknown keys ignored).
    // Per-field validation: invalid value → default + warning.
}
```

Register module: add `pub mod config;` to `src/domain/mod.rs`.

**New in `src/main.rs`**:

```rust
fn load_or_create_config(canon_home: &Path) -> (LedgerConfig, Vec<String>) {
    let path = canon_home.join("config.toml");
    if !path.exists() {
        // Create with defaults + inline docs, emit one-time message
        // On write failure: warning, return defaults
        return (LedgerConfig::default(), warnings);
    }
    // Read + parse. On read failure: warning, return defaults.
}

fn write_default_config(path: &Path) -> Result<()> {
    // Write DEFAULT_CONFIG_CONTENT (the full inline-documented template)
}
```

Integration in `main()`:
```rust
let canon_home = resolve_canon_home(cli.canon_home.as_deref())?;
let (config, config_warnings) = load_or_create_config(&canon_home);
for w in &config_warnings { eprintln!("{w}"); }
// ... existing DB open ...
// After DB open: validate config.root is archive (not source)
if let Some(root_id) = config.root {
    let roots = repo::root::fetch_all(db.conn())?;
    if let Some(root) = roots.iter().find(|r| r.id == root_id) {
        if root.is_source() {
            bail!("Ledger root (id:{root_id}) is a source root, not an archive. \
                   Update [ledger].root in {}", canon_home.join("config.toml").display());
        }
    }
}
```

**`DEFAULT_CONFIG_CONTENT`**: The full self-documenting template from the functional spec — all settings with defaults, explanations, and alternatives inline as comments.

#### Tests

- `test_default_config` — Default struct has Full/Central/None
- `test_parse_valid_full` — All three settings, all valid values
- `test_parse_recording_full` / `test_parse_recording_records` / `test_parse_recording_off` — Each value
- `test_parse_recording_invalid` — Unknown value → Full + warning
- `test_parse_layout_central` / `test_parse_layout_alongside` — Each value
- `test_parse_layout_invalid` — Unknown value → Central + warning
- `test_parse_root_present` — Integer root ID parses
- `test_parse_root_absent` — Missing → None
- `test_parse_empty_content` — All defaults, no warnings
- `test_parse_unknown_keys_ignored` — Extra keys → no errors
- `test_parse_unknown_sections_ignored` — Extra `[foo]` → no errors

### Phase 2: Schema changes

- **Goal**: All three table changes in place. Domain structs updated. All existing code compiles with new fields.

#### Changes

**`repo/db.rs`** — modify `decisions` CREATE TABLE:

Add after existing columns:
```sql
    receipt_root_id INTEGER,
    receipt_rel_path TEXT
```

**`repo/db.rs`** — modify `sources` CREATE TABLE:

Add after existing columns:
```sql
    decision_id INTEGER
```

**`repo/db.rs`** — add `decision_scopes` table:

```sql
CREATE TABLE IF NOT EXISTS decision_scopes (
    decision_id INTEGER NOT NULL,
    root_id INTEGER NOT NULL,
    rel_prefix TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS decision_scopes_decision_id ON decision_scopes(decision_id);
CREATE INDEX IF NOT EXISTS decision_scopes_root_id ON decision_scopes(root_id);
```

**`domain/decision.rs`** — `Decision` struct gains:
```rust
pub receipt_root_id: Option<i64>,
pub receipt_rel_path: Option<String>,
```

**`domain/source.rs`** — `Source` struct gains:
```rust
pub decision_id: Option<i64>,
```

**`domain/source.rs`** — `NewSource` struct gains:
```rust
pub decision_id: Option<i64>,
```

**`repo/source.rs`** — all SELECT queries that build `Source` structs gain `decision_id` column. All row extraction gains the new field. The SOURCE_COLUMNS constant (or equivalent) is updated.

**`repo/decision.rs`** — `fetch_by_id()` (test helper) gains receipt columns in SELECT and row extraction.

**All call sites constructing `NewSource`** gain `decision_id: None` — mechanical, compiler-guided. This includes test helpers in `ops/test_helpers.rs`.

**Migration SQL** for existing databases:
```sql
ALTER TABLE decisions ADD COLUMN receipt_root_id INTEGER;
ALTER TABLE decisions ADD COLUMN receipt_rel_path TEXT;
ALTER TABLE sources ADD COLUMN decision_id INTEGER;
```

#### Tests

- `test_decisions_receipt_columns_exist` — INSERT with receipt_root_id + receipt_rel_path, SELECT verifies
- `test_sources_decision_id_exists` — INSERT with decision_id, SELECT verifies
- `test_sources_decision_id_nullable` — INSERT with NULL decision_id succeeds
- `test_decision_scopes_insert_and_select` — Basic round-trip
- `test_decision_scopes_multiple_per_decision` — Multiple entries for one decision_id

### Phase 3: DecisionParams + DecisionRecorder rework

- **Goal**: Recording mode split (record_enabled/receipt_enabled), decision_id exposure, receipt column placeholders, atomic write primitives.

#### Changes

**`ops/decision.rs`** — `DecisionParams` becomes:

```rust
pub struct DecisionParams {
    pub command: DecisionCommand,
    pub scope: Option<Vec<String>>,
    pub command_line: String,
    pub reason: Option<String>,
    pub record_enabled: bool,
    pub receipt_enabled: bool,
    pub ledger_config: LedgerConfig,
}
```

**`ops/decision.rs`** — `DecisionRecorder` changes:

```rust
impl DecisionRecorder {
    pub fn start(conn: &Connection, params: &DecisionParams) -> Self {
        if !params.record_enabled {
            return Self { id: None, warnings: vec![] };
        }
        // INSERT with receipt_root_id: None, receipt_rel_path: None
        // (Story 2 will populate these)
        // ...
    }

    /// Expose the decision ID for receipt writing and source decision_id.
    pub fn decision_id(&self) -> Option<i64> {
        self.id
    }

    pub fn complete(
        &mut self,
        conn: &Connection,
        status: DecisionStatus,
        counts: DecisionCounts,
        summary: &str,
    ) {
        // ... existing UPDATE logic ...
        // Story 2 will add .incomplete → .toml rename here
    }

    // interrupted() and take_warnings() unchanged
}
```

**`repo/decision.rs`** — `insert_started()` gains receipt columns:

```rust
pub fn insert_started(
    conn: &Connection,
    command: &str,
    scope: Option<&[String]>,
    command_line: &str,
    reason: Option<&str>,
    canon_version: &str,
    receipt_root_id: Option<i64>,
    receipt_rel_path: Option<&str>,
) -> Result<i64>
```

SQL gains `receipt_root_id` and `receipt_rel_path` in INSERT.

**`ops/fs.rs`** — new primitives:

```rust
/// Write content to a .incomplete file at the same location as path.
/// The .incomplete file survives crashes as recoverable evidence.
pub fn write_file_incomplete(path: &Path, content: &[u8]) -> Result<()> {
    let incomplete = path.with_extension("incomplete");
    if let Some(parent) = incomplete.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&incomplete, content)?;
    Ok(())
}

/// Rename .incomplete to the final path (e.g., .toml).
pub fn finalize_file(path: &Path) -> Result<()> {
    let incomplete = path.with_extension("incomplete");
    std::fs::rename(&incomplete, path)?;
    Ok(())
}
```

#### Tests

- `test_recorder_record_enabled_creates_row` — record_enabled=true creates DB row
- `test_recorder_record_disabled_no_row` — record_enabled=false, no row
- `test_recorder_receipt_disabled_still_records_db` — record_enabled=true, receipt_enabled=false → DB row exists
- `test_recorder_decision_id_some_when_enabled` — decision_id() returns Some
- `test_recorder_decision_id_none_when_disabled` — decision_id() returns None
- `test_recorder_insert_started_receipt_columns_null` — receipt_root_id and receipt_rel_path are NULL
- `test_recorder_complete_updates` — Existing behavior preserved
- `test_recorder_warnings_collected` — Existing behavior preserved
- `test_write_file_incomplete_creates_file` — .incomplete file exists with correct content
- `test_finalize_file_renames` — .incomplete renamed to target path
- `test_finalize_file_missing_incomplete_errors` — No .incomplete → error

### Phase 4: Source decision_id integration

- **Goal**: Scan and apply write paths set `decision_id` on source records correctly.

#### Changes

**`repo/source.rs` `insert_destination()`** — add `decision_id` to both SQL paths:

UPDATE path (existing record):
```sql
UPDATE sources SET
    present = 1, excluded = 0,
    device = COALESCE(?, device), inode = COALESCE(?, inode),
    size = ?, mtime = ?, partial_hash = ?,
    object_id = ?, basis_rev = basis_rev + 1,
    scanned_at = ?, last_seen_at = ?,
    decision_id = ?
WHERE root_id = ? AND rel_path = ?
```

INSERT path (new record):
```sql
INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
    partial_hash, object_id, basis_rev, scanned_at, last_seen_at,
    present, excluded, decision_id)
VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?, ?, ?, 0, ?, ?, 1, 0, ?)
```

Value is `new.decision_id` (from `NewSource`).

**`repo/source.rs` `apply_reconciliation()`** — gains `decision_id: Option<i64>` parameter:

```rust
pub fn apply_reconciliation(
    conn: &Connection,
    observation: &FileObservation,
    reconciliation: &Reconciliation,
    now: i64,
    decision_id: Option<i64>,
) -> Result<Source>
```

**New path** (INSERT and UPDATE-stale): includes `decision_id` in SET clause.

**Unchanged path**: `UPDATE sources SET device = ?, inode = ?, last_seen_at = ? WHERE id = ?` — **no decision_id**.

**Modified path**: `UPDATE sources SET device = ?, inode = ?, size = ?, mtime = ?, partial_hash = ?, basis_rev = basis_rev + 1, last_seen_at = ? WHERE id = ?` — **no decision_id**.

**Moved path**: `UPDATE sources SET root_id = ?, rel_path = ?, device = ?, inode = ?, last_seen_at = ? WHERE id = ?` — **no decision_id**.

Only INSERT/UPDATE-stale paths set `decision_id`. All UPDATE paths for existing records preserve it by omission.

**`ops/scan.rs` `scan_root()`** — pass decision_id through to apply_reconciliation. The scan function already has access to the `DecisionRecorder` (via the interface layer). The decision_id flows:

```
interface: recorder = DecisionRecorder::start(conn, &decision)
           → pass recorder.decision_id() to scan_root()
ops:       scan_root() passes decision_id to apply_reconciliation()
repo:      apply_reconciliation() uses it on INSERT only
```

This requires `scan_root()` to accept `decision_id: Option<i64>` as a parameter (or the `ScanOptions` struct gains it).

**All call sites of `apply_reconciliation()` in tests** gain `decision_id: None`.

#### Tests

- `test_insert_destination_sets_decision_id` — New destination has decision_id
- `test_insert_destination_updates_decision_id` — Re-insert overwrites decision_id
- `test_insert_destination_null_decision_id` — None works correctly
- `test_scan_new_sets_decision_id` — New reconciliation with decision_id
- `test_scan_unchanged_preserves_decision_id` — Pre-set value survives Unchanged
- `test_scan_modified_preserves_decision_id` — Pre-set value survives Modified
- `test_scan_moved_preserves_decision_id` — Pre-set value survives Moved
- `test_scan_new_null_when_disabled` — decision_id=None → NULL in DB

### Phase 5: CLI migration + scan exclusion + config threading

- **Goal**: Replace `--no-record` with config-based recording. Add `--no-receipt`. Thread config to all command modules. Exclude `.canon-ledger/` from scanning.

#### Changes

**`src/main.rs` Cli struct**:

```rust
struct Cli {
    #[arg(long, global = true)]
    canon_home: Option<PathBuf>,
    #[arg(long, global = true)]
    debug_sql: bool,
    #[arg(long, global = true)]
    profile: bool,
    // REMOVED: no_record: bool,
    #[arg(long, global = true)]
    no_receipt: bool,  // NEW
    #[command(subcommand)]
    command: Commands,
}
```

**`src/main.rs` dispatch**: Every command function call that currently receives `cli.no_record` switches to receiving `&config` and `cli.no_receipt`. The `command_line` threading is unchanged.

**All command modules** — update DecisionParams construction. Using `src/exclude.rs` `make_decision()` as the reference pattern:

```rust
// Before
fn make_decision(
    command: DecisionCommand,
    scope: Option<Vec<String>>,
    command_line: &str,
    no_record: bool,
    reason: Option<&str>,
    dry_run: bool,
) -> DecisionParams {
    DecisionParams {
        command, scope,
        command_line: command_line.to_string(),
        reason: reason.map(|r| r.to_string()).filter(|r| !r.trim().is_empty()),
        enabled: !no_record && !dry_run,
    }
}

// After
fn make_decision(
    command: DecisionCommand,
    scope: Option<Vec<String>>,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
    dry_run: bool,
) -> DecisionParams {
    DecisionParams {
        command, scope,
        command_line: command_line.to_string(),
        reason: reason.map(|r| r.to_string()).filter(|r| !r.trim().is_empty()),
        record_enabled: config.recording != RecordingMode::Off && !dry_run,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
        ledger_config: config.clone(),
    }
}
```

Commands that inline the pattern (scan.rs, apply.rs, roots.rs, cluster.rs, import_facts.rs, facts.rs, note.rs) follow the same transformation. The compiler catches every call site.

**`src/scan.rs` scan exclusion** — change WalkDir creation:

```rust
// Before
let walker = WalkDir::new(&walk_path).follow_links(false);

// After
let walker = WalkDir::new(&walk_path)
    .follow_links(false)
    .into_iter()
    .filter_entry(|e| {
        !(e.file_type().is_dir() && e.file_name() == ".canon-ledger")
    });
```

The walker type changes. If `ops/scan.rs` takes a concrete `IntoIter` type, it needs to become generic or take `impl Iterator<Item = Result<DirEntry>>`. Check `scan_root()` signature — it currently takes `impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>>`, so `filter_entry()` should be compatible since `FilterEntry` implements the same iterator.

**`src/scan.rs`** — pass `recorder.decision_id()` to `scan_root()`:

```rust
// Before
let result = scan_root(conn, walker, &root, &options, &progress)?;

// After
let result = scan_root(conn, walker, &root, &options, &progress, recorder.decision_id())?;
```

Or via `ScanOptions`:
```rust
options.decision_id = recorder.decision_id();
```

#### Tests

- `test_recording_off_no_db_record` — config recording=Off, execute, no decision row
- `test_recording_records_db_only` — config recording=Records, decision row exists, receipt_enabled=false
- `test_recording_full_both` — config recording=Full, decision row, receipt_enabled=true
- `test_scan_skips_canon_ledger_dir` — Directory with files inside .canon-ledger/, scan finds no sources from it
- `test_scan_does_not_skip_similar_names` — `.canon-ledger-old/` or `canon-ledger/` (no dot) ARE scanned

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Config in domain layer (pure parsing) | No I/O in domain. `parse_ledger_config()` takes a string, returns typed config + warnings. File I/O is in main.rs. |
| Config loads before DB | `recording = "off"` must be known before any DB writes. Root validation needs DB, so it happens after DB open — split validation. |
| `record_enabled` + `receipt_enabled` replace `enabled` | Two independent concerns. DB recording controlled by config. Receipt generation controlled by config + `--no-receipt` flag. |
| `decision_id` on source records via existing write paths | `insert_destination()` and `apply_reconciliation()` already own the SQL. Adding one column to existing statements is minimal. |
| Scan INSERT-only for decision_id | Conservative: scan receipts aren't designed yet. UPDATEs (Modified, Moved, Unchanged) preserve existing provenance. Full semantics deferred to scan receipt design. |
| `.canon-ledger/` exclusion in WalkDir filter_entry | Interface owns walk configuration (established architecture). filter_entry() prevents descending into excluded directories. |
| `--no-record` removal is clean break | Pre-1.0. Config `recording` is the proper control. No deprecation period needed. |
| `write_file_incomplete` / `finalize_file` as separate primitives | Caller controls the lifecycle: write .incomplete, do more work, rename on completion. Interrupted operations leave .incomplete as evidence. |

## Non-Goals

- Writing receipt files (Story 2)
- Receipt path computation or placement logic (Story 2)
- Plan struct enrichment for exclusion receipts (Story 3)
- Scope index population (Story 4)
- `previous_decision_id` in receipt items (Stories 2 + 3)
- Scan receipt design or full scan decision_id semantics

## Test Plan

### Existing Tests (Must Pass)

All existing tests. The `DecisionParams.enabled` → `record_enabled` rename and `--no-record` removal will break existing test code that constructs `DecisionParams` — fix mechanically (compiler-guided).

### New Tests

~38 new tests across 5 phases. See per-phase test sections above.

## Implementation Checklist

- [x] Phase 1: Config types + loading (domain/config.rs, main.rs)
- [x] Phase 2: Schema changes (3 tables, domain structs, repo queries)
- [x] Phase 3: DecisionParams/DecisionRecorder rework + ops/fs primitives
- [x] Phase 4: Source decision_id in insert_destination + apply_reconciliation
- [x] Phase 5: CLI migration (--no-record → --no-receipt) + config threading + scan exclusion
- [x] Verify all existing tests pass
- [x] Update CLAUDE.md: domain/config.rs, sources.decision_id, decisions receipt columns, decision_scopes table, --no-receipt flag, config.toml in Canon Home section, recording mode

## Documentation Updates

- **New page or section**: `config.toml` reference — `[ledger]` section with all settings
- **Update**: Decision provenance page — recording modes (full/records/off), config vs flag control
- **Update**: Command reference — `--no-receipt` flag, `--no-record` removal
- **CLAUDE.md**: domain/config.rs module, sources.decision_id column, decisions receipt columns, decision_scopes table, --no-receipt in CLI Flag Vocabulary, config.toml in Canon Home section

## Backward Compatibility

- **Breaking**: `--no-record` flag removed. Pre-1.0 clean break. Users must switch to `recording = "off"` in config.
- **Additive**: All schema changes are nullable columns or new tables. Existing databases gain them on next open (via CREATE TABLE IF NOT EXISTS). Manual ALTER for the two existing installs.
- **No output changes**: Config loading is silent except for the one-time creation message and any validation warnings.

## Performance Considerations

- Config loaded once at startup — negligible cost.
- Schema changes add nullable columns — no impact on existing queries (SQLite handles this efficiently).
- `decision_id` added to existing INSERT/UPDATE statements — one extra bind parameter, negligible.
- Scan exclusion via `filter_entry()` — one string comparison per directory entry, negligible.
