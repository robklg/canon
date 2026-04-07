# Story: Decision Provenance

**Design Spec**: [~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md](~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md)
**Epic**: [epic-decision-provenance.md](epic-decision-provenance.md)
**Status**: Complete
**Created**: 2026-04-06

## Objective

Deliver the ground layer of decision provenance: automatic, silent recording of every effectful action, with optional user annotation via `--reason` and per-invocation opt-out via `--no-record`. When the user knows their actions are being recorded durably, it frees them to act — the hesitation before deleting, the second-guessing after excluding, these are friction born from the absence of a safety net for decisions, not just for files.

This story assumes all effectful commands already have proper ops-layer extraction with summary composition in ops (delivered by the Ops Layer Completion story).

## Functional Requirements Summary

From the design spec's four user stories:

**Automatic recording**: Every effectful command writes a decision record in two phases — INSERT "started" after confirmation/before execution, UPDATE with outcome after execution completes. 17 commands record. Silent, no output changes. `--dry-run` and declined confirmations don't record. Recording failure warns, doesn't error.

**`--reason` annotation**: Available on 7 commands where user reasoning is meaningful: `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `apply`, `scan`, `roots rm`. Stored in the decision record. No prompting when omitted.

**Manifest notes flow**: When `apply` executes a manifest with non-empty `# === Notes ===`, that content becomes the decision's reason. Explicit `--reason` takes precedence.

**`--no-record` opt-out**: Global flag on all commands. Suppresses recording for that invocation. `--reason` silently ignored when combined with `--no-record`.

**Effectful commands that record**:

| Command | `command` value | Gets `--reason` |
|---|---|---|
| `scan` | `scan` | Yes |
| `apply` | `apply` | Yes |
| `exclude set` | `exclude_set` | Yes |
| `exclude clear` | `exclude_clear` | Yes |
| `exclude duplicates` | `exclude_duplicates` | Yes |
| `exclude set-object` | `exclude_set_object` | Yes |
| `exclude clear-object` | `exclude_clear_object` | No |
| `cluster generate` | `cluster_generate` | No |
| `cluster refresh` | `cluster_refresh` | No |
| `roots rm` | `roots_rm` | Yes |
| `roots suspend` | `roots_suspend` | No |
| `roots unsuspend` | `roots_unsuspend` | No |
| `import-facts` | `import_facts` | No |
| `prune` | `prune` | No |
| `facts delete` | `facts_delete` | No |
| `note clear` | `note_clear` | No |

## Current State

**Prerequisites** (delivered by Ops Layer Completion story):
- All effectful commands have ops-layer execute functions
- All execute functions return typed results with `summary: String`
- `format_count` is in a shared location accessible by ops

**Existing infrastructure**:
- Signal handling implemented in `ops/apply.rs` — two-tier Ctrl+C with `AtomicBool`
- `ApplyResult` has `interrupted: bool` and `remaining: usize`
- `extract_notes()` exists in `src/cluster.rs` for manifest notes extraction
- `signal-hook` crate already in `Cargo.toml`
- `serde_json` already in `Cargo.toml` (for scope serialization)
- Database uses `CREATE TABLE IF NOT EXISTS` pattern — additive, no migration

## Design

### Phase 1: Infrastructure — schema, domain, repo, ops

- **Goal**: Build the complete decision recording infrastructure, end to end, without integrating into any commands yet.

#### Changes

**Schema addition in `repo/db.rs`**:

Add to the SCHEMA constant:

```sql
-- Decisions: record of effectful actions taken by the user
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    command TEXT NOT NULL,
    scope TEXT,                  -- JSON array of path strings, or NULL for global
    command_line TEXT NOT NULL,
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'started',
    count_attempted INTEGER,
    count_completed INTEGER,
    count_failed INTEGER,
    count_skipped INTEGER,
    summary TEXT,
    canon_version TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS decisions_command ON decisions(command);
CREATE INDEX IF NOT EXISTS decisions_created_at ON decisions(created_at);
```

No CHECK constraints on `command` or `status` — forward-compatible. Existing databases gain the table on next open.

**New `src/domain/decision.rs`**:

```rust
/// Stable command identifiers for decision records.
///
/// These strings are written to the decisions table and become permanent history.
/// Values are append-only: never rename, never reuse for different semantics.
/// When removing a command from Canon, keep its variant here so historical
/// records remain interpretable.
pub enum DecisionCommand {
    Scan,
    Apply,
    ExcludeSet,
    ExcludeClear,
    ExcludeDuplicates,
    ExcludeSetObject,
    ExcludeClearObject,
    ClusterGenerate,
    ClusterRefresh,
    RootsRm,
    RootsSuspend,
    RootsUnsuspend,
    ImportFacts,
    Prune,
    FactsDelete,
    NoteClear,
}

impl DecisionCommand {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Apply => "apply",
            Self::ExcludeSet => "exclude_set",
            Self::ExcludeClear => "exclude_clear",
            Self::ExcludeDuplicates => "exclude_duplicates",
            Self::ExcludeSetObject => "exclude_set_object",
            Self::ExcludeClearObject => "exclude_clear_object",
            Self::ClusterGenerate => "cluster_generate",
            Self::ClusterRefresh => "cluster_refresh",
            Self::RootsRm => "roots_rm",
            Self::RootsSuspend => "roots_suspend",
            Self::RootsUnsuspend => "roots_unsuspend",
            Self::ImportFacts => "import_facts",
            Self::Prune => "prune",
            Self::FactsDelete => "facts_delete",
            Self::NoteClear => "note_clear",
        }
    }
}

pub enum DecisionStatus {
    Started,
    Completed,
    Partial,
    Interrupted,
}

impl DecisionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Interrupted => "interrupted",
        }
    }
}

/// A decision record — what happened, when, why.
pub struct Decision {
    pub id: i64,
    pub command: String,
    pub scope: Option<Vec<String>>,
    pub command_line: String,
    pub reason: Option<String>,
    pub status: String,
    pub count_attempted: Option<i64>,
    pub count_completed: Option<i64>,
    pub count_failed: Option<i64>,
    pub count_skipped: Option<i64>,
    pub summary: Option<String>,
    pub canon_version: String,
    pub created_at: i64,
}
```

Note: `Decision` struct uses `String` for command/status (what comes back from DB), while the enums are used for writing. This matches the forward-compatibility design — the struct can represent records written by future Canon versions with unknown command values.

**New `src/repo/decision.rs`**:

```rust
/// Insert the initial "started" record. Returns the row ID.
pub fn insert_started(
    conn: &Connection,
    command: &str,
    scope: Option<&[String]>,
    command_line: &str,
    reason: Option<&str>,
    canon_version: &str,
) -> Result<i64>;

/// Update a started record with completion data.
pub fn update_completed(
    conn: &Connection,
    id: i64,
    status: &str,
    count_attempted: Option<i64>,
    count_completed: Option<i64>,
    count_failed: Option<i64>,
    count_skipped: Option<i64>,
    summary: Option<&str>,
) -> Result<()>;
```

`insert_started` serializes scope with `serde_json::to_string()`, captures current timestamp, returns `conn.last_insert_rowid()`.

**New `src/ops/decision.rs`**:

```rust
/// Parameters for starting a decision record.
pub struct DecisionParams {
    pub command: DecisionCommand,
    pub scope: Option<Vec<String>>,
    pub command_line: String,
    pub reason: Option<String>,
    pub enabled: bool,  // false for --no-record or --dry-run
}

/// Outcome counts for a decision record.
pub struct DecisionCounts {
    pub attempted: Option<i64>,
    pub completed: Option<i64>,
    pub failed: Option<i64>,
    pub skipped: Option<i64>,
}

/// Records a decision. Created before execution, completed after.
/// Catches its own errors — recording failure warns, never halts the command.
pub struct DecisionRecorder {
    id: Option<i64>,  // None if recording is disabled or start failed
}

impl DecisionRecorder {
    /// Insert the initial "started" record.
    /// If disabled (--no-record, --dry-run), returns a no-op recorder.
    /// If the INSERT fails, logs a warning and returns a no-op recorder.
    pub fn start(conn: &Connection, params: &DecisionParams) -> Self;

    /// Update the record with completion data. No-op if disabled or start failed.
    /// Logs a warning if the UPDATE fails.
    pub fn complete(
        &self,
        conn: &Connection,
        status: DecisionStatus,
        counts: DecisionCounts,
        summary: &str,
    );

    /// Update to interrupted status. Best-effort.
    pub fn interrupted(&self, conn: &Connection);
}
```

The recorder is the integration point. Execute functions create it, do work, complete it.

#### Tests

**Domain tests**:
- `test_decision_command_as_str_all_variants` — every variant serializes to expected string
- `test_decision_command_strings_are_unique` — no two variants share a string
- `test_decision_status_as_str` — all four statuses

**Repo tests**:
- `test_insert_started_returns_id` — basic INSERT, positive ID
- `test_insert_started_with_scope` — scope serialized as JSON array, verify with SELECT
- `test_insert_started_null_scope` — NULL when no scope
- `test_insert_started_with_reason` — reason stored
- `test_insert_started_null_reason` — NULL when no reason
- `test_insert_started_status_is_started` — default status
- `test_update_completed_changes_status` — status updated
- `test_update_completed_with_counts` — all count fields
- `test_update_completed_null_counts` — NULL counts allowed
- `test_update_completed_with_summary` — summary stored
- `test_update_completed_partial_status` — "partial" status

**Ops tests**:
- `test_recorder_start_creates_record` — verify DB record exists
- `test_recorder_complete_updates_record` — status, counts, summary all updated
- `test_recorder_disabled_creates_no_record` — no DB row when `enabled: false`
- `test_recorder_interrupted_sets_status` — verify "interrupted" status
- `test_recorder_disabled_complete_is_noop` — no panic, no error

### Phase 2: Recording integration — exclude commands

- **Goal**: Integrate decision recording into all exclude execute functions. Add `--reason` to applicable commands.

#### Changes

**Execute function signature change**:

Each execute function gains `decision: &DecisionParams`:

```rust
// Before
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan) -> Result<ExcludeSetResult>;

// After
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan, decision: &DecisionParams) -> Result<ExcludeSetResult>;
```

Inside the function:
```rust
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan, decision: &DecisionParams) -> Result<ExcludeSetResult> {
    let recorder = DecisionRecorder::start(conn, decision);

    // ... existing write logic, compose summary ...

    recorder.complete(conn, DecisionStatus::Completed, counts, &result.summary);
    Ok(result)
}
```

Apply this pattern to: `execute_set`, `execute_clear`, `execute_duplicates`, `execute_set_objects`, `execute_set_object`, `execute_clear_object`.

**`--reason` flag** on `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`:

```rust
// In ExcludeAction::Set
#[arg(long)]
reason: Option<String>,
```

**Interface constructs `DecisionParams`**:

```rust
let decision = DecisionParams {
    command: DecisionCommand::ExcludeSet,
    scope: Some(resolved.prefixes.clone()),
    command_line: command_line.clone(),
    reason: options.reason.clone().filter(|r| !r.trim().is_empty()),
    enabled: !options.no_record && !options.dry_run,
};
```

Empty string reason (`--reason ""`) treated as None per spec.

**Update all call sites** in `src/exclude.rs` to pass `&decision` to execute functions.

#### Tests

- `test_exclude_set_records_decision` — verify decision record exists after execute
- `test_exclude_set_decision_has_correct_command` — "exclude_set"
- `test_exclude_set_decision_has_scope` — scope matches input
- `test_exclude_set_decision_has_summary` — summary matches result
- `test_exclude_set_no_record_skips` — no decision when disabled
- `test_exclude_set_reason_stored` — reason in record
- `test_exclude_clear_records_decision` — verify recording
- `test_exclude_duplicates_records_decision` — verify recording
- `test_exclude_set_objects_records_decision` — verify recording
- `test_exclude_set_object_records_decision` — single-object exclusion records
- `test_exclude_clear_object_records_decision` — clear records

### Phase 3: Recording integration — apply, scan, cluster

- **Goal**: Integrate recording into the most complex commands. Add `--reason` for apply and scan. Manifest notes flow for apply.

#### Changes

**`ops/apply.rs`** — `execute_apply` gains `decision: &DecisionParams`:

Apply's interrupt handling maps to decision status:
```rust
let status = if result.interrupted {
    DecisionStatus::Interrupted
} else if result.errors > 0 {
    DecisionStatus::Partial
} else {
    DecisionStatus::Completed
};

recorder.complete(conn, status, counts, &result.summary);
```

**Manifest notes flow** — in `src/apply.rs` interface, when constructing `DecisionParams`:

```rust
let reason = if let Some(r) = options.reason.as_ref().filter(|r| !r.trim().is_empty()) {
    Some(r.clone())
} else {
    extract_notes(&manifest_text).filter(|n| !n.trim().is_empty())
};
```

`extract_notes()` already exists in `src/cluster.rs`. This is argument resolution in the interface — resolving "what is the reason?" from CLI args + manifest content before passing it to ops.

**`--reason` flag** on `Apply` and `Scan` commands.

**`ops/scan.rs`** — recording wraps `scan_root`:

Scan is slightly different — it doesn't follow plan/execute. The recording wraps the `scan_root()` call:

```rust
// In a new composed function or directly in the scan interface's ops call
let recorder = DecisionRecorder::start(conn, &decision);
let result = scan_root(conn, &walk, root, &options, &progress)?;
recorder.complete(conn, DecisionStatus::Completed, counts, &result.summary);
```

**`ops/cluster.rs`** — `execute_generate` and `execute_refresh` gain `decision: &DecisionParams`:

Cluster generate/refresh don't get `--reason` (manifest notes section serves this purpose per spec). Recording captures the manifest path in the summary.

#### Tests

- `test_apply_records_decision_with_counts` — counts match transfers
- `test_apply_interrupted_records_interrupted_status` — verify interrupted
- `test_apply_partial_records_partial_status` — errors → partial
- `test_apply_reason_from_flag` — explicit reason
- `test_apply_reason_from_manifest_notes` — notes flow
- `test_apply_reason_flag_overrides_notes` — precedence
- `test_apply_empty_notes_no_reason` — placeholder notes → no reason
- `test_scan_records_decision` — verify recording with scan stats
- `test_scan_reason_stored` — reason from `--reason`
- `test_cluster_generate_records_decision` — verify recording
- `test_cluster_refresh_records_decision` — verify recording

### Phase 4: Recording integration — remaining commands + CLI

- **Goal**: Complete recording integration. Add global `--no-record` flag and `command_line` capture.

#### Changes

**`--no-record` global flag** in `Cli` struct:

```rust
/// Suppress decision recording for this invocation
#[arg(long, global = true)]
no_record: bool,
```

**`command_line` capture** in `main.rs`:

```rust
fn main() -> Result<()> {
    let command_line = std::env::args().collect::<Vec<_>>().join(" ");
    // ... existing setup ...
    // Thread command_line through to command modules
}
```

Captured once at startup, before any processing. The raw argv joined with spaces. Passed through to each command module, which uses it when constructing `DecisionParams`.

**`canon_version`**: Use `env!("CARGO_PKG_VERSION")` — compiled in, zero cost.

**Integrate recording into remaining commands**:

- `ops/roots.rs` — `execute_remove`, `execute_suspend`, `execute_unsuspend` gain `decision: &DecisionParams`
- `--reason` on `roots rm`
- `ops/import_facts.rs` — recording wraps the streaming loop
- `ops/facts.rs` — `execute_delete` gains `decision: &DecisionParams`
- Prune operations gain `decision: &DecisionParams`
- `ops/note.rs` — `execute_clear_recursive` and `execute_clear_exact` gain `decision: &DecisionParams`

**Update `src/main.rs`** — thread `command_line` and `no_record` to all command modules.

**Documentation updates** in `docs/`:
- New page on decision provenance (what it records, when, `--reason`, `--no-record`)
- Update command reference pages for `--reason` on applicable commands
- Update command reference for `--no-record` global flag

**Update CLAUDE.md**:
- `domain/decision.rs` in module listing
- `repo/decision.rs` in module listing
- `ops/decision.rs` in module listing with `DecisionRecorder` pattern
- `decisions` table in database section
- `--no-record` in CLI flag vocabulary
- `--reason` in per-command flag notes

#### Tests

- `test_roots_rm_records_decision` — verify recording
- `test_roots_rm_reason_stored` — reason from `--reason`
- `test_roots_suspend_records_decision` — verify recording
- `test_roots_unsuspend_records_decision` — verify recording
- `test_import_facts_records_decision` — verify recording
- `test_facts_delete_records_decision` — verify recording
- `test_prune_records_decision` — verify recording
- `test_note_clear_records_decision` — verify recording
- `test_no_record_flag_suppresses_all` — global flag, no decision row
- `test_command_line_captured_faithfully` — argv round-trip
- `test_canon_version_populated` — version string present
- `test_empty_reason_treated_as_none` — `--reason ""` → NULL
- `test_dry_run_does_not_record` — no decision on dry-run

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `DecisionCommand` enum in domain, plain TEXT in DB | Compile-time safety for current code; no schema constraints for forward compatibility. |
| Command identifier strings are stable and append-only | Historical records must remain interpretable. Documented as comment on the enum. |
| `Decision` struct uses `String` for command/status | DB reads may return values from future Canon versions. The struct represents what's stored, not what's writeable. |
| `DecisionRecorder` catches its own errors | Recording is valuable but not critical. A failed INSERT/UPDATE must never prevent the command's own effects. |
| Scope as JSON array | Faithful multi-path capture. SQLite JSON functions enable future querying. |
| `--reason` resolution in interface layer | Resolving "what is the reason?" from CLI args + manifest content is argument resolution, not behavioral logic. The ops layer receives the resolved string. |
| `command_line` captured from `std::env::args()` | Verbatim input. Not parsed by Canon after recording. Captured once at startup. |
| `canon_version` from `CARGO_PKG_VERSION` | Compiled in, zero cost, provides temporal context for interpreting records. |

## Non-Goals

- Consumption/querying of decision records (future phase)
- Receipt artifacts with per-file detail (future phase)
- Persistent recording configuration (always per-invocation)
- Structured JSON in the summary field
- Recording for read-only commands

## Test Plan

### Existing Tests (Must Pass)

All existing tests. Recording is additive — no existing behavior changes.

### New Tests

See per-phase test sections above. Summary: ~50 new tests across domain, repo, ops, and integration levels.

## Implementation Checklist

- [x] Phase 1: Infrastructure — schema, domain, repo, ops
- [x] Phase 2: Recording integration — exclude commands
- [x] Phase 3: Recording integration — apply, scan, cluster
- [x] Phase 4: Recording integration — remaining commands + CLI
- [x] Verify all existing tests pass (972 tests, 21 new)
- [ ] Update CLAUDE.md
- [ ] Update docs/

## Documentation Updates

- New page in `docs/` on decision provenance
- Command reference updates for `--reason` on 7 commands
- Command reference update for `--no-record` global flag
- CLAUDE.md updates for new modules, table, flags, patterns

## Backward Compatibility

- New `decisions` table is additive — existing databases gain it automatically
- No changes to existing command output
- `--no-record` and `--reason` are new flags — no conflicts with existing flags
- Older Canon versions ignore the `decisions` table (it's just an unknown table)

## Performance Considerations

One INSERT and one UPDATE per command invocation. Both single-statement operations on an uncontended table. No measurable performance impact.
