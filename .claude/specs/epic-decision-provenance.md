# Epic: Decision Provenance

**Design Spec**: [~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md](~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md)
**Status**: Pending
**Created**: 2026-04-06

## Objective

Canon tracks content identity, location, and state — but not the decisions that drive user activities. The user orients, marks, assembles, triages, dismisses, archives. All of these produce decisions, and all of them are ephemeral. The confidence to let go of a drive isn't just knowing that valuable content is safe — it's knowing you can reconstruct the story of what was on that drive, what you kept, what you released, and why.

This epic delivers the ground layer: automatic, silent recording of every effectful action, with optional user annotation via `--reason`. When the user knows their actions are being recorded durably, it frees them to act. The hesitation before deleting, the second-guessing after excluding — these are friction born from the absence of a safety net for decisions, not just for files.

### Success Criteria

1. Every effectful command writes a two-phase decision record (started → completed/partial/interrupted) without any change to the user's experience
2. `--reason` is available on commands where user reasoning is meaningful
3. Manifest notes flow automatically into apply decision records
4. `--no-record` provides per-invocation opt-out
5. The recording infrastructure is entirely in the operations layer — no behavioral logic in the interface
6. All effectful commands follow the established ops-layer patterns before decision recording is integrated

## Architectural Design

### Overview

Decision provenance adds a new `decisions` table alongside the existing `roots`, `sources`, `objects`, `facts`, and `notes` tables. It follows Canon's standard three-layer pattern: domain types define the data, repo persists it, ops orchestrates recording.

Recording integrates into the operations layer's execute functions. The `DecisionRecorder` wraps each execution: INSERT a "started" record before work begins, UPDATE with outcome after work completes. If the process is killed uncleanly, the "started" record survives as a durable trace.

A prerequisite: all effectful commands must have proper ops-layer extraction with summary composition in ops before recording can be integrated. Several commands currently violate this — their behavioral logic and summary composition lives in the interface layer. This must be fixed first.

```
┌─────────────────────────────────────────────────────────────┐
│ Interface Layer                                              │
│ - Parses --reason, --no-record, captures argv               │
│ - Constructs DecisionParams from CLI args                   │
│ - Prints result.summary (composed by ops)                   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ Operations Layer                                             │
│ - Execute functions take DecisionParams                     │
│ - DecisionRecorder: start() before work, complete() after   │
│ - Summary composed in execute, stored in result type        │
│ - Recording failure warns, never errors                     │
└─────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ repo/decision.rs  │ │ domain/      │ │ ops/decision.rs      │
│ - insert_started  │ │ decision.rs  │ │ - DecisionRecorder   │
│ - update_completed│ │ - Command    │ │ - DecisionParams     │
│                   │ │ - Status     │ │ - DecisionCounts     │
│                   │ │ - Decision   │ │                      │
└──────────────────┘ └──────────────┘ └──────────────────────┘
```

### New Domain Types

**`domain/decision.rs`**:
- `DecisionCommand` enum — stable identifiers for all effectful commands (16 variants). Append-only: strings are permanent history, never renamed or reused.
- `DecisionStatus` enum — `Started`, `Completed`, `Partial`, `Interrupted`
- `Decision` struct — the full record with all fields

### New Repository Functions

**`repo/decision.rs`**:
- `insert_started(conn, command, scope, command_line, reason, canon_version) -> Result<i64>` — INSERT the initial record, return row ID
- `update_completed(conn, id, status, counts, summary) -> Result<()>` — UPDATE with outcome data

### Command Layer Impact

All effectful command execute functions gain a `decision: &DecisionParams` parameter. The interface constructs `DecisionParams` from CLI args (`--no-record`, `--reason`, argv capture) and passes it through.

New global flag: `--no-record`. New per-command flag: `--reason` on 7 commands.

Summary composition moves from interface to ops for all effectful commands (prerequisite work in Story 1).

## Stories

| # | Story | Scope | Dependencies | Status |
|---|-------|-------|--------------|--------|
| 1 | Ops layer completion for effectful commands | Extract roots + object exclusion to ops. Move summary composition to ops for all effectful commands. | — | Pending |
| 2 | Decision provenance | Schema, domain types, repo, recording integration, `--reason`, `--no-record`, manifest notes flow. | Story 1 | Pending |

### Story 1: Ops Layer Completion for Effectful Commands

**Objective**: Bring all effectful commands into compliance with the established ops-layer architecture. Every effectful command should have its behavioral logic in ops, return typed results including the completion summary, and leave the interface layer responsible only for CLI parsing, display, and confirmation presentation.

**Scope**:
- Create `ops/roots.rs` — extract `roots rm` (plan/execute), `roots suspend`, `roots unsuspend`
- Complete `ops/exclude.rs` — add proper execute functions for `exclude set-object` (by hash/file) and `exclude clear-object`
- Normalize `note clear` exact-scope through ops (currently bypasses ops, calls repo directly)
- Relocate `format_count` from `ceremony.rs` to a shared location accessible by ops
- Migrate summary composition from interface to ops result types for all already-extracted effectful commands: `exclude set/clear/duplicates/set-objects`, `apply`, `scan`, `cluster generate/refresh`, `import-facts`, `facts delete`, all `prune` variants, `note clear` recursive

**Key decisions**:
- `format_count` moves to domain layer (pure function, no I/O)
- Suspend/unsuspend are simple operations without plan/execute (same reasoning as single-source exclude in the ADR findings)
- `roots rm` uses plan/execute (has confirmation with counts)
- Summary is always `String`, never `Option<String>` — every command has a completion message

**Dependencies**: None — this is foundational work.

### Story 2: Decision Provenance

**Objective**: Deliver the ground layer of decision provenance — automatic, silent recording of every effectful action with optional `--reason` annotation and `--no-record` opt-out. This is the emotional core of the feature: when the user knows their actions are being recorded durably, it frees them to act.

**Scope**:
- `decisions` table schema (additive, `CREATE TABLE IF NOT EXISTS`)
- `domain/decision.rs` — `DecisionCommand`, `DecisionStatus`, `Decision`
- `repo/decision.rs` — `insert_started`, `update_completed`
- `ops/decision.rs` — `DecisionRecorder`, `DecisionParams`, `DecisionCounts`
- Recording integration into all 17 effectful command execute functions
- `--reason` flag on: `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `apply`, `scan`, `roots rm`
- `--no-record` global flag
- Manifest notes flow into apply decision records
- `command_line` capture from `std::env::args()` in `main.rs`
- Apply interrupt → `DecisionStatus::Interrupted` (uses existing signal handling)
- Documentation updates in `docs/`

**Key decisions**:
- `DecisionCommand` enum in domain, plain TEXT column in DB (compile-time safety, forward-compatible schema)
- Command identifier strings are stable and append-only (documented as comment on enum)
- `DecisionRecorder` catches its own errors — recording failure warns, never halts the command
- Scope stored as JSON array of strings (faithful multi-path capture)
- Summary is "the completion message the user saw" — same string, composed once in ops
- `--reason` resolution for apply: explicit `--reason` takes precedence over manifest notes
- `--dry-run` and declined confirmations produce no record

**Dependencies**: Story 1 (all effectful commands must have ops-layer execution with summary composition)

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `DecisionCommand` enum in domain, plain TEXT in DB | Compile-time safety for current code; no schema constraints for forward compatibility. Command strings are permanent history. |
| Command identifier strings are append-only | Historical records must remain interpretable. Never rename, never reuse for different semantics. `canon_version` provides era context. |
| Recording lives in ops execute functions | Ops owns behavioral logic. Recording is behavior — not presentation. |
| Summary composed once in ops | Single source of truth. Interface prints it; decision record stores it. No duplication. |
| `DecisionRecorder` degrades gracefully on failure | Recording is valuable but not critical. A failed INSERT/UPDATE must never prevent the command's own effects. |
| Two-phase recording (started → completed) | Captures intent before execution, outcome after. Interrupted processes leave "started" records — a durable trace of attempted operations. |
| Scope as JSON array, not normalized | Faithful capture of multi-path commands. Simple, honest, queryable with JSON functions in SQLite. |
| No CHECK constraint on command column | Forward-compatible. Newer Canon versions can write new command values without schema migration. |

## Cross-Cutting Concerns

- **`format_count` relocation**: Story 1 moves this from `ceremony.rs` (interface) to a shared location. Both stories depend on ops being able to format numbers for summaries.
- **Execute function signatures**: Story 2 adds `decision: &DecisionParams` to all execute functions. This is a moderate blast radius — every call site in the interface changes — but the compiler catches missing arguments.
- **Signal handling**: Already implemented for apply (`ops/apply.rs`). When `ApplyResult.interrupted` is true, the recorder uses `DecisionStatus::Interrupted`. Other commands don't have long-running loops, so unclean termination leaves "started" records — acceptable per spec.

## Test Strategy

**Story 1**: Ops-layer unit tests for new functions. Summary format regression tests for every migrated command — verify the string content matches what the interface previously produced. All existing tests must pass (refactor, not behavior change).

**Story 2**: Layer-by-layer tests. Domain: enum serialization. Repo: INSERT/UPDATE round-trips. Ops: recorder lifecycle (start/complete, disabled, failure degradation). Integration: recording happens on execute, doesn't happen on dry-run/decline/no-record. Edge cases: empty reason treated as none, interrupted status on apply.

## Documentation Updates

- **Story 1**: No user-facing documentation changes (internal refactor).
- **Story 2**: New page in `docs/` covering decision provenance — what it records, when, `--reason` flag, `--no-record` opt-out. Updates to command reference pages for `--reason` on applicable commands.

## Non-Goals

- Consumption/querying of decision records (Phase 4 of the vision)
- Receipt artifacts with per-file detail (Phase 3 of the vision)
- Decision records for read-only commands
- Persistent configuration for recording (always per-invocation via `--no-record`)
- Structured JSON in the summary field (summary is the human-readable completion message)

## Risks

| Risk | Mitigation |
|------|------------|
| Summary migration changes visible output | Regression tests compare exact strings. Each command tested individually. |
| Execute function signature changes across codebase | Compiler enforces. Mechanical change — no runtime risk. |
| Recording adds latency to commands | Single INSERT + single UPDATE, both fast. No contention risk. |
| Future command additions forget decision recording | `DecisionParams` parameter on execute functions makes it a compile-time requirement. |

## Version History

| Date | Change |
|------|--------|
| 2026-04-06 | Initial epic design |
