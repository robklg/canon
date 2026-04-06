# Story: Ops Layer Completion for Effectful Commands

**Design Spec**: [~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md](~/store/claude-designs/2026-04-05-decision-provenance-ground-layer.md)
**Epic**: [epic-decision-provenance.md](epic-decision-provenance.md)
**Status**: Complete
**Created**: 2026-04-06

## Objective

All effectful commands must follow the established ops-layer architecture: behavioral logic in ops, typed results including the completion summary, interface layer responsible only for CLI parsing, display, and confirmation presentation. Several commands currently violate this — their behavioral logic and summary composition lives in the interface layer. This story brings them into compliance.

## Functional Requirements Summary

No user-facing changes. This is an internal refactor. Every effectful command's output must remain identical. The change is where the completion message is composed — moving from interface to ops — and where behavioral logic lives for commands that bypass the ops layer.

Commands requiring extraction to ops:
- `roots rm` — all behavioral logic inline in `src/roots.rs`
- `roots suspend` — inline in `src/roots.rs`
- `roots unsuspend` — inline in `src/roots.rs`
- `exclude set-object` (by hash/file) — check in ops, write bypasses ops
- `exclude clear-object` — check in ops, write bypasses ops
- `note clear` exact-scope — bypasses ops, calls repo directly

Commands requiring summary migration (already extracted, summary composed in interface):
- `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-objects`
- `apply` (three summary variants: normal, interrupted, resume)
- `scan` (multi-line: scan stats + hash stats)
- `cluster generate`, `cluster refresh`
- `import-facts`
- `facts delete`
- `prune` (stale, orphaned, excluded variants)
- `note clear` recursive

## Current State

**Well-extracted commands** (ops plan/execute exists, summary in interface):
- `exclude set/clear/duplicates/set-objects` — ops functions return counts; interface composes `println!`
- `apply` — `ApplyResult` has all data; interface composes three summary variants
- `scan` — `ScanStats` and `ScanRootResult` in ops; interface composes multi-line output
- `cluster generate/refresh` — ops returns `ExecuteGenerateResult`/`ExecuteRefreshResult`; interface composes summary
- `import-facts` — streaming model with `ImportStats`; interface composes summary
- `facts delete` — plan/execute in ops; interface composes summary
- `prune` variants — plan/execute in ops; interface composes summary
- `note clear` recursive — plan/execute in ops; interface composes summary

**Not extracted** (behavioral logic in interface):
- `roots rm` — statistics computation, confirmation, repo call, summary all in `src/roots.rs`
- `roots suspend/unsuspend` — state check, repo call, count fetch, summary all in `src/roots.rs`
- `exclude set-object` by hash/file — `check_set_object_by_hash()`/`check_set_object_by_file()` return enum, but interface calls `repo::exclude::exclude_object()` directly
- `exclude clear-object` — `check_clear_object()` returns enum, interface calls `repo::exclude::clear_object_exclusion()` directly
- `note clear` exact-scope — interface calls `repo::note::clear_by_scope()` directly

**`format_count`** lives in `ceremony.rs` (interface layer). Ops needs it for summary composition.

## Design

### Phase 1: Foundation — `format_count` relocation + `ops/roots.rs`

- **Goal**: Move `format_count` to a shared location. Create `ops/roots.rs` with properly extracted root operations.

#### Changes

**Relocate `format_count`**:

Move `format_count` from `src/ceremony.rs` to `src/domain/format.rs` (pure function, no I/O). Re-export from `domain` module. Update all existing call sites in the interface layer. Move existing tests with it.

`ceremony.rs` retains only `confirm()` (terminal I/O).

**Create `src/ops/roots.rs`**:

```rust
use crate::domain::format::format_count;

// --- roots rm: plan/execute ---

pub struct RemoveRootPlan {
    pub root_id: i64,
    pub root_path: String,
    pub role: String,
    pub source_count: i64,
    pub note_count: i64,
}

pub struct RemoveRootResult {
    pub deleted_sources: i64,
    pub deleted_notes: i64,
    pub summary: String,
}

/// Compute what removing a root would affect.
pub fn plan_remove(conn: &Connection, root_id: i64) -> Result<RemoveRootPlan>;

/// Execute the removal. Deletes sources, notes, and the root in a transaction.
pub fn execute_remove(conn: &mut Connection, plan: &RemoveRootPlan) -> Result<RemoveRootResult>;

// --- suspend/unsuspend: simple operations ---

pub struct SuspendResult {
    pub root_id: i64,
    pub root_path: String,
    pub source_count: i64,
    pub summary: String,
}

/// Suspend a root. Errors if already suspended.
pub fn execute_suspend(conn: &Connection, root_id: i64) -> Result<SuspendResult>;

/// Unsuspend a root. Errors if not suspended.
pub fn execute_unsuspend(conn: &Connection, root_id: i64) -> Result<SuspendResult>;
```

Suspend/unsuspend are simple operations without plan/execute — same reasoning as single-source exclude operations (ADR findings: "trivially simple, plan/execute adds ceremony without value").

`execute_remove` owns the transaction. It composes the summary: `"Removed root {id} and {N} sources"` (matching current output).

**Update `src/roots.rs` interface**:

Replace inline behavioral logic with ops calls. Interface becomes:
1. Parse CLI args, resolve root spec
2. Call `ops::roots::plan_remove()`
3. Display confirmation data from plan (gated behind `!yes`)
4. Call `ceremony::confirm()`
5. Call `ops::roots::execute_remove()`
6. Print `result.summary`

Same for suspend/unsuspend but without plan/confirm (no confirmation needed).

#### Tests

- `test_plan_remove_returns_counts` — plan includes correct source and note counts
- `test_execute_remove_deletes_all` — sources, notes, and root deleted
- `test_execute_remove_summary_format` — verify exact summary string
- `test_suspend_already_suspended_errors` — error on double-suspend
- `test_unsuspend_not_suspended_errors` — error when already active
- `test_suspend_summary_includes_path_and_count` — verify summary format
- `test_unsuspend_summary_includes_path_and_count` — verify summary format
- `test_format_count_relocation` — existing tests pass at new location

### Phase 2: Object exclusion + note clear extraction

- **Goal**: Give object exclusion commands proper execute functions in ops. Normalize note clear exact-scope through ops.

#### Changes

**Extend `ops/exclude.rs`**:

Add execute functions that wrap the existing repo calls and compose summaries:

```rust
pub struct ExcludeObjectResult {
    pub object_id: i64,
    pub hash_prefix: String,
    pub source_count: i64,
    pub summary: String,  // "Excluded object: abc123... (affects 5 sources)"
}

pub struct ClearObjectResult {
    pub object_id: i64,
    pub hash_prefix: String,
    pub summary: String,  // "Cleared exclusion from object: abc123..."
}

/// Execute object exclusion by ID (after check_set_object_by_hash/file validated).
pub fn execute_set_object(conn: &Connection, object_id: i64) -> Result<ExcludeObjectResult>;

/// Execute object exclusion clear (after check_clear_object validated).
pub fn execute_clear_object(conn: &Connection, object_id: i64) -> Result<ClearObjectResult>;
```

The existing `check_*` functions remain as the validation/plan phase. The new execute functions handle the write + summary.

**Update `src/exclude.rs` interface** for object commands:

Replace direct `repo::exclude::exclude_object()` calls with `ops::exclude::execute_set_object()`. Replace direct `repo::exclude::clear_object_exclusion()` with `ops::exclude::execute_clear_object()`. Print `result.summary`.

**Extend `ops/note.rs`**:

```rust
pub struct ClearExactResult {
    pub deleted: usize,
    pub summary: String,  // "Cleared 3 notes at /path"
}

pub fn execute_clear_exact(conn: &Connection, scope: &NoteScope) -> Result<ClearExactResult>;
```

**Update `src/note.rs` interface** for exact-scope clear to call ops instead of repo directly.

#### Tests

- `test_execute_set_object_excludes_and_returns_summary` — write happens, summary correct
- `test_execute_set_object_summary_includes_source_count` — verify affected source count in summary
- `test_execute_clear_object_clears_and_returns_summary` — write happens, summary correct
- `test_execute_clear_exact_note_returns_summary` — note deletion and summary
- `test_execute_clear_exact_note_zero_deleted` — summary when no notes at scope

### Phase 3: Summary migration — exclude, facts, prune, notes

- **Goal**: Move summary composition from interface to ops for exclude, facts, prune, and note commands.

#### Changes

For each command, the pattern is identical:

1. Add `pub summary: String` to the ops result type
2. Move the `format!()` composition from interface into the ops execute function
3. Change interface from composing+printing to printing `result.summary`

**`ops/exclude.rs`** — `execute_set`, `execute_clear`, `execute_duplicates`, `execute_set_objects`:

Each already returns a count or result. Add `summary: String` field. Move the `format!()` from `src/exclude.rs` interface into the execute function.

Example for `execute_set`:
```rust
pub struct ExcludeSetResult {
    pub count: usize,
    pub summary: String,
}

pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan) -> Result<ExcludeSetResult> {
    // ... existing write logic ...
    let noun = if plan.source_ids.len() == 1 { "source" } else { "sources" };
    let summary = format!("Excluded {} {noun}", format_count(plan.source_ids.len()));
    Ok(ExcludeSetResult { count: plan.source_ids.len(), summary })
}
```

Interface becomes: `println!("{}", result.summary);`

**`ops/facts.rs`** — `execute_delete`, `execute_prune_stale`, `execute_prune_orphaned`, `execute_prune_excluded`:

Same pattern. Each gains a result type with `summary: String`.

**`ops/note.rs`** — `execute_clear_recursive`:

Already returns count. Add summary field.

#### Tests

Summary format regression tests for each command — verify the string matches what the interface previously produced:

- `test_exclude_set_summary_singular` — "Excluded 1 source"
- `test_exclude_set_summary_plural` — "Excluded 3,847 sources"
- `test_exclude_clear_summary_singular` — "Cleared exclusions for 1 source"
- `test_exclude_clear_summary_plural` — "Cleared exclusions for 42 sources"
- `test_exclude_duplicates_summary_format` — verify format with count + guidance line
- `test_exclude_set_objects_summary_format` — verify object count and source count
- `test_facts_delete_summary_format` — verify format
- `test_prune_stale_summary_format` — verify format
- `test_prune_orphaned_summary_format` — verify format
- `test_prune_excluded_summary_format` — verify format
- `test_note_clear_recursive_summary_format` — verify format

### Phase 4: Summary migration — scan, apply, cluster, import-facts

- **Goal**: Move summary composition for the remaining commands. These are more complex — apply has three summary shapes, scan has multi-line output, cluster has root breakdowns.

#### Changes

**`ops/apply.rs`** — add `summary: String` to `ApplyResult`:

Apply has three summary variants based on state:
1. **Normal**: "Applied: N copied, N renamed, N moved, N errors (X.X GB)"
2. **Interrupted**: "Applied: N copied, N errors. Interrupted — N files remaining."
3. **Resume**: Includes "N already there" counts

The data for all three is already in `ApplyResult` (`copied`, `renamed`, `moved`, `errors`, `interrupted`, `remaining`, `already_there`, `bytes_transferred`). Summary composition moves from `src/apply.rs` into `execute_apply`, composed just before returning the result.

**`ops/scan.rs`** — add `summary: String` to `ScanRootResult`:

Scan summary is multi-line. Current format:
```
Scanned N files: N new, N updated, N moved, N unchanged, N missing
Hashed N files
```

The scan interface currently calls `scan_root()` then `hash_files()` separately. The summary needs to capture both. Two options:
1. Compose partial summary in `scan_root` result, append hash summary after `hash_files`
2. Create an aggregate function that runs both and returns a unified result

Option 1 is simpler and matches the current two-call pattern. `ScanRootResult` gets a `summary: String` for the scan line. The hash stats summary is composed after `hash_files` returns. The interface concatenates them for display; the full multi-line string serves as the decision summary.

**`ops/cluster.rs`** — add `summary: String` to `ExecuteGenerateResult` and `ExecuteRefreshResult`:

Current summary includes: manifest path, source count, root breakdown, archive coverage. The `ExecuteGenerateResult` already has most of this data. Summary composition moves from `src/cluster.rs` interface into the execute functions.

**`ops/import_facts.rs`** — add `summary: String` to `ImportStats`:

Current format: "Processed N lines: N facts imported, N skipped, N stale, N errors". The `ImportStats` struct already has all these counts. Summary composition moves from `src/import_facts.rs` into a method on `ImportStats` or composed after the streaming loop completes.

#### Tests

- `test_apply_summary_normal` — normal completion with all count categories
- `test_apply_summary_interrupted` — interrupted format with remaining count
- `test_apply_summary_resume` — resume format with already-there counts
- `test_apply_summary_zero_errors` — errors omitted or shown as 0
- `test_scan_summary_format` — verify multi-line format with all categories
- `test_scan_summary_no_missing` — format when no files missing
- `test_cluster_generate_summary_format` — manifest path, source count, root breakdown
- `test_cluster_refresh_summary_format` — refresh-specific format
- `test_import_facts_summary_format` — all count categories

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `format_count` moves to `domain/format.rs` | Pure function, no I/O. Ops needs it for summaries. Domain is the right home. |
| Suspend/unsuspend without plan/execute | Trivially simple operations. Plan/execute adds ceremony without value. Same precedent as single-source exclude (ADR findings). |
| `roots rm` uses plan/execute | Has confirmation with counts — the plan provides data the interface needs for the confirmation display. |
| Summary is always `String`, not `Option<String>` | Every command prints a completion message. There's always a summary. |
| Scan summary composed in two parts | Matches existing two-call pattern (`scan_root` + `hash_files`). Simpler than creating a monolithic aggregate function. |

## Non-Goals

- No output format changes (regression tests verify exact strings)
- No new CLI flags or commands
- No changes to confirmation behavior or ceremony
- No new features — purely structural

## Test Plan

### Existing Tests (Must Pass)

All existing tests. This is a refactor — behavior is unchanged. Any test failure indicates a regression.

### New Tests

See per-phase test sections above. Summary: ~30 new tests covering ops function behavior and summary format correctness.

## Implementation Checklist

- [x] Phase 1: `format_count` relocation + `ops/roots.rs`
- [x] Phase 2: Object exclusion + note clear extraction
- [x] Phase 3: Summary migration — exclude, facts, prune, notes
- [x] Phase 4: Summary migration — scan, apply, cluster, import-facts
- [x] Verify all existing tests pass (951 tests, 5 new)
- [ ] Update CLAUDE.md if new patterns established

## Documentation Updates

No user-facing documentation changes. This is an internal refactor.

CLAUDE.md may need minor updates to reflect:
- `ops/roots.rs` in the module listing
- `format_count` new location
- Summary composition convention (ops composes, interface prints)

## Backward Compatibility

No changes in user-visible behavior. All commands produce identical output.

## Performance Considerations

None. Summary composition is string formatting — negligible cost. No new queries, no new I/O paths.
