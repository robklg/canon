# Refactoring Spec: Exclude Plan/Execute — Set & Clear

## Overview

Extract `set()` and `clear()` from the interface layer (`src/exclude.rs`) into the operations layer (`src/ops/exclude.rs`) using the plan/execute pattern defined in the Operations Layer ADR.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (plan/execute for `set` and `clear` only).
**Findings**: `~/store/canon-architecture/2026-03-14-exclude-ceremony-findings.md` — resolved by Design panel. Ceremony differences are intentional; plan/execute is ceremony-agnostic.

This is the first plan/execute extraction in the codebase. It establishes the pattern that all subsequent effectful command extractions will follow.

## Phase 1: Plan/Execute for `set()` and `clear()`

- **Status**: completed
- **Goal**: Extract computation and writes from `set()` and `clear()` into `ops/exclude.rs`, leaving the interface layer responsible only for input resolution, presentation, and ceremony.
- **Scope**: Two plan functions, two execute functions, plan types, and tests.
- **Non-goals**: No changes to `exclude_duplicates()`, `set_objects_by_filter()`, single-target commands, `list_objects()`, ceremony behavior, output format, or output destinations.
- **Dependencies**: Steps 1-5 + 6a completed (selection migration done).

### Architecture

```
Interface (exclude.rs)              Operations (ops/exclude.rs)
─────────────────────               ──────────────────────────
set():                              plan_set():
  parse filters                       select_sources (SourceOnly)
  resolve paths                       filter out already-excluded
  ─── call plan_set() ───────────►    compute confirmation data
  dry-run? display plan, return       return ExcludeSetPlan
  count > 1 && !yes? confirm
  ─── call execute_set() ───────►   execute_set():
  print summary                       loop: set_excluded(id, true)
                                      return count

clear():                            plan_clear():
  parse filters                       fetch active source roots
  resolve paths                       batch fetch sources
  ─── call plan_clear() ─────────►    filter: scope + s.excluded == true
  dry-run? display plan, return       apply --where filters
  count > 1 && !yes? confirm          compute root count
  ─── call execute_clear() ──────►    return ExcludeClearPlan
  print summary
                                    execute_clear():
                                      loop: set_excluded(id, false)
                                      return count
```

### New Types in `ops/exclude.rs`

```rust
/// Parameters for planning a source exclusion set operation.
pub struct ExcludeSetParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding sources. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetPlan {
    /// Source IDs to exclude.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct root count across sources to exclude.
    pub root_count: usize,
    /// Sources with no archived copy (unhashed or not in any archive root).
    pub not_archived_count: usize,
}

/// Parameters for planning a source exclusion clear operation.
pub struct ExcludeClearParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for clearing source-level exclusions.
pub struct ExcludeClearPlan {
    /// Source IDs to clear exclusion from.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct root count across sources to clear.
    pub root_count: usize,
}
```

### New Functions in `ops/exclude.rs`

```rust
/// Compute what `exclude set` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters, then computes
/// confirmation data (root count, archive coverage).
pub fn plan_set(conn: &mut Connection, params: &ExcludeSetParams) -> Result<ExcludeSetPlan>;

/// Execute an exclude-set plan — marks sources as excluded.
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan) -> Result<usize>;

/// Compute what `exclude clear` would do — no side effects.
///
/// Finds source-level excluded sources (s.excluded == true, NOT object-level)
/// matching scope and filters. Uses its own selection logic — this is a
/// different selection contract from select_sources(), which queries visible
/// sources. plan_clear() queries for sources marked for clearing.
pub fn plan_clear(conn: &mut Connection, params: &ExcludeClearParams) -> Result<ExcludeClearPlan>;

/// Execute an exclude-clear plan — clears source-level exclusion.
pub fn execute_clear(conn: &Connection, plan: &ExcludeClearPlan) -> Result<usize>;
```

### `plan_set()` Implementation Notes

1. Build `SelectionParams` with `RolePolicy::SourceOnly`, `IncludeSet::default()`, call `select_sources()`.
2. From `selection.sources`, build a `HashMap<i64, &Source>` for lookups.
3. Filter source IDs for `!s.is_excluded()` — sources that are already excluded (at either level) are skipped.
4. Collect object IDs, call `repo::object::batch_check_archived(conn, &object_ids, None)` for archive coverage.
5. Compute `root_count` (distinct `root_id` values) and `not_archived_count` (unhashed or not in archive).
6. Collect paths via `source.path()`.
7. Return `ExcludeSetPlan`.

This absorbs the current `compute_set_confirmation()` helper and the `batch_fetch_by_ids()` re-fetch (no longer needed — `select_sources()` returns `Source` objects directly).

### `plan_clear()` Implementation Notes (Option D)

`plan_clear()` owns its selection logic directly, using repo primitives and domain predicates. It does NOT route through `select_sources()` because it has a fundamentally different selection contract: "find sources marked as excluded at the source level" rather than "find visible sources."

1. Fetch all roots via `repo::root::fetch_all(conn)`.
2. Filter for active source roots: `r.is_active() && r.is_source()`.
3. Batch fetch sources: `repo::source::batch_fetch_by_roots(conn, &root_ids)`.
4. Filter for scope match: `s.matches_scope(&scopes)` (or all if scopes empty).
5. Filter for source-level exclusion: `s.excluded == true` (NOT `s.is_excluded()`).
6. Apply `--where` filters if present via `filter::apply_filters()`.
7. Compute `root_count` from distinct `root_id` values.
8. Collect paths via `source.path()`.
9. Return `ExcludeClearPlan`.

This eliminates the `get_excluded_sources()` helper entirely.

### `execute_set()` and `execute_clear()` Implementation Notes

Both are simple loops:

```rust
pub fn execute_set(conn: &Connection, plan: &ExcludeSetPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, true)?;
    }
    Ok(plan.source_ids.len())
}

pub fn execute_clear(conn: &Connection, plan: &ExcludeClearPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, false)?;
    }
    Ok(plan.source_ids.len())
}
```

No transaction wrapping — matches current behavior (each `set_excluded` is its own implicit transaction). Transaction scope is a future consideration for all effectful operations, not something to change for one command now.

### Changes to `exclude.rs` (Interface Layer)

**`set()` becomes:**
1. Parse filters (unchanged).
2. Resolve scope paths (unchanged).
3. Build `ExcludeSetParams`, call `ops::exclude::plan_set()`.
4. If plan is empty → print "No sources to exclude" message, return.
5. If `dry_run` → print "Would exclude N sources:" + paths, return.
6. If `count > 1 && !yes` → display confirmation stats from plan, call `ceremony::confirm()`.
7. Call `ops::exclude::execute_set()`.
8. Print summary.

**`clear()` becomes:**
1. Parse filters (unchanged).
2. Resolve scope paths (unchanged — but note: `clear()` currently passes `scope_prefixes` as `&[String]`, which `plan_clear()` needs as `Vec<ScopeMatch>`. The interface does `ScopeMatch::classify_all()` before calling `plan_clear()`).
3. Build `ExcludeClearParams`, call `ops::exclude::plan_clear()`.
4. If plan is empty → print "No excluded sources match" message, return.
5. If `dry_run` → print "Would clear exclusions for N sources:" + paths, return.
6. If `count > 1 && !yes` → display confirmation stats from plan, call `ceremony::confirm()`.
7. Call `ops::exclude::execute_clear()`.
8. Print summary.

**Functions removed from `exclude.rs`:**
- `compute_set_confirmation()` — absorbed into `plan_set()`
- `get_excluded_sources()` — absorbed into `plan_clear()`
- `get_source_path()` — only used by `set()` dry-run, replaced by plan's `paths` field
- `SetConfirmation` struct — replaced by plan struct fields

### Changes to `ops/mod.rs`

Add `pub mod exclude;`

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `plan_clear()` owns its selection logic (Option D) | Different selection contract from `select_sources()`. "Find excluded sources" is not "find visible sources with a post-filter." Direct implementation keeps the ops layer as the authoritative expression of the domain concept. |
| No transaction wrapping in execute functions | Matches current behavior. Transaction scope is a cross-cutting concern for all effectful operations — should be decided consistently, not per-command. |
| Parallel vectors for IDs and paths in plan structs | Both are needed: IDs for execution, paths for display. Parallel vectors are simpler than a custom struct for two fields. Plan is computed once and consumed once. |
| `plan_set()` uses `select_sources()` | The "find non-excluded sources" contract is exactly what `select_sources()` provides. No reason to reimplement. |
| `ExcludeSetPlan` includes `not_archived_count` | Confirmation data is part of the plan — the interface shouldn't need to make additional queries after planning. All data needed for presentation is baked in. |

## Test Requirements

### Existing Tests (Must Pass)

All 35 existing tests in `exclude.rs` must pass, plus all 14 tests in `ops/selection.rs`. The integration test `test_set_single_source_no_confirmation` calls `set()` in the interface layer — it validates the full plan→execute flow works end-to-end.

### Tests to Remove

These test extracted helpers that no longer exist as separate functions:
- `test_set_confirmation_counts_roots` → replaced by `test_plan_set_counts_roots`
- `test_set_confirmation_archive_coverage` → replaced by `test_plan_set_archive_coverage`
- `test_set_confirmation_unhashed_not_archived` → replaced by `test_plan_set_unhashed_not_archived`
- `test_get_excluded_sources_returns_source_level_only` → replaced by `test_plan_clear_returns_source_level_only`
- `test_get_excluded_sources_ignores_object_level_excluded` → replaced by `test_plan_clear_ignores_object_level`
- `test_get_excluded_sources_respects_scope` → replaced by `test_plan_clear_respects_scope`
- `test_get_excluded_sources_returns_correct_path` → replaced by `test_plan_clear_returns_paths`
- `test_clear_confirmation_counts_roots` → replaced by `test_plan_clear_counts_roots`

### New Tests to Add (in `ops/exclude.rs`)

**`plan_set()` tests:**

1. **`test_plan_set_empty_when_no_sources`** — No sources in DB → plan has empty `source_ids`.
2. **`test_plan_set_excludes_non_excluded_sources`** — Mix of excluded and non-excluded sources → plan contains only non-excluded IDs.
3. **`test_plan_set_skips_object_level_excluded`** — Source not source-level excluded but object is excluded → source IS in plan (it's not excluded at source level, `is_excluded()` filters it but source-level flag is false... wait — `select_sources()` with default `IncludeSet` filters out `is_excluded()` sources. So a source that is only object-level excluded will NOT appear in the selection, and thus NOT appear in the plan. This is correct: you can't "set" an exclusion that's already effective via object-level. Test confirms this behavior.
4. **`test_plan_set_counts_roots`** — Sources across 2 roots → `root_count == 2`.
5. **`test_plan_set_archive_coverage`** — Source with archived object and source without → `not_archived_count` is correct.
6. **`test_plan_set_unhashed_not_archived`** — Source with no `object_id` → counts as not archived.
7. **`test_plan_set_includes_paths`** — Plan paths match source paths.
8. **`test_plan_set_respects_scope`** — Sources outside scope are excluded from plan.

**`plan_clear()` tests:**

9. **`test_plan_clear_returns_source_level_only`** — Source with `excluded = true` appears. Source with `excluded = false` does not.
10. **`test_plan_clear_ignores_object_level`** — Source with `excluded = false` but object `excluded = true` → NOT in plan.
11. **`test_plan_clear_respects_scope`** — Scoped to a prefix → only matching excluded sources appear.
12. **`test_plan_clear_returns_paths`** — Plan paths are correctly computed from root_path + rel_path.
13. **`test_plan_clear_counts_roots`** — Excluded sources across 2 roots → `root_count == 2`.
14. **`test_plan_clear_empty_when_none_excluded`** — No excluded sources → empty plan.
15. **`test_plan_clear_ignores_suspended_roots`** — Excluded source on suspended root → NOT in plan.
16. **`test_plan_clear_ignores_archive_roots`** — Excluded source on archive root → NOT in plan (source roots only).

**Execute tests:**

17. **`test_execute_set_marks_excluded`** — After execution, sources have `excluded = true` in DB.
18. **`test_execute_clear_clears_excluded`** — After execution, sources have `excluded = false` in DB.
19. **`test_execute_set_returns_count`** — Returns correct count.
20. **`test_execute_clear_returns_count`** — Returns correct count.

## ADR Step 6 Remaining Work

This spec covers one story of the plan/execute extraction (ADR Step 6). The full picture:

| Story | Scope | Status | Spec |
|-------|-------|--------|------|
| **A: set + clear** | Source-level multi-target plan/execute | **This spec** | This file |
| **B: duplicates** | `exclude_duplicates()` plan/execute. Richer plan struct (group count, skip categories: no-copy, multiple, in-prefer, unhashed). Already uses `domain::exclusion::find_excludable_duplicates()`. | **Completed** | `.claude/specs/2026-03-14-exclude-plan-execute-duplicates.md` |
| **C: object-level + single-target** | `set_objects_by_filter()` plan/execute (different ceremony: dry-run-default, `--yes` to execute). Plus five thin single-target operations (`set_by_id`, `set_by_path`, `set_object_by_hash`, `set_object_by_file`, `clear_object`) and read-only `list_objects()`. | **Phase 1 completed** (plan/execute for `set_objects_by_filter`). Phase 2 (single-target + `list_objects`) pending. | `.claude/specs/2026-03-14-exclude-plan-execute-objects.md` |
| **D: cluster/apply** | Manifest workflow plan/execute. Separate from exclude. | Pending | — |
| **E: scan** | Scan pipeline plan/execute. Separate from exclude. | Pending | — |

**ADR Step 7 (Final audit)** follows after all effectful commands are extracted.

### Key design context for future stories

- **Ceremony differences are intentional** (findings doc, Design panel review). Source-level uses interactive confirmation; object-level uses dry-run-default. Plan/execute is ceremony-agnostic — the ops layer returns data, the interface decides presentation.
- **`plan_clear()` owns its selection logic (Option D)** — precedent for operations that need a different selection contract than `select_sources()`.
- **Single-target operations don't need plan/execute** — `set_by_id()`, `set_by_path()`, etc. are trivially simple (lookup → validate → write). Thin operation functions returning result structs are sufficient.
- **`list_objects()` is read-only** — query operation, not a plan/execute candidate.
- **Transaction scope is deferred** — current behavior (implicit per-write transactions) is preserved. Transaction wrapping for atomicity is a cross-cutting concern to address consistently across all effectful operations, not per-command.

## Implementation Checklist

- [x] Create `src/ops/exclude.rs` with plan types, plan functions, execute functions
- [x] Add `pub mod exclude;` to `src/ops/mod.rs`
- [x] Write 20 tests in `ops/exclude.rs`
- [x] Thin `set()` in `exclude.rs` to call `plan_set()` + `execute_set()`
- [x] Thin `clear()` in `exclude.rs` to call `plan_clear()` + `execute_clear()`
- [x] Remove `compute_set_confirmation()`, `get_excluded_sources()`, `get_source_path()`, `SetConfirmation` from `exclude.rs`
- [x] Remove 8 tests from `exclude.rs` that tested extracted helpers
- [x] Update imports in `exclude.rs` (removed unused `Source`, `filter` module)
- [x] Verify all existing tests pass (665 total: 20 new + 653 prior - 8 removed)
- [x] Update CLAUDE.md with plan/execute pattern conventions
