# Refactoring Spec: Exclude Plan/Execute — Duplicates

## Overview

Extract `exclude_duplicates()` from the interface layer (`src/exclude.rs`) into the operations layer (`src/ops/exclude.rs`) using the plan/execute pattern established by Story A (set + clear).

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (plan/execute for `duplicates` — Story B).
**Predecessor**: `.claude/specs/2026-03-14-exclude-plan-execute-set-clear.md` (Story A, completed).

## Phase 1: Plan/Execute for `exclude_duplicates()`

- **Status**: completed
- **Goal**: Extract computation and writes from `exclude_duplicates()` into `ops/exclude.rs`, leaving the interface layer responsible only for input resolution, presentation, and ceremony.
- **Scope**: One plan function, one execute function, plan/params types, and tests.
- **Non-goals**: No changes to `find_excludable_duplicates()` in domain layer. No changes to ceremony behavior, output format, or output destinations. No changes to Story C, D, or E scope.
- **Dependencies**: Story A completed (pattern established).

### Architecture

```
Interface (exclude.rs)              Operations (ops/exclude.rs)
─────────────────────               ──────────────────────────
exclude_duplicates():               plan_duplicates():
  parse filters                       select_sources (SourceOnly)
  resolve paths                       collect object_ids
  ─── call plan_duplicates() ──────►  fetch sources_by_object
  empty? print message, return        call find_excludable_duplicates()
  dry-run? display plan, return       compute group_count
  count > 1 && !yes? confirm          collect paths
  ─── call execute_duplicates() ──►   return ExcludeDuplicatesPlan
  print summary
                                     execute_duplicates():
                                       loop: set_excluded(id, true)
                                       return count
```

### New Types in `ops/exclude.rs`

```rust
/// Parameters for planning a duplicate exclusion operation.
pub struct ExcludeDuplicatesParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
    pub prefer_prefix: String,
}

/// Computed plan for excluding duplicate sources. Contains all data the
/// interface needs for dry-run display and confirmation — no further
/// queries needed.
pub struct ExcludeDuplicatesPlan {
    /// Source IDs to exclude.
    pub source_ids: Vec<i64>,
    /// Paths corresponding to source_ids (parallel vector, for display).
    pub paths: Vec<String>,
    /// Distinct object groups being excluded (count of unique object_ids).
    pub group_count: usize,
    /// The prefer path used for duplicate resolution.
    pub prefer_prefix: String,
    /// Total sources in scope (before duplicate analysis).
    pub scope_count: usize,
    /// Sources skipped because they have no object_id (unhashed).
    pub skipped_no_hash: usize,
    /// Sources skipped because they're already in the prefer path.
    pub skipped_in_prefer: usize,
    /// Sources skipped because no copy exists in prefer path.
    pub skipped_not_covered: usize,
    /// Sources skipped because multiple copies exist in prefer path.
    pub skipped_multiple: usize,
}
```

### New Functions in `ops/exclude.rs`

```rust
/// Compute what `exclude duplicates` would do — no side effects.
///
/// Selects non-excluded sources matching scope and filters, runs
/// duplicate analysis via `find_excludable_duplicates()`, and computes
/// confirmation data (group count, skip statistics).
pub fn plan_duplicates(conn: &mut Connection, params: &ExcludeDuplicatesParams) -> Result<ExcludeDuplicatesPlan>;

/// Execute a duplicate exclusion plan — marks sources as excluded.
pub fn execute_duplicates(conn: &Connection, plan: &ExcludeDuplicatesPlan) -> Result<usize>;
```

### `plan_duplicates()` Implementation Notes

1. Build `SelectionParams` with `RolePolicy::SourceOnly`, `IncludeSet::default()`, call `select_sources()`.
2. Set `scope_count = selection.sources.len()`.
3. If scope is empty, return empty plan.
4. Build `HashMap<i64, &Source>` from `selection.sources` for lookups.
5. Collect unique `object_ids` from scope sources (those with `object_id`).
6. Call `repo::source::fetch_sources_by_object_ids(conn, &object_ids)` for duplicate lookup.
7. Call `find_excludable_duplicates(&selection.sources, &sources_by_object, &prefer_prefix)`.
8. From `result.to_exclude`, look up paths via source map, compute `group_count` (distinct `object_id` values).
9. Return `ExcludeDuplicatesPlan` with skip stats carried from domain result.

This eliminates the redundant `batch_fetch_by_ids()` re-fetch — `select_sources()` already returns `Source` objects directly.

### `execute_duplicates()` Implementation Notes

Same pattern as `execute_set()` — simple loop, no `is_excluded()` safety check (idempotent, matches Story A convention):

```rust
pub fn execute_duplicates(conn: &Connection, plan: &ExcludeDuplicatesPlan) -> Result<usize> {
    for &source_id in &plan.source_ids {
        repo::source::set_excluded(conn, source_id, true)?;
    }
    Ok(plan.source_ids.len())
}
```

### Changes to `exclude.rs` (Interface Layer)

**`exclude_duplicates()` becomes:**
1. Parse filters (unchanged).
2. Resolve paths (unchanged — `resolve_path` for prefer, `resolve_path` for scope).
3. Build `ExcludeDuplicatesParams`, call `ops::exclude::plan_duplicates()`.
4. If plan is empty → print "Nothing to exclude", return.
5. If `dry_run` → print statistics + paths (same format as current), return.
6. If `count > 1 && !yes` → display confirmation from plan fields, call `ceremony::confirm()`.
7. Call `ops::exclude::execute_duplicates()`.
8. Print summary.

**No functions removed** — the logic is inline in `exclude_duplicates()`, not in separate helpers. The function body is thinned, not helpers absorbed.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `plan_duplicates()` uses `select_sources()` | Standard "find visible sources" contract works for scope selection. Same as `plan_set()`. |
| Skip stats carried from domain result | `ExcludableDuplicatesResult` fields map directly to plan fields — no transformation needed. |
| `group_count` always pre-computed | Cheap (HashSet count), avoids interface needing to query. All presentation data in the plan. |
| No `is_excluded()` check in execute | Idempotent operation, matches `execute_set()` convention. Plan computes the correct set. |
| Redundant `batch_fetch_by_ids()` eliminated | `select_sources()` returns Source objects — no re-fetch needed. |

## Test Requirements

### Existing Tests (Must Pass)

All existing tests (665 total), plus the domain tests in `domain/exclusion.rs` (12 tests) are untouched.

### Tests to Remove from `exclude.rs`

These test behavior that moves to the ops layer:
- `test_exclude_duplicates_excludes_when_one_copy_in_prefer` → replaced by `test_plan_duplicates_excludes_with_prefer_copy`
- `test_exclude_duplicates_skips_when_no_copy_in_prefer` → replaced by `test_plan_duplicates_skips_no_copy`
- `test_exclude_duplicates_skips_when_multiple_copies_in_prefer` → replaced by `test_plan_duplicates_skips_multiple_copies`
- `test_exclude_duplicates_skips_source_already_in_prefer` → replaced by `test_plan_duplicates_skips_in_prefer`
- `test_exclude_duplicates_path_prefix_no_false_positive` → covered by domain tests
- `test_exclude_duplicates_empty_rel_path` → covered by domain tests
- `test_duplicates_group_count` → replaced by `test_plan_duplicates_computes_group_count`

### Test to Keep in `exclude.rs`

- `test_duplicates_single_source_no_confirmation` — integration test validating full plan→execute flow end-to-end.

### New Tests to Add (in `ops/exclude.rs`)

**`plan_duplicates()` tests:**

1. **`test_plan_duplicates_empty_when_no_sources`** — No sources in DB → plan has empty `source_ids`.
2. **`test_plan_duplicates_excludes_with_prefer_copy`** — Source with exactly 1 copy in prefer → in plan.
3. **`test_plan_duplicates_skips_no_copy`** — No copy in prefer → `skipped_not_covered` incremented.
4. **`test_plan_duplicates_skips_multiple_copies`** — 2+ copies in prefer → `skipped_multiple` incremented.
5. **`test_plan_duplicates_skips_unhashed`** — Source with no `object_id` → `skipped_no_hash` incremented.
6. **`test_plan_duplicates_skips_in_prefer`** — Source in prefer path → `skipped_in_prefer` incremented.
7. **`test_plan_duplicates_computes_group_count`** — 4 sources across 2 objects → `group_count == 2`.
8. **`test_plan_duplicates_includes_paths`** — Plan paths match source paths.
9. **`test_plan_duplicates_scope_count`** — `scope_count` reflects total sources in scope.
10. **`test_plan_duplicates_respects_scope`** — Sources outside scope prefix not considered.

**`execute_duplicates()` tests:**

11. **`test_execute_duplicates_marks_excluded`** — After execution, sources have `excluded = true` in DB.
12. **`test_execute_duplicates_returns_count`** — Returns correct count.

### Expected Test Count

665 current − 7 removed + 12 new = **670 total**

## Implementation Checklist

- [x] Add plan/params/execute types and functions to `src/ops/exclude.rs`
- [x] Write 12 tests in `ops/exclude.rs`
- [x] Thin `exclude_duplicates()` in `exclude.rs` to call `plan_duplicates()` + `execute_duplicates()`
- [x] Remove 7 tests from `exclude.rs` that tested extracted behavior
- [x] Update imports in `exclude.rs` (removed `find_excludable_duplicates`, added ops types)
- [x] Verify all tests pass (670 total: 32 ops/exclude + 638 others)
- [x] Update Story B status in Story A spec's remaining work table
