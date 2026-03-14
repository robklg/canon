# Refactoring Spec: Exclude Plan/Execute — Set Objects

## Overview

Extract `set_objects_by_filter()` from the interface layer (`src/exclude.rs`) into the operations layer (`src/ops/exclude.rs`) using the plan/execute pattern established by Stories A and B.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (plan/execute for `set_objects_by_filter` — Story C Phase 1).
**Predecessor**: `.claude/specs/2026-03-14-exclude-plan-execute-duplicates.md` (Story B, completed).

## Phase 1: Plan/Execute for `set_objects_by_filter()`

- **Status**: completed
- **Goal**: Extract computation and writes from `set_objects_by_filter()` into `ops/exclude.rs`, leaving the interface layer responsible only for input resolution, presentation, and ceremony.
- **Scope**: One plan function, one execute function, plan/params types, and tests.
- **Non-goals**: No changes to single-target operations (`set_by_id`, `set_by_path`, `set_object_by_hash`, `set_object_by_file`, `clear_object`), `list_objects()`, `get_object_sources()`, `print_source_locations()`, or ceremony behavior. These are deferred to Phase 2 / ADR Step 7.
- **Dependencies**: Stories A and B completed (pattern established).

### Architecture

```
Interface (exclude.rs)              Operations (ops/exclude.rs)
─────────────────────               ──────────────────────────
set_objects_by_filter():            plan_set_objects():
  parse filters                       select_sources (excluded=true)
  resolve paths                       collect unique object_ids
  ─── call plan_set_objects() ──────► skip unhashed, empty
  empty? print message, return        batch fetch objects
  dry-run? display plan, return       filter non-excluded
  ─── call execute_set_objects() ──► batch fetch sources per object
  print summary                       compute totals + skip stats
                                      return ExcludeSetObjectsPlan

                                    execute_set_objects():
                                      loop: set_excluded(id, true)
                                      return count
```

### Key Differences from Stories A/B

| Aspect | Stories A/B (source-level) | Story C (object-level) |
|--------|--------------------------|----------------------|
| Entity | Sources | Objects |
| Write call | `repo::source::set_excluded()` | `repo::object::set_excluded()` |
| Ceremony | Interactive confirmation | Dry-run-default (`--yes` to execute) |
| IncludeSet | Default (exclude excluded) | `excluded: true` (include excluded sources to find objects) |
| Safety | — | Empty files (size=0) skipped |

### New Types in `ops/exclude.rs`

```rust
/// Parameters for planning an object exclusion operation.
pub struct ExcludeSetObjectsParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
}

/// Computed plan for excluding objects. Contains all data the interface
/// needs for dry-run display and confirmation — no further queries needed.
pub struct ExcludeSetObjectsPlan {
    /// Objects to exclude, with display data.
    pub objects: Vec<ObjectPlanEntry>,
    /// Total source count across all objects.
    pub total_source_count: usize,
    /// Total archive source count across all objects.
    pub total_archive_count: usize,
    /// Sources skipped because they have no hash.
    pub skipped_no_hash: usize,
    /// Empty files skipped (size = 0).
    pub skipped_empty: usize,
    /// Objects already excluded.
    pub skipped_already_excluded: usize,
}

/// A single object entry in the exclusion plan.
pub struct ObjectPlanEntry {
    pub object_id: i64,
    /// Hash prefix for display (first 16 chars).
    pub hash_prefix: String,
    /// Sources linked to this object (sorted: role DESC, root_path, rel_path).
    pub sources: Vec<ObjectSourceInfo>,
}

/// Source info for object exclusion display.
pub struct ObjectSourceInfo {
    pub path: String,
    pub is_archive: bool,
}
```

### New Functions in `ops/exclude.rs`

```rust
/// Compute what `exclude set --objects` would do — no side effects.
///
/// Selects sources matching scope and filters (including already-excluded
/// sources), collects their objects, filters out unhashed/empty/already-excluded,
/// and computes display data per object.
pub fn plan_set_objects(conn: &mut Connection, params: &ExcludeSetObjectsParams) -> Result<ExcludeSetObjectsPlan>;

/// Execute an object exclusion plan — marks objects as excluded.
pub fn execute_set_objects(conn: &Connection, plan: &ExcludeSetObjectsPlan) -> Result<usize>;
```

### `plan_set_objects()` Implementation Notes

1. Build `SelectionParams` with `RolePolicy::SourceOnly`, `IncludeSet { excluded: true, archived: false }`, call `select_sources()`.
2. Iterate `selection.sources` directly (no re-fetch), collect unique `object_ids`. Count `skipped_no_hash` per source. Check `size == 0` for first source per object, count `skipped_empty`.
3. Batch fetch objects via `repo::object::batch_fetch_by_ids()`. Filter for non-excluded, count `skipped_already_excluded`.
4. Batch fetch sources per object via `repo::source::fetch_sources_by_object_ids()` for display data.
5. Per object: sort sources by `root_role` DESC, `root_path`, `rel_path`. Build `ObjectPlanEntry` with hash prefix and source info.
6. Compute `total_source_count` and `total_archive_count` from plan entries.

This eliminates two redundant fetches in the current code:
- `batch_fetch_by_ids()` re-fetch after `select_sources()` (uses `selection.sources` directly)
- Per-object `get_object_sources()` after batch `fetch_sources_by_object_ids()` (uses batch data directly)

### `execute_set_objects()` Implementation Notes

```rust
pub fn execute_set_objects(conn: &Connection, plan: &ExcludeSetObjectsPlan) -> Result<usize> {
    for entry in &plan.objects {
        repo::object::set_excluded(conn, entry.object_id, true)?;
    }
    Ok(plan.objects.len())
}
```

### Changes to `exclude.rs` (Interface Layer)

**`set_objects_by_filter()` becomes:**
1. Parse filters (unchanged).
2. Resolve scope paths (unchanged).
3. Build `ExcludeSetObjectsParams`, call `ops::exclude::plan_set_objects()`.
4. If plan is empty → print "No objects to exclude" + skip stats, return.
5. If `dry_run` → print plan + "Use --yes to execute.", return.
6. Call `ops::exclude::execute_set_objects()`.
7. Print summary.

**Imports removed from `exclude.rs`:**
- `std::collections::HashSet` — was only used by `set_objects_by_filter` computation
- `crate::domain::include::IncludeSet` — was only used by `set_objects_by_filter` selection
- `crate::ops::selection::{self, RolePolicy, SelectionParams}` — was only used by `set_objects_by_filter` selection

**No functions removed** — `get_object_sources()`, `print_source_locations()`, `SourceInfo`, and all single-target operations stay for Phase 2.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `plan_set_objects()` uses `select_sources()` with `excluded: true` | Must see all matching sources to find their objects — excluding already-excluded sources would miss their objects. Preserves current behavior. |
| Empty file safety in plan, not execute | Plan simply doesn't include size=0 objects. Execute is a simple loop. |
| Batch source fetching in plan | Eliminates two redundant fetches in current code. Single `fetch_sources_by_object_ids()` provides all display data. |
| `ObjectSourceInfo` as dedicated type (not reusing `SourceInfo`) | `SourceInfo` is a private interface type. `ObjectSourceInfo` lives in ops and can be used by Phase 2 single-target operations too. |

## Test Requirements

### Existing Tests (Must Pass)

All 670 existing tests.

### Tests to Remove from `exclude.rs`

These test computation logic that moves to the ops layer:
- `test_set_objects_by_filter_skips_empty_files` → replaced by `test_plan_set_objects_skips_empty`
- `test_set_objects_by_filter_skips_already_excluded` → replaced by `test_plan_set_objects_skips_already_excluded`
- `test_set_objects_by_filter_skips_unhashed` → replaced by `test_plan_set_objects_skips_unhashed`

### Tests to Keep in `exclude.rs`

- `test_set_objects_by_filter_excludes_objects` — integration: full plan→execute flow
- `test_set_objects_by_filter_dry_run` — integration: dry-run ceremony

### New Tests to Add (in `ops/exclude.rs`)

**`plan_set_objects()` tests:**

1. **`test_plan_set_objects_empty_when_no_sources`** — No sources → empty plan.
2. **`test_plan_set_objects_includes_non_excluded`** — Non-excluded object → in plan.
3. **`test_plan_set_objects_skips_already_excluded`** — Already excluded object → `skipped_already_excluded` incremented.
4. **`test_plan_set_objects_skips_unhashed`** — Source without `object_id` → `skipped_no_hash` incremented.
5. **`test_plan_set_objects_skips_empty`** — Source with `size=0` → `skipped_empty` incremented.
6. **`test_plan_set_objects_computes_source_counts`** — `total_source_count` and `total_archive_count` correct.
7. **`test_plan_set_objects_hash_prefix`** — `hash_prefix` is first 16 chars of hash.
8. **`test_plan_set_objects_respects_scope`** — Scoped sources only.
9. **`test_plan_set_objects_deduplicates_objects`** — Multiple sources sharing same object → one plan entry.
10. **`test_plan_set_objects_source_sort_order`** — Sources sorted by role DESC, root_path, rel_path.

**`execute_set_objects()` tests:**

11. **`test_execute_set_objects_marks_excluded`** — After execution, objects have `excluded = true` in DB.
12. **`test_execute_set_objects_returns_count`** — Returns correct count.

### Expected Test Count

670 current − 3 removed + 12 new = **679 total**

## Implementation Checklist

- [x] Add plan/params/execute types and functions to `src/ops/exclude.rs`
- [x] Write 12 tests in `ops/exclude.rs`
- [x] Thin `set_objects_by_filter()` in `exclude.rs` to call `plan_set_objects()` + `execute_set_objects()`
- [x] Remove 3 tests from `exclude.rs` that tested extracted behavior
- [x] Remove unused imports from `exclude.rs` (`HashSet`, `IncludeSet`, `selection`)
- [x] Verify all tests pass (679 total: 44 ops/exclude + 635 others)
- [x] Update Story C status in Story A spec's remaining work table
