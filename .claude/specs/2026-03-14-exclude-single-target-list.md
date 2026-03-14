# Refactoring Spec: Exclude — Single-Target Operations & List

## Overview

Extract single-target exclusion operations and `list_objects()` from the interface layer (`src/exclude.rs`) into the operations layer (`src/ops/exclude.rs`) using thin check/execute functions (NOT plan/execute — these are trivially simple operations).

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (Story C Phase 2).
**Predecessor**: `.claude/specs/2026-03-14-exclude-plan-execute-objects.md` (Story C Phase 1, completed).

## Phase 1: Single-Target & List Extraction

- **Status**: completed
- **Goal**: Extract behavioral logic (repo calls + domain predicates) from single-target operations and `list_objects()` into ops, leaving the interface responsible only for path resolution, display formatting, and ceremony.
- **Scope**: Check functions, execute wrappers, result types, data-fetching helper, list query, and tests.
- **Non-goals**: No ceremony changes, no output format changes, no changes to plan/execute operations (Stories A/B/C Phase 1).
- **Dependencies**: Story C Phase 1 completed.

### Architecture

```
Interface (exclude.rs)              Operations (ops/exclude.rs)
─────────────────────               ──────────────────────────
set_by_id():                        check_set_source_by_id():
  ── call check ──────────────────►   fetch source via batch_fetch_by_ids
  AlreadyExcluded? print, return      check is_excluded() predicate
  dry-run? print, return              return SourceExclusionCheck
  ── call exclude_source() ──────►  exclude_source():
  print summary                       set_excluded(id, true)

set_by_path():                      check_set_source_by_path():
  resolve path, find root             fetch source via fetch_by_path
  ── call check ──────────────────►   check is_excluded() predicate
  (same ceremony as set_by_id)        return SourceExclusionCheck

set_object_by_hash():               check_set_object_by_hash():
  ── call check ──────────────────►   fetch object via fetch_by_hash
  (same ceremony as below)            check is_excluded() predicate
                                      fetch sources for display
set_object_by_file():                 return ObjectExclusionCheck
  resolve path, find root
  ── call check ──────────────────► check_set_object_by_file():
  AlreadyExcluded? print, return      fetch source, check hashed
  dry-run? print plan, return         empty-file safety check
  ── call exclude_object() ──────►    fetch object, check excluded
  print summary                       fetch sources for display
                                      return ObjectExclusionCheck
clear_object():
  ── call check ──────────────────► check_clear_object():
  NotExcluded? print, return          fetch object via fetch_by_hash
  dry-run? print, return              check is_excluded() predicate
  ── call clear_object_excl() ───►    return ObjectClearCheck
  print summary
                                    list_excluded_objects():
list_objects():                       fetch excluded objects
  ── call list ───────────────────►   batch fetch source counts
  format + print                      return Vec<ExcludedObjectEntry>
```

### New Types in `ops/exclude.rs`

```rust
/// Outcome of validating a single source for exclusion.
pub enum SourceExclusionCheck {
    /// Source found and eligible for exclusion.
    Ready { source_id: i64, path: String },
    /// Source is already excluded (at source or object level).
    AlreadyExcluded { path: String },
}

/// Outcome of validating a single object for exclusion.
pub enum ObjectExclusionCheck {
    /// Object found and eligible for exclusion.
    Ready {
        object_id: i64,
        hash_prefix: String,
        sources: Vec<ObjectSourceInfo>,
    },
    /// Object is already excluded.
    AlreadyExcluded { hash_prefix: String },
}

/// Outcome of validating a single object for clearing exclusion.
pub enum ObjectClearCheck {
    /// Object found and currently excluded — eligible for clearing.
    Ready { object_id: i64, hash_prefix: String },
    /// Object is not excluded.
    NotExcluded { hash_prefix: String },
}

/// Entry in the excluded objects list.
pub struct ExcludedObjectEntry {
    pub object_id: i64,
    pub hash_prefix: String,
    pub source_count: usize,
}
```

### New Functions in `ops/exclude.rs`

```rust
// Source-level single-target
pub fn check_set_source_by_id(conn: &Connection, source_id: i64) -> Result<SourceExclusionCheck>;
pub fn check_set_source_by_path(conn: &Connection, root_id: i64, rel_path: &str, display_path: &str) -> Result<SourceExclusionCheck>;
pub fn exclude_source(conn: &Connection, source_id: i64) -> Result<()>;

// Object-level single-target
pub fn check_set_object_by_hash(conn: &Connection, hash: &str) -> Result<ObjectExclusionCheck>;
pub fn check_set_object_by_file(conn: &Connection, root_id: i64, rel_path: &str, display_path: &str) -> Result<ObjectExclusionCheck>;
pub fn exclude_object(conn: &Connection, object_id: i64) -> Result<()>;

// Object-level clear
pub fn check_clear_object(conn: &Connection, hash: &str) -> Result<ObjectClearCheck>;
pub fn clear_object_exclusion(conn: &Connection, object_id: i64) -> Result<()>;

// Data fetching (used by check functions, also public for reuse)
pub fn fetch_object_sources(conn: &Connection, object_id: i64) -> Result<Vec<ObjectSourceInfo>>;

// Read-only query
pub fn list_excluded_objects(conn: &Connection) -> Result<Vec<ExcludedObjectEntry>>;
```

### Implementation Notes

**`check_set_source_by_id()`**: Calls `batch_fetch_by_ids(conn, &[source_id])`. If not found → error. If `is_excluded()` → `AlreadyExcluded`. Otherwise → `Ready`.

**`check_set_source_by_path()`**: Calls `fetch_by_path(conn, root_id, rel_path)`. If not found → error (uses `display_path`). If `is_excluded()` → `AlreadyExcluded`. Otherwise → `Ready`.

**`check_set_object_by_hash()`**: Calls `fetch_by_hash(conn, hash)`. If not found → error. If `is_excluded()` → `AlreadyExcluded`. Otherwise → fetches sources via `fetch_object_sources()`, returns `Ready`.

**`check_set_object_by_file()`**: Calls `fetch_by_path(conn, root_id, rel_path)`. Validates source exists, has `object_id`, size > 0. Fetches object, checks `is_excluded()`. If ready → fetches sources. Error messages use `display_path`.

**`check_clear_object()`**: Calls `fetch_by_hash(conn, hash)`. If not found → error. If not excluded → `NotExcluded`. Otherwise → `Ready`.

**`fetch_object_sources()`**: Absorbs `get_object_sources()` from interface. Same logic: `fetch_sources_by_object_ids`, sort by role DESC/root_path/rel_path, map to `ObjectSourceInfo`.

**`list_excluded_objects()`**: Absorbs `list_objects()` computation. Calls `fetch_excluded()`, batch fetches source counts, returns `Vec<ExcludedObjectEntry>`.

**Execute wrappers** (`exclude_source`, `exclude_object`, `clear_object_exclusion`): One-line wrappers around repo write functions. Ensures interface never calls repo directly.

### Changes to `exclude.rs` (Interface Layer)

**Functions thinned** (call ops check + format result):
- `set_by_id()` — calls `check_set_source_by_id()` + `exclude_source()`
- `set_by_path()` — resolves path, calls `check_set_source_by_path()` + `exclude_source()`
- `set_object_by_hash()` — calls `check_set_object_by_hash()` + `exclude_object()`
- `set_object_by_file()` — resolves path, calls `check_set_object_by_file()` + `exclude_object()`
- `clear_object()` — calls `check_clear_object()` + `clear_object_exclusion()`
- `list_objects()` — calls `list_excluded_objects()`, formats output

**Functions removed from `exclude.rs`:**
- `exclude_object_by_id()` — absorbed into check functions + write wrappers
- `get_object_sources()` — absorbed into `ops::exclude::fetch_object_sources()`
- `SourceInfo` struct — replaced by existing `ObjectSourceInfo`

**`print_source_locations()` signature change**: Takes `&[ObjectSourceInfo]` instead of `&[SourceInfo]`. Body unchanged.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Check/execute pattern (not plan/execute) | Single-target operations are trivially simple — plan/execute overhead not warranted. Check validates + returns data, execute does the write. |
| `display_path` parameter for path-based checks | Error messages need the user-visible path. The ops function doesn't do path resolution (that's interface), so it receives the display string. |
| Execute wrappers for single writes | Ensures interface never calls repo directly. One-liners, minimal overhead. |
| `fetch_object_sources()` is public | Used internally by check functions and available for Phase 2+ reuse (single-target operations that need source display). |

## Test Requirements

### Existing Tests (Must Pass)

All 679 existing tests.

### Tests to Remove from `exclude.rs`

These test behavioral logic that moves to ops:
- `test_set_by_id_nonexistent_fails` → replaced by `test_check_source_by_id_not_found`
- `test_set_by_id_already_excluded_skips` → replaced by `test_check_source_by_id_already_excluded`
- `test_set_by_id_not_present_fails` → replaced by `test_check_source_by_id_not_present`
- `test_get_object_sources_returns_paths` → replaced by `test_fetch_object_sources_returns_paths`
- `test_get_object_sources_includes_role` → replaced by `test_fetch_object_sources_includes_role`
- `test_get_object_sources_empty_rel_path` → replaced by `test_fetch_object_sources_empty_rel_path`
- `test_get_object_sources_excludes_not_present` → replaced by `test_fetch_object_sources_excludes_not_present`
- `test_list_objects_shows_excluded` → replaced by `test_list_excluded_objects_returns_entries`
- `test_list_objects_shows_source_count` → replaced by `test_list_excluded_objects_source_counts`
- `test_list_objects_empty` → replaced by `test_list_excluded_objects_empty`

### Tests to Keep in `exclude.rs`

- `test_set_by_id_excludes_source` — integration: full check→execute flow
- `test_set_by_path_nonexistent_file_fails` — integration: path resolution
- `test_set_by_path_not_in_db_fails` — integration: path resolution
- `test_set_objects_by_filter_excludes_objects` — integration: plan→execute flow
- `test_set_objects_by_filter_dry_run` — integration: dry-run ceremony
- `test_set_single_source_no_confirmation` — integration: ceremony
- `test_duplicates_single_source_no_confirmation` — integration: ceremony

### New Tests to Add (in `ops/exclude.rs`)

**Source-level check tests (7):**
1. `test_check_source_by_id_ready` — source found, not excluded → Ready with path
2. `test_check_source_by_id_already_excluded` — source excluded → AlreadyExcluded
3. `test_check_source_by_id_not_found` — nonexistent ID → error
4. `test_check_source_by_id_not_present` — present=false → error
5. `test_check_source_by_path_ready` — source found by path → Ready
6. `test_check_source_by_path_not_found` — no source at path → error
7. `test_check_source_by_path_already_excluded` — excluded source by path → AlreadyExcluded

**Object exclusion check tests (7):**
8. `test_check_object_by_hash_ready` — object found, not excluded → Ready with sources
9. `test_check_object_by_hash_not_found` — no object → error
10. `test_check_object_by_hash_already_excluded` — excluded → AlreadyExcluded
11. `test_check_object_by_file_ready` — source with object → Ready
12. `test_check_object_by_file_not_found` — no source → error
13. `test_check_object_by_file_unhashed` — source without object_id → error
14. `test_check_object_by_file_empty` — source.size == 0 → error

**Object clear check tests (3):**
15. `test_check_clear_object_ready` — excluded object → Ready
16. `test_check_clear_object_not_found` — no object → error
17. `test_check_clear_object_not_excluded` — non-excluded → NotExcluded

**Fetch object sources tests (4):**
18. `test_fetch_object_sources_returns_paths` — paths correctly constructed
19. `test_fetch_object_sources_includes_role` — archive/source roles
20. `test_fetch_object_sources_empty_rel_path` — empty rel_path handled
21. `test_fetch_object_sources_excludes_not_present` — not-present filtered

**List excluded objects tests (3):**
22. `test_list_excluded_objects_returns_entries` — excluded objects returned
23. `test_list_excluded_objects_source_counts` — source counts correct
24. `test_list_excluded_objects_empty` — no excluded → empty vec

### Expected Test Count

679 current − 10 removed + 24 new = **693 total**

## Implementation Checklist

- [x] Add check/result types and functions to `src/ops/exclude.rs`
- [x] Add `fetch_object_sources()` and `list_excluded_objects()` to `src/ops/exclude.rs`
- [x] Write 24 tests in `ops/exclude.rs`
- [x] Thin 6 interface functions in `exclude.rs` to call ops check + execute
- [x] Remove `exclude_object_by_id()`, `get_object_sources()`, `SourceInfo` from `exclude.rs`
- [x] Update `print_source_locations()` signature to use `ObjectSourceInfo`
- [x] Remove 10 tests from `exclude.rs`
- [x] Update imports in `exclude.rs`
- [x] Verify all tests pass (693 total)
- [x] Update Story C status in Story A spec's remaining work table
