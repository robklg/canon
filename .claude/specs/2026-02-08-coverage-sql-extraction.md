# Story: coverage.rs SQL Extraction

**Epic**: [Command Module Architectural Compliance](epic-command-architecture.md)
**Status**: Completed
**Created**: 2026-02-08

## Objective

Extract the 2 remaining inline SQL queries from `coverage.rs` into repo layer calls, bringing the module into full architectural compliance.

## Current State

**coverage.rs** has 2 inline SQL queries:

| Location | SQL | Issue |
|----------|-----|-------|
| `get_matching_sources()` L143-145 | `SELECT id FROM roots` | Should use existing `repo::root::fetch_all()` |
| `compute_per_root_stats()` L200-206 | `SELECT id, path, role FROM roots WHERE ...` | Should filter already-fetched roots with domain predicates |

**What's already compliant:**
- Uses `repo::root::fetch_all()` for spec resolution (L84)
- Uses `repo::source::batch_fetch_by_roots()` correctly
- Uses `repo::object::batch_check_archived()` correctly
- Uses domain predicates throughout (`is_active()`, `is_from_role()`, `matches_scope()`, `is_excluded()`)

## Design

### Change 1: `get_matching_sources()`

**Before:**
```rust
let root_ids: Vec<i64> = conn
    .prepare("SELECT id FROM roots")?
    .query_map([], |row| row.get(0))?
    .collect::<Result<Vec<_>, _>>()?;
```

**After:**
```rust
let roots = repo::root::fetch_all(conn)?;
let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();
```

### Change 2: `compute_per_root_stats()`

**Before:**
```rust
let role_clause = if include_archived {
    "suspended = 0"
} else {
    "role = 'source' AND suspended = 0"
};

let roots: Vec<(i64, String, String)> = conn
    .prepare(&format!(
        "SELECT id, path, role FROM roots WHERE {} ORDER BY id",
        role_clause
    ))?
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
    .collect::<Result<Vec<_>, _>>()?;
```

**After:**
```rust
let all_roots = repo::root::fetch_all(conn)?;
let roots: Vec<&Root> = all_roots
    .iter()
    .filter(|r| r.is_active())  // replaces suspended = 0
    .filter(|r| include_archived || r.is_source())  // replaces role filter
    .collect();
```

Then update the loop to use `root.id`, `root.path`, `root.role` directly.

## Decisions

| Decision | Rationale |
|----------|-----------|
| Reuse `repo::root::fetch_all()` | Already exists and is tested; no new repo functions needed |
| Filter with domain predicates | Consistent with architectural patterns; `is_active()`, `is_source()` already exist |
| No new repo functions | The 2 queries are trivially replaced by existing infrastructure |

## Non-Goals

- Changing coverage calculation logic
- Optimizing query performance
- Adding new CLI flags or features
- Modifying CoverageStats struct
- Changing display output format

## Test Plan

### Existing Tests (Must Pass)

- `test_coverage_archived_counts_sources_not_objects` — Critical regression test for Object Infrastructure bug

### New Tests to Add

**test_get_matching_sources_respects_scope**:
```rust
#[test]
fn test_get_matching_sources_respects_scope() {
    // Setup:
    // - Root /photos (source role) with sources: photo1.jpg, photo2.jpg
    // - Root /videos (source role) with source: video1.mp4

    // Test 1: Scoped to /photos
    // - Call get_matching_sources with scopes for /photos
    // - Assert: Returns exactly 2 source IDs (the photos)

    // Test 2: Unscoped (empty scopes = all)
    // - Call get_matching_sources with empty scopes
    // - Assert: Returns all 3 source IDs
}
```

This test validates the full scope-filtering pipeline:
- Path canonicalization → ScopeMatch classification → Source::matches_scope() predicate

**Rationale**: The refactoring touches the source-fetching path. This integration test ensures scope filtering continues to work correctly end-to-end.

## Implementation Checklist

- [x] Refactor `get_matching_sources()` to use `repo::root::fetch_all()`
- [x] Refactor `compute_per_root_stats()` to use fetched roots + domain predicates
- [x] Add `test_get_matching_sources_respects_scope` test
- [x] Verify all existing tests pass (382 tests)
- [x] Update epic spec with completion status

## Backward Compatibility

Command output must remain identical:
- `canon coverage` per-root breakdown format
- `canon coverage <scope>` scoped output format
- `canon coverage --compact` compact format
- All percentage calculations unchanged
