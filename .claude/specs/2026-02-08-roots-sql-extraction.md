# Story: roots.rs SQL Extraction

**Epic**: [Command Module Architectural Compliance](epic-command-architecture.md)
**Status**: Completed
**Created**: 2026-02-08
**Completed**: 2026-02-08

## Objective

Extract all inline SQL from `roots.rs` command module into `repo/root.rs`, bringing the module into full architectural compliance.

## Current State

**roots.rs** has 13 SQL calls across 5 functions:

| Function | SQL Calls | Issue |
|----------|-----------|-------|
| `list()` | 0 | Already compliant — uses `repo::root::fetch_all()` + domain predicates |
| `fetch_file_counts()` | 1 | Private helper with inline SQL |
| `remove()` | 6 | Heavy inline SQL for confirmation stats and cascading delete |
| `set_comment()` | 1 | Inline UPDATE |
| `suspend()` | 2 | Query to check state + UPDATE |
| `unsuspend()` | 2 | Query to check state + UPDATE |

**repo/root.rs** already has:
- `fetch_all()` — returns `Vec<Root>`
- `batch_fetch_by_ids()` — returns `HashMap<i64, Root>`
- `create()` — returns `Root`
- `update_last_scanned_at()` — returns `()`

**domain/root.rs** is already clean with full predicate coverage.

## Design

### New Repo Functions

```rust
// repo/root.rs

/// Fetch file counts (present sources) for roots.
/// Returns HashMap from root_id to count of present sources.
pub fn fetch_file_counts(conn: &Connection, root_ids: &[i64]) -> Result<HashMap<i64, i64>>;

/// Set the suspended state of a root.
pub fn set_suspended(conn: &Connection, root_id: i64, suspended: bool) -> Result<()>;

/// Set or clear the comment on a root.
pub fn set_comment(conn: &Connection, root_id: i64, comment: Option<&str>) -> Result<()>;

/// Remove a root and all its sources/facts.
/// Returns the number of sources deleted.
pub fn remove(conn: &Connection, root_id: i64) -> Result<i64>;
```

### Command Simplification

**suspend() / unsuspend()**:
- Currently re-query the root to check state and get path for display
- After refactoring: Use `Root` object already obtained from `fetch_all()` during spec resolution
- Call `repo::root::set_suspended(root_id, true/false)`

**set_comment()**:
- Simply call `repo::root::set_comment(root_id, comment)`

**remove()**:
- Already has the `Root` object from spec resolution
- For archive count: Fetch sources via `repo::source::batch_fetch_by_roots()`, extract object_ids, use `repo::object::batch_check_archived()`
- For deletion: Call `repo::root::remove(root_id)`

### Archive Count Strategy

The remove confirmation shows "X in archive, Y not in archive". Two approaches:

**Option A**: New `repo::root::count_archived_sources(root_id)` function
**Option B**: Compose existing functions in command layer

We choose **Option B**:
```rust
// In remove() command:
let sources = repo::source::batch_fetch_by_roots(conn, &[root_id])?;
let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
let archived = repo::object::batch_check_archived(conn, &object_ids, None)?;
let in_archive_count = archived.len();
```

This reuses existing, tested repo functions rather than creating a one-off query.

## Decisions

| Decision | Rationale |
|----------|-----------|
| Use existing `batch_check_archived()` for archive count | Reuse tested code; avoid one-off query |
| `remove()` returns source count, not stats struct | Simple is sufficient; user sees same output |
| No transaction wrapper for remove | Current per-statement transactions are acceptable |
| Eliminate redundant root queries in suspend/unsuspend | Root object already available from spec resolution |

## Non-Goals

- Changing remove confirmation UX
- Adding explicit transaction boundaries
- Optimizing any queries
- Touching domain/root.rs
- Batching considerations for `remove()` (single root at a time)

## Test Plan

### Existing Tests (Must Pass)

- `repo/root.rs` — 14 existing tests for fetch_all, batch_fetch_by_ids, create, update_last_scanned_at
- `domain/root.rs` — 36 existing tests for predicates and parsing

### New Tests to Add

**fetch_file_counts()**:
1. `fetch_file_counts_empty_ids` — Empty input returns empty HashMap
2. `fetch_file_counts_root_with_sources` — Returns correct count
3. `fetch_file_counts_root_no_sources` — Root with no sources not in result (or 0)
4. `fetch_file_counts_multiple_roots` — Multiple roots return correct counts
5. `fetch_file_counts_only_present` — Only counts present=1 sources

**set_suspended()**:
1. `set_suspended_activates` — Sets suspended=true
2. `set_suspended_deactivates` — Sets suspended=false
3. `set_suspended_idempotent` — Setting same value is no-op (no error)

**set_comment()**:
1. `set_comment_adds` — Sets comment on root with no comment
2. `set_comment_updates` — Updates existing comment
3. `set_comment_clears` — Setting None clears comment

**remove()**:
1. `remove_empty_root` — Root with no sources returns 0
2. `remove_with_sources` — Returns correct deleted count
3. `remove_deletes_facts` — Facts for sources are deleted
4. `remove_deletes_root` — Root record is deleted

## Implementation Checklist

- [x] Add `fetch_file_counts()` to repo/root.rs with tests
- [x] Add `set_suspended()` to repo/root.rs with tests
- [x] Add `set_comment()` to repo/root.rs with tests
- [x] Add `remove()` to repo/root.rs with tests
- [x] Refactor `roots.rs:suspend()` to use repo function
- [x] Refactor `roots.rs:unsuspend()` to use repo function
- [x] Refactor `roots.rs:set_comment()` to use repo function
- [x] Refactor `roots.rs:remove()` to use repo function + existing source/object functions
- [x] Remove `fetch_file_counts()` helper from roots.rs (now in repo)
- [x] Verify all existing behavior preserved (381 tests pass)
- [x] Update epic spec with completion status

## Backward Compatibility

Command output must remain identical:
- `canon roots` listing format
- `canon roots suspend` / `unsuspend` messages
- `canon roots comment` messages
- `canon roots remove` confirmation and result messages
