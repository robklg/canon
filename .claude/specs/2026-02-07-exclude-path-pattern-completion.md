# Spec: Exclude Path Pattern Completion

**Date:** 2026-02-07
**Status:** Complete
**Priority:** Medium
**Predecessor:** `.claude/specs/2026-02-07-exclude-duplicates-extraction.md` (established the pattern)

## Problem Statement

The `exclude_duplicates` refactoring established a pattern where "SQL never constructs paths" — all path logic uses `Source::path()` in the domain layer. However, **5 other functions in `exclude.rs`** still use the old pattern with inline SQL `r.path || '/' || s.rel_path`.

This creates inconsistency in the codebase. Pattern consistency:
- Reduces cognitive load for developers
- Ensures future changes follow established patterns
- Improves reliability through uniform behavior
- Makes the R1 inconsistency (empty `rel_path` edge case) impossible

### Functions to Refactor

| Function | Lines | Current Pattern | Issue |
|----------|-------|-----------------|-------|
| `get_source_path()` | 296-307 | `r.path \|\| '/' \|\| s.rel_path` | SQL path construction |
| `set_by_id()` | 310-345 | `r.path \|\| '/' \|\| s.rel_path` | SQL path construction |
| `set_by_path()` | 348-390 | `WHERE r.path \|\| '/' \|\| s.rel_path = ?` | SQL path comparison |
| `set_object_by_file()` | 522-573 | `WHERE r.path \|\| '/' \|\| s.rel_path = ?` | SQL path comparison |
| `get_object_sources()` | 728-747 | `r.path \|\| '/' \|\| s.rel_path` | SQL path construction |

### What's NOT in Scope

The following `rel_path LIKE` patterns are **acceptable** and not included:

| Location | Pattern | Why Acceptable |
|----------|---------|----------------|
| `scan.rs:51` | `rel_path LIKE ?/%` | Relative path prefix within single root, not absolute path construction |
| `repo/source.rs:616` | `rel_path LIKE ?%` | Simple prefix match for pagination, no path construction |

These operate on relative paths only and have different semantics from absolute path construction.

---

## Architectural Direction

### Target Pattern (from predecessor spec)

```
┌──────────────────────────────────────────────────────────────┐
│ Command Layer (exclude.rs)                                   │
│ - Canonicalizes CLI args — ONLY I/O here                     │
│ - Decomposes absolute paths using domain functions           │
│ - Orchestrates: repo fetch → domain logic → repo write       │
└──────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Repo Layer                   │   │ Domain Layer                 │
│ - fetch_by_path(root, rel)   │   │ - find_containing_root()     │
│ - batch_fetch_by_ids()       │   │ - path_strip_prefix()        │
│ - fetch_sources_by_object_ids│   │ - Source::path() [pure]      │
│ - Returns Source with        │   │ - No I/O, fully testable     │
│   root_path populated        │   │                              │
└──────────────────────────────┘   └──────────────────────────────┘
```

### Key Discovery: No New Repo Functions Needed

The existing `repo::source::fetch_by_path(conn, root_id, rel_path)` already exists. The refactoring pattern for path-lookup functions:

```rust
// 1. Canonicalize the input path (command boundary I/O)
let canonical = std::fs::canonicalize(file_path)?;

// 2. Find which root contains this path (domain layer - pure)
let roots = repo::root::fetch_all(conn)?;
let root = domain::root::find_containing_root(&canonical, &roots)?;

// 3. Compute relative path (domain layer - pure)
let rel_path = domain::path::path_strip_prefix(&canonical, &root.path)?;

// 4. Fetch the source (repo layer)
let source = repo::source::fetch_by_path(conn, root.id, &rel_path)?;

// 5. Use source.path() for display
println!("Excluded: {}", source.path());
```

---

## Proposed Implementation

### Phase 1: Add Integration Tests

**Status:** completed

**Goal**: Establish correctness baseline before any refactoring.

**Scope**:

| Test | Description |
|------|-------------|
| `test_set_by_id_excludes_source` | Basic exclusion by ID works |
| `test_set_by_id_nonexistent_fails` | Error on invalid source ID |
| `test_set_by_id_already_excluded_skips` | Idempotent behavior |
| `test_set_by_path_excludes_source` | Basic exclusion by path works |
| `test_set_by_path_nonexistent_fails` | Error on invalid path |
| `test_set_by_path_empty_rel_path` | Edge case: root IS the file (R1 scenario) |
| `test_get_object_sources_returns_paths` | Returns correct absolute paths |
| `test_get_object_sources_includes_role` | Archive vs source distinction preserved |

**Non-goals**: Refactoring, performance changes.

### Phase 2: Refactor Display Functions

**Status:** completed

**Goal**: Migrate simple display-only functions to use `Source::path()`.

**Scope**:

1. **`get_source_path()`** — Replace inline SQL with:
   ```rust
   let sources = repo::source::batch_fetch_by_ids(conn, &[source_id])?;
   Ok(sources.get(&source_id).map(|s| s.path()))
   ```

2. **`get_object_sources()`** — Replace inline SQL with:
   ```rust
   let sources_map = repo::source::fetch_sources_by_object_ids(conn, &[object_id])?;
   let sources = sources_map.get(&object_id).unwrap_or(&Vec::new());
   // Map to SourceInfo using source.path() and source root role
   ```

**Non-goals**: Path lookup functions (Phase 3).

### Phase 3: Refactor Path Lookup Functions

**Status:** completed

**Goal**: Migrate functions that lookup sources by absolute path.

**Scope**:

1. **`set_by_id()`** — Simple refactor:
   - Use `batch_fetch_by_ids()` instead of inline SQL
   - Use `Source::path()` for display
   - Check `source.is_excluded()` using domain predicate

2. **`set_by_path()`** — Path decomposition refactor:
   - Canonicalize input path (already done)
   - Use `repo::root::fetch_all()` + `domain::root::find_containing_root()`
   - Use `domain::path::path_strip_prefix()` to get `rel_path`
   - Use `repo::source::fetch_by_path(conn, root_id, rel_path)`
   - Use `Source::path()` for display

3. **`set_object_by_file()`** — Same pattern as `set_by_path()`:
   - Decompose absolute path to (root_id, rel_path)
   - Fetch source using existing repo function
   - Access `source.object_id` for the object lookup

**Non-goals**: Changing public API signatures.

---

## Test Requirements

### Existing Tests (must continue to pass)

The following tests exist in `exclude.rs` and must not regress:

- `get_matching_sources` — 6 tests
- `get_excluded_sources` — 4 tests
- `get_object_excluded_sources` — 4 tests
- `exclude_duplicates` — 6 tests

### Phase 1: New Integration Tests (completed)

| Test | Description |
|------|-------------|
| `test_set_by_id_excludes_source` | Source is excluded when valid ID provided |
| `test_set_by_id_nonexistent_fails` | Returns error for non-existent source ID |
| `test_set_by_id_already_excluded_skips` | Prints message, doesn't error when already excluded |
| `test_set_by_id_not_present_fails` | Returns error for source that exists but isn't present |
| `test_set_by_path_nonexistent_file_fails` | Returns error when file doesn't exist on disk |
| `test_set_by_path_not_in_db_fails` | Returns error when file exists but not in database |
| `test_get_object_sources_returns_paths` | Returns correctly constructed absolute paths |
| `test_get_object_sources_includes_role` | Correctly identifies archive vs source role |
| `test_get_object_sources_empty_rel_path` | Verifies R1 fix (no trailing slash) |
| `test_get_object_sources_excludes_not_present` | Only returns present sources |

**Note:** 10 tests added (2 more than spec). The `test_get_object_sources_empty_rel_path` test now verifies that `Source::path()` correctly handles empty `rel_path` (no trailing slash) — the R1 inconsistency was fixed in Phase 2.

### Phase 2-3: Verification

After refactoring, all Phase 1 tests must still pass. No additional tests needed — the integration tests verify the behavior is preserved.

---

## Risk Analysis

### R1: Empty rel_path Handling

**Current behavior**: SQL `r.path || '/' || s.rel_path` produces `/root/path/` (trailing slash) when `rel_path` is empty.

**After refactor**: `Source::path()` produces `/root/path` (no trailing slash).

**Mitigation**: The Phase 1 test `test_set_by_path_empty_rel_path` will document current behavior. If behavior changes, we'll evaluate whether the new behavior is correct (it should be).

### R2: Root Resolution Edge Cases

**Concern**: `find_containing_root()` might not find the right root for edge cases.

**Mitigation**: This function is already well-tested in the domain layer. The integration tests will catch any issues.

### R3: No Direct Tests Currently

**Finding**: The 5 functions being refactored have ZERO direct test coverage.

**Mitigation**: Phase 1 adds tests before any code changes. This is the same strategy used successfully in the predecessor spec.

---

## Related Documents

- `.claude/specs/2026-02-07-exclude-duplicates-extraction.md` — Established the pattern
- `CLAUDE.md` — Architecture documentation (path handling principle)

---

## Panel Notes

### Session 1 (2026-02-07) — Pattern Consistency Review

**[Steve]**: The predecessor spec intentionally focused on `exclude_duplicates()` as an example pattern. The 5 remaining functions in the same file weren't flagged as follow-up work because:
1. They're simpler (single-source operations)
2. They're lower traffic
3. The goal was to establish the pattern, not clean everything

However, the user correctly identifies that pattern consistency improves code quality. Inconsistent patterns create cognitive load and risk future violations.

**[Bruce]**: Key discovery: `repo::source::fetch_by_path(conn, root_id, rel_path)` already exists. No new repo functions needed. The refactoring pattern:
1. Canonicalize (command boundary)
2. Decompose path using `find_containing_root()` + `path_strip_prefix()`
3. Fetch using existing repo function
4. Display using `Source::path()`

**[August]**: The 5 target functions have ZERO direct test coverage. Phase 1 must add integration tests before any refactoring. This follows the same strategy as the predecessor spec (Phase 1: tests first).

**[Matthew]**: Work is well-scoped:
- Phase 1: ~100 lines of test code
- Phase 2: ~20 lines changed (display functions are simple)
- Phase 3: ~60 lines changed (path decomposition adds some code)

All changes contained within `exclude.rs`. Low risk due to established patterns and comprehensive test coverage.
