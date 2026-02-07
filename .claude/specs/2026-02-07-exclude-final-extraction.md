# Spec: Exclude Final SQL Extraction

**Date:** 2026-02-07
**Status:** Complete
**Priority:** Medium
**Predecessor:** `.claude/specs/2026-02-07-exclude-path-pattern-completion.md` (established patterns)

## Problem Statement

The previous exclude specs successfully extracted all write operations and established the "SQL never constructs paths" pattern. However, several functions in `exclude.rs` still contain inline SQL that violates the strict layer separation now expected in the codebase:

| Function | Lines | SQL Type | Violation |
|----------|-------|----------|-----------|
| `is_excluded()` | 182-195 | READ | Inline SQL when domain predicate exists |
| `is_object_excluded()` | 199-208 | READ | Inline SQL when domain predicate exists |
| `set_objects_by_filter()` | 624-632, 658-662 | READ | Inline SQL for object info and source counts |
| `list_objects()` | 842-854 | READ | Inline SQL for listing excluded objects |

### Why This Matters

D2 from the original spec stated read helpers could remain in command layer. However, the codebase has matured:
- Strict layer separation is now established and expected
- Inline SQL in commands creates inconsistency
- Domain predicates (`Source::is_excluded()`, `Object::is_excluded()`) already exist but aren't being used
- Future maintainers will wonder why some commands have inline SQL and others don't

---

## Architectural Direction

All SQL must live in repo layer. Commands orchestrate:
1. Fetch domain objects via repo functions
2. Apply domain predicates and logic
3. Write back via repo functions
4. Format output for user

---

## Proposed Implementation

### Phase 1: Deprecate is_excluded()

**Status:** completed

**Goal:** Replace inline SQL helpers with domain predicates.

**Analysis of current usage:**

`is_excluded(conn, source_id)` called at:
- `exclude.rs` Line 52: In `set` function — batch fetch Sources, use `s.is_excluded()`
- `exclude.rs` Line 493: In `exclude_duplicates` — use Source from `scope_sources_map`
- `apply.rs` Line 908: In `check_excluded_sources_filtered` — batch fetch Sources, use `s.is_excluded()`

`is_object_excluded(conn, object_id)` called at:
- Line 652: In `set_objects_by_filter` — **DEFERRED TO PHASE 2** (part of larger loop refactor)
- Line 790: In `exclude_object_by_id` — fetch Object and use `object.is_excluded()`

**Scope:**
1. `exclude.rs` Line 52: Batch fetch Sources, filter with `s.is_excluded()`
2. `exclude.rs` Line 493: Use `source.is_excluded()` from existing map lookup
3. `exclude.rs` Line 790: Fetch Object via `batch_fetch_by_ids`, use `object.is_excluded()`
4. `apply.rs` Line 908: Batch fetch Sources, filter with `s.is_excluded()`
5. Delete the `is_excluded()` function from `exclude.rs`

**Non-goals:**
- Line 652 (deferred to Phase 2's `set_objects_by_filter` refactor)
- Deleting `is_object_excluded()` (still called at line 652, will be deleted in Phase 2)
- Changing other parts of the functions

### Phase 2: Extract set_objects_by_filter() inline SQL

**Status:** completed

**Goal:** Move inline SQL to repo layer.

**Current inline SQL (lines 624-632):**
```sql
SELECT o.id, o.hash_value, s.size
FROM sources s
JOIN objects o ON s.object_id = o.id
WHERE s.id = ?
```

**Current inline SQL (lines 658-662):**
```sql
SELECT COUNT(*) FROM sources WHERE object_id = ? AND present = 1
```

**Refactoring approach:**
1. Use `repo::source::batch_fetch_by_ids()` — Sources already have `object_id` and `size`
2. Use `repo::object::batch_fetch_by_ids()` — Objects have `hash_value` and `excluded`
3. Source count per object: Add `repo::object::count_present_sources()` or compute from existing data

**Scope:**
1. Refactor `set_objects_by_filter()` to use repo functions
2. Add `repo::object::count_present_sources_by_ids()` if needed (or compute in-memory)
3. Delete inline SQL

### Phase 3: Extract list_objects() inline SQL

**Status:** completed

**Goal:** Move listing query to repo layer.

**Current inline SQL (lines 842-854):**
```sql
SELECT o.id, o.hash_value, (
    SELECT COUNT(*) FROM sources s WHERE s.object_id = o.id AND s.present = 1
) as source_count
FROM objects o
WHERE o.excluded = 1
ORDER BY o.id
```

**Refactoring approach:**
1. Add `repo::object::fetch_excluded()` — returns `Vec<Object>` of excluded objects
2. For source counts, either:
   - Add to Object struct (if widely needed)
   - Compute via separate repo function
   - Compute in-memory from sources

**Scope:**
1. Add `repo::object::fetch_excluded()`
2. Determine source count approach
3. Refactor `list_objects()` to use repo functions

---

## Test Requirements

### Phase 1 Tests

No new tests required — existing tests cover the behavior. The refactoring is internal.

**Verification:** All 302 existing tests must pass.

### Phase 2 Tests

| Test | Description |
|------|-------------|
| `test_set_objects_by_filter_excludes_objects` | Basic object exclusion via filter works |
| `test_set_objects_by_filter_skips_empty_files` | Empty files (size=0) are skipped |
| `test_set_objects_by_filter_skips_already_excluded` | Already-excluded objects are skipped |
| `test_set_objects_by_filter_skips_unhashed` | Sources without objects are skipped |
| `test_set_objects_by_filter_dry_run` | Dry run shows but doesn't execute |

### Phase 3 Tests

| Test | Description |
|------|-------------|
| `test_list_objects_shows_excluded` | Lists excluded objects with correct info |
| `test_list_objects_shows_source_count` | Source count is accurate |
| `test_list_objects_empty` | Handles no excluded objects gracefully |

---

## Panel Notes

### Session 1 (2026-02-07) — Initial Review

**[Steve]**: The previous specs achieved their goals, but D2's allowance for "read helpers to remain" was appropriate at the time. The codebase has matured — we should now complete the extraction. All SQL should live in repo layer.

Phases are ordered by dependency and complexity:
- Phase 1 is pure deletion (replace with existing predicates)
- Phase 2 requires minor repo additions
- Phase 3 is self-contained listing functionality

**[Bruce]**: Key observations:
1. `is_excluded()` is used in 2 places where we already have the Source object — the inline SQL is redundant
2. `set_objects_by_filter()` does source→object JOIN but we can do this via existing batch functions
3. `list_objects()` is isolated and could use a new `fetch_excluded()` function

The source count subquery is the main decision point. Options:
- New repo function `count_present_sources_by_ids()`
- Add count to Object struct (adds a field not always needed)
- Compute in-memory from `fetch_sources_by_object_ids()` (may be expensive for many objects)

**[August]**: Test coverage gaps identified:
- `set_objects_by_filter()` — ZERO direct tests (critical gap)
- `list_objects()` — ZERO direct tests
- `set_object_by_hash()` — ZERO direct tests
- `set_object_by_file()` — ZERO direct tests
- `clear_object()` — ZERO direct tests

Phase 2 MUST add tests before refactoring. Following the established pattern: tests first, then refactor.

**[Matthew]**: Work is well-scoped:
- Phase 1: ~15 lines changed, 2 functions deleted
- Phase 2: ~40 lines changed, 5 tests added, possibly 1 small repo function
- Phase 3: ~30 lines changed, 3 tests added, 1 repo function

All phases can be done independently. Recommend doing them sequentially for cleaner commits.

---

## Idempotency Analysis

Per CLAUDE.md guidance on concurrency:

- `set_objects_by_filter()`: Idempotent. If it fails halfway, user can re-run. Already-excluded objects are skipped.
- `list_objects()`: Read-only. No transaction needed.
- `is_excluded()` / `is_object_excluded()`: Read-only helpers. No transaction impact.

No new transaction requirements introduced by this refactoring.
