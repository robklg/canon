# Refactoring Spec: Exclude Module Consolidation

## Overview

Migrate `exclude.rs` read operations to use the unified domain model, making it consistent with the other 7 command modules that already follow this pattern.

### Context

The "Big Four" domain model is complete:

| Entity | Domain | Repository | Status |
|--------|--------|------------|--------|
| Source | `source.rs` | `source_repo.rs` | Complete |
| Fact | `fact.rs` | `fact_repo.rs` | Complete |
| Root | `root.rs` | `root_repo.rs` | Complete |
| Object | `object.rs` | `object_repo.rs` | Complete |

Seven command modules already use this infrastructure:
- `worklist.rs`, `compare.rs`, `ls.rs`, `cluster.rs`, `coverage.rs`, `facts.rs`, `roots.rs`

Only `exclude.rs` still uses ad-hoc SQL patterns for read operations.

### Why This Matters

1. **Consistency** — Future maintainers may copy the wrong pattern
2. **Testability** — Domain predicates are unit-testable; ad-hoc SQL is not
3. **Maintainability** — Changes to exclusion logic should be in one place (`source.is_excluded()`)

---

## Scope

### In Scope

Migrate these read functions to use `source_repo` + domain predicates:

1. **`get_matching_sources()`** (lines 222-265)
   - Current: Custom pagination with `exclude_clause()`, `build_scope_clause()`
   - Target: `source_repo::batch_fetch_by_roots()` + domain predicates

2. **`get_excluded_sources()`** (lines 267-322)
   - Current: Custom pagination with scope clause, filters for `s.excluded = 1`
   - Target: Same batch fetch + filter on `s.excluded` field (source-level only)

3. **`get_object_excluded_sources()`** (lines 933-990)
   - Current: Custom pagination, joins objects, filters for `o.excluded = 1 AND s.excluded = 0`
   - Target: Same batch fetch + `object_repo` for exclusion check

4. **Remove `exclude_clause()`** (lines 212-220)
   - Only used internally by `get_matching_sources()`
   - Dead code after migration

5. **Update CLAUDE.md**
   - Remove `exclude::exclude_clause()` from documentation

### Out of Scope

- **Write operations** — `set()`, `clear()`, `set_by_id()`, `set_by_path()`, `exclude_duplicates()`, object exclusion commands
- **Single-row helpers** — `is_excluded()`, `is_object_excluded()`, `get_source_path()`, `get_object_sources()`
- **filter.rs** — Complex, works fine

### Non-Goals

- Changing output format
- Changing command behavior

---

## Analysis of Current Functions

### `get_matching_sources(conn, scope_prefixes, filters, include_excluded) -> Vec<i64>`

**Current logic:**
1. Paginate with `id > last_id LIMIT 1000`
2. Filter: `present = 1`, `role = 'source'`, `suspended = 0`
3. Apply scope clause
4. Apply exclusion clause (unless `include_excluded`)
5. Apply `--where` filters via `filter::apply_filters()`

**Target:**
```rust
fn get_matching_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_excluded: bool,
) -> Result<Vec<i64>> {
    // Get all source root IDs
    let roots = root_repo::fetch_all(conn)?;
    let source_root_ids: Vec<i64> = roots.iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    // Batch fetch all sources from source roots
    let sources = source_repo::batch_fetch_by_roots(conn, &source_root_ids)?;

    // Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // Apply domain predicates
    let filtered: Vec<i64> = sources.into_iter()
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| include_excluded || !s.is_excluded())
        .map(|s| s.id)
        .collect();

    // Apply --where filters
    if filters.is_empty() {
        return Ok(filtered);
    }
    filter::apply_filters(conn, &filtered, filters)
}
```

### `get_excluded_sources(conn, scope_prefixes, filters) -> Vec<(i64, String)>`

**Current logic:**
1. Same pagination pattern
2. Filter: `present = 1`, `role = 'source'`, `suspended = 0`, **`s.excluded = 1`**
3. Returns source ID and full path

**Critical distinction:** This function returns **only directly excluded** sources (where `s.excluded = 1`). It does NOT include sources excluded via their object. This is intentional — used by `clear()` which only clears source-level exclusion.

**Target:**
```rust
fn get_excluded_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String)>> {
    // ... same batch fetch pattern ...

    // Filter for DIRECTLY excluded sources only (not object-excluded)
    let filtered: Vec<(i64, String)> = sources.into_iter()
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| s.excluded)  // Direct source-level exclusion only — NOT s.is_excluded()
        .map(|s| (s.id, s.path()))
        .collect();

    // Apply --where filters and preserve paths...
}
```

### `get_object_excluded_sources(conn, scope_prefixes, filters) -> Vec<(i64, String, String)>`

**Current logic:**
1. Same pagination pattern
2. Filter: `present = 1`, `role = 'source'`, `suspended = 0`
3. **`o.excluded = 1 AND s.excluded = 0`** — sources excluded via object but NOT directly
4. Returns source ID, full path, and short hash

**Target:**
```rust
fn get_object_excluded_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String, String)>> {
    // ... same batch fetch pattern ...

    // We need object info for the hash, so fetch objects too
    let object_ids: Vec<i64> = sources.iter()
        .filter_map(|s| s.object_id)
        .collect();
    let objects = object_repo::batch_fetch_by_ids(conn, &object_ids)?;

    // Filter for sources excluded via object (not directly)
    let filtered: Vec<(i64, String, String)> = sources.into_iter()
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| !s.excluded)  // NOT directly excluded
        .filter(|s| {
            // But object IS excluded
            s.object_id.map_or(false, |oid| {
                objects.get(&oid).map_or(false, |o| o.excluded)
            })
        })
        .map(|s| {
            let hash = s.object_id
                .and_then(|oid| objects.get(&oid))
                .map(|o| o.hash_value[..16.min(o.hash_value.len())].to_string())
                .unwrap_or_default();
            (s.id, s.path(), hash)
        })
        .collect();

    // Apply --where filters...
}
```

---

## Test Requirements

### Philosophy

Tests are written **before migration** (TDD style). This:
1. Documents expected behavior before we touch anything
2. Ensures we understand what the code currently does
3. Gives us a safety net during migration

All tests must pass on the current implementation, then continue passing after migration.

### Risk Inventory

| Risk | Description | Mitigation |
|------|-------------|------------|
| Exclusion level distinctions | Using `s.is_excluded()` instead of `s.excluded` field | Tests 7-8, 11-12 |
| Root filtering | Missing `is_active()` or `is_source()` checks | Tests 1-2 |
| Scope filtering | Incorrect `matches_scope()` usage | Tests 3, 9, 14 |
| Filter integration | Breaking `--where` filter application | Smoke tests |
| Path construction | Using wrong method to build full path | Tests 4, 10 |
| Object hash lookup | Missing or incorrect `object_repo` integration | Test 13 |

### Unit Tests (14 total)

**For `get_matching_sources()` (6 tests):**

1. **`test_get_matching_sources_excludes_suspended_roots`**
   - Setup: Source in suspended root
   - Assert: Source not returned

2. **`test_get_matching_sources_excludes_archive_roots`**
   - Setup: Source in archive role root
   - Assert: Source not returned

3. **`test_get_matching_sources_respects_scope`**
   - Setup: Sources in /a/b and /a/c, scope is /a/b
   - Assert: Only /a/b sources returned

4. **`test_get_matching_sources_excludes_source_level_excluded`**
   - Setup: Source with `excluded = 1`
   - Assert: Source not returned when `include_excluded = false`

5. **`test_get_matching_sources_excludes_object_level_excluded`**
   - Setup: Source with `excluded = 0` but object has `excluded = 1`
   - Assert: Source not returned when `include_excluded = false`

6. **`test_get_matching_sources_includes_excluded_when_flag_set`**
   - Setup: Excluded source (both types)
   - Assert: Sources returned when `include_excluded = true`

**For `get_excluded_sources()` (4 tests):**

7. **`test_get_excluded_sources_returns_source_level_only`**
   - Setup: Source with `excluded = 1`
   - Assert: Source is returned

8. **`test_get_excluded_sources_ignores_object_level_excluded`**
   - Setup: Source with `excluded = 0`, object with `excluded = 1`
   - Assert: Source is NOT returned (critical distinction!)

9. **`test_get_excluded_sources_respects_scope`**
   - Setup: Excluded sources in /a/b and /a/c, scope is /a/b
   - Assert: Only /a/b source returned

10. **`test_get_excluded_sources_returns_correct_path`**
    - Setup: Source with known root_path and rel_path
    - Assert: Returned path is correctly constructed

**For `get_object_excluded_sources()` (4 tests):**

11. **`test_get_object_excluded_sources_returns_object_level_only`**
    - Setup: Source with `excluded = 0`, object with `excluded = 1`
    - Assert: Source is returned

12. **`test_get_object_excluded_sources_ignores_source_level_excluded`**
    - Setup: Source with `excluded = 1`, object with `excluded = 1`
    - Assert: Source is NOT returned (critical distinction!)

13. **`test_get_object_excluded_sources_returns_hash_prefix`**
    - Setup: Source linked to object with known hash
    - Assert: Returned hash is first 16 chars of object.hash_value

14. **`test_get_object_excluded_sources_respects_scope`**
    - Setup: Object-excluded sources in /a/b and /a/c, scope is /a/b
    - Assert: Only /a/b source returned

### Smoke Tests (Behavioral Validation)

In addition to unit tests, capture baseline outputs before migration:

```bash
canon exclude list > /tmp/baseline_exclude_list.txt
canon exclude list /some/scope > /tmp/baseline_exclude_list_scoped.txt
```

After migration, outputs must be identical.

---

## Phases

### Phase 1: Write Unit Tests

- **Status**: ✅ completed
- **Goal**: Establish test coverage before any code changes
- **Scope**: Write all 14 unit tests against current implementation

**Steps:**
1. Add `#[cfg(test)] mod tests` section to `exclude.rs`
2. Create test helper to set up in-memory database with test data
3. Implement all 14 tests
4. Verify all tests pass on current implementation

**Test data setup:**
- Root 1: source role, active, path `/test/source`
- Root 2: archive role, active, path `/test/archive`
- Root 3: source role, suspended, path `/test/suspended`
- Source A: in root 1, not excluded, no object
- Source B: in root 1, `excluded = 1` (source-level)
- Source C: in root 1, `excluded = 0`, linked to excluded object (object-level)
- Source D: in root 1, `excluded = 1`, linked to excluded object (both levels)
- Source E: in root 2, not excluded (archive root)
- Source F: in root 3, not excluded (suspended root)

### Phase 2: Capture Baselines

- **Status**: ✅ completed
- **Results**:
  - Full list: 1,038,861 directly excluded, 28 object-excluded
  - Scoped: 0 directly excluded, 14 object-excluded
- **Goal**: Behavioral validation data
- **Scope**: Capture command outputs before migration

**Steps:**
```bash
canon exclude list 2>&1 | tee /tmp/baseline_exclude_list.txt
canon exclude list /some/real/scope 2>&1 | tee /tmp/baseline_exclude_list_scoped.txt
```

### Phase 3: Migration

- **Status**: ✅ completed
- **Results**: All 14 tests pass, all baselines match
- **Goal**: Migrate all three functions, verify tests still pass
- **Scope**: Code changes only

**Steps:**

1. **Add imports:**
   ```rust
   use crate::{root_repo, source_repo, object_repo};
   ```

2. **Migrate `get_matching_sources()`:**
   - Rewrite using batch fetch + domain predicates
   - Use `s.is_excluded()` for combined check (when `include_excluded = false`)

3. **Migrate `get_excluded_sources()`:**
   - Rewrite using batch fetch
   - Use `s.excluded` field (NOT `s.is_excluded()`) for source-level only

4. **Migrate `get_object_excluded_sources()`:**
   - Rewrite using batch fetch
   - Add `object_repo::batch_fetch_by_ids()` for object data
   - Filter: `!s.excluded && object.excluded`

5. **Run tests:**
   ```bash
   cargo test exclude
   ```

6. **Verify all 14 tests pass**

### Phase 4: Cleanup

- **Status**: ✅ completed
- **Changes**:
  - Added `#[allow(dead_code)]` to `build_scope_clause()` in scope.rs (now unused)
  - Removed `exclude::exclude_clause()` from CLAUDE.md
- **Goal**: Remove dead code
- **Scope**: Deletions only

**Steps:**
1. Delete `exclude_clause()` function
2. Remove unused imports (`build_scope_clause`, `rusqlite::types::Value`)
3. Remove `BATCH_SIZE` constant if no longer used
4. Run `cargo test` to verify no breakage

### Phase 5: Verification & Documentation

- **Status**: ✅ completed
- **Results**:
  - All 192 tests pass
  - Baselines verified identical
  - CLAUDE.md updated
  - No build warnings
- **Goal**: Final validation and documentation update

**Steps:**
1. Compare outputs:
   ```bash
   canon exclude list > /tmp/after_exclude_list.txt
   diff /tmp/baseline_exclude_list.txt /tmp/after_exclude_list.txt
   ```
2. Update CLAUDE.md — remove `exclude::exclude_clause()` reference
3. Run full test suite: `cargo test`

---

## Invariants

### Architectural

1. **Read operations use domain predicates** — No SQL WHERE clauses for role/scope/exclusion
2. **Source struct is the source of truth** — `source.excluded`, `source.is_excluded()`, `source.matches_scope()`
3. **Batch fetching for all source queries** — No custom pagination

### Behavioral (Enforced by Tests)

1. **`get_excluded_sources()` returns source-level exclusions only** — Tests 7-8
2. **`get_object_excluded_sources()` returns object-level exclusions only** — Tests 11-12
3. **`get_matching_sources(include_excluded=false)` checks both** — Tests 4-5

### Backward Compatibility

1. **Output format unchanged** — Same paths, same counts
2. **Command behavior unchanged** — Same sources selected

---

## Summary

| Phase | Description | Tests | Lines Changed |
|-------|-------------|-------|---------------|
| 1 | Write unit tests | +14 tests | +~150 lines |
| 2 | Capture baselines | - | - |
| 3 | Migration | 14 pass | ~80 lines changed |
| 4 | Cleanup | 14 pass | -20 lines |
| 5 | Verification | 14 pass | CLAUDE.md update |

**Total: 14 new tests, ~80 lines migrated, ~20 lines removed**

The codebase will be fully consistent with the unified domain model after this work.

---

## References

- Previous specs: `.claude/specs/2026-01-24-source-infrastructure.md`, `.claude/specs/2026-01-25-object-infrastructure.md`
- Domain modules: `src/source.rs`, `src/object.rs`, `src/root.rs`
- Repository modules: `src/source_repo.rs`, `src/object_repo.rs`, `src/root_repo.rs`
- Current implementation: `src/exclude.rs`
