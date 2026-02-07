# Spec: Exclude Duplicates Domain Extraction

**Date:** 2026-02-07
**Status:** Complete
**Priority:** Medium
**Predecessor:** `.claude/specs/2026-02-06-exclude-domain-model.md` (Phases 1-2 complete)

## Problem Statement

The `exclude_duplicates` function in `exclude.rs` is critical functionality that lacks direct test coverage. It contains complex SQL with path-prefix matching that has a known inconsistency with Rust path computation. While Phases 1-2 of the exclude domain model successfully extracted all write operations to the repo layer, this function's read logic remains in the command layer with inline SQL.

### Why This Matters

1. **No test coverage for critical behavior**: `exclude_duplicates` marks files as excluded based on duplicate detection. If it incorrectly excludes the wrong file (the original instead of the duplicate), users lose the ability to recover files they thought were safely duplicated. There are **zero direct tests** for this function.

2. **Complex SQL with known inconsistency**: The path-prefix matching logic uses `LIKE ? || '/%'` patterns that behave differently from the Rust `path_is_under()` function for edge cases.

3. **Architectural violation**: SQL queries in command layer violate the established pattern where repo layer handles data access and domain layer handles business logic.

### Why This Sets a Pattern

This refactoring establishes an **example pattern** for path handling across the codebase:

- **All path semantics live in the domain layer** via pure functions (`path_is_under()`, `Source::path()`)
- **SQL never constructs or compares paths** — repo layer returns domain types, domain layer does path logic
- **Query-time operations are pure** — can work on DB data without disk access (disconnected storage friendly)
- **Canonicalization happens at command boundaries** — CLI argument processing, not during domain logic

---

## Risk Analysis

### R1: Empty rel_path Inconsistency

**Finding**: SQL and Rust compute paths differently when `rel_path` is empty.

| Computation | `rel_path = "file.txt"` | `rel_path = ""` |
|-------------|-------------------------|-----------------|
| SQL: `root \|\| '/' \|\| rel_path` | `/root/path/file.txt` | `/root/path/` (trailing slash) |
| Rust: `Source::path()` | `/root/path/file.txt` | `/root/path` (no slash) |

**Impact**:
- When finding duplicates, a source with empty `rel_path` would have its path returned with a trailing slash
- The LIKE check `'/root/path/' LIKE '/root/path/%'` returns TRUE (matches)
- The equality check `'/root/path/' = '/root/path'` returns FALSE (no match)

**Practical Risk**: Low. Roots are typically directories containing files, not files themselves. A source with empty `rel_path` means the root IS the file, which is rare. However, this is a correctness bug that should be fixed.

**Recommendation**: When extracting to domain logic, ensure path computation uses `Source::path()` consistently.

### R2: Path-Prefix False Positives

**Concern**: Does `LIKE '/a/b/%'` incorrectly match `/a/bc/d`?

**Finding**: No. SQLite's LIKE requires the literal `/` after the prefix, so `/a/bc/d` does NOT match `/a/b/%`. This is correct behavior.

### R3: Untested Core Logic

| Scenario | Expected Behavior | Test Coverage |
|----------|-------------------|---------------|
| One copy in prefer path | Exclude source | None |
| No copy in prefer path | Skip source | None |
| Multiple copies in prefer path | Skip source (ambiguous) | None |
| Source already in prefer path | Skip source | None |
| Unhashed source | Skip source | None |

**Recommendation**: Add integration tests before any refactoring.

---

## Architectural Direction

### Current State

```
exclude_duplicates() in exclude.rs
├── Inline SQL: Get source info (object_id, root_path, rel_path)
├── Inline SQL: Find duplicates in prefer path (complex LIKE query)
├── Business logic: Decide what to exclude
└── repo::source::set_excluded() [already migrated]
```

### Target State (Domain-Driven Query Decomposition)

```
┌──────────────────────────────────────────────────────────────┐
│ Command Layer (exclude.rs)                                   │
│ - Canonicalizes CLI args (prefer_prefix) — ONLY I/O here     │
│ - Orchestrates: repo fetch → domain logic → repo write       │
│ - Applies side effects via repo::source::set_excluded()      │
└──────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Repo Layer                   │   │ Domain Layer                 │
│ - batch_fetch_by_ids()       │   │ - find_excludable_duplicates │
│ - fetch_sources_by_object_ids│   │ - path_is_under() [pure]     │
│ - Returns Source with        │   │ - Source::path() [pure]      │
│   root_path populated        │   │ - No I/O, fully testable     │
└──────────────────────────────┘   └──────────────────────────────┘
```

### Key Principles

**P1: Dependencies point inward**: Command depends on Domain and Repo. Domain has no dependencies. This enables:
- Unit testing of business logic without database
- Consistent path computation via `Source::path()`
- Reusable repo functions for other commands

**P2: Path operations are pure at query time**: The domain layer has two categories of path functions:

| Category | Functions | I/O? | When Used |
|----------|-----------|------|-----------|
| Pure | `path_is_under()`, `path_strip_prefix()`, `Source::path()`, `Source::matches_scope()` | No | Query time |
| Boundary | `canonicalize_scope()`, `canonicalize_scopes()` | Yes | CLI argument processing |

Query-time operations use only pure functions, enabling:
- Disconnected storage operation (work purely from DB)
- Deterministic, testable behavior
- No hidden filesystem dependencies

**P3: SQL never constructs paths**: The SQL concatenation `r.path || '/' || s.rel_path` has known inconsistencies with Rust's `Source::path()` (see R1). The repo layer returns `Source` objects; path construction is the domain's responsibility.

**P4: Cross-platform readiness**: Pure path functions use `std::path::Path` methods which abstract platform differences. Device ID and inode are already optional fields, acknowledging they're Unix-specific. This positions the codebase for potential Windows support without requiring immediate changes.

---

## Proposed Implementation

### Phase 1: Add Integration Tests (Immediate)

**Status:** completed

**Goal**: Establish correctness baseline before any refactoring.

**Scope**:
1. `test_exclude_duplicates_excludes_when_one_copy_in_prefer`
2. `test_exclude_duplicates_skips_when_no_copy_in_prefer`
3. `test_exclude_duplicates_skips_when_multiple_copies_in_prefer`
4. `test_exclude_duplicates_skips_source_already_in_prefer`
5. `test_exclude_duplicates_path_prefix_no_false_positive`

**Non-goals**: Refactoring, performance optimization.

### Phase 1b: Empty rel_path Edge Case Test

**Status:** completed

**Goal**: Add test coverage for the R1 inconsistency before it can cause issues.

**Scope**:
1. `test_exclude_duplicates_empty_rel_path` — Verify behavior when a source has `rel_path = ""` (root IS the file). This test documents current behavior and will verify the fix in Phase 3.

**Rationale**: The existing tests don't cover this edge case. While low practical risk, adding this test now establishes the correctness baseline and will catch any regression during refactoring.

**Result**: Test passes with current implementation. Despite the SQL/Rust path inconsistency (R1), the current behavior is correct for this scenario because the SQL `LIKE ? || '/%'` pattern happens to match paths with trailing slashes. The test documents this behavior and will catch any regression during Phase 3 refactoring.

### Phase 2: Repo Layer Extraction

**Status:** completed

**Goal**: Add repo function for fetching sources by object ID (duplicate detection).

**Scope**:

1. Add `repo::source::fetch_sources_by_object_ids()`:
   ```rust
   /// Fetch all sources that share given object IDs.
   /// Used for finding duplicates — given content hashes, find all file locations.
   ///
   /// # Returns
   /// HashMap where key is object_id and value is Vec of all Sources with that object.
   /// Sources include full root_path for path computation via Source::path().
   pub fn fetch_sources_by_object_ids(
       conn: &Connection,
       object_ids: &[i64],
   ) -> Result<HashMap<i64, Vec<Source>>>
   ```

**Note**: `batch_fetch_with_root_paths()` is NOT needed — existing `batch_fetch_by_ids()` already returns `Source` objects with `root_path` populated (via JOIN). The `Source::path()` method handles path construction.

**Non-goals**: Domain logic extraction (Phase 3).

### Phase 3: Domain Logic Extraction

**Status:** completed

**Goal**: Extract duplicate-detection logic to pure domain function.

**Scope**:
1. Add `domain::exclusion` module (or add to existing domain module):
   ```rust
   /// Given sources and their duplicates, determine which sources in `scope`
   /// should be excluded because they have exactly one copy under `prefer_prefix`.
   ///
   /// Pure function - no I/O, fully testable.
   pub fn find_excludable_duplicates(
       scope_sources: &[Source],
       all_sources_by_object: &HashMap<i64, Vec<Source>>,
       prefer_prefix: &str,
   ) -> Vec<i64>  // source_ids to exclude
   ```

2. Migrate `exclude_duplicates()` to use:
   - Repo functions for data fetching
   - Domain function for decision logic
   - `repo::source::set_excluded()` for side effects (already done)

**Benefits**:
- Pure domain function can be exhaustively unit-tested
- Path handling uses `Source::path()` consistently (fixes R1)
- Business logic is explicit and reviewable

---

## Test Requirements

### Phase 1 Integration Tests (completed)

| Test | Description |
|------|-------------|
| `exclude_duplicates_excludes_when_one_copy_in_prefer` | Source is excluded when exactly one copy exists in prefer path |
| `exclude_duplicates_skips_when_no_copy_in_prefer` | Source is NOT excluded when no copy exists in prefer path |
| `exclude_duplicates_skips_when_multiple_copies_in_prefer` | Source is NOT excluded when multiple copies exist (ambiguous) |
| `exclude_duplicates_skips_source_already_in_prefer` | Source in prefer path is never excluded |
| `exclude_duplicates_path_prefix_no_false_positive` | `/a/bc` is NOT under `/a/b` (false positive prevention) |

### Phase 1b Integration Test (completed)

| Test | Description |
|------|-------------|
| `exclude_duplicates_empty_rel_path` | Source with `rel_path=""` (root IS the file) behaves correctly. Documents R1 edge case. |

### Phase 2 Repo Tests (completed)

| Test | Description |
|------|-------------|
| `fetch_sources_by_object_ids_empty_input` | Empty object_ids returns empty HashMap |
| `fetch_sources_by_object_ids_returns_grouped` | Returns sources grouped by object_id |
| `fetch_sources_by_object_ids_includes_root_path` | Returned Sources have root_path populated, `Source::path()` works |
| `fetch_sources_by_object_ids_excludes_non_present` | Only present sources are returned |
| `fetch_sources_by_object_ids_handles_large_batch` | >1000 object_ids processed correctly via chunking |

### Phase 3 Domain Tests (completed)

| Test | Description |
|------|-------------|
| `find_excludable_empty_scope` | Empty scope returns empty result |
| `find_excludable_skips_unhashed` | Sources without object_id are skipped |
| `find_excludable_skips_already_in_prefer` | Sources already in prefer path are skipped |
| `find_excludable_one_copy_excludes` | Source is excluded when exactly one copy in prefer |
| `find_excludable_no_copy_skips` | Source is skipped when no copy in prefer |
| `find_excludable_multiple_copies_skips` | Source is skipped when multiple copies (ambiguous) |
| `find_excludable_ignores_excluded_copies` | Already-excluded copies don't count |
| `find_excludable_ignores_object_excluded_copies` | Object-level excluded copies don't count |
| `find_excludable_uses_source_path` | Path comparison uses Source::path() correctly |
| `find_excludable_empty_rel_path_in_scope` | Sources with empty rel_path handled correctly |
| `find_excludable_mixed_scenarios` | Complex scenario with various outcomes |

---

## Related Documents

- `.claude/specs/2026-02-06-exclude-domain-model.md` — Predecessor spec (Phases 1-2 complete)
- `.claude/specs/2026-02-01-write-infrastructure.md` — Established patterns for domain types and repo functions
- `CLAUDE.md` — Architecture documentation

---

## Panel Notes

### Session 1 (2026-02-07)

**[Steve]**: The architectural direction is clear: batch fetch (repo) → pure decision logic (domain) → side effects (command). This is the same pattern used successfully in scan.rs. Phase 1 (tests) should be done immediately. Phases 2-3 can be scheduled when there's time for the refactoring.

**[Bruce]**: The empty rel_path inconsistency (R1) is a real bug, but low practical risk. Fixing it properly requires using `Source::path()` in the repo layer or ensuring SQL matches Rust behavior. The domain extraction in Phase 3 naturally fixes this.

**[August]**: Phase 1 is critical. We cannot refactor safely without tests proving current behavior is correct. The 5 proposed tests cover the core scenarios. Additional edge cases (empty rel_path handling) can be added in Phase 3 when domain tests are written.

**[Matthew]**: Phases are appropriately sized. Phase 1 is a focused testing effort. Phases 2-3 are larger but can be done incrementally. Recommend completing Phase 1 now and deferring Phases 2-3 until needed.

### Session 2 (2026-02-07) — Path Semantics Review

**[Steve]**: This refactoring establishes an **example pattern** for the codebase. The key principles documented:
1. Path semantics live in domain layer via pure functions
2. SQL never constructs or compares paths
3. Query-time operations are pure (disconnected storage friendly)
4. Canonicalization happens at command boundaries only

Simplified Phase 2: `batch_fetch_with_root_paths()` is redundant — existing `batch_fetch_by_ids()` already returns `Source` with `root_path`. Only `fetch_sources_by_object_ids()` is needed.

**[Bruce]**: The domain layer correctly separates pure path functions (`path_is_under`, `Source::path()`) from I/O functions (`canonicalize_scope`). Query-time operations use only pure functions. The inline SQL in `exclude.rs` duplicates domain logic — that's the code smell this refactoring fixes.

**[August]**: Added Phase 1b for the empty `rel_path` edge case test. This should be added before Phases 2-3 to establish the baseline. Also added Phase 2 repo tests including batch size boundary testing.

**[Matthew]**: Implementation size is tight and focused:
- Phase 1b: 1 test (~20 lines)
- Phase 2: 1 repo function (~40 lines) + 4 tests
- Phase 3: 1 domain function (~50 lines) + 5 tests + refactor of exclude_duplicates

Cross-platform note: Device ID and inode are already optional fields, acknowledging Unix-specific nature. Pure path functions use `std::path::Path` which abstracts platform differences.

### Session 3 (2026-02-07) — Implementation Complete

**[Steve]**: All phases completed successfully. The refactoring establishes the pattern:
- `domain/exclusion.rs` — New module with pure `find_excludable_duplicates()` function
- `repo/source.rs` — Added `fetch_sources_by_object_ids()` for duplicate lookup
- `exclude.rs` — Refactored to use repo → domain → repo pattern

Key architectural wins:
1. **No more SQL path construction** — All path logic uses `Source::path()` in domain layer
2. **11 pure unit tests** — Domain logic fully testable without database
3. **R1 inconsistency fixed** — Empty `rel_path` now handled by `Source::path()` consistently

**[August]**: Test coverage summary:
- Phase 1: 5 integration tests (existing behavior)
- Phase 1b: 1 edge case test (empty rel_path)
- Phase 2: 5 repo tests (fetch_sources_by_object_ids)
- Phase 3: 11 domain tests (find_excludable_duplicates)

Total: 22 new tests added, all 302 tests passing.

**[Matthew]**: Final implementation size was as estimated:
- `domain/exclusion.rs`: 115 lines of code + 165 lines of tests
- `repo/source.rs`: ~50 lines added
- `exclude.rs`: net reduction (removed inline SQL, cleaner flow)

---

## Future Candidates

Other locations with SQL path logic that could follow this pattern:

| Location | Current Code | Issue | Priority |
|----------|--------------|-------|----------|
| `scan.rs:44-51` | `rel_path LIKE prefix/%` | Relative path prefix matching | Low — works correctly, different semantics |
| `domain/scope.rs` | `build_scope_clause()` | Marked `#[allow(dead_code)]` — all callers migrated | **Cleanup — remove dead code** |
| `repo/source.rs:614` | `fetch_source_ids_by_prefix` uses `LIKE ?%` | Simple prefix match for pagination | Low — no path construction |

### Completed: Remove Dead Code

The `build_scope_clause()` function in `domain/scope.rs` was dead code:
- Was marked with `#[allow(dead_code)]`
- All callers had migrated to domain predicates (`Source::matches_scope()`)
- Contained SQL path construction logic that contradicted the "SQL never constructs paths" principle

**Done**: Removed `build_scope_clause()` and its 5 tests as part of this commit.

---

## Insights for Future Refactoring

### Pattern Established: "SQL Never Constructs Paths"

This refactoring establishes a reusable pattern for path-related operations:

1. **Repo layer returns domain types** with `root_path` populated via JOIN
2. **Domain layer computes paths** using `Source::path()` which handles edge cases (empty `rel_path`)
3. **Domain layer compares paths** using pure functions like `path_is_under()`
4. **Command layer canonicalizes CLI arguments** — this is the ONLY place filesystem I/O happens for paths

### When to Apply This Pattern

Apply when you see:
- SQL with `root || '/' || rel_path` concatenation
- SQL with `LIKE ? || '/%'` for path prefix matching
- Rust code duplicating path construction logic that exists in `Source::path()`

### Test Strategy That Worked

1. **Add integration tests first** (Phase 1) — proves existing behavior before refactoring
2. **Add edge case tests** (Phase 1b) — documents known issues
3. **Add repo tests** (Phase 2) — verifies data access layer
4. **Add domain unit tests** (Phase 3) — enables exhaustive testing of pure logic
5. **More tests than spec is OK** — additional coverage strengthens confidence

### Cross-Platform Consideration

The pattern naturally supports future Windows compatibility:
- `std::path::Path` methods abstract platform differences
- Pure domain functions don't touch the filesystem
- Canonicalization at command boundary can be extended for platform-specific normalization
