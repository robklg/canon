# Inline SQL Extraction: apply.rs and scan.rs

## Overview

Extract remaining inline SQL from command modules to the repository layer. The core principle: **all database access goes through repo layer, all business logic uses domain objects and predicates**.

**Status**: All phases completed ✅

## Context

After recent refactoring of `exclude.rs`, `scan.rs`, and `apply.rs`, some inline SQL statements remain. Additionally, `domain/root.rs` has SQL which is a layer violation that must be fixed first.

**Guiding principles**:
1. SQL belongs in repo layer, even if "simple"
2. Domain layer must be pure — NO database access
3. Command layer uses domain objects (Source, Root, Object), not raw SQL values
4. Business logic uses domain predicates (is_active, is_excluded, is_archive), not inline conditions
5. Keep complexity low — batch operations only when they reduce complexity, not for performance

**Non-goals**:
- Performance optimization (N+1 queries during scan/transfer are not a concern)
- The goal is consistency, clarity, and strict separation — not speed

## Phases

### Phase 0: Fix domain/root.rs Layer Violation
- **Status**: completed
- **Goal**: Remove SQL from domain layer; domain must be pure
- **Why first**: This is a fundamental architectural violation. Functions in `domain/root.rs` are called from multiple commands. Fixing this establishes correct patterns before other work.

#### Current violations in `domain/root.rs`:

| Function | Line | Issue |
|----------|------|-------|
| `parse_root_spec()` | L165-188 | Inline SQL to look up root by ID or path |
| `resolve_root_path_impl()` | L232-235 | Inline SQL to fetch all roots |
| `resolve_archive_path()` | L251-254 | Inline SQL to fetch all roots |

#### Refactoring approach:

These functions mix three concerns:
1. **Pure domain logic**: `RootSpec::parse()`, `find_containing_root()` — keep in domain
2. **Filesystem I/O**: `fs::canonicalize()` — keep as-is (command boundary)
3. **Database access**: `conn.query_row()`, `conn.prepare()` — move to repo

**Solution**: Refactor to use `repo::root::fetch_all()` which already exists and returns `Vec<Root>` domain objects. Apply domain predicates (`root.is_active()`, `root.is_archive()`) for filtering.

| Function | Refactored Approach |
|----------|---------------------|
| `parse_root_spec()` | Fetch all roots via repo, filter with domain predicates, apply `RootSpec` matching |
| `resolve_root_path_impl()` | Already calls `find_containing_root()` (pure); just need to get roots from repo |
| `resolve_archive_path()` | Same pattern — fetch via repo, filter with `root.is_archive()` |

**Consideration**: These functions currently take `&Connection`. After refactor, callers will need to pass roots or we create orchestration helpers. Options:
1. Caller fetches roots, passes to pure domain function
2. Create `resolve_*` functions in command layer that orchestrate repo + domain
3. Keep functions in `domain/root.rs` but have them take `&[Root]` instead of `&Connection`

Recommend option 3: Keep the orchestration in `domain/root.rs` but change signatures to take `&[Root]` (already-fetched domain objects). Callers fetch via `repo::root::fetch_all()` then call domain functions.

#### Callers to update:

| Function | Caller Location |
|----------|-----------------|
| `parse_root_spec()` | roots.rs:154, roots.rs:231, coverage.rs:85, apply.rs:799 |
| `resolve_root_path()` | main.rs:532, main.rs:540 |
| `resolve_root_path_any()` | scan.rs:158 |
| `resolve_archive_path()` | cluster.rs:192 |

#### Existing test coverage:

Tests exist in `domain/root.rs` for:
- `RootSpec::parse()` — 6 tests covering ID parsing, path parsing, edge cases (already pure, no changes needed)
- `find_containing_root()` — 6 tests covering exact match, nested paths, prefix edge cases (already pure, no changes needed)
- Root predicates — tests for `is_suspended()`, `is_active()`, `is_source()`, `is_archive()`, `matches_scope()`

**Not currently tested** (because they require database):
- `parse_root_spec()` orchestration function — will become testable after refactor
- `resolve_root_path_impl()` — will become testable after refactor
- `resolve_archive_path()` — will become testable after refactor

After refactoring to take `&[Root]`, these functions become pure and can have unit tests added.

### Phase 1: apply.rs SQL Extractions
- **Status**: completed
- **Goal**: Extract inline SQL to repo layer
- **Dependencies**: Phase 0 complete

#### 1.1 Use existing repo function

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L929-951 `check_stale_destination_records` | Inline batched SQL for present paths | `repo::source::batch_check_paths_exist()` | **Function already exists** at repo/source.rs:277-316. Just replace inline SQL with call to existing function. |

#### 1.2 Write operations

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L1639-1643 `relocate_source` | Inline UPDATE | `repo::source::update_location()` | Updates root_id, rel_path, timestamps |
| L1649-1652 `mark_source_not_present` | Inline UPDATE | `repo::source::mark_missing(&[id])` | Use existing batch function with single-item slice. Simpler than adding a variant. |

#### 1.3 Aggregation queries

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L1227-1233 `check_archive_hash_coverage` | Inline COUNT/SUM | `repo::source::count_unhashed_for_root()` | Returns (total, unhashed) counts |

### Phase 2: apply.rs Domain Object Usage
- **Status**: completed
- **Goal**: Use domain objects and predicates instead of raw SQL values
- **Dependencies**: Phase 1 complete
- **Key insight**: This phase is about using the right abstractions, not performance. Whether we fetch one-at-a-time or in batch, the important thing is using `Source` objects and predicates like `is_active()`.

#### 2.1 Use already-cached roots

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L219-224 archive root lookup | Separate query `SELECT path FROM roots WHERE id = ? AND role = 'archive'` | Look up from `roots` (already fetched at L214), verify `root.is_archive()` | Roots are already cached. Find by ID, use domain predicate. |

#### 2.2 Use domain objects instead of raw values

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L1280-1288 `check_suspended_sources_filtered` | Inline query returning raw `suspended` boolean | Fetch via repo, use `source.is_active()` predicate | Use domain predicate instead of raw boolean. Batch or loop — choose whichever is simpler. |
| L1398-1405 `check_source_states_db` | Inline query returning `(size, mtime, partial_hash, present)` tuple | Fetch via repo, compare `source.size`, `source.mtime`, `source.partial_hash`, `source.present` | Use Source struct fields instead of raw tuple. |

**Note on `check_source_states_db`**: Compares DB state (Source) against snapshot state (LockEntry). The comparison logic stays in command layer — this is orchestration. But we use Source struct fields, not raw SQL values.

**Batching is optional**: If fetching sources one-at-a-time in a loop is simpler and clearer than batch fetching, that's fine. The goal is domain object usage, not performance.

### Phase 3: scan.rs SQL Extractions
- **Status**: completed
- **Goal**: Extract inline SQL to repo layer
- **Dependencies**: Can run in parallel with Phase 1-2

#### 3.1 Specialized queries

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L51-52 `classify_sources_in_empty_dir` | `SELECT id, device FROM sources WHERE root_id = ? AND rel_path LIKE ?` | `repo::source::fetch_device_info_by_prefix()` | Returns `Vec<(i64, Option<i64>)>` for mount protection |

#### 3.2 Write operations

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L330-334 `create_root` | Inline INSERT | `repo::root::create()` | Returns new root ID |
| L225-228 update last_scanned_at | Inline UPDATE | `repo::root::update_last_scanned_at()` | |
| L297-299 link source to object | Inline UPDATE | `repo::source::set_object_id()` | |
| L717-733 `get_or_create_object` | Inline SELECT + INSERT | `repo::object::get_or_create()` | Idempotent get-or-create pattern |
| L739-746 `store_hash_fact` | Inline INSERT/UPSERT | `repo::fact::store_object_fact()` | |

#### 3.3 Redundant root fetching

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| L126-131 get all unsuspended roots | Inline query with dynamic WHERE | `repo::root::fetch_all()` + filter with `root.is_active()` and role predicates |
| L340-343 `check_overlapping_roots` | Inline SELECT all paths | Use `repo::root::fetch_all()` |
| L775-777 `find_candidates` unsuspended roots | Inline SELECT | `repo::root::fetch_all()` + filter with `root.is_active()` |

### Phase 4: scan.rs Domain Object Usage
- **Status**: completed (done during Phase 3)
- **Goal**: Use cached Root data and domain predicates
- **Dependencies**: Phase 3 complete
- **Note**: These items were completed during Phase 3 since they were simpler to do together with the SQL extraction.

| Location | Current | Target | Notes |
|----------|---------|--------|-------|
| ✅ L164-168 check if root suspended | ~~Separate query~~ | `roots.iter().find(\|r\| r.id == id)` then `root.is_suspended()` | Done in Phase 3 |
| ✅ L764-768 `find_candidates` suspended check | ~~Separate query~~ | Same pattern: look up from `all_roots`, use `root.is_suspended()` | Done in Phase 3 |

## Future Cleanup (Not In This Spec)

The following files have inline SQL that should be extracted in separate efforts:

### `roots.rs` — Root Management Commands
- 12 inline SQL statements
- `suspend()`/`unsuspend()` → `repo::root::set_suspended()`
- `set_comment()` → `repo::root::set_comment()`
- `remove()` → `repo::root::delete()` with cascade
- `count_sources_for_roots()` → `repo::source::count_by_roots()`
- Archive coverage counting → complex query, needs design

### `facts.rs` — Fact Management
- ~40 inline SQL statements
- Value distribution queries
- Cleanup and prune operations
- Needs comprehensive spec

### `import_facts.rs` — Fact Import
- 11 inline SQL statements
- Import and staleness validation logic

## Design Decisions

### Domain Layer Must Be Pure
No database access in `domain/`. Functions take domain objects as input, return domain results. Orchestration happens in command layer.

### Repo Organization by Entity
Functions are organized by **entity** (source, root, object, fact), not by command.

### Domain Objects Over Raw Values
Even for "simple" checks, use domain objects:
- `source.is_active()` instead of checking `suspended` boolean from SQL
- `root.is_archive()` instead of `role = 'archive'` in SQL

### Repo Function Return Types
Creation and get-or-create functions return complete domain objects, not raw IDs:
- **Create functions** → Return domain object (e.g., `create()` → `Root`)
- **Get-or-create functions** → Return domain object (e.g., `get_or_create()` → `Object`)
- **Mutation functions** → Return `Result<()>` (e.g., `set_excluded()`)

This follows the `insert_destination()` pattern — no follow-up fetch required by the command layer.

### Simplicity Over Performance
- Batch operations only when they reduce complexity
- If a simple loop with single-item fetches is clearer, use that
- Performance during scan/transfer is not a concern — these are I/O-bound operations
- The goal is clarity and consistency, not speed

## Test Requirements

### Existing Tests
- `exclude.rs` has comprehensive tests
- `scan.rs` has `process_file` and `mark_missing_sources` tests
- `apply.rs` has integration tests
- `domain/root.rs` has tests for `find_containing_root()` (pure function)

### New Tests to Add

**Phase 0:** ✅ Completed (9 tests added)
- `parse_root_spec_impl()` with `&[Root]`:
  - ✅ Find root by ID (exists, not exists)
  - ⚠️ Find root by path — requires `fs::canonicalize()`, not unit-testable without filesystem
  - ✅ Role filtering: `require_role = Some("source")` accepts source, rejects archive
  - ✅ Role filtering: `require_role = Some("archive")` accepts archive, rejects source
  - ✅ Role filtering: `require_role = None` accepts any role
  - ✅ Suspension filtering: `parse_root_spec` excludes suspended, `parse_root_spec_any` includes
- `resolve_root_path_impl()` / `resolve_archive_path()`:
  - ⚠️ These still require `fs::canonicalize()` so remain integration-testable only
  - The underlying `find_containing_root()` is tested (6 tests)

**Phase 1:** ✅ Completed (6 tests added)
- ✅ `repo::source::batch_check_paths_exist()` — already has tests (3 boundary tests: 999, 1000, 1001 paths)
- ✅ `repo::source::count_unhashed_for_root()` — 4 tests (empty, all hashed, some unhashed, excludes not present)
- ✅ `repo::source::update_location()` — 2 tests (fields updated, nonexistent source)

**Phase 2:** ✅ Completed
- Existing integration tests catch regressions
- No new unit tests needed — rewiring to use existing domain objects

**Phase 3:** ✅ Completed (20 tests added)
- ✅ `repo::source::fetch_device_info_by_prefix()` — 5 tests (empty, matches all, matches prefix, excludes not present, returns device)
- ✅ `repo::source::set_object_id()` — 2 tests (links source, nonexistent source)
- ✅ `repo::root::create()` — 3 tests (returns new ID, with comment, multiple roots)
- ✅ `repo::root::update_last_scanned_at()` — 3 tests (sets timestamp, overwrites, nonexistent root)
- ✅ `repo::object::get_or_create()` — 4 tests (creates new, returns existing, idempotent, different hashes)
- ✅ `repo::fact::store_object_fact()` — 3 tests (inserts new, upserts existing, different keys)

## Files to Modify

### Phase 0
- `src/domain/root.rs` — change function signatures to take `&[Root]` instead of `&Connection`
- `src/main.rs` — update `resolve_root_path()` callers (2 locations)
- `src/scan.rs` — update `resolve_root_path_any()` caller
- `src/apply.rs` — update `parse_root_spec()` caller
- `src/roots.rs` — update `parse_root_spec()` callers (2 locations)
- `src/coverage.rs` — update `parse_root_spec()` caller
- `src/cluster.rs` — update `resolve_archive_path()` caller

### Phase 1
- `src/repo/source.rs` — add `count_unhashed_for_root()`, `update_location()` (note: `batch_check_paths_exist()` already exists)
- `src/apply.rs` — replace inline SQL with repo calls

### Phase 2
- `src/apply.rs` — refactor archive lookup, `check_suspended_sources_filtered`, `check_source_states_db`

### Phase 3
- `src/repo/source.rs` — add `fetch_device_info_by_prefix()`, `set_object_id()`
- `src/repo/root.rs` — add `create()`, `update_last_scanned_at()`
- `src/repo/object.rs` — add `get_or_create()`
- `src/repo/fact.rs` — add `store_object_fact()`
- `src/scan.rs` — replace inline SQL with repo calls

### Phase 4
- `src/scan.rs` — use cached Root structs + domain predicates
