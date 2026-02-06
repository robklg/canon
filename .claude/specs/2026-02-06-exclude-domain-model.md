# Spec: Exclude Domain Model

**Date:** 2026-02-06
**Status:** In Progress

## Problem Statement

The `exclude.rs` module has 11 inline SQL write statements for toggling exclusion flags on sources and objects. While the read side already uses established patterns (`repo::source::batch_fetch_by_roots`, domain predicates), the write side has inline SQL scattered through 8+ functions.

This follows the pattern established in the write-infrastructure spec and scan-domain-model spec:
- **apply.rs**: Complete (Phases A+B)
- **scan.rs**: Complete (Phases 1-4)
- **exclude.rs**: Next in priority (Medium priority, 11 write operations)

## Goal

Extract exclusion write operations to the repository layer with comprehensive tests. Establish simple patterns for flag-toggle operations that don't require returning full domain objects.

---

## Architectural Decisions

### D1: Flag toggles don't need full object return

**Decision:** Unlike `insert_destination()` or `apply_reconciliation()` which return complete `Source` objects after write, exclusion flag toggles return `Result<()>` (single) or `Result<u64>` (batch count).

**Rationale:**
- Write-infrastructure D8 recommends SELECT after write "for correctness" — but that's for operations where the caller needs the resulting state
- For exclusion flags, callers already have the source/object and just need confirmation the write succeeded
- Returning count for batch operations enables verification without extra queries

**Implications:**
- `repo::source::set_excluded()` returns `Result<()>`
- `repo::source::batch_set_excluded()` returns `Result<u64>` (count of rows updated)
- `repo::object::set_excluded()` returns `Result<()>`

### D2: Existing helper functions can remain in command module

**Decision:** Functions like `is_excluded()`, `get_source_path()`, `get_object_sources()` can remain in `exclude.rs` for now.

**Rationale:**
- These are read helpers used locally within the command
- The domain already has `Source::is_excluded()` which should be preferred
- Migrating these would be scope creep for this phase
- Can be cleaned up in a future refactoring pass

**Implications:**
- Phase 1 focuses only on write operations
- Read helpers remain unchanged
- Consider deprecating command-local `is_excluded()` in favor of domain predicate

### D3: Two-level exclusion semantics preserved

**Decision:** Source exclusion and object exclusion remain separate operations. Repo functions don't combine them.

**Rationale:**
- Source exclusion: Marks ONE specific file as excluded
- Object exclusion: Marks ALL files with that content as excluded
- These have different semantics and safety implications
- The domain `Source::is_excluded()` predicate handles the OR logic

**Implications:**
- `repo::source::set_excluded()` only touches `sources.excluded`
- `repo::object::set_excluded()` only touches `objects.excluded`
- Command layer decides which to call based on user intent

### D4: Batch operations use established chunking pattern

**Decision:** `batch_set_excluded()` uses the same `BATCH_SIZE = 1000` chunking as other repo functions.

**Rationale:**
- SQLite has variable limits (~999-32K depending on version)
- Consistent with `repo::source::mark_missing()`, `repo::object::batch_check_archived()`, etc.
- Tested pattern that handles large inputs safely

---

## Implementation Phases

### Phase 1: Core Exclusion Writes

**Status:** completed

**Goal:** Add repo functions for toggling exclusion flags on sources and objects.

**Scope:**

1. Add to `repo/source.rs`:
   ```rust
   /// Set the exclusion flag for a single source.
   ///
   /// # Behavior
   /// - Updates `excluded` column to the specified value
   /// - No error if source doesn't exist (0 rows affected)
   /// - Does NOT affect object-level exclusion
   ///
   /// # Returns
   /// Ok(()) on success. To verify the source existed, use batch variant.
   pub fn set_excluded(conn: &Connection, source_id: i64, excluded: bool) -> Result<()>

   /// Set the exclusion flag for multiple sources.
   ///
   /// # Behavior
   /// - Updates `excluded` column for all specified sources
   /// - Handles large inputs via chunking (BATCH_SIZE = 1000)
   /// - Sources that don't exist are silently skipped
   ///
   /// # Returns
   /// Count of rows actually updated (may be less than input if some sources don't exist).
   pub fn batch_set_excluded(conn: &Connection, source_ids: &[i64], excluded: bool) -> Result<u64>
   ```

2. Add to `repo/object.rs`:
   ```rust
   /// Set the exclusion flag for an object.
   ///
   /// # Behavior
   /// - Updates `excluded` column to the specified value
   /// - No error if object doesn't exist (0 rows affected)
   /// - Affects all sources linked to this object (via Source::is_excluded())
   ///
   /// # Returns
   /// Ok(()) on success.
   pub fn set_excluded(conn: &Connection, object_id: i64, excluded: bool) -> Result<()>
   ```

3. Migrate inline SQL in `exclude.rs`:
   - Line 70: `set()` → `repo::source::set_excluded()`
   - Line 116: `clear()` → `repo::source::set_excluded()`
   - Line 340: `set_by_id()` → `repo::source::set_excluded()`
   - Line 385: `set_by_path()` → `repo::source::set_excluded()`
   - Line 540: `exclude_duplicates()` → `repo::source::set_excluded()`
   - Line 754: `set_objects_by_filter()` → `repo::object::set_excluded()`
   - Line 830: `exclude_object_by_id()` → `repo::object::set_excluded()`
   - Line 865: `clear_object()` → `repo::object::set_excluded()`

**Non-goals:**
- Read-side migrations (helper functions)
- `exclude_duplicates` complex query (lines 474-484)
- Object lookup by hash (needed for Phase 2)
- Performance optimization

### Phase 2: Object Lookup Functions

**Status:** completed

**Goal:** Add repo functions for object lookup by hash.

**Scope:**

1. Add to `repo/object.rs`:
   ```rust
   /// Fetch an object by its hash value.
   ///
   /// # Returns
   /// - `Ok(Some(Object))` if found
   /// - `Ok(None)` if no object with that hash exists
   pub fn fetch_by_hash(conn: &Connection, hash: &str) -> Result<Option<Object>>
   ```

2. Migrate inline SQL in `exclude.rs`:
   - `set_object_by_hash()` → `repo::object::fetch_by_hash()`
   - `clear_object()` → `repo::object::fetch_by_hash()` + `object.is_excluded()` domain predicate

**Non-goals:**
- `fetch_by_hash_with_sources()` — deferred as premature; current code fetches sources separately when needed

**Dependencies:** Phase 1 complete

### Phase 3: Complex Query Migration (Deferred)

**Status:** pending

**Goal:** Migrate the complex duplicate-detection query in `exclude_duplicates`.

**Scope:**
- Analyze where path-prefix matching logic belongs (domain vs repo)
- Consider batch approach vs current per-source iteration
- Migrate or refactor `exclude_duplicates` query (lines 474-484)

**Dependencies:** Phases 1 and 2 complete

---

## Test Requirements

### Phase 1 Tests

#### Repo Layer Tests (`repo/source.rs`)

| Test | Description |
|------|-------------|
| `set_excluded_marks_source` | Verify `set_excluded(id, true)` sets excluded=1 |
| `set_excluded_clears_source` | Verify `set_excluded(id, false)` sets excluded=0 |
| `set_excluded_nonexistent_source` | No error when source doesn't exist |
| `batch_set_excluded_empty_list` | Returns 0 for empty input |
| `batch_set_excluded_multiple` | Updates multiple sources correctly |
| `batch_set_excluded_returns_count` | Returns actual count of rows updated |
| `batch_set_excluded_skips_nonexistent` | Count excludes sources that don't exist |
| `batch_set_excluded_handles_large_batch` | >1000 IDs processed correctly via chunking |

#### Repo Layer Tests (`repo/object.rs`)

| Test | Description |
|------|-------------|
| `set_excluded_marks_object` | Verify `set_excluded(id, true)` sets excluded=1 |
| `set_excluded_clears_object` | Verify `set_excluded(id, false)` sets excluded=0 |
| `set_excluded_nonexistent_object` | No error when object doesn't exist |

### Phase 2 Tests

#### Repo Layer Tests (`repo/object.rs`)

| Test | Description |
|------|-------------|
| `fetch_by_hash_returns_object` | Valid hash returns `Some(Object)` with correct fields |
| `fetch_by_hash_not_found` | Invalid hash returns `None` (no error) |
| `fetch_by_hash_returns_excluded_flag` | Object with `excluded=1` reflects in result |

### Existing Tests That Must Pass

All existing tests in `exclude.rs` (lines 980-1319):
- `test_get_matching_sources_*` (6 tests)
- `test_get_excluded_sources_*` (4 tests)
- `test_get_object_excluded_sources_*` (4 tests)

These tests verify the read side which must remain correct after the write-side migration.

---

## Related Documents

- `.claude/specs/2026-02-01-write-infrastructure.md` — Established patterns for domain types and repo functions
- `.claude/specs/2026-02-01-scan-domain-model.md` — Recent example of domain model extraction
- `CLAUDE.md` — Architecture documentation (update when complete)

---

## Validation Responsibilities

| Layer | Validates |
|-------|-----------|
| **Domain** | `Source::is_excluded()` — combines source + object exclusion |
| **Repo** | Database constraints (source/object exists via FK) |
| **Command** | Business rules (empty file safety check, confirmation prompts) |

---

## Panel Notes

**[Bruce]**: The two-level exclusion model (source + object) is critical. The domain `Source::is_excluded()` predicate handles the OR correctly. Repo functions should not try to combine these — they're separate operations with different semantics.

**[August]**: Existing tests cover the read side well, but there are NO tests for the write operations themselves. Phase 1 requires at minimum: `set_excluded_marks_source`, `batch_set_excluded_handles_large_batch`, `set_excluded_marks_object`.

**[Matthew]**: Phase 1 scope is appropriate — 3 new functions, 11 tests, migration of 8 UPDATE statements. The `exclude_duplicates` complex query is correctly deferred to Phase 3.
