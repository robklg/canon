# Story: facts.rs SQL Extraction

**Epic**: [Command Module Architectural Compliance](epic-command-architecture.md)
**Status**: Pending
**Created**: 2026-02-08

## Objective

Extract all inline SQL from `facts.rs` into repo layer functions, bringing the module into full architectural compliance. This is the largest and final module in the epic, with approximately 35 SQL operations across query, delete, and prune functions.

## Current State

**facts.rs** has ~35 SQL operations distributed across several function categories:

| Function | SQL Calls | Category |
|----------|-----------|----------|
| `get_matching_sources()` | 1 | Root ID fetch |
| `show_builtin_distribution()` | 10 | Per-builtin SELECT queries |
| `delete_facts()` | 10 | COUNT + DELETE with temp tables |
| `prune_stale()` | 2 | COUNT + DELETE |
| `prune_orphaned_objects()` | 8 | Multiple COUNT + DELETE |
| `prune_excluded_facts()` | 4 | COUNT + DELETE |

**What's already good:**
- `get_matching_sources()` uses `repo::source::batch_fetch_by_roots()` + domain predicates
- `show_value_distribution()` uses `repo::fact::batch_fetch_key_for_sources()`
- `show_transformed_distribution()` uses `repo::fact::batch_fetch_key_for_sources()`
- `show_grouped_distribution()` uses `repo::source::batch_fetch_by_ids()` + domain resolution
- `show_all_keys()` uses `repo::fact::count_fact_keys()`

**What violates the architecture:**
1. Line 157: `SELECT id FROM roots` — should use existing `repo::root::fetch_all()`
2. Lines 427-610: `show_builtin_distribution()` — 10 inline SELECT queries
3. Lines 892-1068: `delete_facts()` — inline SQL for counting and deleting
4. Lines 1074-1120: `prune_stale()` — inline SQL for staleness detection
5. Lines 1152-1302: `prune_orphaned_objects()` — complex multi-table deletion
6. Lines 1317-1427: `prune_excluded_facts()` — inline SQL for exclusion-based deletion

**Performance context:**
- `canon facts` without scope takes ~14 seconds (acceptable)
- Extraction should maintain current performance characteristics
- Read operations already use efficient batch fetching
- Write operations use temp tables for efficient JOINs

## Design

### Phase 1: Query/Read Operations

**Goal:** Extract inline SQL from read operations. Establishes pattern for the command layer.

#### 1.1 Replace root ID query

```rust
// Before (line 157)
let root_ids: Vec<i64> = conn
    .prepare("SELECT id FROM roots")?
    .query_map([], |row| row.get(0))?
    .collect::<Result<Vec<_>, _>>()?;

// After
let roots = repo::root::fetch_all(conn)?;
let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();
```

#### 1.2 Refactor show_builtin_distribution()

**Current approach:** 10 inline queries for different built-in keys (source.ext, source.size, etc.)

**New approach:** Use existing `repo::source::batch_fetch_by_ids()` and extract columns in Rust.

```rust
// Fetch all sources once
let sources = repo::source::batch_fetch_by_ids(conn, source_ids)?;

// Extract values based on key
let values: Vec<FactValue> = match base_key {
    "source.ext" => sources.values()
        .map(|s| {
            let ext = Path::new(&s.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();
            FactValue::Text(ext)
        })
        .collect(),
    "source.size" => sources.values()
        .map(|s| FactValue::Num(s.size as f64))
        .collect(),
    // ... etc
};
```

**Rationale:**
- Avoids adding 10 new repo functions
- Uses data already available in Source struct
- Column extraction is O(n) in Rust — negligible compared to DB round-trip
- Simplifies the match statement

### Phase 2: Delete Facts

**Goal:** Extract `delete_facts()` SQL to repo layer.

#### New Repo Function

```rust
// repo/fact.rs

/// Options for fact deletion criteria.
pub struct DeleteCriteria {
    /// Entity type: "source" or "object"
    pub entity_type: String,
    /// Only delete facts of this value type (text, num, or time)
    pub value_type: Option<String>,
}

/// Count facts matching criteria for sources.
/// Returns (fact_count, entity_count).
pub fn count_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    criteria: &DeleteCriteria,
) -> Result<(i64, i64)>;

/// Delete facts matching criteria for sources.
/// Returns number of facts deleted.
pub fn delete_by_criteria(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    criteria: &DeleteCriteria,
) -> Result<usize>;
```

**Implementation details:**
- Uses `populate_temp_sources()` for efficient source ID handling
- For object entity type, creates temp_objects table from source.object_id
- Builds value type clause dynamically (`value_text IS NOT NULL`, etc.)

### Phase 3: Prune Operations

**Goal:** Extract `prune_stale()`, `prune_orphaned_objects()`, and `prune_excluded_facts()`.

#### 3.1 prune_stale

```rust
// repo/fact.rs

/// Count stale source facts where observed_basis_rev != current basis_rev.
pub fn count_stale(conn: &Connection) -> Result<i64>;

/// Delete stale source facts.
/// Returns number of facts deleted.
pub fn delete_stale(conn: &Connection) -> Result<usize>;
```

#### 3.2 prune_orphaned_objects

This is the most complex operation — deletes from multiple tables in cascade order.

```rust
// repo/object.rs

/// Statistics about orphaned objects and their associated data.
pub struct OrphanedStats {
    pub object_count: i64,
    pub source_count: i64,      // non-present sources referencing orphaned objects
    pub source_fact_count: i64,
    pub object_fact_count: i64,
}

/// Find statistics about orphaned objects (objects with no present sources).
pub fn find_orphaned_stats(conn: &Connection) -> Result<OrphanedStats>;

/// Delete orphaned objects and all associated data.
/// Deletes in order: source facts, sources, object facts, objects.
/// Returns actual counts deleted.
///
/// IMPORTANT: Should be called within a transaction for atomicity.
pub fn delete_orphaned(conn: &Connection) -> Result<OrphanedStats>;
```

**Transaction requirement:**
The command layer will wrap `delete_orphaned()` in a transaction:

```rust
// In facts.rs prune_orphaned_objects()
if !dry_run {
    let tx = conn.transaction()?;
    let deleted = repo::object::delete_orphaned(&tx)?;
    tx.commit()?;
    // report deleted stats
}
```

#### 3.3 prune_excluded_facts

```rust
// repo/fact.rs

/// Count facts for excluded entities.
/// Returns (source_fact_count, object_fact_count).
pub fn count_excluded(conn: &Connection, scope: &str) -> Result<(i64, i64)>;

/// Delete facts for excluded entities.
/// scope: "source", "object", or "all"
/// Returns (source_facts_deleted, object_facts_deleted).
pub fn delete_excluded(conn: &Connection, scope: &str) -> Result<(usize, usize)>;
```

## Decisions

| Decision | Rationale |
|----------|-----------|
| Use `batch_fetch_by_ids()` for builtin distribution | Avoids 10 new repo functions; data already in Source struct |
| Transaction for `delete_orphaned()` | Multi-table cascade must be atomic |
| Separate count/delete functions | Supports dry-run mode without code duplication |
| Keep value_type filter as dynamic SQL | Cleaner than separate functions per type |
| Pure functions stay in command layer | `is_protected_fact()`, `format_number()`, `format_root_display()` are pure presentation |

## Non-Goals

- Performance optimization beyond current behavior
- Changing output format or CLI flags
- Refactoring the structure of `show_builtin_distribution()` beyond SQL extraction
- Adding transactions to individual fact deletions (intentional per-fact isolation)
- Moving `format_number()` or `format_root_display()` to domain layer (presentation only)

## Test Plan

### Existing Tests

**facts.rs has ZERO unit tests.** The module relies entirely on CLI integration testing.

**repo/fact.rs has good coverage** from the import_facts story:
- `batch_fetch_for_sources`, `batch_fetch_key_for_sources`
- `count_fact_keys`, `fetch_type_map`, `upsert`
- `fetch_source_facts`, `object_has_fact`, `delete_by_id`

### New Tests to Add

#### Phase 1 Tests (command layer)

Pure function tests in `facts.rs`:
1. `test_format_number_with_commas` — 1000 → "1,000", 1000000 → "1,000,000"
2. `test_format_number_small` — numbers < 1000 unchanged
3. `test_format_root_display_short_path` — no truncation when path fits
4. `test_format_root_display_long_path` — truncation with "..." prefix
5. `test_is_protected_fact_source` — `source.policy` → true
6. `test_is_protected_fact_policy` — `policy.reviewed` → true
7. `test_is_protected_fact_content` — `content.Make` → false

#### Phase 2 Tests (repo layer)

In `repo/fact.rs`:
1. `test_count_by_criteria_source_entity` — counts source facts only
2. `test_count_by_criteria_object_entity` — counts object facts only
3. `test_count_by_criteria_value_type_text` — filters to text facts only
4. `test_count_by_criteria_value_type_num` — filters to num facts only
5. `test_delete_by_criteria_removes_matching` — deletes correct facts
6. `test_delete_by_criteria_preserves_non_matching` — leaves other facts

#### Phase 3 Tests (repo layer)

In `repo/fact.rs`:
1. `test_count_stale_detects_mismatch` — finds facts where observed_basis_rev != source.basis_rev
2. `test_count_stale_ignores_null_basis` — null observed_basis_rev not counted
3. `test_delete_stale_removes_mismatched` — deletes stale facts
4. `test_count_excluded_source` — counts facts for excluded sources
5. `test_count_excluded_object` — counts facts for excluded objects
6. `test_delete_excluded_source_only` — scope="source" deletes only source facts
7. `test_delete_excluded_all` — scope="all" deletes both

In `repo/object.rs`:
1. `test_find_orphaned_stats_no_orphans` — all objects have present sources
2. `test_find_orphaned_stats_with_orphans` — correctly counts orphaned objects
3. `test_delete_orphaned_cascade` — deletes objects, sources, and facts in order
4. `test_delete_orphaned_preserves_present` — doesn't delete objects with present sources

#### Integration Tests

Consider adding to CLI integration tests:
1. `facts prune --stale --dry-run` shows correct count
2. `facts prune --orphaned --dry-run` shows correct count
3. `facts delete <key> --dry-run` shows correct count

## Implementation Checklist

### Phase 1: Query/Read Operations ✅ COMPLETE

- [x] Replace `SELECT id FROM roots` with `repo::root::fetch_all()`
- [x] Refactor `show_builtin_distribution()` to use `repo::source::batch_fetch_by_ids()`
- [x] Extract column values in Rust instead of per-column SQL queries
- [x] Add pure function tests (`format_number`, `is_protected_fact`, etc.) — 11 tests added
- [x] Verify all tests pass — 428 tests pass
- [x] Update epic spec

### Phase 2: Delete Facts

- [ ] Add `repo::fact::count_by_criteria()` with tests
- [ ] Add `repo::fact::delete_by_criteria()` with tests
- [ ] Refactor `delete_facts()` to use repo functions
- [ ] Remove temp table management from command layer
- [ ] Verify all tests pass
- [ ] Update epic spec

### Phase 3: Prune Operations

- [ ] Add `repo::fact::count_stale()` with tests
- [ ] Add `repo::fact::delete_stale()` with tests
- [ ] Add `repo::object::OrphanedStats` struct
- [ ] Add `repo::object::find_orphaned_stats()` with tests
- [ ] Add `repo::object::delete_orphaned()` with tests
- [ ] Add `repo::fact::count_excluded()` with tests
- [ ] Add `repo::fact::delete_excluded()` with tests
- [ ] Refactor `prune_stale()` to use repo functions
- [ ] Refactor `prune_orphaned_objects()` with transaction
- [ ] Refactor `prune_excluded_facts()` to use repo functions
- [ ] Remove inline SQL from facts.rs
- [ ] Verify all tests pass
- [ ] Update epic spec to mark facts.rs complete

## Backward Compatibility

All command output must remain identical:
- Fact listing format unchanged
- Distribution display format unchanged
- Delete/prune dry-run messages unchanged
- Prune warnings and notes unchanged

## Dependencies

This story benefits from patterns established in earlier stories:
- `repo::fact::upsert()` pattern from import_facts
- Temp table pattern (`populate_temp_sources()`) already in use
- Transaction management pattern from import_facts

## Performance Considerations

**Phase 1 concern:** Fetching full sources via `batch_fetch_by_ids()` instead of single-column queries.

Analysis:
- Current: 1 query per built-in key type (10 queries for all keys)
- Proposed: 1 query via `batch_fetch_by_ids()`

The proposed approach fetches more columns but makes fewer queries. For large source sets, network round-trips dominate. The change should be neutral or slightly better.

**Mitigation:** If performance regresses measurably, we can add targeted `repo::source::batch_fetch_column()` functions. But start simple.

**Phases 2-3:** No performance change expected — same SQL, just moved to repo layer.

## Architectural Notes

After this story completes, `facts.rs` will follow the standard pattern:

```rust
// Query path
let roots = repo::root::fetch_all(conn)?;
let sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;
let filtered = sources.into_iter()
    .filter(|s| s.is_active())
    .filter(|s| s.matches_scope(scopes))
    // ... domain predicates
    .collect();

// For built-in distribution
let sources = repo::source::batch_fetch_by_ids(conn, &source_ids)?;
// Extract values in Rust, apply transforms, aggregate

// For delete/prune
let (count, _) = repo::fact::count_by_criteria(conn, source_ids, key, &criteria)?;
if !dry_run {
    repo::fact::delete_by_criteria(conn, source_ids, key, &criteria)?;
}
```

This completes the epic — all command modules will be architecturally compliant.
