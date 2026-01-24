# Refactoring Spec: Fact Infrastructure

## Why This Refactoring Matters

### The Problem: Fact Access is Scattered and Inconsistent

Canon's fact system powers critical features — `--where` filters, cluster manifests, coverage reports. Currently:

1. **Multiple fetch patterns** — `facts.rs` uses temp tables + UNION ALL, `cluster.rs` fetches per-source sequentially, `filter.rs` fetches ad-hoc during evaluation
2. **No unified domain model** — "What is a Fact?" has no single answer. `expr.rs` has `FactValue`, but there's no `Fact` struct
3. **SQL contains business logic** — Entity type handling (source vs object) is embedded in SQL strings
4. **Untestable** — Neither `facts.rs` nor `cluster.rs` has unit tests for their fact handling logic

When a user runs `canon facts --key content.Make`, they trust the aggregation is correct. A bug in fact handling could mean:
- Wrong coverage percentages
- Missing facts in cluster manifests
- Inconsistent `--where` filter behavior

### The Vision: Unified Fact Domain Model

Following the Source Infrastructure template, we want:
- What is a Fact? → One `Fact` struct (or reuse/extend `FactValue`)
- How do I fetch facts for sources? → One `fact_repo::batch_fetch_for_sources()`
- How do source vs object facts work? → Domain logic, not SQL

### Relationship to Source Infrastructure

This project builds on the completed Source Infrastructure:
- `facts.rs` and `cluster.rs` already need source selection
- Source selection should use `Source` domain model + `source_repo`
- Fact Infrastructure adds the fact-fetching layer on top

---

## Architectural Model

### Current State

```
facts.rs → Complex SQL with embedded fact logic
            (entity_type handling, temp tables, UNION ALL)

cluster.rs → Different SQL with same logic duplicated
             (per-source fetch, manual source/object handling)

filter.rs → Ad-hoc fact fetching during evaluation
```

### Target State

```
┌─────────────────────────────────────────────────────────┐
│  Application Layer (commands)                           │
│  facts.rs, cluster.rs                                   │
│  - Use source_repo for source selection                 │
│  - Use fact_repo for fact fetching                      │
│  - Apply domain predicates                              │
└─────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────┐    ┌──────────────────────────────┐
│  Domain Layer        │    │  Infrastructure Layer        │
│                      │    │                              │
│  source.rs (done)    │    │  source_repo.rs (done)       │
│  fact.rs (new):      │    │  fact_repo.rs (new):         │
│  - FactValue (reuse) │    │  - batch_fetch_for_sources() │
│  - FactEntry struct  │    │                              │
│  - fact predicates   │    │  SQL does ONE thing:         │
│                      │    │  "Give me facts for sources" │
└──────────────────────┘    └──────────────────────────────┘
```

---

## Scope

### In Scope (This Project)
- `fact.rs` — Domain types (consolidate/reuse from `expr.rs`)
- `fact_repo.rs` — Batch fetch facts for source IDs
- Migrate `facts.rs` command — Use Source + Fact infrastructure
- Document cluster.rs requirements (for future migration)

### Out of Scope (This Project)
- `cluster.rs` migration — Separate follow-up project (see "Cluster.rs Requirements" below)
- `filter.rs` changes — Keep existing ad-hoc approach for now
- Write operations (`import_facts.rs`)
- Schema changes to facts table
- Root Infrastructure, Object Infrastructure

### Non-Goals
- Changing the facts table schema
- Changing command output or behavior
- Supporting non-SQLite backends

---

## Design Analysis: What Do Consumers Need?

### facts.rs Needs

Examining `facts.rs` (1784 lines), it needs:

1. **Source selection** (lines 170-231): Get source IDs matching scope/filters
   - Currently: Custom SQL with `build_scope_clause()`, `exclude_clause()`
   - Should use: `Source` domain model (already available)

2. **Fact key listing** (lines 233-333): Count distinct fact keys across sources
   - Input: Set of source IDs
   - Output: `(key, count, fact_type)` tuples
   - Handles: Both source facts AND object facts for those sources

3. **Value distribution** (lines 335-455): Group fact values with counts
   - Input: Source IDs + specific key
   - Output: `(value, count)` tuples
   - Handles: Type conversion for display

4. **Built-in distribution** (lines 576-824): Same but for derived facts
   - Uses `FactValue` + modifiers from `expr.rs`
   - Fetches raw data, applies transforms in Rust

5. **Grouped distribution** (lines 985-1199): Multi-dimensional grouping
   - Needs: Per-source fact values (not just aggregates)
   - Uses: `fetch_source_data()` + `fetch_stored_fact_values()`

**Key insight**: `facts.rs` needs both aggregate queries AND per-source fact access.

### cluster.rs Needs (For Future Migration)

Examining `cluster.rs` (864 lines), it needs:

1. **Source selection** (lines 380-452): Get sources matching scope/filters
   - Currently: Custom SQL with `build_scope_clause()`
   - Should use: `Source` domain model

2. **Per-source fact snapshot** (lines 478-576): ALL facts for each source
   - Input: Single source ID
   - Output: `HashMap<String, serde_json::Value>` (all facts)
   - Handles: Source facts + object facts merged

3. **100% coverage detection** (lines 651-766): Which facts exist on ALL sources
   - Input: All sources in manifest
   - Output: List of keys with 100% coverage + their types

4. **Type consistency checking** (lines 599-648): Warn about mixed types
   - Needs: Per-source type information

**Key insight**: `cluster.rs` needs per-source complete fact snapshots, not just aggregates.

### Common Interface Requirements

Both modules need:
```rust
// Fetch all facts for a set of sources, returning per-source results
// Transparently merges source facts + object facts
fn batch_fetch_for_sources(
    conn: &Connection,
    source_ids: &[i64]
) -> Result<HashMap<i64, Vec<FactEntry>>>

struct FactEntry {
    key: String,
    value: FactValue,
    entity_type: String,  // "source" or "object" (for debugging/tracing)
}
```

This serves:
- `facts.rs`: Aggregate across the HashMap values
- `cluster.rs`: Iterate per-source, snapshot to JSON

---

## Data Model

### FactEntry Struct

```rust
/// A single fact entry associated with a source.
///
/// Note: Facts are stored on either sources or objects, but for query purposes
/// we associate them with sources. A source's "effective facts" include both
/// its direct facts AND its object's facts (if it has an object_id).
#[derive(Debug, Clone)]
pub struct FactEntry {
    /// The fact key (e.g., "content.Make", "source.policy.reviewed")
    pub key: String,
    /// The typed fact value
    pub value: FactValue,
    /// Where this fact is stored: "source" or "object"
    /// Useful for debugging and for operations that need to distinguish
    pub entity_type: String,
    /// The entity ID (source_id or object_id depending on entity_type)
    pub entity_id: i64,
}
```

### Reuse FactValue from expr.rs

`expr.rs` already defines:
```rust
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),
    Path(String),
}
```

We'll reuse this directly. The `fact.rs` module will re-export it for convenience.

### No Predicates Needed (Initially)

Unlike `Source` which has rich predicates (`is_excluded()`, `matches_scope()`), facts don't have complex business rules. The value is in:
1. Unified fetching (source + object facts merged)
2. Typed values (not raw SQL columns)
3. Testable repository layer

If predicates emerge during migration, we'll add them.

---

## Repository Layer

### fact_repo.rs

```rust
//! Fact repository — infrastructure layer for fetching facts.
//!
//! This module provides batch fetch functions that return typed fact data.
//! It transparently handles the source/object fact distinction.

use std::collections::HashMap;
use anyhow::Result;
use crate::db::Connection;
use crate::fact::FactEntry;

/// Batch size for SQL IN clauses (consistent with source_repo)
pub const BATCH_SIZE: usize = 1000;

/// Fetch all facts for the given source IDs.
///
/// Returns a map from source_id to list of FactEntry.
/// Each source's facts include:
/// - Direct source facts (entity_type = 'source', entity_id = source_id)
/// - Object facts (entity_type = 'object', entity_id = object_id) if source has object_id
///
/// Object facts are associated with the SOURCE id in the result map,
/// making it easy to get "all facts for this source" without separate lookups.
pub fn batch_fetch_for_sources(
    conn: &Connection,
    source_ids: &[i64]
) -> Result<HashMap<i64, Vec<FactEntry>>>

/// Fetch facts for a specific key only.
///
/// More efficient when you only need one fact key across many sources.
/// Returns map from source_id to Option<FactEntry> (None if source lacks this fact).
pub fn batch_fetch_key_for_sources(
    conn: &Connection,
    source_ids: &[i64],
    key: &str
) -> Result<HashMap<i64, Option<FactEntry>>>

/// Count fact keys across sources.
///
/// Returns (key, count, dominant_type) tuples.
/// Used by `canon facts` (no --key) to show fact coverage.
pub fn count_fact_keys(
    conn: &Connection,
    source_ids: &[i64]
) -> Result<Vec<(String, usize, FactType)>>
```

### SQL Strategy

The repository will use a similar pattern to `facts.rs` current approach:
1. Populate temp table with source IDs (for large sets)
2. JOIN to get source facts
3. JOIN through sources to get object facts
4. UNION ALL for efficiency

But the SQL is isolated in the repository — commands just call `batch_fetch_for_sources()`.

---

## Phases

### Phase 1: Domain Types
- **Status**: ✅ completed
- **Goal**: Create `fact.rs` with types, re-export `FactValue` from `expr.rs`
- **Scope**:
  - Create `src/fact.rs`
  - Define `FactEntry` struct
  - Re-export `FactValue`, `FactType` from `expr.rs`
  - Add to `lib.rs` / `main.rs` module declarations
- **Tests Added** (7 total):
  - `fact_entry_new_creates_entry` — basic construction with Text value
  - `fact_entry_with_num_value` — Num variant
  - `fact_entry_with_time_value` — Time variant
  - `fact_entry_with_path_value` — Path variant
  - `is_source_fact_returns_true_for_source` — predicate for source facts
  - `is_object_fact_returns_true_for_object` — predicate for object facts
  - `fact_entry_clone_creates_independent_copy` — Clone trait
- **Dependencies**: None
- **Notes**:
  - Added `FactEntry::new()` constructor and `is_source_fact()`/`is_object_fact()` predicates
  - Dead code warnings expected until Phase 2 uses the types

### Phase 2: Repository Layer
- **Status**: ✅ completed
- **Goal**: Create `fact_repo.rs` with batch fetch functions
- **Scope**:
  - Create `src/fact_repo.rs`
  - Implement `batch_fetch_for_sources()`
  - Implement `batch_fetch_key_for_sources()` (optimization for single-key queries)
  - Implement `count_fact_keys()` (for fact listing)
  - Use temp table pattern for large source sets
- **Tests Added** (17 total):
  - `batch_fetch_for_sources_empty_ids` — empty input returns empty map
  - `batch_fetch_for_sources_no_facts` — source exists but has no facts
  - `batch_fetch_for_sources_source_facts` — fetches direct source facts
  - `batch_fetch_for_sources_object_facts` — fetches object facts via source's object_id
  - `batch_fetch_for_sources_mixed_facts` — source with both source and object facts
  - `batch_fetch_for_sources_no_object_id` — source without object_id gets no object facts
  - `batch_fetch_for_sources_multiple_sources` — multiple sources, shared objects
  - `batch_fetch_for_sources_time_value` — Time variant conversion
  - `batch_fetch_key_for_sources_empty_ids` — empty input
  - `batch_fetch_key_for_sources_found` — specific key exists
  - `batch_fetch_key_for_sources_missing` — specific key doesn't exist
  - `batch_fetch_key_for_sources_partial` — some sources have key, some don't
  - `count_fact_keys_empty_ids` — empty input
  - `count_fact_keys_basic` — counts single key correctly
  - `count_fact_keys_type_detection` — identifies Text/Num/Time types
  - `count_fact_keys_multiple_sources` — counts across sources
  - `count_fact_keys_shared_object` — multiple sources sharing object counted correctly
- **Dependencies**: Phase 1
- **Notes**:
  - Uses temp table pattern (`populate_temp_sources`) for large sets — no IN clause chunking needed
  - UNION ALL pattern matches existing `facts.rs` SQL for consistency
  - Object facts keyed by source_id in result map (not entity_id)

### Phase 3: Migrate facts.rs Source Selection
- **Status**: ✅ completed
- **Goal**: Replace `get_matching_sources()` with Source domain model
- **Scope**:
  - Use `source_repo::batch_fetch_by_roots()` + domain predicates
  - Same pattern as `ls.rs`, `worklist.rs`, `coverage.rs`
  - Keep existing fact queries temporarily
- **Changes**:
  - Rewrote `get_matching_sources()` to return `(Vec<i64>, usize)` with excluded_count
  - Removed SQL-based pagination, now uses batch fetch + domain predicates
  - Updated `run()` to use excluded_count instead of separate query
  - Updated `delete_fact_values()` call site
  - Removed imports: `rusqlite::types::Value`, `build_scope_clause`, `exclude`
  - Added imports: `source_repo`, `HashSet`
- **Validation**:
  - `canon facts` (no args): ✓ identical behavior
  - `canon facts --key content.device.make`: ✓ identical behavior
  - `canon facts --where 'source.ext=jpg'`: ✓ identical behavior
- **Dependencies**: Phase 2

### Phase 4: Migrate facts.rs Fact Fetching
- **Status**: pending
- **Goal**: Replace SQL fact queries with `fact_repo` calls
- **Scope**:
  - Replace `show_all_keys()` SQL with `count_fact_keys()`
  - Replace `show_value_distribution()` SQL with `batch_fetch_key_for_sources()`
  - Replace `show_transformed_distribution()` SQL with `batch_fetch_key_for_sources()`
  - Replace `fetch_stored_fact_values()` with `batch_fetch_key_for_sources()`
  - Keep `show_builtin_distribution()` as-is (derives from source columns, not facts table)
- **Validation**:
  - All `canon facts` variations: IDENTICAL output
  - `canon facts --by-root --key content.Make`: IDENTICAL output
  - `canon facts --group-by content.Make|year --key content.Model`: IDENTICAL output
- **Dependencies**: Phase 3

### Phase 5: Cleanup and Documentation
- **Status**: pending
- **Goal**: Remove dead code, document patterns
- **Scope**:
  - Remove unused SQL query functions from `facts.rs`
  - Update CLAUDE.md with Fact domain model documentation
  - Update Source Infrastructure spec's Future Work section
- **Dependencies**: Phase 4

---

## Cluster.rs Requirements

This section documents what `cluster.rs` needs from Fact Infrastructure, ensuring our design supports the future Cluster Migration project.

### Must Support

1. **Per-source complete fact snapshot**
   - `cluster.rs` builds a `LockEntry` per source with ALL facts as JSON
   - `batch_fetch_for_sources()` returns per-source results ✓

2. **100% coverage detection**
   - Need to know which facts appear on ALL sources
   - Can compute from `batch_fetch_for_sources()` result: count keys, keep those with count == source_count

3. **Type consistency checking**
   - Need to detect mixed types (e.g., same key has Time on some sources, Text on others)
   - `FactEntry` includes typed `FactValue` — can track types during aggregation

4. **JSON serialization**
   - Lock file stores facts as `HashMap<String, serde_json::Value>`
   - `FactValue` can convert to `serde_json::Value` (add helper if needed)

### Verified Compatible

The `batch_fetch_for_sources()` interface serves both:
- **facts.rs**: Aggregate values across sources → group by key, count
- **cluster.rs**: Snapshot per source → iterate map, serialize each source's facts

---

## Test Requirements

### Current State

Neither `facts.rs` nor `cluster.rs` has unit tests (`mod tests` section).

### Tests to Add

**Phase 1 (fact.rs)**:
- `FactEntry` construction
- `FactValue` to JSON conversion (if we add helper)

**Phase 2 (fact_repo.rs)** — In-memory SQLite tests:
- `batch_fetch_for_sources_empty_ids` — returns empty map
- `batch_fetch_for_sources_no_facts` — sources exist but have no facts
- `batch_fetch_for_sources_source_facts` — fetches direct source facts
- `batch_fetch_for_sources_object_facts` — fetches object facts via source
- `batch_fetch_for_sources_mixed` — source with both source and object facts
- `batch_fetch_for_sources_no_object` — source without object_id
- `batch_fetch_key_for_sources_found` — specific key exists
- `batch_fetch_key_for_sources_missing` — specific key doesn't exist
- `count_fact_keys_basic` — counts correctly
- `count_fact_keys_type_detection` — identifies dominant type

**Phase 3-4 (behavioral)** — Before/after validation:
- Capture output before migration
- Compare byte-for-byte after migration
- Key scenarios:
  - `canon facts` (listing all keys)
  - `canon facts --key content.Make` (value distribution)
  - `canon facts --key source.mtime|year` (builtin with modifier)
  - `canon facts --by-root --key content.Make` (grouped)
  - `canon facts <scope> --where 'source.ext=jpg'` (filtered)

---

## Invariants (Must Remain True)

### Architectural
1. **Domain types in `fact.rs` have no I/O** — Pure data structures
2. **Repository does data access only** — No business logic in SQL
3. **Facts associated with sources** — Even object facts are keyed by source_id in results
4. **Reuse `FactValue` from `expr.rs`** — Don't duplicate type definitions

### Implementation
1. **Batch size is 1000** — Consistent with `source_repo`
2. **Temp table for large sets** — Same pattern as existing code
3. **Handle NULL gracefully** — Sources without facts, sources without objects

### Correctness
1. **Source + object facts merged** — A source's facts include its object's facts
2. **Type preserved** — `FactValue` enum carries type information
3. **Command output unchanged** — Byte-identical after migration

---

## Known Risks

### Abstraction Mismatch
- **Risk**: Design works for `facts.rs` but not `cluster.rs`
- **Mitigation**: "Cluster.rs Requirements" section validates compatibility before implementation

### Performance Regression
- **Risk**: Fetching all facts upfront uses more memory than streaming
- **Mitigation**: For `facts.rs`, we already use temp tables and batch processing. Profile if concerned.

### filter.rs Coupling
- **Risk**: Changing fact fetching breaks `filter.rs`
- **Mitigation**: `filter.rs` is explicitly out of scope. It can continue using its current approach.

---

## References

- Completed: `.claude/specs/2026-01-24-source-infrastructure.md`
- Existing types: `src/expr.rs` (FactValue, FactType, Modifier)
- Current implementation: `src/facts.rs`, `src/cluster.rs`
