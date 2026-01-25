# Refactoring Spec: Object Infrastructure

## Why This Refactoring Matters

### The Problem: Object Operations Are Scattered and N+1

Canon's object system handles content identity (by hash) and archive detection. Currently:

1. **No `Object` struct** — Object data is fetched as tuples or individual fields scattered across commands
2. **N+1 archive detection** — `cluster.rs:find_in_archive()` queries per-source; `ls.rs:check_archived()` does the same
3. **Duplicated SQL patterns** — Archive detection logic (JOIN objects → sources → roots WHERE role='archive') appears in multiple places
4. **Untestable** — Archive detection is embedded in SQL, can't be unit tested

### The Vision: Complete the "Big Four" Domain Model

With Source, Fact, and Root infrastructure complete, Object is the final core entity:

| Entity | Domain Module | Repository Module | Status |
|--------|---------------|-------------------|--------|
| Source | `source.rs` | `source_repo.rs` | ✅ |
| Fact | `fact.rs` | `fact_repo.rs` | ✅ |
| Root | `root.rs` | `root_repo.rs` | ✅ |
| **Object** | `object.rs` | `object_repo.rs` | ❌ This project |

### What This Enables

1. **Batch archive detection** — Eliminate N+1 queries in cluster.rs, ls.rs, coverage.rs
2. **Testable predicates** — `object.is_excluded()` can be unit tested
3. **Consistent patterns** — All four core entities follow the same architecture
4. **Future flexibility** — Foundation for potential object-level operations

---

## Architectural Model

### Current State

```
cluster.rs
├── batch_fetch_object_hashes()  → Helper function (should be in repo)
├── find_in_archive()            → N+1 per-source query (by hash_value)
└── Uses hash_value for archive lookup (indirect via objects table)

ls.rs
├── check_archived()             → N+1 per-source query (by object_id)
└── get_archive_paths()          → N+1 per-source query (by object_id)

coverage.rs
└── count_archived_from_temp()   → Bulk query via temp table (good pattern)
    └── Supports Option<archive_root_id> for specific archive
```

### Target State

```
┌─────────────────────────────────────────────────────────────────┐
│  Commands (cluster.rs, ls.rs, coverage.rs)                      │
│  - Use object_repo for all object operations                    │
│  - Use domain predicates for exclusion checks                   │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          ▼                                       ▼
┌──────────────────────┐            ┌──────────────────────────────┐
│  Domain Layer        │            │  Infrastructure Layer        │
│                      │            │                              │
│  object.rs:          │            │  object_repo.rs:             │
│  - Object struct     │            │  - batch_fetch_by_ids()      │
│  - is_excluded()     │            │  - batch_check_archived()    │
│                      │            │  - batch_find_archive_paths()│
│                      │            │                              │
│  This is the TRUTH   │            │  SQL does data access only   │
│  about objects       │            │                              │
└──────────────────────┘            └──────────────────────────────┘
```

---

## Scope

### In Scope

1. **`object.rs`** — Domain types and predicates
2. **`object_repo.rs`** — Batch fetch and archive detection functions
3. **Migrate `cluster.rs`** — Replace `find_in_archive()` and `batch_fetch_object_hashes()`
4. **Migrate `ls.rs`** — Replace `check_archived()` and `get_archive_paths()`
5. **Migrate `coverage.rs`** — Replace `count_archived_from_temp()` with repo function

### Out of Scope

- **Write operations** — `scan.rs:get_or_create_object()` stays as-is
- **Schema changes** — No changes to objects table
- **Exclusion management** — `exclude.rs` write operations stay as-is
- **Filter infrastructure** — Keep existing ad-hoc approach

### Non-Goals

- Changing command output or behavior
- Performance optimization beyond batch fetching
- Supporting non-SQLite backends

---

## Data Model

### Object Struct

```rust
/// Content identified by hash.
///
/// Objects represent unique content in canon. Multiple sources can reference
/// the same object (same content at different paths). Objects are created
/// when sources are hashed during scanning.
#[derive(Debug, Clone)]
pub struct Object {
    /// Database ID
    pub id: i64,
    /// Hash algorithm (e.g., "sha256")
    pub hash_type: String,
    /// The content hash
    pub hash_value: String,
    /// Whether this object (and all its sources) is excluded
    pub excluded: bool,
}
```

### Domain Predicates

```rust
impl Object {
    /// Check if this object is excluded.
    ///
    /// Object-level exclusion excludes ALL sources linked to this object.
    /// This is used for content-based exclusion (e.g., known bad files).
    pub fn is_excluded(&self) -> bool {
        self.excluded
    }
}
```

### Field Verification

Checking command needs:
- `cluster.rs`: needs `hash_type`, `hash_value` for lock file ✓
- `ls.rs`: needs `id` for archive checking ✓
- `coverage.rs`: needs `id` for archive counting ✓
- `source.rs`: already has `object_excluded` denormalized ✓

---

## Repository Layer

### object_repo.rs

```rust
//! Object repository — infrastructure layer for fetching objects.
//!
//! This module provides batch fetch functions for objects and archive detection.
//! Archive detection answers: "Is this content in any archive root?"

use std::collections::{HashMap, HashSet};
use anyhow::Result;
use crate::db::Connection;
use crate::object::Object;

/// Batch size for SQL IN clauses (consistent with other repos)
pub const BATCH_SIZE: usize = 1000;

/// Fetch objects by their IDs.
///
/// Returns HashMap for O(1) lookup. Missing IDs are not included.
pub fn batch_fetch_by_ids(
    conn: &Connection,
    object_ids: &[i64]
) -> Result<HashMap<i64, Object>>

/// Check which objects have copies in archive root(s).
///
/// Returns set of object IDs that have at least one source in an archive root.
/// An object is "archived" if EXISTS a source with that object_id under a
/// root with role='archive' and present=1.
///
/// If `archive_root_id` is Some, checks only that specific archive.
/// If `archive_root_id` is None, checks all archive roots.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_check_archived(
    conn: &Connection,
    object_ids: &[i64],
    archive_root_id: Option<i64>,
) -> Result<HashSet<i64>>

/// Find archive paths for objects.
///
/// Returns map from object_id to list of archive paths where that content exists.
/// Only includes objects that have archive copies.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_find_archive_paths(
    conn: &Connection,
    object_ids: &[i64]
) -> Result<HashMap<i64, Vec<String>>>
```

### SQL Patterns

**batch_fetch_by_ids** — Simple object lookup:
```sql
SELECT id, hash_type, hash_value, excluded
FROM objects
WHERE id IN (?)
```

**batch_check_archived (any archive)** — Archive detection:
```sql
SELECT DISTINCT s.object_id
FROM sources s
JOIN roots r ON s.root_id = r.id
WHERE r.role = 'archive' AND s.present = 1
  AND s.object_id IN (?)
```

**batch_check_archived (specific archive)** — Archive detection with root filter:
```sql
SELECT DISTINCT s.object_id
FROM sources s
WHERE s.root_id = ? AND s.present = 1
  AND s.object_id IN (?)
```

**batch_find_archive_paths** — Archive paths:
```sql
SELECT s.object_id, r.path, s.rel_path
FROM sources s
JOIN roots r ON s.root_id = r.id
WHERE r.role = 'archive' AND s.present = 1
  AND s.object_id IN (?)
ORDER BY s.object_id, r.path, s.rel_path
```

All functions use `BATCH_SIZE` chunking to handle large ID sets.

---

## Phases

### Phase 1: Domain Types

- **Status**: ✅ completed
- **Goal**: Create `object.rs` with Object struct and predicates
- **Scope**:
  - Create `src/object.rs`
  - Define `Object` struct with all fields
  - Implement `is_excluded()` predicate
  - Add to `main.rs` module declarations
- **Tests to Add** (~4 tests):
  - `object_is_excluded_true`
  - `object_is_excluded_false`
  - `object_fields_populated`
  - `object_clone_creates_copy`
- **Dependencies**: None
- **Notes**: The Object domain is simple — unlike Source which has complex predicates, Object just wraps table columns. The real domain logic is in archive detection (Phase 2).

### Phase 2: Repository Layer

- **Status**: ✅ completed
- **Goal**: Create `object_repo.rs` with batch functions
- **Scope**:
  - Create `src/object_repo.rs`
  - Implement `batch_fetch_by_ids()`
  - Implement `batch_check_archived()` with `Option<i64>` for archive root
  - Implement `batch_find_archive_paths()`
  - Use `BATCH_SIZE` chunking for all functions
- **Tests to Add** (~15 tests):
  - **batch_fetch_by_ids:**
    - `batch_fetch_by_ids_empty_returns_empty`
    - `batch_fetch_by_ids_found`
    - `batch_fetch_by_ids_partial_missing_ids_ignored`
    - `batch_fetch_by_ids_includes_excluded_objects`
  - **batch_check_archived (core logic):**
    - `batch_check_archived_empty_returns_empty`
    - `batch_check_archived_finds_archived_objects`
    - `batch_check_archived_excludes_non_archive_roots` — source in source-role root doesn't count
    - `batch_check_archived_requires_present_source` — source with present=0 doesn't count
    - `batch_check_archived_deduplicates_multiple_archive_sources` — same object in multiple archives returns once
    - `batch_check_archived_specific_root_filters_correctly` — `Some(root_id)` only checks that root
    - `batch_check_archived_specific_root_ignores_other_archives` — other archives don't count when specific root given
  - **batch_find_archive_paths:**
    - `batch_find_archive_paths_empty_returns_empty`
    - `batch_find_archive_paths_returns_correct_path_format` — path = root_path + "/" + rel_path (or just root_path if rel_path empty)
    - `batch_find_archive_paths_multiple_paths_per_object` — same content in multiple archives
  - **Batch size handling (critical):**
    - `batch_check_archived_handles_large_id_sets` — test with >1000 IDs to verify chunking works
- **Dependencies**: Phase 1

### Phase 3: Migrate cluster.rs

- **Status**: ✅ completed
- **Goal**: Replace object helpers with object_repo
- **Scope**:
  - Replace `batch_fetch_object_hashes()` with `object_repo::batch_fetch_by_ids()`
    - Current returns `HashMap<i64, (String, String)>` (hash_type, hash_value)
    - New returns `HashMap<i64, Object>` — extract hash fields from Object
  - Replace `find_in_archive(hash_value)` with `object_repo::batch_find_archive_paths(object_ids)`
    - **Key change**: Current uses hash_value lookup (indirect via objects table JOIN)
    - New uses object_id directly (cleaner, same semantics since hash→object is 1:1)
  - Batch the archive detection for all sources at once (eliminate N+1)
  - Remove dead code: `batch_fetch_object_hashes()`, `find_in_archive()`
- **Validation**:
  - `canon cluster generate /scope` — same sources, same archive detection
  - `canon cluster generate /scope --include-archived` — same behavior
- **Dependencies**: Phase 2

### Phase 4: Migrate ls.rs

- **Status**: ✅ completed
- **Goal**: Replace archive detection with object_repo
- **Scope**:
  - Replace `check_archived(object_id)` with `object_repo::batch_check_archived(object_ids, None)`
  - Replace `get_archive_paths(object_id)` with `object_repo::batch_find_archive_paths(object_ids)`
  - Batch the archive detection for all sources at once (eliminate N+1)
  - Remove dead code: `check_archived()`, `get_archive_paths()`
- **Validation**:
  - `canon ls -l /scope` — same output, archive markers correct
  - `canon ls --archived=show /scope` — archive paths displayed correctly
  - `canon ls --duplicates /scope` — same output
- **Dependencies**: Phase 2

### Phase 5: Migrate coverage.rs

- **Status**: ✅ completed
- **Goal**: Replace archive counting with object_repo
- **Scope**:
  - Replace `count_archived_from_temp()` with `object_repo::batch_check_archived()`
  - Pass `archive_root_id` parameter (supports `--archive` flag)
  - Remove temp table usage for archive counting (repo handles batching)
  - Remove dead code: `count_archived_from_temp()`
- **Validation**:
  - `canon coverage` — same stats
  - `canon coverage --archive path:/archive` — same stats for specific archive
- **Migration pitfall** (caught by smoke test):
  - Initial migration counted `archived_set.len()` — unique archived objects
  - Old code counted **sources** whose object was archived (duplicates counted separately)
  - Fix: count sources whose `object_id` is in the archived set, not set size
  - See "Lessons Learned" section below
- **Dependencies**: Phase 2

### Phase 6: Cleanup and Documentation

- **Status**: ✅ completed
- **Goal**: Remove dead code, update documentation
- **Scope**:
  - Verify all dead code removed in Phases 3-5
  - Update `CLAUDE.md` with Object domain model documentation
  - Update this spec with learnings
- **Dependencies**: Phase 5

---

## Test Requirements

### Existing Tests

No unit tests exist for object-related functions currently.

### New Tests to Add

**Phase 1** (`object.rs`): ~4 tests for Object struct and predicates

**Phase 2** (`object_repo.rs`): ~15 tests covering:
- Empty input handling (all functions)
- Found/not found/partial cases
- Archive detection core logic:
  - Only `role='archive'` roots count
  - Only `present=1` sources count
  - Deduplication when object in multiple archives
- Specific archive root filtering (`Option<i64>` parameter)
- Path format correctness (root_path + rel_path handling)
- **Batch size chunking with >1000 IDs** (critical — learned from Cluster Infrastructure)

**Total**: ~19 new tests

### Behavioral Validation

In addition to unit tests, capture baseline outputs before Phase 3:
```bash
# cluster.rs
canon cluster generate /scope -o /tmp/test.toml --dest /archive
cp /tmp/test.toml /tmp/baseline_cluster.toml

# ls.rs
canon ls -l /scope > /tmp/baseline_ls.txt
canon ls --archived=show /scope > /tmp/baseline_ls_archived.txt

# coverage.rs
canon coverage > /tmp/baseline_coverage.txt
canon coverage --archive path:/archive > /tmp/baseline_coverage_specific.txt
```

After each phase, compare outputs to verify identical behavior.

---

## Invariants

### Architectural

1. **Object struct is pure data** — No methods that do I/O
2. **Predicates are pure functions** — No database access, no side effects
3. **Repository does data access only** — No domain logic in SQL
4. **Archive detection is object-centric** — Query by object_id, not source_id or hash_value

### Implementation

1. **Batch size is 1000** — Consistent with source_repo, fact_repo
2. **Archive = role='archive' AND present=1** — Only present sources in archive roots count
3. **object_excluded already denormalized** — Source.object_excluded comes from JOIN; Object.is_excluded() is for direct object operations
4. **Callers filter NULL object_ids** — Repository functions assume valid object IDs; sources with `object_id=None` must be filtered by caller before calling archive detection functions

### Correctness

1. **Command output unchanged** — Same behavior after migration
2. **Archive detection semantics preserved** — Same sources marked as archived
3. **Specific archive root support** — coverage.rs `--archive` flag works via `Option<i64>` parameter

---

## Design Decisions

### Decision 1: Archive Detection in object_repo

**Chosen**: Put archive detection functions in `object_repo.rs`

**Why**: Archive detection is fundamentally object-centric — "is this CONTENT in an archive?" The query uses object_id as the key. While it JOINs through sources and roots, the question being answered is about the object.

**Trade-off**: Could argue for a separate "archive" module. But that would be premature — there's no other archive-specific logic that would live there. Keep it simple.

### Decision 2: Return HashSet for batch_check_archived

**Chosen**: Return `HashSet<i64>` of archived object IDs

**Why**: Callers need to check "is this object_id archived?" — a Set provides O(1) lookup. Returning `HashMap<i64, bool>` would waste space on `false` entries.

**Trade-off**: Callers must handle "not in set = not archived" logic. Acceptable because it's obvious and efficient.

### Decision 3: Single Function with Option<i64> for Archive Root

**Chosen**: `batch_check_archived(conn, object_ids, archive_root_id: Option<i64>)`

**Why**:
- Matches existing `count_archived_from_temp()` pattern in coverage.rs
- One function is simpler than two nearly-identical functions
- `None` = any archive, `Some(id)` = specific archive

**Trade-off**: Slightly more complex function signature. Acceptable because:
- The SQL differs only in the WHERE clause (role='archive' vs root_id=?)
- Callers already have `Option<i64>` from flag parsing

### Decision 4: Keep Object Struct Minimal

**Chosen**: Object struct has only `id`, `hash_type`, `hash_value`, `excluded`

**Why**: These are the only columns in the objects table. Unlike Source, there's no denormalization needed — objects don't have relationships that need to be cached for predicate evaluation.

**Trade-off**: If we later need "count of sources linked to this object", we'd need to add it or fetch separately. YAGNI for now.

### Decision 5: Use object_id Not hash_value for Archive Detection

**Chosen**: Archive detection uses object_id, not hash_value

**Why**:
- Current `cluster.rs:find_in_archive()` uses hash_value, requiring a JOIN through objects table
- Using object_id is more direct — sources already have object_id
- Semantically equivalent: same hash → same object_id (UNIQUE constraint)

**Trade-off**: Requires callers to have object_id. All current callers (cluster, ls, coverage) already have it via Source.object_id.

---

## Lessons Learned

### Lesson 1: Objects vs Sources — Count Carefully

**What happened**: During Phase 5 migration, the initial implementation counted `archived_set.len()` (unique archived objects) instead of counting sources whose object was archived.

**The bug**:
```rust
// WRONG: counts unique objects
stats.archived_sources = archived_set.len() as i64;

// RIGHT: counts sources whose object is in the set
stats.archived_sources = hashed_sources
    .iter()
    .filter(|s| s.object_id.map_or(false, |oid| archived_set.contains(&oid)))
    .count() as i64;
```

**Why it matters**: When multiple sources share the same `object_id` (duplicates), the wrong approach undercounts. In this case: 35,306 sources → 4,987 objects.

**How it was caught**: Baseline comparison showed massive discrepancy in coverage stats.

**Takeaway**: When migrating from source-centric SQL to object-centric batch functions, be explicit about what you're counting. The batch function returns **objects**, but the caller may need to count **sources**.

### Lesson 2: Baseline Smoke Tests Are Essential

The behavioral validation (capturing baseline outputs before migration) caught this bug immediately. Unit tests verified the batch functions worked correctly — but the integration with the calling code had a semantic error that only showed up in real output comparison.

---

## Future Considerations

### What This Enables

1. **Object-level operations** — If we add commands to work with objects directly (e.g., "find all sources for this hash"), the infrastructure is ready
2. **Batch exclusion** — Could add `object_repo::batch_set_excluded()` for bulk operations
3. **Object statistics** — Could add "how many sources reference this object" if needed

### What Remains After This

With Object Infrastructure complete, the unified domain model covers all four core entities. Remaining areas:

- **Filter infrastructure** — Low priority, works fine as-is
- **Write operations** — Different concerns (transactions, validation), don't need same pattern
- **SQL clause builders** — `exclude_clause()` still used by `exclude.rs`; can be cleaned up opportunistically

---

## References

- Template: `.claude/specs/2026-01-24-source-infrastructure.md`
- Completed: `.claude/specs/2026-01-24-fact-infrastructure.md`
- Completed: `.claude/specs/2026-01-24-root-infrastructure.md`
- Completed: `.claude/specs/2026-01-24-cluster-infrastructure.md`
- Current implementation: `src/cluster.rs`, `src/ls.rs`, `src/coverage.rs`
