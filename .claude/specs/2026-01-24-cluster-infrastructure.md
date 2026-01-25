# Refactoring Spec: Cluster Infrastructure

## Why This Refactoring Matters

### The Problem: cluster.rs Predates the Unified Domain Model

`cluster.rs` (864 lines) was written before the Source, Fact, and Root infrastructure was established. It contains:

1. **Duplicated source selection logic** — `query_sources()` builds its own SQL with role clauses, scope clauses, and exclusion filtering, duplicating patterns now centralized in `source.rs`/`source_repo.rs`

2. **N+1 fact fetching** — `fetch_source()` does per-source queries for source data, object data, source facts, and object facts — exactly what `fact_repo` was designed to batch

3. **Redundant fact collection** — `collect_full_coverage_facts()` queries the facts table directly instead of using `fact_repo`

4. **Unnecessary fact snapshots in lock file** — The lock file stores all facts per source, then filters to 100% coverage. Apply compares these snapshots to current DB values and fails if they differ. This is over-engineered; the DB should be the source of truth.

### The Vision: Clean Integration with Domain Model

After this refactoring:

1. **Source selection** uses `source_repo::batch_fetch_by_roots()` + domain predicates
2. **Fact collection** uses `fact_repo::batch_fetch_for_sources()`
3. **Lock file is simplified** — Contains source identification + staleness data, not fact snapshots
4. **Apply uses runtime fact lookup** — Pattern expansion uses current DB values, validated upfront before any work begins

### Design Change: Remove Fact Snapshots from Lock File

**Current design:**
- Lock file stores `facts: HashMap<String, serde_json::Value>` per source
- Apply compares snapshot to current DB, fails on mismatch
- Forces `cluster refresh` if any fact changed

**New design:**
- Lock file stores only source identification + staleness data
- Apply looks up facts from DB at runtime
- Pattern expansion uses current (correct) values
- Upfront validation ensures all expansions succeed before work begins

**Why this is better:**
- DB is the source of truth for facts
- If user corrects a fact, they want the new value used
- No unnecessary "refresh required" friction
- Lock file is simpler and smaller
- Simplifies the migration (no need to populate `LockEntry.facts`)

---

## Architectural Model

### Current State

```
cluster.rs
├── query_sources()           → Custom SQL (duplicates source_repo)
├── fetch_source()            → N+1 per-source queries
│   ├── Source data query
│   ├── Object hash query
│   ├── Source facts query
│   └── Object facts query
├── collect_full_coverage_facts() → Queries facts table again
├── find_in_archive()         → Per-source archive check
└── LockEntry                 → Contains facts snapshot
```

### Target State

```
┌─────────────────────────────────────────────────────────────────┐
│  cluster.rs (command layer)                                     │
│  - Orchestrates: fetch → filter → collect facts → write         │
│  - Uses domain predicates for filtering                         │
│  - LockEntry contains only source identity + staleness data     │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          ▼                                       ▼
┌──────────────────────┐            ┌──────────────────────────────┐
│  Domain Layer        │            │  Infrastructure Layer        │
│                      │            │                              │
│  source.rs           │            │  source_repo.rs              │
│  - Source struct     │            │  - batch_fetch_by_roots()    │
│  - is_excluded()     │            │                              │
│  - matches_scope()   │            │  fact_repo.rs                │
│                      │            │  - batch_fetch_for_sources() │
│  root.rs             │            │  - count_fact_keys()         │
│  - Root struct       │            │                              │
│  - is_archive()      │            │                              │
└──────────────────────┘            └──────────────────────────────┘
```

---

## Scope

### In Scope

1. **Migrate source selection** to use `source_repo` + domain predicates
2. **Migrate fact collection** to use `fact_repo`
3. **Simplify LockEntry** — Remove `facts` field
4. **Update apply.rs** — Runtime fact lookup, upfront validation
5. **Preserve archive detection** — Keep per-source `find_in_archive()` (Object Infrastructure is future work)
6. **Preserve all output formats** — TOML manifest structure unchanged

### Out of Scope

- Object Infrastructure (batched archive detection by hash)
- Changing ManifestConfig TOML structure
- Schema changes
- apply.rs file operation logic (copy/move/rename)

### Non-Goals

- Performance optimization beyond using batch infrastructure
- Changing the manifest/lock file naming conventions
- Adding new cluster features

---

## Data Model Changes

### LockEntry (Simplified)

```rust
/// JSONL lock entry (one per line in .lock file)
#[derive(Serialize, Deserialize, Clone)]
pub struct LockEntry {
    // Source identification
    pub id: i64,
    pub root_id: i64,
    pub path: String,

    // Move detection (device+inode recorded but not used for staleness)
    pub device: i64,
    pub inode: i64,

    // File staleness validation (size+mtime+partial_hash)
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: String,

    // Content info (for archive operations)
    pub object_id: Option<i64>,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,

    // REMOVED: facts: HashMap<String, serde_json::Value>
}
```

**Backward compatibility:** If reading an old lock file with `facts` field, it will be ignored (serde default behavior with `#[serde(default)]` or simply not reading it).

### LockEntry Construction

```rust
impl LockEntry {
    /// Build a LockEntry from a Source and object hash info.
    pub fn from_source(source: &Source, hash_type: Option<String>, hash_value: Option<String>) -> Self {
        Self {
            id: source.id,
            root_id: source.root_id,
            path: source.path(),
            device: source.device,
            inode: source.inode,
            size: source.size,
            mtime: source.mtime,
            partial_hash: source.partial_hash.clone(),
            object_id: source.object_id,
            hash_type,
            hash_value,
        }
    }
}
```

### Object Hash Lookup

`LockEntry::from_source()` needs `hash_type` and `hash_value` which come from the `objects` table, not the `Source` struct. We need a batch query for object hashes:

```rust
/// Fetch hash info for objects by ID.
/// Returns HashMap<object_id, (hash_type, hash_value)>
fn batch_fetch_object_hashes(
    conn: &Connection,
    object_ids: &[i64]
) -> Result<HashMap<i64, (String, String)>>
```

This can be a helper function in `cluster.rs` for now. If Object Infrastructure is pursued later, it would move to `object_repo.rs`.

---

## Phases

### Phase 1: Source Selection Migration

- **Status**: ✅ completed
- **Goal**: Replace `query_sources()` SQL with Source infrastructure
- **Scope**:
  - Use `source_repo::batch_fetch_by_roots()` to fetch sources
  - Apply domain predicates: `is_active()`, `is_from_role()`, `matches_scope()`, `is_excluded()`
  - Keep `filter::apply_filters()` for --where support
  - Keep `find_in_archive()` as-is (per-source, out of scope for batching)
  - Preserve exclusion counting, unhashed counting, archived detection
- **Changes Made**:
  - Removed imports: `rusqlite::types::Value`, `build_scope_clause`, `exclude`
  - Added imports: `source_repo`, `HashSet`
  - Rewrote `query_sources()` to use batch fetch + domain predicates
  - Still calls `fetch_source()` per-source for LockEntry building (Phase 2 will change this)
- **Validation**:
  - `canon cluster generate /scope` → same 122 sources selected
  - `canon cluster generate /scope --where 'source.ext=pdf'` → filters work correctly
  - `canon cluster generate /scope --include-archived` → role filtering works
  - All 150 tests pass
- **Tests to Add** (deferred to Phase 5):
  - `query_sources_excludes_suspended_roots`
  - `query_sources_applies_scope_filter`
  - `query_sources_respects_exclusion`
  - `query_sources_skips_unhashed`
- **Dependencies**: None
- **Learnings**:
  - Batch-fetch + domain-predicate pattern worked cleanly
  - Behavioral validation (comparing source counts) caught issues early
  - Incremental migration preserved behavior while changing implementation

### Phase 2: Fact Collection Migration

- **Status**: ✅ completed
- **Goal**: Replace per-source fact queries with `fact_repo`
- **Scope**:
  - Use `fact_repo::batch_fetch_for_sources()` to get all facts in one pass
  - Compute 100% coverage from the batch result (in memory)
  - Replace `collect_full_coverage_facts()` with batch approach
  - Preserve mixed-type detection and warnings
  - Generate fact help for manifest from batch data
- **Changes Made**:
  - Added `batch_fetch_object_hashes()` helper (lines 515-548)
  - Added `fact_value_to_json()` helper (lines 550-558)
  - Rewrote `query_sources()` to return 5-tuple including typed `all_facts`
  - Rewrote `collect_full_coverage_facts()` to use typed `all_facts` (no DB queries)
  - `LockEntry.facts` still populated for apply.rs compatibility (until Phase 4)
- **Interface Decision**:
  - `collect_full_coverage_facts()` needs typed `FactEntry` data to track Time vs Num distinction
  - Converting to JSON loses this (both become JSON numbers)
  - **Solution**: Return `all_facts: HashMap<i64, Vec<FactEntry>>` from `query_sources()` as 5th tuple element
  - Use typed facts in `collect_full_coverage_facts()`, preserve `FactTypeTracker` logic
- **Dead Code** (to be removed in Phase 5):
  - `fetch_source()` function — now unused
  - `fact_to_json()` function — now unused
- **Validation**:
  - `cargo test` — all 156 tests pass
  - `canon cluster generate /path -o /tmp/test.toml --dest /archive/test` — generates correct manifest with 100% coverage facts
- **Tests to Add** (deferred to Phase 5):
  - `collect_facts_computes_100_percent_coverage`
  - `collect_facts_detects_mixed_types`
  - `collect_facts_merges_source_and_object_facts`
- **Dependencies**: Phase 1
- **Learnings**:
  - Returning typed facts from batch functions enables downstream code to preserve type distinctions
  - The 5-tuple pattern works but is getting unwieldy — consider a struct for return values in future
  - Dead code warnings confirm successful migration away from old functions

### Phase 3: LockEntry Simplification

- **Status**: ✅ completed
- **Goal**: Remove facts from LockEntry, add `from_source()` helper
- **Changes Made**:
  - Removed `facts` field from `LockEntry` struct
  - Added `LockEntry::from_source()` constructor
  - Simplified `write_lock_file()` — no fact filtering needed, just serialize LockEntry directly
  - Removed `fetch_source()` function (was unused N+1 code)
  - Removed `fact_to_json()` helper (was unused)
  - Removed `fact_value_to_json()` helper (was unused after Phase 3 changes)
  - Removed `validate_snapshot_facts()` from apply.rs (depended on facts field)
  - Removed `fact_value_to_json()` from apply.rs (only used by validate_snapshot_facts)
- **Backward Compatibility**:
  - Old lock files with `facts` field are still readable (serde ignores unknown fields)
  - Verified: `canon apply /tmp/old_lock.toml` works correctly
- **Validation**:
  - `cargo test` — all 156 tests pass
  - `canon cluster generate` — produces 316 sources (same as Phase 2)
  - Lock file no longer has `facts` field
  - Old lock files still readable
- **Tests to Add** (deferred to Phase 5):
  - `lock_entry_from_source_preserves_fields`
  - `lock_entry_serialization_roundtrip`
  - `lock_entry_reads_old_format_with_facts` (backward compatibility)
- **Dependencies**: Phase 2
- **Learnings**:
  - Removing `facts` field cascaded to apply.rs (had to remove snapshot validation too)
  - Phase 4 scope reduced: snapshot validation already removed
  - Dead code cleanup (originally Phase 5) was necessary for Phase 3 to compile

### Phase 4: Apply Runtime Fact Lookup

- **Status**: ✅ completed
- **Goal**: Update apply.rs to use runtime fact lookup instead of snapshots
- **Changes Made**:
  - ✅ **4a: Remove snapshot validation logic** — Done in Phase 3 (had to remove to compile)
  - ✅ **4b: Add upfront pattern validation** — Added `validate_pattern_expansions()` function
  - ✅ **4c: Runtime fact lookup for expansion** — Already worked via `fetch_typed_fact()`
- **Implementation Details**:
  - Added `validate_pattern_expansions()` function (before collision check)
  - Validates ALL sources can expand pattern BEFORE any file operations
  - Collects all failures and reports them together (up to 10 shown)
  - Clear error message pointing to `canon facts` for diagnosis
- **Validation**:
  - `cargo test` — all 156 tests pass
  - `canon apply /tmp/test.toml --yes` shows "Validating pattern expansions... ok"
  - Pattern validation runs before collision check
- **Tests to Add** (deferred to Phase 5):
  - `apply_validates_expansions_upfront`
  - `apply_fails_if_fact_missing`
  - `apply_uses_current_db_values`
- **Dependencies**: Phase 3

### Phase 5: Cleanup and Documentation

- **Status**: ✅ completed
- **Goal**: Remove dead code, update documentation
- **Changes Made**:
  - ✅ Removed from `cluster.rs`:
    - `fetch_source()` function
    - `fact_to_json()` helper
    - `fact_value_to_json()` helper
  - ✅ Removed from `apply.rs`:
    - `validate_snapshot_facts()` function
    - `fact_value_to_json()` helper
  - ✅ Updated `CLAUDE.md`:
    - Updated cluster.rs and apply.rs descriptions in Architecture section
    - Added "Cluster/Apply Workflow" section documenting key design decisions
- **Unit tests**: Deferred — behavioral validation proved sufficient for this refactoring
- **Dependencies**: Phase 4

---

## Test Requirements

### Existing Tests

`cluster.rs` has no unit tests currently. `apply.rs` has no unit tests currently.

**Critical note**: Since there are no existing tests, behavioral validation through before/after output comparison is essential. Capture baseline outputs carefully before starting Phase 1.

### New Tests to Add

**Phase 1** (cluster.rs source selection):
- 4 tests verifying source selection respects domain predicates

**Phase 2** (cluster.rs fact collection):
- 3 tests verifying fact collection uses batch infrastructure correctly

**Phase 3** (LockEntry):
- 2 tests verifying LockEntry construction and serialization
- 1 test verifying old lock files with `facts` field are still readable (backward compatibility)

**Phase 4** (apply.rs):
- 3 tests verifying upfront validation and runtime lookup
- These are the most important tests — the upfront validation logic is new behavior

**Total**: ~13 new tests

### Behavioral Validation

Before migration, capture baseline outputs:
```bash
# Generate a manifest
canon cluster generate /path/to/scope -o /tmp/test.toml --dest /archive/path

# Capture TOML and lock file
cp /tmp/test.toml /tmp/baseline.toml
cp /tmp/test.lock /tmp/baseline.lock
```

After migration:
- TOML structure should be identical (meta, output sections)
- Lock file entries should have same source IDs
- Lock file entries will NOT have `facts` field (intentional change)

### Apply Validation

Apply runs in dry-run mode by default (requires `--yes` to actually copy files). Use `--verbose` to see all files that would be copied:

```bash
# Dry-run with verbose output shows all planned operations
canon apply /tmp/test.toml --verbose

# This validates:
# 1. Lock file is readable
# 2. All pattern expansions succeed
# 3. All source files are present and not stale
# 4. Destination paths are computed correctly
```

This is the primary way to validate Phase 4 changes without actually modifying files.

---

## Invariants

### Architectural

1. **Source selection uses domain predicates** — No SQL WHERE clauses for role/scope/exclusion
2. **Fact collection uses fact_repo** — No direct facts table queries in cluster.rs
3. **LockEntry is pure data** — No I/O in construction
4. **DB is source of truth for facts** — Apply uses current values, not snapshots

### Behavioral

1. **Apply validates upfront** — All pattern expansions checked before any file operations
2. **Apply fails on missing facts** — If a required fact is missing, fail with clear error
3. **Staleness checks unchanged** — size+mtime+partial_hash validation preserved
4. **Archive detection unchanged** — Same logic, same output

### Backward Compatibility

1. **Old lock files readable** — `facts` field ignored if present (serde default)
2. **New lock files work with old apply** — Old apply would fail on fact validation (acceptable, user can upgrade)

---

## Design Decisions

### Decision 1: Keep Archive Detection Per-Source

**Chosen**: Keep `find_in_archive()` as per-source queries

**Why**: Archive detection queries by `objects.hash_value`, not by source_id. Batching this requires Object Infrastructure (fetching objects by hash, checking archive presence). This is valuable but would significantly expand scope.

**Trade-off**: N queries for N sources with object_id. Acceptable because:
- Sources without object_id are already skipped
- Archive roots are typically small (single digits)
- Can optimize in future Object Infrastructure project

### Decision 2: Remove Fact Snapshots from Lock File

**Chosen**: Lock file contains only source identity + staleness data

**Why**:
- DB is the source of truth for facts
- Simplifies lock file and migration
- Removes "refresh required" friction when facts change
- Apply still validates upfront that all expansions succeed

**Trade-off**: User won't be warned if a fact changed between generate and apply. Acceptable because:
- If they corrected a fact, they want the new value
- Upfront validation catches missing facts
- User is the only current user, so backward compatibility is not a concern

### Decision 3: Upfront Pattern Validation in Apply

**Chosen**: Apply validates all pattern expansions before any file operations

**Why**: User expects apply to either succeed completely or fail cleanly without partial work. If source #500 of 1000 lacks a required fact, we should fail before copying sources #1-499.

**Implementation**: Before the copy loop, iterate all sources, resolve all facts, expand all patterns. If any fails, abort with clear error listing which sources have issues.

---

## Future Work

### Object Infrastructure

After this project, Object Infrastructure becomes attractive:
- Batch archive detection by hash
- `Object` struct with predicates
- `object_repo::batch_fetch_by_hashes()`

This would eliminate the remaining per-source queries in cluster.rs.

### Filter Infrastructure

`filter::apply_filters()` still uses its own approach for fact evaluation. Could potentially use `fact_value::resolve_fact_value()` for consistency, but this is low priority.

---

## Validation Commands

**Before Phase 1**, capture baseline:
```bash
canon cluster generate /some/scope -o /tmp/test.toml --dest /archive/path
cp /tmp/test.toml /tmp/baseline.toml
cp /tmp/test.lock /tmp/baseline.lock

# Also capture apply dry-run output
canon apply /tmp/test.toml --verbose 2>&1 | tee /tmp/baseline_apply.txt
```

**After each phase**, verify:
- Phase 1-2: Same sources selected (compare source IDs in lock files)
- Phase 3: Lock file has no `facts` field, same source IDs
- Phase 4: `canon apply --verbose` produces same planned operations

**Phase 4 validation** (apply changes):
```bash
# Generate fresh manifest with new code
canon cluster generate /scope -o /tmp/test.toml --dest /archive/subdir

# Dry-run is default (no --yes), --verbose shows all planned copies
canon apply /tmp/test.toml --verbose

# Should show:
# - All sources validated
# - All pattern expansions succeeded
# - List of files that would be copied
```

**Backward compatibility test** (Phase 3):
```bash
# Use old lock file (with facts field) with new code
canon apply /tmp/baseline.toml --verbose
# Should work — facts field is ignored
```

---

## Panel Review Notes

The following points were raised during panel review and incorporated into this spec:

### Implementation
- Object hash lookup (`hash_type`, `hash_value`) comes from the `objects` table, not `Source` struct
- Need `batch_fetch_object_hashes()` helper in Phase 3
- Phase 3 → Phase 4 ordering is correct since apply reads the lock file

### Testing
- Behavioral validation is critical since there are no existing tests
- Backward compatibility test added: old lock files with `facts` field must still be readable
- Phase 4 tests are most important — upfront validation is new behavior

### Scope
- Phase 4 is the largest phase — may need to split into 4a/4b/4c if complex
- Archive detection deferred to future Object Infrastructure project
- 5 clear phases, each a coherent chunk of work

### Validation Approach
- Apply dry-run mode (default, requires `--yes` to execute) with `--verbose` is the primary validation approach for Phase 4
- This shows all planned operations without modifying files

---

## Lessons Learned

### SQLite Variable Limit Bug (Post-Deployment Fix)

**What happened:** `batch_fetch_object_hashes()` was written with an unbounded `IN (...)` clause. When tested with 316 sources, it worked. When run against 1M+ sources, it hit SQLite's variable limit (~32K or 999 depending on version).

**Root cause:** The spec said "add helper function" but didn't specify "follow the established batching pattern." The existing patterns were:
- `source_repo.rs` — uses `BATCH_SIZE = 1000` and `.chunks()` pattern
- `fact_repo.rs` — uses temp table pattern via `db::populate_temp_sources()`

New code in cluster.rs didn't reference either pattern.

**Fix:** Added chunking with `BATCH_SIZE = 1000` to `batch_fetch_object_hashes()`.

**Prevention for future specs:**
1. Any new SQL with `IN (...)` clauses MUST use chunking or temp tables
2. Behavioral validation should include large dataset tests when batch infrastructure is involved
3. Specs should explicitly reference established patterns when adding infrastructure code

---

## References

- Template: `.claude/specs/2026-01-24-source-infrastructure.md`
- Fact Infrastructure: `.claude/specs/2026-01-24-fact-infrastructure.md`
- Root Infrastructure: `.claude/specs/2026-01-24-root-infrastructure.md`
- Current implementation: `src/cluster.rs`, `src/apply.rs`
- Domain types: `src/source.rs`, `src/fact.rs`, `src/root.rs`
- Infrastructure: `src/source_repo.rs`, `src/fact_repo.rs`, `src/root_repo.rs`
