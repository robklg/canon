# Refactoring Spec: Query Command Extractions (ADR Phase 2 Step 6)

## Overview

Extract remaining query computation from interface modules to ops layer. Three commands: `coverage` (statistics), `ls` (duplicate detection), `facts` (distribution computation + pruning). All already use `ops::selection::select_sources()` for source selection.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` (Phase 2 Step 6)

## Phases

### Phase 1: Coverage + Ls extraction
- **Status**: completed
- **Goal**: Extract coverage statistics computation and ls duplicate detection to ops layer.
- **Scope**:
  - Create `ops/coverage.rs` with `compute_stats()` returning typed `CoverageStats`
  - Move `compute_stats_from_source_refs` (core logic, ~40 lines) + `compute_scoped_stats`/`compute_per_root_stats` orchestration to ops
  - Move `CoverageStats` struct to ops (currently in coverage.rs)
  - Move `find_duplicate_groups` (~70 lines) to `ops/ls.rs` (or add to existing ops module)
  - Rewire interface modules to call ops functions, keep display/formatting
  - Move the existing test from each module alongside the extracted code
- **Non-goals**: Extracting the filtering/aggregation logic in ls `run()` (lines 96-170) — that's tightly coupled to output format. Extracting `show_duplicates` wholesale.
- **Dependencies**: Foundation complete

#### Coverage extraction

**New module**: `src/ops/coverage.rs`

**What moves**:
- `CoverageStats` struct (lines 24-61) → ops/coverage.rs (pub)
- `compute_stats_from_source_refs` (lines 218-255) → ops function
- `compute_scoped_stats` (lines 137-152) → ops function
- `compute_per_root_stats` (lines 155-204) → ops function

**Types**:
```rust
// Already exists as CoverageStats in coverage.rs, moves to ops
pub struct CoverageStats { /* same fields */ }

pub struct PerRootStats {
    pub root_path: String,
    pub root_id: i64,
    pub stats: CoverageStats,
}

pub struct CoverageResult {
    pub stats: CoverageStats,
    pub per_root: Vec<PerRootStats>,
}

/// Compute coverage statistics for a set of sources.
pub fn compute_coverage(
    conn: &mut Connection,
    sources: &[Source],
    archive_root_id: Option<i64>,
) -> Result<CoverageStats>;

/// Compute per-root breakdown.
pub fn compute_coverage_by_root(
    conn: &mut Connection,
    sources: &[Source],
    archive_root_id: Option<i64>,
) -> Result<CoverageResult>;
```

The interface keeps: `run()` orchestration, all `display_*` and `print_*` functions, `format_number`.

#### Ls duplicate extraction

**Add to existing ops module or new**: `src/ops/ls.rs`

**What moves**:
- `find_duplicate_groups` (lines 387-458) → ops/ls.rs

**Type**:
```rust
/// A group of sources sharing the same content hash.
pub struct DuplicateGroup {
    pub hash_value: String,
    pub total_size: i64,
    pub sources: Vec<(String, i64)>,  // (path, root_id)
}

/// Find groups of sources that share the same object_id.
pub fn find_duplicate_groups(
    conn: &mut Connection,
    source_ids: &[i64],
) -> Result<Vec<DuplicateGroup>>;
```

The interface keeps: `run()` orchestration, `show_duplicates()` display, all formatting functions.

### Phase 2a: Facts distribution extraction
- **Status**: completed
- **Goal**: Extract distribution computation from `facts.rs` to `ops/facts.rs`.
- **Non-goals**: Extracting write operations (Phase 2b). Extracting display helpers (`is_root_key`, `format_root_display`, `format_number`). Extracting `show_aliases()`. Changing the default grouping behavior (size buckets, mtime→year).
- **Dependencies**: Phase 1 complete

#### New module: `src/ops/facts.rs`

#### Types

```rust
/// A single entry in a value distribution.
pub struct DistributionEntry {
    pub value: String,
    pub count: i64,
}

/// Result of computing a value distribution for a fact key.
pub struct DistributionResult {
    pub entries: Vec<DistributionEntry>,  // sorted by count desc, truncated to limit
    pub sources_with_value: i64,
    pub total_sources: usize,
    pub skipped_type_mismatch: i64,  // transform errors (stored facts with incompatible types)
}

/// Information about a single fact key (for all-keys enumeration).
pub struct KeyInfo {
    pub key: String,
    pub count: i64,
    pub category: BuiltinKeyCategory,
    pub fact_type: FactType,
}

/// Result of enumerating all available fact keys.
pub struct AllKeysResult {
    pub keys: Vec<KeyInfo>,
    pub total_sources: usize,
}

/// A sub-group within a grouped distribution (e.g., one root's contribution).
pub struct GroupedEntry {
    pub group_values: Vec<String>,
    pub count: i64,
    pub root_id: Option<i64>,
    pub root_path: Option<String>,
}

/// A main value and its grouped breakdown.
pub struct GroupedValueResult {
    pub main_value: String,
    pub total_count: i64,
    pub sub_groups: Vec<GroupedEntry>,  // sorted by count desc
}

/// Result of a grouped distribution computation.
pub struct GroupedDistributionResult {
    pub groups: Vec<GroupedValueResult>,  // sorted by total_count desc, truncated to limit
    pub sources_with_value: i64,
    pub total_sources: usize,
}
```

#### Function signatures

```rust
/// Enumerate all fact keys with counts and coverage.
///
/// Combines builtin keys (filtered by visibility + show_hidden) with stored fact
/// keys from the database. Builtins always have count == total_sources.
pub fn compute_all_keys(
    conn: &mut Connection,
    source_ids: &[i64],
    show_hidden: bool,
) -> Result<AllKeysResult>;

/// Compute value distribution for a single fact key.
///
/// Internally dispatches based on key type:
/// - Builtin key WITHOUT transforms → default grouping (size buckets, mtime→year, etc.)
/// - Builtin key WITH transforms → resolve_fact_value()
/// - Stored key WITHOUT transforms → SQL grouping via batch_fetch_key_for_sources
/// - Stored key WITH transforms → fetch raw values, apply transforms, aggregate
///
/// Results are sorted by count descending and truncated to limit (0 = no limit).
pub fn compute_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    limit: usize,
) -> Result<DistributionResult>;

/// Compute grouped distribution (--by-root, --group-by).
///
/// Fetches sources + stored facts, resolves values via resolve_fact_value(),
/// aggregates into nested groups. No default grouping applied (matches existing
/// behavior of show_grouped_distribution).
pub fn compute_grouped_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    main_key: &ParsedFactKey,
    grouping_keys: &[ParsedFactKey],
    limit: usize,
) -> Result<GroupedDistributionResult>;
```

#### Internal dispatch in `compute_distribution`

The function checks `is_builtin_or_derived(&key.base_key)`:
- If builtin AND `!key.has_transforms()`: fetch sources via `batch_fetch_by_ids`, extract raw `FactValue` via `get_builtin_value()` (needs to be made `pub` in `expr/value.rs`), apply default grouping:
  - `source.size` → size bucket labels ("< 1 KB", "1 KB - 1 MB", etc.)
  - `source.mtime` → year via chrono
  - Others → `fact_value_to_display()`
- If builtin WITH transforms: fetch sources, use `resolve_fact_value()`
- If stored WITHOUT transforms: use `batch_fetch_key_for_sources`, group by `fact_value_to_display()`
- If stored WITH transforms: use `batch_fetch_key_for_sources`, apply transforms, count `skipped_type_mismatch`

#### Duplicate removal

`facts.rs` has its own copies of `fact_value_to_display()` and `apply_transforms()` that duplicate `expr/value.rs`. After extraction:
- `ops/facts.rs` uses `expr::value::fact_value_to_display()` and `expr::value::resolve_fact_value()`
- The duplicates in `facts.rs` are removed
- `get_builtin_value()` in `expr/value.rs` is made `pub` (needed by `compute_distribution` for default grouping path)

#### Interface changes (src/facts.rs)

The interface keeps:
- `run()` — orchestration, source selection, dispatch, header/footer println
- `show_aliases()` — pure display
- `format_number()`, `format_root_display()`, `is_root_key()` — display helpers
- Display functions renamed from `show_*` to `display_*` that take typed results

The interface removes:
- `show_all_keys()` — replaced by `display_all_keys(&AllKeysResult, show_all)`
- `show_value_distribution()` — replaced by `display_distribution(&DistributionResult, ...)`
- `show_transformed_distribution()` — same
- `show_builtin_distribution()` — same
- `show_grouped_distribution()` — replaced by `display_grouped_distribution(&GroupedDistributionResult, ...)`
- `get_fact_category()` — moved to ops (used by compute_distribution to set category on builtin results)
- `is_builtin_or_derived()` — moved to ops (used by compute_distribution for dispatch)
- `fact_value_to_display()` — duplicate removed (use `expr::value::fact_value_to_display`)
- `apply_transforms()` — duplicate removed

The `run()` dispatch becomes:
```rust
if let Some(fact_key) = key_arg {
    let main_key = ParsedFactKey::parse(fact_key)?;
    if !grouping_keys.is_empty() {
        let result = ops::facts::compute_grouped_distribution(
            conn, &source_ids, &main_key, &grouping_keys, limit,
        )?;
        display_grouped_distribution(&result, &main_key, &grouping_keys, total_sources);
    } else {
        let result = ops::facts::compute_distribution(conn, &source_ids, &main_key, limit)?;
        display_distribution(&result, fact_key, &main_key);
    }
} else {
    let result = ops::facts::compute_all_keys(conn, &source_ids, show_all)?;
    display_all_keys(&result, show_all);
}
```

### Phase 2b: Facts write operations extraction
- **Status**: pending
- **Goal**: Extract delete/prune operations to ops layer using plan/execute pattern.
- **Non-goals**: Changing prune behavior. Adding transaction management where none exists.
- **Dependencies**: Phase 2a complete

#### Types

```rust
// --- delete ---
pub struct DeletePlan {
    pub key: String,
    pub entity_type: String,
    pub fact_count: i64,
    pub entity_count: i64,
}

// --- prune stale ---
pub struct PruneStalePlan {
    pub stale_count: i64,
}

// --- prune orphaned ---
pub struct PruneOrphanedPlan {
    pub object_count: i64,
    pub source_count: i64,
    pub source_fact_count: i64,
    pub object_fact_count: i64,
}

// --- prune excluded ---
pub struct PruneExcludedPlan {
    pub scope: String,
    pub source_fact_count: i64,
    pub object_fact_count: i64,
}
```

#### Function signatures

```rust
/// Validate that a key is not protected from deletion.
pub fn validate_delete_key(key: &str) -> Result<()>;

/// Plan fact deletion: count matching facts.
pub fn plan_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
) -> Result<DeletePlan>;

/// Execute fact deletion.
pub fn execute_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
) -> Result<i64>;

/// Plan stale fact pruning: count stale facts.
pub fn plan_prune_stale(conn: &Connection) -> Result<PruneStalePlan>;

/// Execute stale fact pruning.
pub fn execute_prune_stale(conn: &Connection) -> Result<usize>;

/// Plan orphaned object pruning: count orphaned objects/sources/facts.
pub fn plan_prune_orphaned(conn: &mut Connection) -> Result<PruneOrphanedPlan>;

/// Execute orphaned object pruning (owns transaction).
pub fn execute_prune_orphaned(db: &mut Db) -> Result<PruneOrphanedPlan>;

/// Validate prune-excluded scope parameter.
pub fn validate_prune_excluded_scope(scope: &str) -> Result<()>;

/// Plan excluded fact pruning: count facts for excluded entities.
pub fn plan_prune_excluded(conn: &Connection, scope: &str) -> Result<PruneExcludedPlan>;

/// Execute excluded fact pruning.
pub fn execute_prune_excluded(conn: &Connection, scope: &str) -> Result<(usize, usize)>;
```

#### Interface changes

- `delete_facts()` becomes: validate key → select sources → plan → display/confirm → execute
- `prune_stale()` becomes: plan → display/confirm → execute
- `prune_orphaned_objects()` becomes: plan → display/confirm → execute (ops owns transaction)
- `prune_excluded_facts()` becomes: validate scope → plan → display/confirm → execute

#### Note on `execute_delete` signature

`execute_delete` takes the raw parameters rather than a `DeletePlan` struct because storing `source_ids` (potentially hundreds of thousands) in the plan would be wasteful. The plan struct contains only the count data needed for display and confirmation.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Coverage gets its own ops module | It has a distinct CoverageStats type and compute function — clean module boundary |
| Ls duplicate detection gets its own ops module | Small but distinct — `find_duplicate_groups` is self-contained |
| Split facts into Phase 2a (distribution) and 2b (writes) | Distribution is complex (unified dispatch, 3 functions, 6 types); writes are mechanical |
| Don't extract ls filtering/aggregation | Too coupled to output format (builds tuples for display columns) |
| Unified `compute_distribution` replaces 3 functions | All produce the same shape (value, count pairs); internal dispatch by key type |
| Default grouping (size buckets, mtime→year) lives in ops | It's computation policy, not display formatting |
| `get_builtin_value()` made pub | Needed by `compute_distribution` for the no-transform builtin path |
| Remove duplicate `fact_value_to_display`/`apply_transforms` | Already exist in `expr/value.rs`; ops uses those |
| `execute_delete` takes raw params, not plan struct | Avoids cloning large source_ids vector into plan |
| `is_root_key` stays in interface | Only used for display formatting (grouped distribution column headers) |
| Grouped distribution has no default grouping | Matches existing behavior — `show_grouped_distribution` uses `resolve_fact_value` which doesn't bucket |

## Test Requirements

### Existing Tests
All 748 existing tests must pass.

### New Tests to Add

**Phase 1 — ops/coverage.rs:** (completed)
- `coverage_stats_from_sources` — basic stat computation
- `coverage_stats_with_excluded` — excluded sources not counted
- `coverage_stats_with_archived` — archived detection works
- Move existing `test_coverage_archived_counts_sources_not_objects` from coverage.rs

**Phase 1 — ops/ls.rs:** (completed)
- `find_duplicates_groups_by_object` — basic grouping
- `find_duplicates_no_duplicates` — all unique → empty result
- Move existing `test_ls_archived_flag_counts_sources_not_objects` from ls.rs (if it tests behavioral logic)

**Phase 2a — ops/facts.rs (distribution):**
- `test_compute_all_keys_includes_builtins_and_stored` — verify merged list with correct counts and categories
- `test_compute_distribution_stored_fact` — stored fact value grouping, sorted by count
- `test_compute_distribution_with_limit` — truncation behavior
- `test_compute_distribution_builtin_size_buckets` — size bucketing logic
- `test_compute_distribution_builtin_mtime_year` — year grouping for mtime
- `test_compute_distribution_with_transforms` — stored fact with |year modifier
- `test_compute_grouped_basic` — nested grouping structure with root tracking
- `test_compute_grouped_missing_main_value` — sources without main key value excluded

**Phase 2b — ops/facts.rs (writes):**
- `test_validate_delete_key_protected` — source.* and policy.* rejected
- `test_plan_delete_counts` — correct fact/entity counts
- `test_plan_prune_stale_counts` — correct stale count

## Implementation Checklist
- [x] Phase 1: Create ops/coverage.rs, extract stats computation
- [x] Phase 1: Create ops/ls.rs, extract find_duplicate_groups
- [x] Phase 1: Rewire coverage.rs and ls.rs interfaces
- [x] Phase 1: Add/move tests (6 new: 3 coverage + 3 ls)
- [x] Phase 1: Verify all tests pass (748 pass)
- [x] Phase 2a: Create ops/facts.rs with distribution types and compute functions
- [x] Phase 2a: Make `get_builtin_value()` pub in expr/value.rs
- [x] Phase 2a: Register `pub mod facts;` in ops/mod.rs
- [x] Phase 2a: Rewire facts.rs: replace show_* with display_* taking typed results
- [x] Phase 2a: Remove duplicate fact_value_to_display/apply_transforms from facts.rs
- [x] Phase 2a: Add 8 distribution tests
- [x] Phase 2a: Verify all tests pass (756 pass)
- [ ] Phase 2b: Add plan/execute for delete_facts
- [ ] Phase 2b: Add plan/execute for prune_stale, prune_orphaned, prune_excluded
- [ ] Phase 2b: Move is_protected_fact to ops as validate_delete_key
- [ ] Phase 2b: Rewire facts.rs write operations
- [ ] Phase 2b: Add 3 write operation tests
- [ ] Phase 2b: Verify all tests pass
