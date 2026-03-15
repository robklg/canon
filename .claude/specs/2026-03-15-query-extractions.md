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

### Phase 2: Facts extraction
- **Status**: pending
- **Goal**: Extract distribution computation and pruning operations to ops layer.
- **Scope**:
  - Create `ops/facts.rs` with distribution computation functions
  - Extract pruning operations with plan/execute pattern
  - Rewire interface
- **Non-goals**: Extracting the trivial helper functions (is_root_key, fact_value_to_display, etc.)
- **Dependencies**: Phase 1 complete

**Deferred to spec-writing time**: Facts is complex enough that the detailed spec should be written when we're ready to implement it. The distribution functions share a common pattern (fetch → aggregate → sort) that should be designed once.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Coverage gets its own ops module | It has a distinct CoverageStats type and compute function — clean module boundary |
| Ls duplicate detection gets its own ops module | Small but distinct — `find_duplicate_groups` is self-contained |
| Facts deferred to Phase 2 | 865 lines of behavioral logic with 9 functions — needs its own design pass |
| Don't extract ls filtering/aggregation | Too coupled to output format (builds tuples for display columns) |

## Test Requirements

### Existing Tests
All 743 existing tests must pass.

### New Tests to Add

**Phase 1 — ops/coverage.rs:**
- `coverage_stats_from_sources` — basic stat computation
- `coverage_stats_with_excluded` — excluded sources not counted
- `coverage_stats_with_archived` — archived detection works
- Move existing `test_coverage_archived_counts_sources_not_objects` from coverage.rs

**Phase 1 — ops/ls.rs:**
- `find_duplicates_groups_by_object` — basic grouping
- `find_duplicates_no_duplicates` — all unique → empty result
- Move existing `test_ls_archived_flag_counts_sources_not_objects` from ls.rs (if it tests behavioral logic)

## Implementation Checklist
- [x] Phase 1: Create ops/coverage.rs, extract stats computation
- [x] Phase 1: Create ops/ls.rs, extract find_duplicate_groups
- [x] Phase 1: Rewire coverage.rs and ls.rs interfaces
- [x] Phase 1: Add/move tests (6 new: 3 coverage + 3 ls)
- [x] Phase 1: Verify all tests pass (748 pass)
- [ ] Phase 2: Facts extraction (separate implementation pass)
